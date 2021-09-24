package executor

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/session"
	migrator "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
)

var retryPeriodSeconds int64
var hash int32

func getFromNodeIndex(count int32) int32 {
	return atomic.AddInt32(&hash, 1) % count
}

var migrateActions = MigrateActions{actionList: map[string]*Action{}}

type MigrateActions struct {
	actionList map[string]*Action
}
type Action struct {
	from    *MigratorNode
	to      *MigratorNode
	replica *Replica
}

type MigratorNode struct {
	table       string
	pegasusNode *util.PegasusNode
	replicas    []*Replica
}

type Replica struct {
	part      *replication.PartitionConfiguration
	operation migrator.BalanceType
}

func MigrateAllReplicaToNodes(client *Client, period int64, from []string, to []string) error {
	retryPeriodSeconds = period

	var origins []*util.PegasusNode
	var targets []*util.PegasusNode

	for _, addr := range from {
		n, err := client.Nodes.GetNode(addr, session.NodeTypeReplica)
		if err != nil {
			return fmt.Errorf("list node failed: %s", err)
		}
		origins = append(origins, n)
	}

	for _, addr := range to {
		n, err := client.Nodes.GetNode(addr, session.NodeTypeReplica)
		if err != nil {
			return fmt.Errorf("list node failed: %s", err)
		}
		targets = append(targets, n)
	}

	if origins == nil || targets == nil {
		return fmt.Errorf("invalid origin or target node")
	}

	tables, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
	if err != nil {
		return fmt.Errorf("list app failed: %s", err.Error())
	}

	var targetStartNodeIndex = 0
	var remainingReplica = math.MaxInt16
	for {
		if remainingReplica <= 0 {
			fmt.Printf("INFO: completed for all the targets has migrate\n")
			return ListNodes(client)
		}
		round := targetStartNodeIndex / len(targets)
		currentTargetNode := targets[targetStartNodeIndex%len(targets)]
		fmt.Printf("\n\n********[%d]start migrate replicas to %s******\n", round, currentTargetNode.String())
		fmt.Printf("INFO: migrate out all primary from current node %s\n", currentTargetNode.String())
		// assign all primary replica to secondary on target node to avoid read influence
		migratePrimariesOut(client, currentTargetNode)
		tableCompleted := 0
		for {
			// migrate enough replica to one target node per round.
			// pick next node if all tables have been handled completed.
			if tableCompleted >= len(tables){
				targetStartNodeIndex++
				break
			}
			tableCompleted = 0
			remainingReplica = 0
			for _, tb := range tables {
				needMigrateReplicaCount, currentNodeHasBalanced, validOriginNode :=
					migrateReplicaPerTable(client, round, tb.AppName, origins, targets, currentTargetNode)
				remainingReplica = remainingReplica + needMigrateReplicaCount
				// table migrate completed if all replicas have been migrated or
				// target node has been balanced or
				// origin nodes has no valid replica can be migrated
				if needMigrateReplicaCount <= 0 || currentNodeHasBalanced || validOriginNode == 0 {
					tableCompleted++
					continue
				}
			}
			time.Sleep(10 * time.Second)
		}
	}

}

func migratePrimariesOut(client *Client, node *util.PegasusNode) {
	for {
		err := migrator.MigratePrimariesOut(client.Meta, node)
		if err != nil {
			fmt.Printf("WARN: wait, migrate primary out of %s is invalid now, err = %s\n", node.String(), err)
			time.Sleep(10 * time.Second)
			continue
		}

		tables, err := client.Meta.ListAvailableApps()
		if err != nil {
			fmt.Printf("WARN: wait, migrate primary out of %s is invalid when list app, err = %s\n", node.String(), err)
			time.Sleep(10 * time.Second)
			continue
		}

		tableHasPrimaryCount := 0
		for _, tb := range tables {
			partitions, err := migrator.ListPrimariesOnNode(client.Meta, node, tb.AppName)
			if err != nil {
				fmt.Printf("WARN: wait, migrate primary out of %s is invalid when list primaries on node, err = %s\n", node.String(), err)
				time.Sleep(10 * time.Second)
				continue
			}
			if len(partitions) > 0 {
				tableHasPrimaryCount++
			}
		}
		if tableHasPrimaryCount > 0 {
			fmt.Printf("WARN: wait, migrate primary out of %s hasn't completed\n", node.String())
			time.Sleep(10 * time.Second)
			continue
		} else {
			fmt.Printf("INFO: migrate primary out of %s successfully\n", node.String())
			return
		}
	}
}

func migrateReplicaPerTable(client *Client, round int, table string, origins []*util.PegasusNode,
	targets []*util.PegasusNode, currentTargetNode *util.PegasusNode) (int, bool, int) {
	var totalReplicaCount = 0
	replicas := syncWaitNodeReplicaInfo(client, table)
	for _, node := range replicas {
		totalReplicaCount = totalReplicaCount + len(node.replicas)
	}
	min := totalReplicaCount / len(targets)
	max := min + round

	var countNeedMigrate = 0
	for _, node := range origins {
		countNeedMigrate = countNeedMigrate + len(replicas[node.String()].replicas)
	}

	var balanceNode = 0
	for _, node := range targets {
		if len(replicas[node.String()].replicas) >= min {
			balanceNode++
		}
	}

	validOriginNodes := getValidOriginNodes(client, table, origins, currentTargetNode)
	if len(validOriginNodes) == 0 {
		fmt.Printf("INFO: [%s]no valid repica can be migrated with table\n", table)
		return countNeedMigrate, false, 0
	}

	if countNeedMigrate <= 0 {
		fmt.Printf("INFO: [%s]completed for no replicas can be migrated with table\n", table)
		return countNeedMigrate, false, len(validOriginNodes)
	}

	currentCountOfTargetNode := len(replicas[currentTargetNode.String()].replicas)
	if currentCountOfTargetNode >= min && balanceNode < len(targets) {
		fmt.Printf("INFO: [%s]balance, no need migrate replicas to %s, currentCount=%d, expect=min(%d) or max(%d)\n", table, currentTargetNode.String(), currentCountOfTargetNode, min, max)
		return countNeedMigrate, true, len(validOriginNodes)
	}

	if currentCountOfTargetNode >= max {
		fmt.Printf("INFO: [%s]balance with need max: no need migrate replicas to %s, currentCount=%d, expect=min(%d) or max(%d)\n", table, currentTargetNode.String(), currentCountOfTargetNode, min, max)
		return countNeedMigrate, true, len(validOriginNodes)
	}

	var balanceReplicaCount int
	if balanceNode < len(targets) {
		balanceReplicaCount = min
	} else {
		balanceReplicaCount = max
	}
	var wg sync.WaitGroup
	loopCount := int(math.Min(float64(len(origins)), float64(balanceReplicaCount-currentCountOfTargetNode)))
	wg.Add(loopCount)
	for loopCount > 0 {
		go func(to *util.PegasusNode) {
			migrateReplica(client, table, validOriginNodes, currentTargetNode)
			wg.Done()
		}(currentTargetNode)
		loopCount--
	}
	fmt.Printf("INFO: [%s]async migrate task call complete, wait all works successfully\n", table)
	wg.Wait()
	fmt.Printf("INFO: [%s]all works successfully\n\n", table)
	return countNeedMigrate, false, len(validOriginNodes)
}

func getValidOriginNodes(client *Client, table string, origins []*util.PegasusNode, target *util.PegasusNode) []*util.PegasusNode {
	replicas := syncWaitNodeReplicaInfo(client, table)

	targetMigrateNode := replicas[target.String()]
	var validOriginNodes []*util.PegasusNode
	for _, origin := range origins {
		originMigrateNode := replicas[origin.String()]
		for _, replica := range originMigrateNode.replicas {
			if !targetMigrateNode.contain(replica.part.Pid) {
				validOriginNodes = append(validOriginNodes, origin)
				break
			}
		}
	}

	return validOriginNodes
}

func migrateReplica(client *Client, table string, froms []*util.PegasusNode, to *util.PegasusNode) {
	nodesReplicaInfo := syncWaitNodeReplicaInfo(client, table)
	origin := nodesReplicaInfo[froms[getFromNodeIndex(int32(len(froms)))].String()]
	target := nodesReplicaInfo[to.String()]
	if len(origin.replicas) == 0 {
		fmt.Printf("WARN: the pegasusNode[%s] has no replica to migrate\n", origin.pegasusNode.String())
		return
	}

	var action Action
	for _, replica := range origin.replicas {
		action = Action{
			replica: replica,
			from:    origin,
			to:      target,
		}

		if target.contain(replica.part.Pid) {
			fmt.Printf("WARN: actions[%s] target has existed the replica, will retry next replica\n", action.toString())
			continue
		}

		if !migrateActions.put(&action) {
			fmt.Printf("WARN: the replica move to target of actions[%s] has assgin other task, will retry next replica\n", action.toString())
			continue
		}
		action.migrateAndWaitComplete(client, table)
		action.downgradeToSecondaryAndWaitComplete(client, table)
		fmt.Printf("INFO: migrate complete, action: %s\n", action.toString())
		return
	}
}

func (act *Action) migrateAndWaitComplete(client *Client, table string) {
	var start = time.Now().Unix()
	var send = true
	for {
		nodeInfo := syncWaitNodeReplicaInfo(client, table)
		if !nodeInfo[act.from.pegasusNode.String()].contain(act.replica.part.Pid) {
			fmt.Printf("WARN: origin has no the replica[%s], break the task\n", act.toString())
			return
		}

		if nodeInfo[act.to.pegasusNode.String()].contain(act.replica.part.Pid) {
			fmt.Printf("WARN: target has existed the replica[%s], break the task\n", act.toString())
			return
		}

		if send {
			err := client.Meta.Balance(act.replica.part.Pid, act.replica.operation, act.from.pegasusNode, act.to.pegasusNode)
			if err != nil {
				fmt.Printf("WARN: wait, migrate action[%s] now is invalid: %s\n", act.toString(), err.Error())
				time.Sleep(10 * time.Second)
				continue
			}
			send = false
		}

		if time.Now().Unix()-start > retryPeriodSeconds {
			fmt.Printf("WARN: running %d minute, will retry send action[%s]\n", (time.Now().Unix()-start)/60, act.toString())
			start = time.Now().Unix()
			send = true
			continue
		}
		fmt.Printf("WARN: wait action %s\n", act.toString())
		time.Sleep(10 * time.Second)
	}
}

func (act *Action) downgradeToSecondaryAndWaitComplete(client *Client, table string) {
	for {
		resp, err := client.Meta.QueryConfig(table)
		if err != nil {
			fmt.Printf("WARN: wait, queryconfig action[%s] now is invalid: %s\n", act.toString(), err.Error())
			time.Sleep(10 * time.Second)
			continue
		}

		var selectReplica *replication.PartitionConfiguration
		for _, partition := range resp.Partitions {
			if partition.Pid.String() != act.replica.part.Pid.String() {
				continue
			} else {
				selectReplica = partition
				break
			}
		}

		if selectReplica == nil {
			return
		}

		upgradePrimaryNode := client.Nodes.MustGetReplica(selectReplica.Secondaries[0].GetAddress())
		if selectReplica.Primary.GetAddress() == act.to.pegasusNode.TCPAddr() {
			err = client.Meta.Balance(act.replica.part.Pid, migrator.BalanceMovePri, act.to.pegasusNode, upgradePrimaryNode)
			if err != nil {
				fmt.Printf("WARN: wait, downgrade action[%s] now is invalid: %s\n", act.toString(), err.Error())
				time.Sleep(10 * time.Second)
				continue
			}
		} else {
			return
		}
	}
}

var migrateActionsMu sync.Mutex

func (acts *MigrateActions) put(currentAction *Action) bool {
	migrateActionsMu.Lock()
	defer func() {
		migrateActionsMu.Unlock()
	}()

	if acts.exist(currentAction) {
		return false
	}

	acts.actionList[currentAction.toString()] = currentAction
	return true
}

func (acts *MigrateActions) exist(currentAction *Action) bool {
	for _, action := range acts.actionList {
		if action.replica.part.Pid.String() == currentAction.replica.part.Pid.String() {
			if action.to.pegasusNode.String() == currentAction.to.pegasusNode.String() {
				fmt.Printf("WARN: has called the action: %s, current: %s\n", action.toString(), currentAction.toString())
				return true
			}
		}
	}
	return false
}

func (act *Action) toString() string {
	return fmt.Sprintf("[%s]%s:%s=>%s", act.replica.operation.String(), act.replica.part.Pid.String(), act.from.pegasusNode.String(), act.to.pegasusNode.String())
}

func syncWaitNodeReplicaInfo(client *Client, table string) map[string]*MigratorNode {
	for {
		nodes, err := syncNodeReplicaInfo(client, table)
		if err == nil {
			return nodes
		}
		fmt.Printf("WARN: [%s]wait, table may be unhealthy: %s\n", table, err.Error())
		time.Sleep(10 * time.Second)
	}
}

func syncNodeReplicaInfo(client *Client, table string) (map[string]*MigratorNode, error) {
	var nodeReplicaInfoMap = make(map[string]*MigratorNode)

	nodeInfos, err := client.Meta.ListNodes()
	if err != nil {
		return nil, err
	}

	for _, info := range nodeInfos {
		pegasusNode := client.Nodes.MustGetReplica(info.Address.GetAddress())
		nodeReplicaInfoMap[pegasusNode.String()] = &MigratorNode{
			table:       table,
			pegasusNode: pegasusNode,
			replicas:    []*Replica{},
		}
	}

	var totalCount = 0
	resp, err := client.Meta.QueryConfig(table)
	if err != nil {
		return nil, err
	}
	totalCount = totalCount + 3*len(resp.Partitions)
	for _, partition := range resp.Partitions {
		if partition.Primary.GetRawAddress() == 0 {
			return nil, fmt.Errorf("table[%s] primary unhealthy, please check and wait healthy", table)
		}
		pegasusNode := client.Nodes.MustGetReplica(partition.Primary.GetAddress())
		replicasInfoMap := nodeReplicaInfoMap[pegasusNode.String()]
		if replicasInfoMap == nil {
			return nil, fmt.Errorf("can't find the pegasusNode[%s] primary replicas info", client.Nodes.MustGetReplica(partition.Primary.GetAddress()).CombinedAddr())
		}
		replicasInfoMap.replicas = append(replicasInfoMap.replicas, &Replica{
			part:      partition,
			operation: migrator.BalanceCopyPri,
		})

		for _, sec := range partition.Secondaries {
			if sec.GetRawAddress() == 0 {
				return nil, fmt.Errorf("table[%s] secondary unhealthy, please check and wait healthy", table)
			}
			pegasusNode := client.Nodes.MustGetReplica(sec.GetAddress())
			replicasInfoMap := nodeReplicaInfoMap[pegasusNode.String()]
			if replicasInfoMap == nil {
				return nil, fmt.Errorf("can't find the pegasusNode[%s] secondary replicas info", client.Nodes.MustGetReplica(partition.Primary.GetAddress()).CombinedAddr())
			}
			replicasInfoMap.replicas = append(replicasInfoMap.replicas, &Replica{
				part:      partition,
				operation: migrator.BalanceCopySec,
			})
		}
	}

	var replicaCount = 0
	for _, info := range nodeReplicaInfoMap {
		replicaCount = replicaCount + len(info.replicas)
	}
	if replicaCount != totalCount {
		return nil, fmt.Errorf("cluster unhealthy[expect=%d vs actual=%d], please check and wait healthy", totalCount, replicaCount)
	}
	return nodeReplicaInfoMap, nil
}

func (node *MigratorNode) contain(gpid *base.Gpid) bool {
	for _, replica := range node.replicas {
		if replica.part.Pid.String() == gpid.String() {
			return true
		}
	}
	return false
}
