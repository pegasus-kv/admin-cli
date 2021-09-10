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

type Replica struct {
	part      *replication.PartitionConfiguration
	operation migrator.BalanceType
}
type MigratorNode struct {
	table       string
	pegasusNode *util.PegasusNode
	replicas    []*Replica
}

/*func (node *MigratorNode) toString() string {
	str := fmt.Sprintf("[%s]\n", node.pegasusNode.String())
	for _, replica := range node.replicas {
		str = fmt.Sprintf("%s%s:%s\n", str, replica.part.Pid.String(), replica.operation.String())
	}
	return str
}*/

type Action struct {
	from    *MigratorNode
	to      *MigratorNode
	replica *Replica
}

func (act *Action) toString() string {
	return fmt.Sprintf("[%s]%s:%s=>%s", act.replica.operation.String(), act.replica.part.Pid.String(), act.from.pegasusNode.String(), act.to.pegasusNode.String())
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

/*func (acts *MigrateActions) delete(currentAction *Action) bool {
	migrateActionsMu.Lock()
	defer func() {
		migrateActionsMu.Unlock()
	}()
	delete(acts.actionList, currentAction.toString())
	return true
}*/

func (acts *MigrateActions) exist(currentAction *Action) bool {
	for _, action := range acts.actionList {
		if action.replica.part.Pid.String() == currentAction.replica.part.Pid.String() {
			if action.from.pegasusNode.String() == currentAction.from.pegasusNode.String() ||
				action.to.pegasusNode.String() == currentAction.to.pegasusNode.String() {
				return true
			}
		}
	}
	return false
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
		return fmt.Errorf("invalid origin or target pegasusNode")
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
		currentTargetNode := targets[targetStartNodeIndex%len(targets)]
		fmt.Printf("\n\n********start migrate replicas to %s******\n", currentTargetNode.String())
		tableCompleted := 0
		tableInvalid := 0
		for {
			if tableCompleted >= len(tables) || tableInvalid >= 1 {
				targetStartNodeIndex++
				break
			}
			tableCompleted = 0
			remainingReplica = 0
			for _, tb := range tables {
				validOriginNodes := getValidOriginNodes(client, tb.AppName, origins, currentTargetNode)
				if len(validOriginNodes) == 0 {
					fmt.Printf("INFO: [%s]no valid repica can be migrated with table\n", tb.AppName)
					tableInvalid++
					continue
				}

				var totalReplicaCount = 0
				replicas := syncNodeReplicaInfoWithRetry(client, tb.AppName)
				for _, node := range replicas {
					totalReplicaCount = totalReplicaCount + len(node.replicas)
				}
				min := totalReplicaCount / len(targets)
				max := min + 1

				var countNeedMigrate = 0
				for _, node := range origins {
					countNeedMigrate = countNeedMigrate + len(replicas[node.String()].replicas)
					remainingReplica = remainingReplica + countNeedMigrate
				}
				if countNeedMigrate <= 0 {
					fmt.Printf("INFO: [%s]completed for no replicas can be migrated with table\n", tb.AppName)
					tableCompleted++
					continue
				}

				currentCountOfTargetNode := len(replicas[currentTargetNode.String()].replicas)
				if currentCountOfTargetNode >= max {
					fmt.Printf("INFO: [%s]no need migrate replicas to %s, currentCount=%d, expect=min(%d) or max(%d)\n", tb.AppName, currentTargetNode.String(), currentCountOfTargetNode, min, max)
					tableCompleted++
					continue
				}

				var wg sync.WaitGroup
				loopCount := int(math.Min(float64(len(origins)), float64(max-currentCountOfTargetNode)))
				wg.Add(loopCount)
				for loopCount > 0 {
					go func(to *util.PegasusNode) {
						migrateAllReplica(client, tb.AppName, validOriginNodes, currentTargetNode)
						wg.Done()
					}(currentTargetNode)
					loopCount--
				}
				fmt.Printf("INFO: [%s]async migrate task call complete, wait all works successfully\n", tb.AppName)
				wg.Wait()
				fmt.Printf("INFO: [%s]all works successfully\n\n", tb.AppName)
			}
		}
	}
}

func migrateAllReplica(client *Client, table string, froms []*util.PegasusNode, to *util.PegasusNode) {
	nodesReplicaInfo := syncNodeReplicaInfoWithRetry(client, table)
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

func (act *Action) migrateAndWaitComplete(client *Client, table string) {
	var start = time.Now().Unix()
	var send = true
	for {
		nodeInfo := syncNodeReplicaInfoWithRetry(client, table)
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

func syncNodeReplicaInfoWithRetry(client *Client, table string) map[string]*MigratorNode {
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

func getValidOriginNodes(client *Client, table string, origins []*util.PegasusNode, target *util.PegasusNode) []*util.PegasusNode {
	replicas := syncNodeReplicaInfoWithRetry(client, table)

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

func (node *MigratorNode) contain(gpid *base.Gpid) bool {
	for _, replica := range node.replicas {
		if replica.part.Pid.String() == gpid.String() {
			return true
		}
	}
	return false
}
