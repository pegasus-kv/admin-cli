package executor

import (
	"fmt"
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
	pegasusNode *util.PegasusNode
	replicas    []*Replica
}

func (node *MigratorNode) toString() string {
	str := fmt.Sprintf("[%s]\n", node.pegasusNode.String())
	for _, replica := range node.replicas {
		str = fmt.Sprintf("%s%s:%s\n", str, replica.part.Pid.String(), replica.operation.String())
	}
	return str
}

type Action struct {
	from    string
	to      string
	replica *Replica
}

func (act *Action) toString() string {
	return fmt.Sprintf("[%s]%s:%s=>%s", act.replica.operation.String(), act.replica.part.Pid.String(), act.from, act.to)
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

func (acts *MigrateActions) delete(currentAction *Action) bool {
	migrateActionsMu.Lock()
	defer func() {
		migrateActionsMu.Unlock()
	}()
	delete(acts.actionList, currentAction.toString())
	return true
}

func (acts *MigrateActions) exist(currentAction *Action) bool {
	for _, action := range acts.actionList {
		if action.replica.part.Pid.String() == currentAction.replica.part.Pid.String() {
			if action.from == currentAction.from || action.to == currentAction.to {
				return true
			}
		}
	}
	return false
}

func MigrateAllReplicaToNodes(client *Client, from []string, to []string) error {
	var origins []*util.PegasusNode
	var targets []*util.PegasusNode

	for _, addr := range from {
		n, err := client.Nodes.GetNode(addr, session.NodeTypeReplica)
		if err != nil {
			return err
		}
		origins = append(origins, n)
	}

	for _, addr := range to {
		n, err := client.Nodes.GetNode(addr, session.NodeTypeReplica)
		if err != nil {
			return err
		}
		targets = append(targets, n)
	}

	if origins == nil || targets == nil {
		return fmt.Errorf("invalid origin or target pegasusNode")
	}

	var currentNodeCountOfNeedMaxCount = 0
	var startNodeIndex = 0
	for {
		replicas, err := syncNodeReplicaInfo(client)
		if err != nil {
			return err
		}

		var countNeedMigrate = 0
		for _, node := range origins {
			countNeedMigrate = countNeedMigrate + len(replicas[node.String()].replicas)
		}
		if countNeedMigrate <= 0 {
			fmt.Printf("INFO: migrate complete\n")
			return ListNodes(client)
		}

		currentTargetNode := targets[startNodeIndex]
		currentCountOfTargetNode := len(replicas[currentTargetNode.String()].replicas)
		minCount := countNeedMigrate / len(targets) + currentCountOfTargetNode
		maxCount := minCount + 1

		fmt.Printf("********INFO: start migrate replicas to %s, currentCount=%d, expect=%d(+1)******\n", currentTargetNode.String(), currentCountOfTargetNode, minCount)
		nodeCountOfNeedMaxCount := countNeedMigrate % len(targets)
		if currentCountOfTargetNode == maxCount && currentNodeCountOfNeedMaxCount <= nodeCountOfNeedMaxCount {
			startNodeIndex++
			currentNodeCountOfNeedMaxCount++
		}

		if currentCountOfTargetNode == minCount {
			startNodeIndex++
		}

		var wg sync.WaitGroup
		wg.Add(len(origins))
		var success = true
		for range origins {
			go func(to *util.PegasusNode) {
				err := migrateAllReplica(client, origins, currentTargetNode)
				if err != nil {
					success = false
					fmt.Printf("ERROR:migrate failed: %s\n", err)
				}
				wg.Done()
			}(currentTargetNode)
		}
		wg.Wait()
		if !success {
			err := ListNodes(client)
			if err != nil {
				return fmt.Errorf("migrate failed, please check the previous error log, list pegasusNode = %s", err)
			}
			return fmt.Errorf("migrate failed, please check the previous error log")
		}
		fmt.Printf("\n")
	}
}

func migrateAllReplica(client *Client, froms []*util.PegasusNode, to *util.PegasusNode) error {
	replicas, err := syncNodeReplicaInfo(client)
	if err != nil {
		return err
	}

	origin := replicas[froms[getFromNodeIndex(int32(len(froms)))].String()]
	target := replicas[to.String()]
	if len(origin.replicas) == 0 {
		fmt.Printf("WARNING: the pegasusNode[%s] has no replica to migrate\n", origin.pegasusNode.String())
		return nil
	}

	var action Action
	for _, replica := range origin.replicas {
		action = Action{
			replica: replica,
			from:    origin.pegasusNode.String(),
			to:      to.String(),
		}

		if target.contain(replica.part.Pid) {
			fmt.Printf("WARNING: actions[%s] target has existed the replica, will retry next replica\n", action.toString())
			continue
		}

		if !migrateActions.put(&action) {
			fmt.Printf("WARNING: actions[%s] is running, will retry next replica\n", action.toString())
			continue
		}

		err := origin.migrate(client, replica.operation, replica.part.Pid, target)
		if err != nil {
			return fmt.Errorf("send proposal %s failed, please update replica by query meta, err=%s", action.toString(), err.Error())
		}

		fmt.Printf("INFO: send migrate action[%s] succesfully, pleasing wait\n", action.toString())
		time.Sleep(10 * time.Second)
		for {
			replicas, err := syncNodeReplicaInfo(client)
			if err != nil {
				return err
			}

			if !replicas[origin.pegasusNode.String()].contain(replica.part.Pid) {
				break
			}

			if replicas[target.pegasusNode.String()].contain(replica.part.Pid) {
				fmt.Printf("WARNING: other task has migrate the replica[%s], break the task", action.toString())
				break
			}

			time.Sleep(10 * time.Second)
			fmt.Printf("WARN: running, wait action[%s]\n", action.toString())
			err = origin.migrate(client, replica.operation, replica.part.Pid, target)
			if err != nil {
				return fmt.Errorf("send proposal %s failed, please update replica by query meta", action.toString())
			}
			continue
		}
		break
	}
	err = client.Meta.Propose(action.replica.part.Pid, admin.ConfigType_CT_DOWNGRADE_TO_SECONDARY, client.Nodes.MustGetReplica(action.replica.part.Primary.GetAddress()), to)
	if err != nil {
		return err
	}
	time.Sleep(10 * time.Second)
	migrateActions.delete(&action)
	fmt.Printf("INFO: migrate complete, action: %s\n", action.toString())
	return nil
}

func (node *MigratorNode) migrate(client *Client, copyType migrator.BalanceType, gpid *base.Gpid, to *MigratorNode) error {
	if to.contain(gpid) {
		fmt.Printf("WARN: the action[%s(%s) %s=>%s] is invalid for the target pegasusNode has existed the replica\n",
			gpid.String(), copyType, node.pegasusNode.String(), to.pegasusNode.String())
		return fmt.Errorf("invalid replica")
	}
	return client.Meta.Balance(gpid, copyType, node.pegasusNode, to.pegasusNode)
}

func syncNodeReplicaInfo(client *Client) (map[string]*MigratorNode, error) {
	var nodeReplicaInfoMap = make(map[string]*MigratorNode)

	nodeInfos, err := client.Meta.ListNodes()
	if err != nil {
		return nil, err
	}

	for _, info := range nodeInfos {
		pegasusNode := client.Nodes.MustGetReplica(info.Address.GetAddress())
		nodeReplicaInfoMap[pegasusNode.String()] = &MigratorNode{
			pegasusNode: pegasusNode,
			replicas:    []*Replica{},
		}
	}

	tables, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
	if err != nil {
		return nil, err
	}

	var totalCount = 0
	for _, tb := range tables {
		resp, err := client.Meta.QueryConfig(tb.AppName)
		if err != nil {
			return nil, err
		}
		totalCount = totalCount + 3*len(resp.Partitions)
		for _, partition := range resp.Partitions {
			if partition.Primary.GetRawAddress() == 0 {
				return nil, fmt.Errorf("table[%s] primary unhealthy, please check and wait healthy", tb.AppName)
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
					return nil, fmt.Errorf("table[%s] secondary unhealthy, please check and wait healthy", tb.AppName)
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
