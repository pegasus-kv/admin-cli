package nodesmigrator

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/session"
	migrator "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/util"
)

type Migrator struct {
	nodes          map[string]*MigratorNode
	ongoingActions *MigrateActions
	totalActions   *MigrateActions

	origins []*util.PegasusNode
	targets []*util.PegasusNode
}

func (m *Migrator) run(client *executor.Client, table string, round int, origin *MigratorNode, maxConcurrent int) int {
	balanceTargets := make(map[string]int)
	invalidTargets := make(map[string]int)
	for {
		target := m.selectNextTargetNode()
		m.updateNodesReplicaInfo(client, table)
		m.updateOngoingActionList()
		remainingCount := m.getRemainingReplicaCount(origin)
		if remainingCount <= 0 || len(balanceTargets) == len(m.targets) || len(invalidTargets) == len(m.targets) {
			logInfo(fmt.Sprintf("INFO: [%s]completed(remaining=%d, balance=%v, invalid=%v) for no replicas can be migrated",
				table, remainingCount, len(balanceTargets) == len(m.targets), len(invalidTargets) == len(m.targets)), true)
			return m.getTotalRemainingReplicaCount()
		}

		expectCount := m.getExpectReplicaCount(round)
		currentCount := m.getCurrentReplicaCount(target)
		if currentCount >= expectCount {
			balanceTargets[target.String()] = 1
			logInfo(fmt.Sprintf("INFO: [%s]balance: no need migrate replicas to %s, current=%d, expect=max(%d), total_balance=%d",
				table, target.String(), currentCount, expectCount, len(balanceTargets)), true)
			time.Sleep(1 * time.Second)
			continue
		}

		if !m.existValidReplica(origin, target) {
			invalidTargets[target.String()] = 1
			logWarn(fmt.Sprintf("WARN: [%s]invalid: no invalid migrate replicas to %s, total_invalid=%d",
				table, target.String(), len(balanceTargets)), true)
			time.Sleep(1 * time.Second)
			continue
		}

		currentConcurrentCount := target.concurrent(m.ongoingActions)
		if currentConcurrentCount == maxConcurrent {
			logWarn(fmt.Sprintf("WARN: [%s] %s has excceed the max concurrent = %d", table, target.String(),
				currentConcurrentCount), true)
			time.Sleep(1 * time.Second)
			continue
		}

		concurrent := int(math.Min(float64(maxConcurrent-target.concurrent(m.ongoingActions)), float64(expectCount-currentCount)))
		m.submitMigrateTask(client, table, origin, target, concurrent)
		logInfo(fmt.Sprintf("INFO: [%s]send %s migrate task, ongiong task = %d", table, target.String(),
			target.concurrent(m.ongoingActions)), true)
		time.Sleep(10 * time.Second)
	}
}

var originIndex int32 = -1

func (m *Migrator) selectNextOriginNode() *MigratorNode {
	currentOriginNode := m.origins[int(atomic.AddInt32(&originIndex, 1))%len(m.origins)]
	return &MigratorNode{node: currentOriginNode}
}

var targetIndex int32 = -1

func (m *Migrator) selectNextTargetNode() *MigratorNode {
	currentTargetNode := m.targets[int(atomic.AddInt32(&targetIndex, 1))%len(m.targets)]
	return &MigratorNode{node: currentTargetNode}
}

func (m *Migrator) updateNodesReplicaInfo(client *executor.Client, table string) {
	for {
		if err := m.syncNodesReplicaInfo(client, table); err != nil {
			logWarn(fmt.Sprintf("WARN: [%s]table may be unhealthy: %s", table, err.Error()), true)
			time.Sleep(10 * time.Second)
			continue
		}
		return
	}
}

func (m *Migrator) syncNodesReplicaInfo(client *executor.Client, table string) error {
	nodes, err := client.Meta.ListNodes()
	if err != nil {
		return err
	}

	for _, n := range nodes {
		pegasusNode := client.Nodes.MustGetReplica(n.Address.GetAddress())
		m.nodes[pegasusNode.String()] = &MigratorNode{
			node:     pegasusNode,
			replicas: []*Replica{},
		}
	}

	resp, err := client.Meta.QueryConfig(table)
	if err != nil {
		return err
	}
	expectTotalCount := 3 * len(resp.Partitions)
	currentTotalCount := 0
	for _, partition := range resp.Partitions {
		if partition.Primary.GetRawAddress() == 0 {
			return fmt.Errorf("table[%s] primary unhealthy, please check and wait healthy", table)
		}
		if err := m.fillReplicasInfo(client, table, partition.Pid, partition.Primary.GetAddress(), migrator.BalanceCopyPri); err != nil {
			return err
		}
		currentTotalCount++
		for _, sec := range partition.Secondaries {
			if sec.GetRawAddress() == 0 {
				return fmt.Errorf("table[%s] secondary unhealthy, please check and wait healthy", table)
			}
			if err := m.fillReplicasInfo(client, table, partition.Pid, sec.GetAddress(), migrator.BalanceCopySec); err != nil {
				return err
			}
			currentTotalCount++
		}
	}

	if currentTotalCount != expectTotalCount {
		return fmt.Errorf("cluster unhealthy[expect=%d vs actual=%d], please check and wait healthy",
			expectTotalCount, currentTotalCount)
	}
	return nil
}

func (m *Migrator) fillReplicasInfo(client *executor.Client, table string,
	gpid *base.Gpid, addr string, balanceType migrator.BalanceType) error {

	pegasusNode := client.Nodes.MustGetReplica(addr)
	migratorNode := m.nodes[pegasusNode.String()]
	if migratorNode == nil {
		return fmt.Errorf("[%s]can't find [%s] replicas info", table, pegasusNode.CombinedAddr())
	}
	migratorNode.replicas = append(migratorNode.replicas, &Replica{
		gpid:      gpid,
		operation: balanceType,
	})
	return nil
}

func (m *Migrator) getCurrentReplicaCount(node *MigratorNode) int {
	return len(m.nodes[node.String()].replicas)
}

func (m *Migrator) getRemainingReplicaCount(node *MigratorNode) int {
	return len(m.nodes[node.String()].replicas)
}

func (m *Migrator) getTotalRemainingReplicaCount() int {
	var remainingCount = 0
	for _, node := range m.origins {
		remainingCount = remainingCount + len(m.nodes[node.String()].replicas)
	}
	return remainingCount
}

func (m *Migrator) getExpectReplicaCount(round int) int {
	totalReplicaCount := 0
	for _, node := range m.nodes {
		totalReplicaCount = totalReplicaCount + len(node.replicas)
	}
	return (totalReplicaCount / len(m.targets)) + round
}

func (m *Migrator) existValidReplica(origin *MigratorNode, target *MigratorNode) bool {
	originMigrateNode := m.nodes[origin.String()]
	targetMigrateNode := m.nodes[target.String()]
	for _, replica := range originMigrateNode.replicas {
		if !targetMigrateNode.contain(replica.gpid) {
			return true
		}
	}

	return false
}

func (m *Migrator) submitMigrateTask(client *executor.Client, table string, origin *MigratorNode, target *MigratorNode, concurrentCount int) {
	var wg sync.WaitGroup
	wg.Add(concurrentCount)
	for concurrentCount > 0 {
		go func(to *MigratorNode) {
			m.sendMigrateRequest(client, table, origin, target)
			wg.Done()
		}(target)
		concurrentCount--
	}
	wg.Wait()
}

func (m *Migrator) sendMigrateRequest(client *executor.Client, table string, origin *MigratorNode, target *MigratorNode) {
	from := m.nodes[origin.String()]
	to := m.nodes[target.String()]
	if len(from.replicas) == 0 {
		logWarn(fmt.Sprintf("WARN: the node[%s] has no replica to migrate", target.node.String()), false)
		return
	}

	if from.primaryCount() != 0 {
		logPanic(fmt.Sprintf("FATAL: the origin[%s] should not exist primary replica", target.node.String()))
	}

	var action *Action
	for _, replica := range from.replicas {
		action = &Action{
			replica: replica,
			from:    from,
			to:      to,
		}

		if to.contain(replica.gpid) {
			logWarn(fmt.Sprintf("WARN: actions[%s] target has existed the replica", action.toString()), false)
			continue
		}

		if m.totalActions.exist(action) {
			logWarn(fmt.Sprintf("WARN: action[%s] has assgin other task", action.toString()), false)
			continue
		}

		m.totalActions.put(action)
		m.ongoingActions.put(action)
		err := m.executeMigrateAction(client, action)
		if err != nil {
			m.totalActions.delete(action)
			m.ongoingActions.delete(action)
			logWarn(fmt.Sprintf("WARN: send failed: %s", err.Error()), false)
			continue
		}
		logInfo(fmt.Sprintf("INFO: send action: %s", action.toString()), true)
		return
	}
}

func (m *Migrator) executeMigrateAction(client *executor.Client, action *Action) error {
	err := client.Meta.Balance(action.replica.gpid, action.replica.operation, action.from.node, action.to.node)
	if err != nil {
		return fmt.Errorf("migrate action[%s] now is invalid: %s", action.toString(), err.Error())
	}
	return nil
}

func (m *Migrator) updateOngoingActionList() {
	for name, act := range m.ongoingActions.actionList {
		node := m.nodes[act.to.String()]
		if node.contain(act.replica.gpid) {
			logInfo(fmt.Sprintf("INFO: %s has completed", name), true)
			m.ongoingActions.delete(act)
		} else {
			logWarn(fmt.Sprintf("INFO: %s is running", name), true)
		}
	}
}

func createNewMigrator(client *executor.Client, from []string, to []string) (*Migrator, error) {
	origins, targets, err := convert2MigratorNodes(client, from, to)
	if err != nil {
		return nil, fmt.Errorf("invalid origin or target node, error = %s", err.Error())
	}

	return &Migrator{
		nodes: map[string]*MigratorNode{},
		totalActions: &MigrateActions{
			actionList: map[string]*Action{},
		},
		ongoingActions: &MigrateActions{
			actionList: map[string]*Action{},
		},

		origins: origins,
		targets: targets,
	}, nil
}

func convert2MigratorNodes(client *executor.Client, from []string, to []string) ([]*util.PegasusNode, []*util.PegasusNode, error) {
	origins, err := convert(client, from)
	if err != nil {
		return nil, nil, err
	}
	targets, err := convert(client, to)
	if err != nil {
		return nil, nil, err
	}
	return origins, targets, nil
}

func convert(client *executor.Client, nodes []string) ([]*util.PegasusNode, error) {
	var pegasusNodes []*util.PegasusNode
	for _, addr := range nodes {
		node, err := client.Nodes.GetNode(addr, session.NodeTypeReplica)
		if err != nil {
			return nil, fmt.Errorf("list node failed: %s", err)
		}
		pegasusNodes = append(pegasusNodes, node)
	}
	if pegasusNodes == nil {
		return nil, fmt.Errorf("invalid nodes list")
	}
	return pegasusNodes, nil
}
