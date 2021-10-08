package nodesmigrator

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/session"
	migrator "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/util"
)

type Migrator struct {
	nodes map[string]*MigratorNode

	origins []*util.PegasusNode
	targets []*util.PegasusNode
}

func (m *Migrator) run(client *executor.Client, table string, round int, target *MigratorNode) int {
	for {
		m.updateNodesReplicaInfo(client, table)
		remainingCount := m.getRemainingReplicaCount()
		if remainingCount <= 0 {
			fmt.Printf("INFO: [%s]completed for no replicas can be migrated\n", table)
			return remainingCount
		}

		validOriginNodes := m.getValidOriginNodes(target)
		if len(validOriginNodes) == 0 {
			fmt.Printf("INFO: [%s]no valid replicas can be migratede\n", table)
			return remainingCount
		}

		expectCount := m.getExpectReplicaCount(round)
		currentCount := m.getCurrentReplicaCount(target)
		if currentCount >= expectCount {
			fmt.Printf("INFO: [%s]balance: no need migrate replicas to %s, current=%d, expect=max(%d)\n",
				table, target.String(), currentCount, expectCount)
			return remainingCount
		}

		maxConcurrentCount := int(math.Min(float64(len(validOriginNodes)), float64(expectCount-currentCount)))
		m.submitMigrateTaskAndWait(client, table, validOriginNodes, target, maxConcurrentCount)
	}
}

func (m *Migrator) getCurrentTargetNode(index int) (int, *MigratorNode) {
	round := index/len(m.targets) + 1
	currentTargetNode := m.targets[index%len(m.targets)]
	return round, &MigratorNode{node: currentTargetNode}
}

func (m *Migrator) updateNodesReplicaInfo(client *executor.Client, table string) {
	for {
		if err := m.syncNodesReplicaInfo(client, table); err != nil {
			fmt.Printf("WARN: [%s]wait, table may be unhealthy: %s\n", table, err.Error())
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
		if err := m.fillReplicasInfo(client, table, partition, migrator.BalanceCopyPri); err != nil {
			return err
		}
		currentTotalCount++
		for _, sec := range partition.Secondaries {
			if sec.GetRawAddress() == 0 {
				return fmt.Errorf("table[%s] secondary unhealthy, please check and wait healthy", table)
			}
			if err := m.fillReplicasInfo(client, table, partition, migrator.BalanceCopySec); err != nil {
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
	partition *replication.PartitionConfiguration, balanceType migrator.BalanceType) error {
	pegasusNode := client.Nodes.MustGetReplica(partition.Primary.GetAddress())
	migratorNode := m.nodes[pegasusNode.String()]
	if migratorNode == nil {
		return fmt.Errorf("[%s]can't find [%s] replicas info", table, pegasusNode.CombinedAddr())
	}
	migratorNode.replicas = append(migratorNode.replicas, &Replica{
		part:      partition,
		operation: balanceType,
	})
	return nil
}

func (m *Migrator) getCurrentReplicaCount(node *MigratorNode) int {
	return len(m.nodes[node.String()].replicas)
}

func (m *Migrator) getRemainingReplicaCount() int {
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

func (m *Migrator) getValidOriginNodes(target *MigratorNode) []*MigratorNode {
	targetMigrateNode := m.nodes[target.String()]
	var validOriginNodes []*MigratorNode
	for _, origin := range m.origins {
		originMigrateNode := m.nodes[origin.String()]
		for _, replica := range originMigrateNode.replicas {
			if !targetMigrateNode.contain(replica.part.Pid) {
				validOriginNodes = append(validOriginNodes, originMigrateNode)
				break
			}
		}
	}
	return validOriginNodes
}

var migrateActions = MigrateActions{actionList: map[string]*Action{}}

func (m *Migrator) submitMigrateTaskAndWait(client *executor.Client, table string, origins []*MigratorNode, target *MigratorNode, maxConcurrentCount int) {
	var wg sync.WaitGroup
	wg.Add(maxConcurrentCount)
	for maxConcurrentCount > 0 {
		go func(to *MigratorNode) {
			m.migrateAndWaitComplete(client, table, origins, target)
			wg.Done()
		}(target)
		maxConcurrentCount--
	}
	fmt.Printf("INFO: [%s]async migrate task call complete, wait all works successfully\n", table)
	wg.Wait()
	fmt.Printf("INFO: [%s]all works successfully\n\n", table)
}

func (m *Migrator) migrateAndWaitComplete(client *executor.Client, table string, origins []*MigratorNode, target *MigratorNode) {
	from := origins[getFromNodeIndex(int32(len(origins)))]
	if len(from.replicas) == 0 {
		fmt.Printf("WARN: the pegasusNode[%s] has no replica to migrate\n", target.node.String())
		return
	}

	var action *Action
	for _, replica := range from.replicas {
		action = &Action{
			replica: replica,
			from:    from,
			to:      target,
		}

		if target.contain(replica.part.Pid) {
			fmt.Printf("WARN: actions[%s] target has existed the replica, will retry next replica\n", action.toString())
			continue
		}

		if !migrateActions.put(action) {
			fmt.Printf("WARN: the replica move to target of actions[%s] has assgin other task, will retry next replica\n", action.toString())
			continue
		}
		m.executeMigrateAction(client, action, table)
		if action.replica.operation == migrator.BalanceCopyPri {
			target.downgradeOneReplicaToSecondary(client, table, action.replica.part.Pid)
		}
		fmt.Printf("INFO: migrate complete, action: %s\n", action.toString())
		return
	}
}

func (m *Migrator) executeMigrateAction(client *executor.Client, action *Action, table string) {
	for {
		if !m.nodes[action.from.node.String()].contain(action.replica.part.Pid) {
			fmt.Printf("WARN: origin has no the replica[%s], break the task\n", action.toString())
			return
		}

		if m.nodes[action.to.node.String()].contain(action.replica.part.Pid) {
			fmt.Printf("WARN: target has existed the replica[%s], break the task\n", action.toString())
			return
		}

		err := client.Meta.Balance(action.replica.part.Pid, action.replica.operation, action.from.node, action.to.node)
		if err != nil {
			fmt.Printf("WARN: wait, migrate action[%s] now is invalid: %s\n", action.toString(), err.Error())
			time.Sleep(10 * time.Second)
			continue
		}

		fmt.Printf("WARN: wait action %s\n", action.toString())
		time.Sleep(10 * time.Second)
	}
}

func createNewMigrator(client *executor.Client, from []string, to []string) (*Migrator, error) {
	origins, targets, err := convert2MigratorNodes(client, from, to)
	if err != nil {
		return nil, fmt.Errorf("invalid origin or target node, error = %s", err.Error())
	}

	return &Migrator{
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

var hash int32

func getFromNodeIndex(count int32) int32 {
	return atomic.AddInt32(&hash, 1) % count
}
