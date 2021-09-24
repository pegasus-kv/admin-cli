package executor

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	migrator "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
)

type MigratorNode struct {
	// todo: for ci pass, the variable is necessary in later pr
	//table       string
	//pegasusNode *util.PegasusNode
	replicas []*Replica
}

type Replica struct {
	// todo: for ci pass, the variable is necessary in later pr
	//part      *replication.PartitionConfiguration
	//operation migrator.BalanceType
}

func MigrateAllReplicaToNodes(client *Client, period int64, from []string, to []string) error {
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

	var targetNodeIndex = 0
	var remainingReplica = math.MaxInt16
	for {
		if remainingReplica <= 0 {
			fmt.Printf("INFO: completed for all the targets has migrate\n")
			return ListNodes(client)
		}
		round := targetNodeIndex/len(targets) + 1
		currentTargetNode := targets[targetNodeIndex%len(targets)]
		fmt.Printf("\n\n********[%d]start migrate replicas to %s******\n", round, currentTargetNode.String())
		fmt.Printf("INFO: migrate out all primary from current node %s\n", currentTargetNode.String())
		// assign all primary replica to secondary on target node to avoid read influence
		migratePrimariesOut(client, currentTargetNode)
		tableCompleted := 0
		for {
			// migrate enough replica to one target node per round.
			// pick next node if all tables have been handled completed.
			if tableCompleted >= len(tables) {
				targetNodeIndex++
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
				fmt.Printf("WARN: wait, migrate primary out of %s is invalid when list primaries on node, err = %s\n",
					node.String(), err)
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
	replicas := syncAndWaitNodeReplicaInfo(client, table)
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
		fmt.Printf("INFO: [%s]balance, no need migrate replicas to %s, currentCount=%d, expect=min(%d) or max(%d)\n",
			table, currentTargetNode.String(), currentCountOfTargetNode, min, max)
		return countNeedMigrate, true, len(validOriginNodes)
	}

	if currentCountOfTargetNode >= max {
		fmt.Printf("INFO: [%s]balance with need max: no need migrate replicas to %s, currentCount=%d, expect=min(%d) or max(%d)\n",
			table, currentTargetNode.String(), currentCountOfTargetNode, min, max)
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
	//todo
	return nil
}

func syncAndWaitNodeReplicaInfo(client *Client, table string) map[string]*MigratorNode {
	// todo
	return nil
}

func migrateReplica(client *Client, table string, froms []*util.PegasusNode, to *util.PegasusNode) {
	// todo
}
