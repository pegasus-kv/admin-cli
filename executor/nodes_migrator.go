package executor

import (
	"fmt"
	"math"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/util"
)

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
	//todo
}

func migrateReplicaPerTable(client *Client, round int, table string, origins []*util.PegasusNode,
	targets []*util.PegasusNode, currentTargetNode *util.PegasusNode) (int, bool, int) {
	//todo
	return 0, false, 0
}
