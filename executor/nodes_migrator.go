package executor

import (
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/session"
	migrator "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
	"math"
	"strings"
	"sync"
	"time"
)

var migrateActions = MigrateActions{}

type MigratorNode struct {
	node      *util.PegasusNode
	primary   []*base.Gpid
	secondary []*base.Gpid
}

type Action struct {
	from string
	to string
	gpid *base.Gpid
}

func (act *Action) toString() string {
	return fmt.Sprintf("%s:%s=>%s", act.gpid.String(), act.from, act.to)
}

type MigrateActions struct {
	actionList map[string]*Action
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

func (acts *MigrateActions) exist (currentAction *Action) bool {
	for _, action := range acts.actionList {
		if action.gpid.String() == currentAction.gpid.String() {
			if action.from == currentAction.from || action.to == currentAction.to {
				return true
			}
		}
	}
	return false
}


func MigrateAllReplicaToNodes(client *Client, from []string, to []string) error {
	var origin []*util.PegasusNode
	var target []*util.PegasusNode

	var countNeedMigrate = 0
	for _, addr := range from {
		n, err := client.Nodes.GetNode(addr, session.NodeTypeReplica)
		if err != nil {
			return err
		}
		origin = append(origin, n)
	}

	for _, addr := range to {
		n, err := client.Nodes.GetNode(addr, session.NodeTypeReplica)
		if err != nil {
			return err
		}
		target = append(target, n)
	}

	replicas, err := syncNodeReplicaInfo(client)
	if err != nil {
		return err
	}

	for _, n := range origin {
		countNeedMigrate = countNeedMigrate + len(replicas[n.TCPAddr()].primary) + len(replicas[n.TCPAddr()].secondary)
	}

	countPerRound := int64(math.Min(float64(countNeedMigrate/len(target)), 1))
	return nil
}


func (origin *MigratorNode) migrateAllReplica(client *Client, to *MigratorNode, count int) error {
	if len(origin.primary) != 0 {
		return fmt.Errorf("please assign all primary to seconday")
	}

	for n, gpid := range origin.secondary {
		if n == count {
			break
		}

		action := Action{
			gpid: gpid,
			from: origin.node.String(),
			to: to.node.String(),
		}
		if !migrateActions.put(&action) {
			fmt.Printf("WARNING: actions[%s] is running", action.toString())
			continue
		}

		err := origin.migrate(client, gpid, to)
		errRetry := origin.migrate(client, gpid, to)
		if err == nil && errRetry == nil {
			return fmt.Errorf("please update replica by query meta")
		}

		if (err != nil && err.Error() == "invalid replica") || (errRetry != nil && errRetry.Error() == "invalid replica") {
			continue
		}

		if (err != nil && strings.Contains(err.Error(), "ERR_INVALID_PARAMETERS")) ||
			(errRetry != nil && strings.Contains(errRetry.Error(), "ERR_INVALID_PARAMETERS")) {
			fmt.Printf("INFO: send migrate action[%s(%s) %s=>%s] succesfully, pleasing wait\n", gpid.String(), migrator.BalanceCopySec, origin.node.String(), to.node.String())
			for true {
				replicas, err := syncNodeReplicaInfo(client)
				if err != nil {
					return err
				}

				if replicas[origin.node.TCPAddr()].contain(gpid) || !replicas[to.node.TCPAddr()].contain(gpid) {
					time.Sleep(10 * time.Second)
					fmt.Printf("WARN: running, wait action[%s(%s) %s=>%s]", gpid.String(), migrator.BalanceCopySec, origin.node.String(), to.node.String())
					continue
				}
			}
		}
	}
	fmt.Printf("INFO: migrate complete, total=%d, need=%d", origin.secondary, count)
	return nil
}

func (origin *MigratorNode) migrate(client *Client, gpid *base.Gpid, to *MigratorNode) error {
	if to.contain(gpid) {
		fmt.Printf("WARN: the action[%s(%s) %s=>%s] is invalid for the target node has existed the replica\n",
			gpid.String(), migrator.BalanceCopySec, origin.node.String(), to.node.String())
		return fmt.Errorf("invalid replica")
	}
	return client.Meta.Balance(gpid, migrator.BalanceCopySec, origin.node, to.node)
}

func syncNodeReplicaInfo(client *Client) (map[string]*MigratorNode, error) {
	var nodeReplicaInfoMap = make(map[string]*MigratorNode)

	nodeInfos, err := client.Meta.ListNodes()
	if err != nil {
		return nil, err
	}

	for _, info := range nodeInfos {
		pegasusNode := client.Nodes.MustGetReplica(info.Address.GetAddress())
		nodeReplicaInfoMap[info.Address.GetAddress()] = &MigratorNode{
			node:      pegasusNode,
			primary:   []*base.Gpid{},
			secondary: []*base.Gpid{},
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
		totalCount = totalCount + 3 * len(resp.Partitions)
		for _, partition := range resp.Partitions {
			if partition.Primary.GetRawAddress() == 0 {
				return nil, fmt.Errorf("table[%s] primary unhealthy, please check and wait healthy", tb.AppName)
			}
			replicas := nodeReplicaInfoMap[partition.Primary.GetAddress()]
			if replicas == nil {
				return nil, fmt.Errorf("can't find the node[%s] replicas info", client.Nodes.MustGetReplica(partition.Primary.GetAddress()).CombinedAddr())
			}
			replicas.primary = append(replicas.primary, partition.Pid)

			for _, sec := range partition.Secondaries {
				if sec.GetRawAddress() == 0 {
					return nil, fmt.Errorf("table[%s] secondary unhealthy, please check and wait healthy", tb.AppName)
				}
				replicas := nodeReplicaInfoMap[sec.GetAddress()]
				if replicas == nil {
					return nil, fmt.Errorf("can't find the node[{%s}] replicas info", client.Nodes.MustGetReplica(partition.Primary.GetAddress()).CombinedAddr())
				}
				replicas.secondary = append(replicas.secondary, partition.Pid)
			}
		}
	}

	var replicaCount = 0
	for _, info := range nodeReplicaInfoMap {
		replicaCount = replicaCount + len(info.primary) + len(info.secondary)
	}
	if replicaCount != totalCount {
		return nil, fmt.Errorf("table[%s] unhealthy, please check and wait healthy", table)
	}
	return nodeReplicaInfoMap,nil
}

func (origin *MigratorNode) contain(replica *base.Gpid) bool {
	for _, gpid := range origin.secondary {
		if gpid.String() == replica.String() {
			return true
		}
	}

	for _, gpid := range origin.secondary {
		if gpid.String() == replica.String() {
			return true
		}
	}
	return false
}
