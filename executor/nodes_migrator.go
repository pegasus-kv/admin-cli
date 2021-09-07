package executor

import (
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	migrator "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
	"strings"
	"time"
)

func MigrateAllReplicaToNodes(client *Client, from []string, to []string) error {
	return nil
}

type MigratorNode struct {
	table string
	node *util.PegasusNode
	primary []*base.Gpid
	secondary []*base.Gpid
}

func(origin *MigratorNode) migrateAllReplica(client *Client, to *MigratorNode, count int) error {
	if len(origin.primary) != 0 {
		return fmt.Errorf("please assign all primary to seconday")
	}

	for n, gpid := range origin.secondary {
		if n == count {
			break
		}

		err := client.Meta.Balance(gpid,migrator.BalanceCopySec, origin.node, to.node)
		errRetry := client.Meta.Balance(gpid,migrator.BalanceCopySec, origin.node, to.node)
		if err == nil && errRetry == nil {
			return fmt.Errorf("please update replica by query meta")
		}

		if (err != nil && err.Error() == "invalid replica") || (errRetry != nil && errRetry.Error() == "invalid replica") {
			continue
		}

		if (err != nil && strings.Contains(err.Error(),"ERR_INVALID_PARAMETERS")) ||
			(errRetry != nil && strings.Contains(errRetry.Error(),"ERR_INVALID_PARAMETERS")) {
			fmt.Printf("INFO: send migrate action[%s(%s) %s=>%s] succesfully, pleasing wait\n", gpid.String(), migrator.BalanceCopySec, origin.node.String(), to.node.String())
			for true {
				resp, err := client.Meta.QueryConfig(origin.table)
				if err != nil {
					return err
				}

				var running bool
				for _, partition := range resp.Partitions {
					if partition.Pid.String() != gpid.String() {
						continue
					}

					if client.Nodes.MustGetReplica(partition.Primary.GetAddress()).CombinedAddr() == origin.node.CombinedAddr() {
						return fmt.Errorf("please retry assign all primary to seconday")
					}

					for _, sec := range partition.Secondaries {
						currentAddr := client.Nodes.MustGetReplica(sec.GetAddress()).CombinedAddr()
						if currentAddr == origin.node.CombinedAddr() || currentAddr != to.node.CombinedAddr() {
							running = true
							break
						}
					}
				}
				if running {
					time.Sleep(10 * time.Second)
					fmt.Printf("WARN: running, wait action[%s(%s) %s=>%s]",gpid.String(), migrator.BalanceCopySec, origin.node.String(), to.node.String())
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
	return client.Meta.Balance(gpid,migrator.BalanceCopySec, origin.node, to.node)
}

func (origin *MigratorNode) contain(replica *base.Gpid)  bool {
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
