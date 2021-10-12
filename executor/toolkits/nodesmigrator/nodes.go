package nodesmigrator

import (
	"fmt"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	migrator "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
)

type MigratorNode struct {
	node     *util.PegasusNode
	replicas []*Replica
}

type Replica struct {
	gpid      *base.Gpid
	operation migrator.BalanceType
}

func (r *Replica) String() string {
	return fmt.Sprintf("%s|%s", r.gpid.String(), r.operation.String())
}

func (m *MigratorNode) contain(gpid *base.Gpid) bool {
	for _, replica := range m.replicas {
		if replica.gpid.String() == gpid.String() {
			return true
		}
	}
	return false
}

func (m *MigratorNode) concurrent(acts *MigrateActions) int {
	return acts.getConcurrent(m)
}

func (m *MigratorNode) secondaryCount() int {
	count := 0
	for _, replica := range m.replicas {
		if replica.operation == migrator.BalanceCopySec {
			count++
		}
	}
	return count
}

func (m *MigratorNode) String() string {
	return m.node.String()
}
