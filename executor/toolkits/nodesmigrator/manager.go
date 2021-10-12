package nodesmigrator

import (
	"fmt"
	"math"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/pegasus-kv/admin-cli/executor"
)

func MigrateAllReplicaToNodes(client *executor.Client, from []string, to []string, tables []string, concurrent int) error {
	nodesMigrator, err := createNewMigrator(client, from, to)
	if err != nil {
		return err
	}

	var tableList []string
	if len(tables) != 0 && tables[0] != "" {
		tableList = tables
	} else {
		tbs, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
		for _, tb := range tbs {
			tableList = append(tableList, tb.AppName)
		}
		if err != nil {
			return fmt.Errorf("list app failed: %s", err.Error())
		}
	}

	origin := nodesMigrator.selectOneOriginNode()
	firstOrigin := origin
	totalRemainingReplica := math.MaxInt16
	round := -1
	for {
		if totalRemainingReplica <= 0 {
			fmt.Printf("INFO: completed for all the targets has migrate\n")
			return executor.ListNodes(client)
		}

		if origin.String() == firstOrigin.String() {
			round++
		}
		fmt.Printf("\n\n********[%d:%s]start migrate replicas, remainingReplica=%d******\n", round, origin.String(), totalRemainingReplica)
		origin.downgradeAllReplicaToSecondary(client)

		totalRemainingReplica = 0
		for _, tb := range tableList {
			remainingCount := nodesMigrator.run(client, tb, round, origin, concurrent)
			totalRemainingReplica = totalRemainingReplica + remainingCount
		}
		origin = nodesMigrator.selectOneOriginNode()
		time.Sleep(10 * time.Second)
	}
}
