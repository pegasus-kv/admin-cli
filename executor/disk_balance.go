/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package executor

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/radmin"
	"github.com/XiaoMi/pegasus-go-client/session"
	adminClient "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
)

func DiskMigrate(client *Client, replicaServer string, pidStr string, from string, to string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pid, err := util.Str2Gpid(pidStr)
	if err != nil {
		return err
	}

	node, err := client.Nodes.GetNode(replicaServer, session.NodeTypeReplica)
	if err != nil {
		return err
	}
	replica := node.Replica()

	resp, err := replica.DiskMigrate(ctx, &radmin.ReplicaDiskMigrateRequest{
		Pid:        pid,
		OriginDisk: from,
		TargetDisk: to,
	})

	if err != nil {
		if resp != nil && resp.Hint != nil {
			return fmt.Errorf("Internal server error [%s:%s]", err, *resp.Hint)
		}
		return err
	}

	return nil
}

func DiskBalance(client *Client, replicaServer string, minSize int64, auto bool) error {
	err := changeDiskCleanerInterval(client, replicaServer, 1)
	if err != nil {
		return err
	}
	defer func() {
		err = changeDiskCleanerInterval(client, replicaServer, 86400)
		if err != nil {
			fmt.Println("revert disk cleaner failed")
		}
	}()

	for {
		action, err := getNextMigrateAction(client, replicaServer, minSize)
		if err != nil {
			return err
		}
		if action.replica.Status != "secondary" {
			err := forceAssignReplicaToSecondary(client, replicaServer, action.replica.Gpid)
			if err != nil {
				return err
			}
			time.Sleep(time.Second * 10)
			continue
		}
		err = DiskMigrate(client, replicaServer, action.replica.Gpid, action.from, action.to)
		if err == nil {
			fmt.Printf("migrate(%s) has started, wait complete...\n", action.toString())
			for {
				// TODO(jiashuo1): using DiskMigrate RPC to query status, consider support queryDiskMigrateStatus RPC
				err = DiskMigrate(client, replicaServer, action.replica.Gpid, action.from, action.to)
				if err == nil {
					time.Sleep(time.Second * 10)
					continue
				}

				if strings.Contains(err.Error(), "ERR_BUSY") {
					fmt.Printf("migrate(%s) is running, msg=%s, wait complete...\n", action.toString(), err.Error())
					time.Sleep(time.Second * 10)
					continue
				}
				fmt.Printf("migrate(%s) is completed，result=%s, wait disk cleaner remove garbage...\n\n", action.toString(), err.Error())
				break
			}
			time.Sleep(time.Second * 90)
			continue
		}
		if auto {
			time.Sleep(time.Second * 90)
			continue
		}
		break
	}
	return nil
}

type DiskStats struct {
	DiskCapacity    DiskCapacityStruct
	ReplicaCapacity []ReplicaCapacityStruct
}

type MigrateDisk struct {
	AverageUsage int64
	currentNode  string
	HighDisk     DiskStats
	LowDisk      DiskStats
}

func (m *MigrateDisk) toString() string {
	return fmt.Sprintf("Node=%s, HighDisk=%s[%dMB(%d%%)]=>LowDisk=%s[%dMB(%d%%)]", m.currentNode,
		m.HighDisk.DiskCapacity.Disk, m.HighDisk.DiskCapacity.Usage, m.HighDisk.DiskCapacity.Ratio,
		m.LowDisk.DiskCapacity.Disk, m.LowDisk.DiskCapacity.Usage, m.LowDisk.DiskCapacity.Ratio)
}

type MigrateAction struct {
	node    string
	replica ReplicaCapacityStruct
	from    string
	to      string
}

func (m *MigrateAction) toString() string {
	return fmt.Sprintf("node=%s, replica=%s, %s=>%s", m.node, m.replica.Gpid, m.from, m.to)
}

func changeDiskCleanerInterval(client *Client, replicaServer string, cleanInterval int64) error {
	fmt.Printf("set gc_disk_migration_origin_replica_interval_seconds = %ds ", cleanInterval)
	err := ConfigCommand(client, session.NodeTypeReplica, replicaServer, "gc_disk_migration_origin_replica_interval_seconds", "set", cleanInterval)
	if err != nil {
		return err
	}
	return nil
}

func getNextMigrateAction(client *Client, replicaServer string, minSize int64) (*MigrateAction, error) {
	disks, totalUsage, totalCapacity, err := queryDiskCapacityInfo(client, replicaServer)
	if err != nil {
		return nil, err
	}
	diskMigrateInfo, err := getMigrateDiskInfo(client, replicaServer, disks, totalUsage, totalCapacity)
	if err != nil {
		return nil, err
	}

	migrateAction, err := computeMigrateAction(diskMigrateInfo, minSize)
	if err != nil {
		return nil, err
	}
	return migrateAction, nil
}

func forceAssignReplicaToSecondary(client *Client, replicaServer string, gpid string) error {
	fmt.Printf("WARNING: the select replica is not secondary, will force assign it secondary\n")
	_, err := client.Meta.MetaControl(admin.MetaFunctionLevel_fl_steady)
	if err != nil {
		return err
	}
	secondaryNode, err := getReplicaSecondaryNode(client, gpid)
	if err != nil {
		return err
	}
	replica, err := util.Str2Gpid(gpid)
	if err != nil {
		return err
	}
	err = client.Meta.Balance(replica, adminClient.BalanceMovePri,
		util.NewNodeFromTCPAddr(replicaServer, session.NodeTypeReplica), secondaryNode)
	if err != nil {
		return err
	}
	return nil
}

func getReplicaSecondaryNode(client *Client, gpid string) (*util.PegasusNode, error) {
	replica, err := util.Str2Gpid(gpid)
	if err != nil {
		return nil, err
	}
	tables, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
	if err != nil {
		return nil, fmt.Errorf("can't get the table name of replica %s when migrate the replica", gpid)
	}
	var tableName string
	for _, tb := range tables {
		if tb.AppID == replica.Appid {
			tableName = tb.AppName
			break
		}
	}
	if tableName == "" {
		return nil, fmt.Errorf("can't find the table for %s when migrate the replica", gpid)
	}

	resp, err := client.Meta.QueryConfig(tableName)
	if err != nil {
		return nil, fmt.Errorf("can't get the table %s configuration when migrate the replica(%s): %s", tableName, gpid, err)
	}

	var secondaryNode *util.PegasusNode
	for _, partition := range resp.Partitions {
		if partition.Pid.String() == replica.String() {
			secondaryNode = util.NewNodeFromTCPAddr(partition.Secondaries[0].GetAddress(), session.NodeTypeReplica)
		}
	}

	if secondaryNode == nil {
		return nil, fmt.Errorf("can't get the replica %s secondary node", gpid)
	}
	return secondaryNode, nil
}

func queryDiskCapacityInfo(client *Client, replicaServer string) ([]DiskCapacityStruct, int64, int64, error) {
	diskCapacityOnNode, err := queryDiskInfo(client, CapacitySize, replicaServer, "", "", false)
	if err != nil {
		return nil, 0, 0, err
	}
	util.SortStructsByField(diskCapacityOnNode, "Usage")
	var disks []DiskCapacityStruct
	var totalUsage int64
	var totalCapacity int64
	for _, disk := range diskCapacityOnNode {
		if s, ok := disk.(DiskCapacityStruct); ok {
			disks = append(disks, s)
			totalUsage += s.Usage
			totalCapacity += s.Capacity
		} else {
			return nil, 0, 0, fmt.Errorf("can't covert to DiskCapacityStruct")
		}
	}

	if disks == nil {
		return nil, 0, 0, fmt.Errorf("the node(%s) has no ssd", replicaServer)
	}
	if len(disks) == 1 {
		return nil, 0, 0, fmt.Errorf("the node(%s) only has one disk, can't balance", replicaServer)
	}

	return disks, totalUsage, totalCapacity, nil
}

func getMigrateDiskInfo(client *Client, replicaServer string, disks []DiskCapacityStruct,
	totalUsage int64, totalCapacity int64) (*MigrateDisk, error) {
	highUsageDisk := disks[len(disks)-1]
	highDiskInfo, err := queryDiskInfo(client, CapacitySize, replicaServer, "", highUsageDisk.Disk, false)
	if err != nil {
		return nil, err
	}
	lowUsageDisk := disks[0]
	lowDiskInfo, err := queryDiskInfo(client, CapacitySize, replicaServer, "", lowUsageDisk.Disk, false)
	if err != nil {
		return nil, err
	}

	if highUsageDisk.Ratio < 10 {
		return nil, fmt.Errorf("no need balance since the high disk still enough capacity(balance threshold=10%%): "+
			"high(%s): %dMB(%d%%); low(%s): %dMB(%d%%)", highUsageDisk.Disk, highUsageDisk.Usage,
			highUsageDisk.Ratio, lowUsageDisk.Disk, lowUsageDisk.Usage, lowUsageDisk.Ratio)
	}

	averageUsage := totalUsage / int64(len(disks))
	averageRatio := totalUsage * 100 / totalCapacity
	if highUsageDisk.Ratio-lowUsageDisk.Ratio < 5 {
		return nil, fmt.Errorf("no need balance since the disk is balanced:"+
			" high(%s): %dMB(%d%%); low(%s): %dMB(%d%%); average: %dMB(%d%%)",
			highUsageDisk.Disk, highUsageDisk.Usage, highUsageDisk.Ratio, lowUsageDisk.Disk,
			lowUsageDisk.Usage, lowUsageDisk.Ratio, averageUsage, averageRatio)
	}

	replicaCapacityOnHighDisk, err := convertReplicaCapacityStruct(highUsageDisk.Disk, highDiskInfo)
	if err != nil {
		return nil, err
	}
	replicaCapacityOnLowDisk, err := convertReplicaCapacityStruct(lowUsageDisk.Disk, lowDiskInfo)
	if err != nil {
		return nil, err
	}
	return &MigrateDisk{
		AverageUsage: averageUsage,
		currentNode:  replicaServer,
		HighDisk: DiskStats{
			DiskCapacity:    highUsageDisk,
			ReplicaCapacity: replicaCapacityOnHighDisk,
		},
		LowDisk: DiskStats{
			DiskCapacity:    lowUsageDisk,
			ReplicaCapacity: replicaCapacityOnLowDisk,
		},
	}, nil
}

func computeMigrateAction(migrate *MigrateDisk, minSize int64) (*MigrateAction, error) {
	lowDiskCanReceiveMax := migrate.AverageUsage - migrate.LowDisk.DiskCapacity.Usage
	highDiskCanSendMax := migrate.HighDisk.DiskCapacity.Usage - migrate.AverageUsage
	sizeNeedMove := int64(math.Min(float64(lowDiskCanReceiveMax), float64(highDiskCanSendMax)))

	var selectReplica *ReplicaCapacityStruct
	for i := len(migrate.HighDisk.ReplicaCapacity) - 1; i >= 0; i-- {
		if migrate.HighDisk.ReplicaCapacity[i].Size > sizeNeedMove {
			continue
		} else {
			selectReplica = &migrate.HighDisk.ReplicaCapacity[i]
			break
		}
	}

	if selectReplica == nil {
		return nil, fmt.Errorf("can't balance(%s): sizeNeedMove=%dMB,but the min replica(%s) size is %dMB on high disk",
			migrate.toString(),
			sizeNeedMove,
			migrate.HighDisk.ReplicaCapacity[0].Gpid,
			migrate.HighDisk.ReplicaCapacity[0].Size)
	}

	// if select replica size is too small, it will need migrate many replica and result in `replica count not balance` among disk
	if selectReplica.Size < minSize {
		return nil, fmt.Errorf("not suggest balance(%s): the qualified(must<=sizeNeedMove(%dMB)) replica size(%s=%dMB) is too small(must>=minSize(%dMB))",
			migrate.toString(), sizeNeedMove, selectReplica.Gpid, selectReplica.Size, minSize)
	}

	fmt.Printf("ACTION:disk migrate(sizeNeedMove=%dMB): %s, gpid(%s)=%s(%dMB)\n",
		sizeNeedMove, migrate.toString(), selectReplica.Status, selectReplica.Gpid, selectReplica.Size)

	return &MigrateAction{
		node:    migrate.currentNode,
		replica: *selectReplica,
		from:    migrate.HighDisk.DiskCapacity.Disk,
		to:      migrate.LowDisk.DiskCapacity.Disk,
	}, nil
}

func convertReplicaCapacityStruct(disk string, replicaCapacityInfos []interface{}) ([]ReplicaCapacityStruct, error) {
	util.SortStructsByField(replicaCapacityInfos, "Size")
	var replicas []ReplicaCapacityStruct
	for _, replica := range replicaCapacityInfos {
		if r, ok := replica.(ReplicaCapacityStruct); ok {
			replicas = append(replicas, r)
		} else {
			return nil, fmt.Errorf("can't covert to ReplicaCapacityStruct")
		}
	}
	if replicas == nil {
		return nil, fmt.Errorf("the disk(%s) has no replica", disk)
	}
	return replicas, nil
}
