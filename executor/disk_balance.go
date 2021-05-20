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
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/radmin"
	"github.com/XiaoMi/pegasus-go-client/session"
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

// TODO(jiashuo1) need generate migrate strategy(step) depends the disk-info result to run
func DiskBalance(client *Client, replicaServer string) error {
	_, err := getNextMigrateAction(client, replicaServer)
	if err != nil {
		return err
	}
	return nil
}

type DiskStats struct {
	NodeCapacity    NodeCapacityStruct
	ReplicaCapacity []ReplicaCapacityStruct
}

type MigrateDisk struct {
	AverageUsage int64
	HighDisk     DiskStats
	LowDisk      DiskStats
}

type MigrateAction struct {
	node string
	gpid string
	from string
	to   string
}

func getNextMigrateAction(client *Client, replicaServer string) (*MigrateAction, error) {

	fmt.Println("[Node Capacity]")
	diskCapacityOnNode, err := QueryDiskInfo(client, CapacitySize, replicaServer, "", "")
	if err != nil {
		return nil, err
	}
	util.SortStructsByField(diskCapacityOnNode, "Usage")
	var disks []NodeCapacityStruct
	var totalUsage int64
	for _, disk := range diskCapacityOnNode {
		if s, ok := disk.(NodeCapacityStruct); ok {
			disks = append(disks, s)
			totalUsage += s.Usage
		} else {
			return nil, fmt.Errorf("can't covert to NodeCapacityStruct")
		}
	}

	if disks == nil {
		return nil, fmt.Errorf("the node has no ssd")
	}
	if len(disks) == 1 {
		return nil, fmt.Errorf("only has one disk, can't balance")
	}
	averageUsage := totalUsage / int64(len(disks))

	highUsageDisk := disks[len(disks)-1]
	fmt.Printf("[High Disk(%s)]\n", highUsageDisk.Disk)
	highDiskInfo, err := QueryDiskInfo(client, CapacitySize, replicaServer, "", highUsageDisk.Disk)
	if err != nil {
		return nil, err
	}
	lowUsageDisk := disks[0]
	fmt.Printf("[Low Disk(%s)]\n", lowUsageDisk.Disk)
	lowDiskInfo, err := QueryDiskInfo(client, CapacitySize, replicaServer, "", lowUsageDisk.Disk)
	if err != nil {
		return nil, err
	}

	if highUsageDisk.Capacity-highUsageDisk.Usage <= averageUsage ||
		(highUsageDisk.Capacity-highUsageDisk.Usage > averageUsage && (highUsageDisk.Capacity-highUsageDisk.Usage-averageUsage)*100/averageUsage < 5) {
		return nil, fmt.Errorf("no need balance: high(%s): %dMB; low(%s): %dMB; average: %dMB(delta=%d%%)",
			highUsageDisk.Disk, highUsageDisk.Usage, lowUsageDisk.Disk, lowUsageDisk.Usage, averageUsage,
			(highUsageDisk.Usage-averageUsage)*100/averageUsage)
	}

	replicaCapacityOnHighDisk, err := convertReplicaCapacityStruct(highDiskInfo)
	if err != nil {
		return nil, err
	}
	replicaCapacityOnLowDisk, err := convertReplicaCapacityStruct(lowDiskInfo)
	if err != nil {
		return nil, err
	}
	migrateDisk := MigrateDisk{
		AverageUsage: averageUsage,
		HighDisk: DiskStats{
			NodeCapacity:    highUsageDisk,
			ReplicaCapacity: replicaCapacityOnHighDisk,
		},
		LowDisk: DiskStats{
			NodeCapacity:    lowUsageDisk,
			ReplicaCapacity: replicaCapacityOnLowDisk,
		},
	}

	lowDiskCanReceiveMax := migrateDisk.AverageUsage - migrateDisk.LowDisk.NodeCapacity.Usage
	highDiskCanSendMax := migrateDisk.HighDisk.NodeCapacity.Usage - averageUsage
	SizeToMove := math.Min(float64(lowDiskCanReceiveMax), float64(highDiskCanSendMax))

	var selectReplica ReplicaCapacityStruct
	for i := len(migrateDisk.HighDisk.ReplicaCapacity) - 1; i > 0; i++ {
		if migrateDisk.HighDisk.ReplicaCapacity[i].Size > SizeToMove {
			continue
		} else {
			selectReplica = migrateDisk.HighDisk.ReplicaCapacity[i]
			break
		}
	}

	fmt.Printf("disk migrate(sizeToMove=%fMB): node=%s, from=%s, to=%s, gpid(best)=%s(size=%f)\n",
		SizeToMove, replicaServer,
		migrateDisk.HighDisk.NodeCapacity.Disk,
		migrateDisk.LowDisk.NodeCapacity.Disk,
		selectReplica.Replica,
		selectReplica.Size)
	return &MigrateAction{
		node: replicaServer,
		gpid: selectReplica.Replica,
		from: highUsageDisk.Disk,
		to:   lowUsageDisk.Disk,
	}, nil
}

func convertReplicaCapacityStruct(replicaCapacityInfos []interface{}) ([]ReplicaCapacityStruct, error) {
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
		return nil, fmt.Errorf("the ssd has no replica")
	}
	return replicas, nil
}

/*func computeMigratePlan(disk *MigrateDisk) (*MigrateAction, error) {
	return &MigrateAction{"123", "123", "123", "123"}, nil
}*/
