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

package util

import (
	"fmt"
	"strings"

	"github.com/pegasus-kv/collector/aggregate"
)

func GetPartitionStat(perfSession *aggregate.PerfSession, counter string, gpid string) int64 {
	counters, err := perfSession.GetPerfCounters(fmt.Sprintf("%s@%s", counter, gpid))
	if err != nil {
		panic(err)
	}

	if len(counters) != 1 {
		panic(fmt.Sprintf("The perf filter results count(%d) > 1", len(counters)))
	}

	return int64(counters[0].Value)
}

func GetNodeStat(perfClient *aggregate.PerfClient, partitionCounterNames string, nodeCounterNames string) map[string]*aggregate.NodeStat {
	var nodeStats = make(map[string]*aggregate.NodeStat)

	if len(partitionCounterNames) != 0 {
		partitionStats := perfClient.GetPartitionStats()
		for _, partition := range partitionStats {
			fileNodesStatsMap(partition.Addr, nodeStats, partition.Stats, partitionCounterNames)
		}
	}

	if len(nodeCounterNames) != 0 {
		nodesStats := perfClient.GetNodeStats("replica")
		for _, node := range nodesStats {
			fileNodesStatsMap(node.Addr, nodeStats, node.Stats, nodeCounterNames)
		}
	}
	return nodeStats
}

func fileNodesStatsMap(addr string, nodeStats map[string]*aggregate.NodeStat, data map[string]float64, filters string) {
	for name, value := range data {
		name = shortName(name)
		if !strings.Contains(filters, name) {
			continue
		}
		if nodeStats[addr] == nil {
			nodeStats[addr] = &aggregate.NodeStat{
				Addr:  addr,
				Stats: make(map[string]float64),
			}
		}
		nodeStats[addr].Stats[name] = value
	}
}

func shortName(name string) string {
	res := strings.Split(name, "*")
	if len(res) == 0 {
		return name
	}
	return res[len(res)-1]
}
