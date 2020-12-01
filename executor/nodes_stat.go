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
	"admin-cli/executor/util"
	"github.com/ghodss/yaml"
	"github.com/olekukonko/tablewriter"
	"github.com/pegasus-kv/collector/aggregate"
	"strconv"
)

var nodeStatsTemplate = `---
Usage:
  disk.capacity.total(MB): DiskTotal
  disk.available.total(MB): DiskAvailable
  disk.available.total.ratio: DiskAvaRatio
  memused.res(MB): memUsed
  rdb_block_cache_memory_usage: BlockCache
  rdb_index_and_filter_blocks_mem_usage: IndexMem
Request:
  get_qps: Get
  multi_get_qps: Mget
  put_qps: Put
  multi_put_qps: Mput
  get_bytes: GetBytes
  multi_get_bytes: MGetBytes
  put_bytes: PutBytes
  multi_put_bytes: MputBytes
`

func ShowNodesStat(client *Client) error {
	var nodesStats map[string]*aggregate.NodeStat

	partitionCounters := `
  	get_qps: Get
  	multi_get_qps
    put_qps: Put
  	multi_put_qps
  	get_bytes
  	multi_get_bytes
  	put_bytes
  	multi_put_bytes
  	rdb_block_cache_memory_usage
  	rdb_index_and_filter_blocks_mem_usage
`

	nodeCounters := `
  	disk.capacity.total(MB): DiskTotal
  	disk.available.total(MB): DiskAvailable
  	disk.available.total.ratio: DiskAvaRatio
  	memused.res(MB): memUsed
`

	nodesStats = util.GetNodeStat(client.Perf, partitionCounters, nodeCounters)
	printNodesStatsTabular(client, nodesStats)
	return nil
}

func printNodesStatsTabular(client *Client, nodes map[string]*aggregate.NodeStat) {
	var sections map[string]interface{}
	_ = yaml.Unmarshal([]byte(nodeStatsTemplate), &sections)

	for sect, columns := range sections {
		// print section
		table := tablewriter.NewWriter(client.Writer)
		table.SetBorders(tablewriter.Border{Left: false, Right: false, Top: true, Bottom: false})
		table.SetRowSeparator("=")
		table.SetHeader([]string{sect})
		table.Render()

		var keys []string
		header := []string{"Node"}
		for key, attrs := range columns.(map[string]interface{}) {
			keys = append(keys, key)
			header = append(header, attrs.(string))
		}

		tabWriter := tablewriter.NewWriter(client.Writer)
		tabWriter.SetHeader(header)
		tabWriter.SetAutoFormatHeaders(false)
		for _, node := range nodes {
			// each table displays as a row
			var row []string
			row = append(row, client.Nodes.MustGetReplica(node.Addr).CombinedAddr())
			for _, key := range keys {
				row = append(row, strconv.FormatFloat(node.Stats[key], 'f', -1, 64))
			}
			tabWriter.Append(row)
		}
		tabWriter.Render()
		//_, _ = fmt.Fprintln(client.Writer)
	}
}
