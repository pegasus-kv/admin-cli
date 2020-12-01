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

package tabular

import (
	"fmt"
	"io"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/pegasus-kv/collector/aggregate"
	"gopkg.in/yaml.v2"
)

var tableStatsTemplate string = `--- 
Memory: 
  rdb_index_and_filter_blocks_mem_usage: 
    unit: size
  rdb_memtable_mem_usage: 
    unit: size
Peformance: 
  recent_abnormal_count: ~
  recent_expire_count: ~
  recent_filter_count: ~
Read: 
  get_qps: ~
  multi_get_qps: ~
  read_bytes: 
    unit: size
  scan_qps: ~
Storage: 
  rdb_estimate_num_keys: ~
  sst_storage_mb: ~
Write: 
  check_and_mutate_qps: ~
  check_and_set_qps: ~
  incr_qps: ~
  multi_put_qps: ~
  put_qps: ~
  write_bytes: 
    unit: size
`

// The default statFormatter if no unit is specified
func defaultStatFormatter(v float64) string {
	return humanize.SIWithDigits(v, 2, "")
}

// Used for counter with `"unit" : "size"`.
func sizeStatFormatter(v float64) string {
	return humanize.Bytes(uint64(v))
}

type statFormatter func(float64) string

// PrintTableStatsTabular prints table stats in a number of sections,
// according to the predefined template.
func PrintTableStatsTabular(writer io.Writer, tables map[int32]*aggregate.TableStats) {
	var sections map[string]interface{}
	_ = yaml.Unmarshal([]byte(tableStatsTemplate), &sections)

	for sect, columns := range sections {
		// print section
		table := tablewriter.NewWriter(writer)
		table.SetBorders(tablewriter.Border{Left: false, Right: false, Top: true, Bottom: false})
		table.SetRowSeparator("=")
		table.SetHeader([]string{sect})
		table.Render()

		header := []string{"AppID", "Name", "Partitions"}
		var formatters []statFormatter
		for columnName, attrs := range columns.(map[interface{}]interface{}) {
			header = append(header, columnName.(string))

			if attrs == nil {
				formatters = append(formatters, defaultStatFormatter)
			} else {
				attrsMap := attrs.(map[interface{}]interface{})
				unit := attrsMap["unit"]
				if unit == "size" {
					formatters = append(formatters, sizeStatFormatter)
				} else {
					panic(fmt.Sprintf("invalid unit %s in template", unit))
				}
			}
		}

		tabWriter := tablewriter.NewWriter(writer)
		tabWriter.SetBorder(false)
		tabWriter.SetHeader(header)
		tabWriter.SetAlignment(tablewriter.ALIGN_CENTER)
		for _, tb := range tables {
			// each table displays as a row
			var row []string
			row = append(row, fmt.Sprintf("%d", tb.AppID), tb.TableName, fmt.Sprintf("%d", len(tb.Partitions)))
			for i, key := range header[3:] {
				row = append(row, formatters[i](tb.Stats[key]))
			}
			tabWriter.Append(row)
		}
		tabWriter.Render()
		fmt.Fprintln(writer)
	}
}
