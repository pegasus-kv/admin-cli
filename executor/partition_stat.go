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
	"time"

	"github.com/pegasus-kv/collector/aggregate"
)

// ShowPartitionsStats is partition-stat command
func ShowPartitionsStats(client *Client, tableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.Meta.QueryConfig(ctx, tableName)
	if err != nil {
		return err
	}
	appID := resp.AppID

	// filter out the partitions belongs to this table.
	var appPartitions []*aggregate.PartitionStats
	partitions := client.Perf.GetPartitionStats()
	for _, p := range partitions {
		if p.Gpid.Appid == appID {
			appPartitions = append(appPartitions, p)
		}
	}

	return nil
}
