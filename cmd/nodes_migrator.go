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

package cmd

import (
	"github.com/desertbit/grumble"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/shell"

	"strings"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "node-migrator",
		Help: "migrate all replicas from nodes to other node",
		Flags: func(a *grumble.Flags) {
			a.String("f", "from", "", "from")
			a.String("t", "to", "", "to")
			a.Int64("p", "period", 60, "period seconds")
		},
		Run: func(c *grumble.Context) error {
			from := strings.Split(c.Flags.String("from"), ",")
			to := strings.Split(c.Flags.String("to"), ",")
			period := c.Flags.Int64("period")
			return executor.MigrateAllReplicaToNodes(pegasusClient, period, from, to)
		},
	})
}
