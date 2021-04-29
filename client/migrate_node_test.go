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
package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMigratePrimariesOut(t *testing.T) {
	fakePegasusCluster = newFakeCluster(4)
	createFakeTable("test", 16)
	assertReplicasNotOnSameNode(t)

	for i := range fakePegasusCluster.nodes {
		replicaServer := fakePegasusCluster.nodes[i]
		err := MigratePrimariesOut(fakePegasusCluster.meta, replicaServer.n)
		assert.NoError(t, err)

		assertReplicasNotOnSameNode(t)

		assert.Empty(t, replicaServer.primaries)
		assertNoMissingReplicaInCluster(t, 16)
	}
}
