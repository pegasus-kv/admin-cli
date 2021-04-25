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
	"context"
	"fmt"
	"reflect"

	"github.com/XiaoMi/pegasus-go-client/idl/base"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/session"
)

type Meta interface {
	Close() error

	ListApps() ([]*admin.AppInfo, error)

	QueryConfig(tableName string) (*replication.QueryCfgResponse, error)

	MetaControl(level admin.MetaFunctionLevel) (oldLevel admin.MetaFunctionLevel, err error)

	QueryClusterInfo() (map[string]string, error)

	UpdateAppEnvs(tableName string, envs map[string]string) error

	ClearAppEnvs(tableName string, clearPrefix string) error

	DelAppEnvs(tableName string, keys []string) error

	CreateApp(tableName string, envs map[string]string, partitionCount int) (int32, error)

	DropApp(tableName string, reserveSeconds int64) error

	ModifyDuplication(tableName string, dupid int, status admin.DuplicationStatus) error

	AddDuplication(tableName string, remoteCluster string, freezed bool) (*admin.DuplicationAddResponse, error)

	QueryDuplication(tableName string) (*admin.DuplicationQueryResponse, error)

	ListNodes() ([]*admin.NodeInfo, error)

	RecallApp(originTableID int, newTableName string) (*admin.AppInfo, error)
}

type rpcBasedMeta struct {
	meta *session.MetaManager
}

// NewRPCBasedMeta creates the connection to meta.
func NewRPCBasedMeta(metaAddrs []string) Meta {
	return &rpcBasedMeta{
		meta: session.NewMetaManager(metaAddrs, session.NewNodeSession),
	}
}

func wrapHintIntoError(hint string, err error) error {
	if err != nil {
		if hint != "" {
			return fmt.Errorf("%s [hint: %s]", err, hint)
		}
	}
	return err
}

func (m *rpcBasedMeta) Close() error {
	return m.meta.Close()
}

func (m *rpcBasedMeta) callMeta(methodName string, req interface{}, callback func(resp interface{})) error {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	ret := reflect.ValueOf(m.meta).MethodByName(methodName).Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(req),
	})
	err := ret[1].Interface().(error)
	if err != nil {
		return err
	}

	resp := ret[0].Interface()
	callback(resp)
	return nil
}

func (m *rpcBasedMeta) ListApps() ([]*admin.AppInfo, error) {
	var result []*admin.AppInfo
	req := &admin.ListAppsRequest{Status: admin.AppStatus_AS_AVAILABLE}
	err := m.callMeta("ListApps", req, func(resp interface{}) {
		result = resp.(*admin.ListAppsResponse).Infos
	})
	return result, err
}

func (m *rpcBasedMeta) QueryConfig(tableName string) (*replication.QueryCfgResponse, error) {
	var result *replication.QueryCfgResponse
	req := &replication.QueryCfgRequest{AppName: tableName}
	err := m.callMeta("QueryConfig", req, func(resp interface{}) {
		result = resp.(*replication.QueryCfgResponse)
	})
	if err == nil {
		if result.GetErr().Errno == base.ERR_OBJECT_NOT_FOUND.String() {
			return nil, fmt.Errorf("table(%s) doesn't exist", tableName)
		}
		if result.GetErr().Errno != base.ERR_OK.String() {
			return nil, fmt.Errorf("query config failed: %s", result.GetErr())
		}
	}
	return result, err
}

func (m *rpcBasedMeta) MetaControl(level admin.MetaFunctionLevel) (oldLevel admin.MetaFunctionLevel, err error) {
	req := &admin.MetaControlRequest{Level: level}
	err = m.callMeta("MetaControl", req, func(resp interface{}) {
		oldLevel = resp.(*admin.MetaControlResponse).OldLevel
	})
	return oldLevel, err
}

func (m *rpcBasedMeta) QueryClusterInfo() (map[string]string, error) {
	var result map[string]string
	req := &admin.ClusterInfoRequest{}
	err := m.callMeta("QueryClusterInfo", req, func(resp interface{}) {
		keys := resp.(*admin.ClusterInfoResponse).Keys
		values := resp.(*admin.ClusterInfoResponse).Values
		for i := range keys {
			result[keys[i]] = values[i]
		}
	})
	return result, err
}

func (m *rpcBasedMeta) updateAppEnvs(req *admin.UpdateAppEnvRequest) error {
	var hint string
	err := m.callMeta("UpdateAppEnv", req, func(resp interface{}) {
		hint = resp.(*admin.UpdateAppEnvResponse).HintMessage
	})
	return wrapHintIntoError(hint, err)
}

func (m *rpcBasedMeta) UpdateAppEnvs(tableName string, envs map[string]string) error {
	var keys []string
	var values []string
	for key, value := range envs {
		keys = append(keys, key)
		values = append(values, value)
	}
	req := &admin.UpdateAppEnvRequest{
		AppName: tableName,
		Op:      admin.AppEnvOperation_APP_ENV_OP_SET,
		Keys:    keys,
		Values:  values,
	}
	return m.updateAppEnvs(req)
}

func (m *rpcBasedMeta) ClearAppEnvs(tableName string, clearPrefix string) error {
	req := &admin.UpdateAppEnvRequest{
		AppName:     tableName,
		Op:          admin.AppEnvOperation_APP_ENV_OP_CLEAR,
		ClearPrefix: &clearPrefix,
	}
	return m.updateAppEnvs(req)
}

func (m *rpcBasedMeta) DelAppEnvs(tableName string, keys []string) error {
	req := &admin.UpdateAppEnvRequest{
		AppName: tableName,
		Op:      admin.AppEnvOperation_APP_ENV_OP_DEL,
		Keys:    keys,
	}
	return m.updateAppEnvs(req)
}

func (m *rpcBasedMeta) CreateApp(tableName string, envs map[string]string, partitionCount int) (int32, error) {
	var appID int32
	req := &admin.CreateAppRequest{
		AppName: tableName,
		Options: &admin.CreateAppOptions{
			PartitionCount: int32(partitionCount),
			Envs:           envs,

			// constants
			ReplicaCount: 3,
			IsStateful:   true,
			AppType:      "pegasus",
		},
	}
	err := m.callMeta("CreateApp", req, func(resp interface{}) {
		appID = resp.(*admin.CreateAppResponse).Appid
	})
	return appID, err
}

func (m *rpcBasedMeta) DropApp(tableName string, reserveSeconds int64) error {
	req := &admin.DropAppRequest{
		AppName: tableName,
		Options: &admin.DropAppOptions{
			SuccessIfNotExist: true,
			ReserveSeconds:    &reserveSeconds,
		},
	}
	err := m.callMeta("DropApp", req, func(resp interface{}) {})
	return err
}

func (m *rpcBasedMeta) ModifyDuplication(tableName string, dupid int, status admin.DuplicationStatus) error {
	req := &admin.DuplicationModifyRequest{
		AppName: tableName,
		Dupid:   int32(dupid),
		Status:  &status,
	}
	var hint string
	err := m.callMeta("ModifyDuplication", req, func(resp interface{}) {
		hintPtr := resp.(*admin.DuplicationAddResponse).Hint
		if hintPtr != nil {
			hint = *hintPtr
		}
	})
	return wrapHintIntoError(hint, err)
}

func (m *rpcBasedMeta) AddDuplication(tableName string, remoteCluster string, freezed bool) (*admin.DuplicationAddResponse, error) {
	var result *admin.DuplicationAddResponse
	req := &admin.DuplicationAddRequest{
		AppName:           tableName,
		RemoteClusterName: remoteCluster,
		Freezed:           freezed,
	}
	err := m.callMeta("AddDuplication", req, func(resp interface{}) {
		result = resp.(*admin.DuplicationAddResponse)
	})
	return result, err
}

func (m *rpcBasedMeta) QueryDuplication(tableName string) (*admin.DuplicationQueryResponse, error) {
	var result *admin.DuplicationQueryResponse
	req := &admin.DuplicationQueryRequest{
		AppName: tableName,
	}
	err := m.callMeta("AddDuplication", req, func(resp interface{}) {
		result = resp.(*admin.DuplicationQueryResponse)
	})
	return result, err
}

func (m *rpcBasedMeta) ListNodes() ([]*admin.NodeInfo, error) {
	var result []*admin.NodeInfo
	req := &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	}
	err := m.callMeta("ListNodes", req, func(resp interface{}) {
		result = resp.(*admin.ListNodesResponse).Infos
	})
	return result, err
}

func (m *rpcBasedMeta) RecallApp(originTableID int, newTableName string) (*admin.AppInfo, error) {
	var result *admin.AppInfo
	req := &admin.RecallAppRequest{
		AppID:       int32(originTableID),
		NewAppName_: newTableName,
	}
	err := m.callMeta("RecallApp", req, func(resp interface{}) {
		result = resp.(*admin.RecallAppResponse).Info
	})
	return result, err
}
