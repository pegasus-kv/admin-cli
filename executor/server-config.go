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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/go-resty/resty/v2"
	"github.com/pegasus-kv/admin-cli/executor/util"
)

type httpRequest func(addr string, cmd command) (string, error)
type printResponse func(nodeType session.NodeType, resp map[*util.PegasusNode]*cmdResult)

type action struct {
	request httpRequest
	print   printResponse
}

var actionsMap = map[string]action{
	"list": {listConfig, printConfigList},
	"get":  {getConfig, printConfigValue},
	"set":  {updateConfig, printConfigUpdate},
}

var sectionsMap = map[session.NodeType]string{
	session.NodeTypeMeta:    "meta_server,security",
	session.NodeTypeReplica: "pegasus.server,security,replication,block_service,pegasus.collector",
	// TODO(jiashuo1) support collector
}

type command struct {
	name  string
	value int64
}

type response struct {
	Name    string
	Section string
	Tags    string
	Value   string
}

//TODO(jiashuo1) not support update collector config
func ConfigCommand(client *Client, nodeType session.NodeType, nodeAddr string, name string, actionType string, value int64) error {
	var nodes []*util.PegasusNode
	if len(nodeAddr) == 0 {
		// send http-commands to all nodeType nodes
		nodes = client.Nodes.GetAllNodes(nodeType)
	} else {
		n, err := client.Nodes.GetNode(nodeAddr, nodeType)
		if err != nil {
			return err
		}
		nodes = append(nodes, n)
	}

	if ac, ok := actionsMap[actionType]; ok {
		cmd := command{
			name:  name,
			value: value,
		}
		results := batchCallHTTP(nodes, ac.request, cmd)
		ac.print(nodeType, results)
	} else {
		return fmt.Errorf("invalid request type: %s", actionType)
	}

	return nil
}

func batchCallHTTP(nodes []*util.PegasusNode, request httpRequest, cmd command) map[*util.PegasusNode]*cmdResult {
	results := make(map[*util.PegasusNode]*cmdResult)

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, n := range nodes {
		go func(node *util.PegasusNode) {
			_, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			result, err := request(node.TCPAddr(), cmd)
			mu.Lock()
			if err != nil {
				results[node] = &cmdResult{err: err}
			} else {
				results[node] = &cmdResult{resp: result}
			}
			mu.Unlock()
			wg.Done()
		}(n)
	}
	wg.Wait()

	return results
}

func callHTTP(url string) (string, error) {
	resp, err := resty.New().R().Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to call \"%s\": %s", url, err)
	}
	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("failed to call \"%s\": code=%d", url, resp.StatusCode())
	}
	return string(resp.Body()), nil
}

func listConfig(addr string, cmd command) (string, error) {
	url := fmt.Sprintf("http://%s/configs", addr)
	return callHTTP(url)
}

func printConfigList(nodeType session.NodeType, results map[*util.PegasusNode]*cmdResult) {
	fmt.Printf("CMD: list\n")
	for n, cmdRes := range results {
		if cmdRes.err != nil {
			fmt.Printf("[%s] %s\n", n.CombinedAddr(), cmdRes.err)
			continue
		}

		var respMap map[string]response
		err := json.Unmarshal([]byte(cmdRes.resp), &respMap)
		if err != nil {
			fmt.Printf("[%s] %s\n", n.CombinedAddr(), err)
			continue
		}

		fmt.Printf("[%s]\n", n.CombinedAddr())
		for _, value := range respMap {
			if value.Tags == "flag_tag::FT_MUTABLE" && strings.Contains(sectionsMap[nodeType], value.Section) {
				fmt.Printf("\t[%s] %s=%s\n", value.Section, value.Name, value.Value)
			}
		}
		fmt.Println()
	}
}

func getConfig(addr string, cmd command) (string, error) {
	url := fmt.Sprintf("http://%s/config?name=%s", addr, cmd.name)
	return callHTTP(url)
}

func printConfigValue(nodeType session.NodeType, results map[*util.PegasusNode]*cmdResult) {
	fmt.Printf("CMD: get \n")
	for n, cmdRes := range results {
		if cmdRes.err != nil {
			fmt.Printf("[%s] %s\n", n.CombinedAddr(), cmdRes.err)
			continue
		}

		var resp response
		err := json.Unmarshal([]byte(cmdRes.resp), &resp)
		if err != nil {
			fmt.Printf("[%s] %s\n", n.CombinedAddr(), cmdRes.resp)
			continue
		}

		if !strings.Contains(sectionsMap[nodeType], resp.Section) {
			fmt.Printf("[%s] %s is not config on %s\n", n.CombinedAddr(), resp.Name, nodeType)
			continue
		}

		fmt.Printf("[%s] %s=%s\n", n.CombinedAddr(), resp.Name, resp.Value)
	}
}

func updateConfig(addr string, cmd command) (string, error) {
	url := fmt.Sprintf("http://%s/updateConfig?%s=%d", addr, cmd.name, cmd.value)
	return callHTTP(url)
}

func printConfigUpdate(nodeType session.NodeType, results map[*util.PegasusNode]*cmdResult) {
	fmt.Printf("CMD: set \n")
	for n, cmdRes := range results {
		if cmdRes.err != nil {
			fmt.Printf("[%s] %s\n", n.CombinedAddr(), cmdRes.err)
			continue
		}

		var resMap map[string]string
		err := json.Unmarshal([]byte(cmdRes.resp), &resMap)
		if err != nil {
			fmt.Printf("[%s] %s\n", n.CombinedAddr(), err)
			continue
		}

		fmt.Printf("[%s] %s\n", n.CombinedAddr(), resMap["update_status"])
	}
}
