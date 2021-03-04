package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/go-resty/resty/v2"
	"github.com/pegasus-kv/admin-cli/executor/util"
)

var actionsMap = map[string]httpRequest{
	"list": listConfig,
	"get":  getConfig,
	"set":  updateConfig,
}

type command struct {
	name  string
	value int64
}
type httpRequest func(addr string, cmd command) (string, error)

func HTTPCommand(client *Client, nodeType session.NodeType, nodeAddr string, name string, actionType string, value int64) error {
	var nodes []*util.PegasusNode
	if len(nodeAddr) == 0 {
		// send remote-commands to all nodeType nodes
		nodes = client.Nodes.GetAllNodes(nodeType)
	} else {
		n, err := client.Nodes.GetNode(nodeAddr, nodeType)
		if err != nil {
			return err
		}
		nodes = append(nodes, n)
	}

	if request, ok := actionsMap[actionType]; ok {
		cmd := command{
			name:  name,
			value: value,
		}
		results := batchCallHTTP(nodes, request, cmd)
		printResults(actionType, cmd, results)
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

func listConfig(addr string, cmd command) (string, error) {
	url := fmt.Sprintf("http://%s/configs", addr)
	return callHTTP(url)
}

func getConfig(addr string, cmd command) (string, error) {
	url := fmt.Sprintf("http://%s/config?name=%s", addr, cmd.name)
	return callHTTP(url)
}

func updateConfig(addr string, cmd command) (string, error) {
	url := fmt.Sprintf("http://%s/updateConfig?%s=%d", addr, cmd.name, cmd.value)
	return callHTTP(url)
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

// TODO(jiashuo1) format result need be refactored
func printResults(action string, cmd command, results map[*util.PegasusNode]*cmdResult) {
	if action == "set" {
		fmt.Printf("CMD: %s %s %d\n", action, cmd.name, cmd.value)
	} else {
		fmt.Printf("CMD: %s %s\n", action, cmd.name)
	}

	for n, cmdRes := range results {
		if cmdRes.err != nil {
			fmt.Printf("[%s] %s", n.CombinedAddr(), cmdRes.err)
			return
		}

		var resMap map[string]string
		err := json.Unmarshal([]byte(cmdRes.resp), &resMap)
		if err != nil {
			fmt.Printf("[%s] %s\n", n.CombinedAddr(), cmdRes.resp)
			return
		}

		if action == "list" {
			fmt.Printf("[%s]\n", n.CombinedAddr())
			for name, value := range resMap {
				var respValue map[string]interface{}
				err := json.Unmarshal([]byte(value), &respValue)
				if err != nil {
					fmt.Printf("[%s] unmarshal failed: %s\n", n.CombinedAddr(), err)
				}
				if respValue["tags"] == "flag_tag::FT_MUTABLE" {
					fmt.Printf("\t[%s] %s=%s\n", respValue["section"], name, respValue["value"])
				}
			}
			fmt.Println()
			continue
		}

		if action == "get" {
			fmt.Printf("[%s] %s=%s\n", n.CombinedAddr(), resMap["name"], resMap["value"])
			continue
		}

		if action == "set" {
			fmt.Printf("[%s] %s\n", n.CombinedAddr(), resMap["update_status"])
			continue
		}
	}
}
