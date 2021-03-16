package cmd

import (
	"fmt"
	"strconv"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/desertbit/grumble"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/shell"
)

func init() {
	rootCmd := &grumble.Command{
		Name: "config-command",
		Help: "send http get/post to query/update config",
	}

	rootCmd.AddCommand(&grumble.Command{
		Name:  "meta",
		Help:  "send config command to meta server",
		Flags: commandFlagFunc,
		Run: func(c *grumble.Context) error {
			return executeCommand(c, session.NodeTypeMeta)
		},
		AllowArgs: true,
	})

	rootCmd.AddCommand(&grumble.Command{
		Name:  "replica",
		Help:  "send config command to replica server",
		Flags: commandFlagFunc,
		Run: func(c *grumble.Context) error {
			return executeCommand(c, session.NodeTypeReplica)
		},
		AllowArgs: true,
	})

	shell.AddCommand(rootCmd)
}

func commandFlagFunc(f *grumble.Flags) {
	/*define the flags*/
	f.String("n", "node", "", "specify server node address, such as 127.0.0.1:34801, empty mean all node")
}

func executeCommand(c *grumble.Context, ntype session.NodeType) error {
	var name string
	var action string
	var value int64
	if len(c.Args) == 0 {
		return errMsg()
	}
	if len(c.Args) == 1 && c.Args[0] == "list" {
		action = c.Args[0]
		return executor.ConfigCommand(pegasusClient, ntype, c.Flags.String("node"), name, action, value)
	}
	if len(c.Args) == 2 && c.Args[1] == "get" {
		name = c.Args[0]
		action = c.Args[1]
		return executor.ConfigCommand(pegasusClient, ntype, c.Flags.String("node"), name, action, value)
	}
	if len(c.Args) == 3 && c.Args[1] == "set" {
		name = c.Args[0]
		action = c.Args[1]
		valueInt, err := strconv.ParseInt(c.Args[2], 10, 64)
		if err != nil {
			return err
		}
		value = valueInt
		return executor.ConfigCommand(pegasusClient, ntype, c.Flags.String("node"), name, action, value)
	}

	return errMsg()
}

func errMsg() error {
	return fmt.Errorf("invalid command: \n\tconfig-commad meta/replica list: query all config\n\tconfig-commad" +
		" meta/replica {configName} `get` or `set {value}`: get or set config value")
}
