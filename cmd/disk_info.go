package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "disk-capacity",
		Help: "query disk capacity info ",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.Bool("r", "resolve", false, "resolve input or output address")
			f.Bool("j", "json", false, "Use JSON as the format of the output results. By default tabular format is used.")
			f.String("n", "node", "", "Specify node address(ip:port)")
			f.String("d", "disk", "", "Specify disk tag")
			f.String("a", "app", "", "Specify app name")
			f.String("o", "out", "", "save out put into file")
		},
		Run: func(c *grumble.Context) error {
			return executor.QueryDiskInfo(
				pegasusClient,
				executor.CapacitySize,
				c.Flags.String("node"),
				c.Flags.String("app"),
				c.Flags.String("disk"),
				c.Flags.String("out"),
				c.Flags.Bool("json"),
				c.Flags.Bool("resolve"))
		},
	})

	shell.AddCommand(&grumble.Command{
		Name: "disk-replica",
		Help: "query disk replica count info",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.Bool("r", "resolve", false, "resolve input or output address")
			f.Bool("j", "json", false, "Use JSON as the format of the output results. By default tabular format is used.")
			f.String("n", "node", "", "Specify node address(ip:port)")
			f.String("a", "app", "", "Specify app name")
			f.String("o", "out", "", "save out put into file")
		},
		Run: func(c *grumble.Context) error {
			return executor.QueryDiskInfo(
				pegasusClient,
				executor.ReplicaCount,
				c.Flags.String("node"),
				c.Flags.String("app"),
				"",
				c.Flags.String("out"),
				c.Flags.Bool("json"),
				c.Flags.Bool("resolve"))
		},
	})
}