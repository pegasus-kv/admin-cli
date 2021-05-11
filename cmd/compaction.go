package cmd

import (
	"github.com/desertbit/grumble"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/shell"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "add-compaction-operation",
		Help: "add compaction operation and the corresponding rules",
		Flags: func(f *grumble.Flags) {
			/**
			 *	operations
			 **/
			f.String("o", "operation-type", "", "operation type, for example: delete/update-ttl")
			// update ttl operation
			f.String("u", "update-ttl", "", "update ttl operation type, for example: from_now/from_current/timestamp")
			f.Uint("e", "expire-timestamp", 0, "expire timestamp")
			/**
			 *  rules
			 **/
			// hashkey filter
			f.StringL("hashkey-pattern", "", "hash key pattern")
			f.StringL("hashkey-match", "anywhere", "hash key's match type, for example: anywhere/prefix/postfix")
			// sortkey filter
			f.StringL("sortkey-pattern", "", "sort key pattern")
			f.StringL("sortkey-match", "anywhere", "sort key's match type, for example: anywhere/prefix/postfix")
			// ttl filter
			f.Int64L("start-ttl", -1, "ttl filter, start ttl")
			f.Int64L("stop-ttl", -1, "ttl filter, stop ttl")
		},
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			return executor.SetCompaction(
				pegasusClient,
				c.UseTable,
				c.Flags.String("operation-type"),
				c.Flags.String("update-ttl"),
				c.Flags.Uint("expire-timestamp"),
				c.Flags.String("hashkey-pattern"),
				c.Flags.String("hashkey-match"),
				c.Flags.String("sortkey-pattern"),
				c.Flags.String("sortkey-match"),
				c.Flags.Int64("start-ttl"),
				c.Flags.Int64("stop-ttl"),
			)
		}),
	})
}
