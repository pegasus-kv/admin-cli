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
			f.String("t", "table", "", "table")
			/**
			 *	operation
			 **/
			f.String("o", "operation-type", "delete", "operation type, for example: delete/update-ttl")
			// update ttl operation
			f.String("u", "update-ttl", "", "update ttl operation type, for example: from_now/from_current/timestamp")
			f.Uint("e", "expire-timestamp", 0, "expire timestamp")
			/**
			 *  rule
			 **/
			// hashkey filter
			f.String("h", "hashkey-pattern", "", "hash key pattern")
			f.StringL("hashkey-match", "anywhere", "hash key's match type, for example: anywhere/prefix/postfix")
			// sortkey filter
			f.String("s", "sortkey-pattern", "", "sort key pattern")
			f.StringL("sortkey-match", "anywhere", "sort key's match type, for example: anywhere/prefix/postfix")
			// expire time filter
			f.Int64L("start-timestamp", -1, "expire time filter, start timestamp")
			f.Int64L("stop-timestamp", -1, "expire time filter, stop timestamp")
		},
		Run: func(c *grumble.Context) error {
			return executor.SetCompaction(
				pegasusClient,
				c.Flags.String("table"),
				c.Flags.String("operation-type"),
				c.Flags.String("update-ttl-type"),
				c.Flags.Uint("expire-timestamp"),
				c.Flags.String("hashkey-pattern"),
				c.Flags.String("hashkey-match"),
				c.Flags.String("sortkey-pattern"),
				c.Flags.String("sortkey-match"),
				c.Flags.Int64("start-timestamp"),
				c.Flags.Int64("stop-timestamp"),
			)
		},
	})
}
