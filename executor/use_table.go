package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
)

func UseTable(client *Client, table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, err := client.Meta.QueryConfig(ctx, table)
	if err != nil {
		return err
	}

	if resp.GetErr().Errno != base.ERR_OK.String() {
		return fmt.Errorf("Use Table(%s) failed: %s", table, resp.GetErr().String())
	}

	return nil
}
