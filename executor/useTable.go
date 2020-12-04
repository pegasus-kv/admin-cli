package executor

import (
	"context"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"time"
)

func UseTables(client *Client, table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, err := client.Meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return err
	}

	for _, info := range resp.Infos {
		if info.AppName == table {
			return nil
		}
	}
	return fmt.Errorf(fmt.Sprintf("Table \"%s\" is not existed!", table))
}
