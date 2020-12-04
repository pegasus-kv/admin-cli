package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

func UseTable(client *Client, table string) error {
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
	return fmt.Errorf(fmt.Sprintf("Table \"%s\" doesn't exist!", table))
}
