package executor

import (
	"fmt"
)

func SetCompaction(client *Client, operationType string, updateTTLType string, expireTimestamp uint,
	hashkeyPattern string, hashkeyMatch string,
	sortkeyPattern string, sortkeyMatch string,
	startTimestamp int64, stopTimestamp int64) error {
	if operationType != "delete" && operationType != "update-ttl" {
		return fmt.Errorf("operation type must be delete or update-ttl")
	}

	if err := generateOperations(); err != nil {
		return err
	}
	return nil
}

func generateOperations() error {

}
