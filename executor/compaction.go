package executor

import (
	"encoding/json"
	"fmt"
)

var userSpecifiedCompaction = "user_specified_compaction"

func SetCompaction(client *Client, tableName string,
	operationType string, updateTTLType string, expireTimestamp uint,
	hashkeyPattern string, hashkeyMatch string,
	sortkeyPattern string, sortkeyMatch string,
	startTimestamp int64, stopTimestamp int64) error {
	json, err := generateCompactionEnv(client, tableName,
		operationType, updateTTLType, expireTimestamp,
		hashkeyPattern, hashkeyMatch,
		sortkeyPattern, sortkeyMatch,
		startTimestamp, stopTimestamp)
	if err != nil {
		return err
	}

	if err = SetAppEnv(client, tableName, userSpecifiedCompaction, json); err != nil {
		return err
	}
	return nil
}

// json helpers
type compactionRule struct {
	RuleType string `json:"type"`
	Params   string `json:"params"`
}
type compactionOperation struct {
	OpType string           `json:"type"`
	Params string           `json:"params"`
	Rules  []compactionRule `json:"rules"`
}
type updateTTLParams struct {
	UpdateTTLOpType string `json:"type"`
	Timestamp       uint   `json:"timestamp"`
}
type compactionOperations struct {
	Ops []compactionOperation `json:"ops"`
}
type keyRuleParams struct {
	Pattern   string `json:"pattern"`
	MatchType string `json:"match_type"`
}
type timeRangeRuleParams struct {
	StartTimestamp uint64 `json:"start_timestamp"`
	StopTimestamp  uint64 `json:"stop_timestamp"`
}

func generateCompactionEnv(client *Client, tableName string,
	operationType string, updateTTLType string, expireTimestamp uint,
	hashkeyPattern string, hashkeyMatch string,
	sortkeyPattern string, sortkeyMatch string,
	startTimestamp int64, stopTimestamp int64) (string, error) {
	var err error
	var operation = &compactionOperation{}
	switch operationType {
	case "delete":
		operation.OpType = "FOT_DELETE"
	case "update-ttl":
		if operation, err = generateUpdateTTLOperation(updateTTLType, expireTimestamp); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("invalid operation type {%s}", operationType)
	}

	if operation.Rules, err = generateRules(hashkeyPattern, hashkeyMatch,
		sortkeyPattern, sortkeyMatch, startTimestamp, stopTimestamp); err != nil {
		return "", err
	}
	if len(operation.Rules) == 0 {
		return "", fmt.Errorf("no rules specified")
	}

	compactionJSON, err := GetAppEnv(client, tableName, userSpecifiedCompaction)
	if err != nil {
		return "", err
	}
	var operations compactionOperations
	if compactionJSON != "" {
		_ = json.Unmarshal([]byte(compactionJSON), &operations)
	}

	operations.Ops = append(operations.Ops, *operation)
	res, _ := json.Marshal(operations)
	return string(res), nil
}

func generateUpdateTTLOperation(updateTTLType string, expireTimestamp uint) (*compactionOperation, error) {
	var params updateTTLParams
	params.Timestamp = expireTimestamp
	switch updateTTLType {
	case "from_now":
		params.UpdateTTLOpType = "UTOT_FROM_NOW"
	case "from_current":
		params.UpdateTTLOpType = "UTOT_FROM_CURRENT"
	case "timestamp":
		params.UpdateTTLOpType = "UTOT_TIMESTAMP"
	default:
		return nil, fmt.Errorf("invalid update ttl type {%s}", updateTTLType)
	}

	paramsBytes, _ := json.Marshal(params)
	var operation = &compactionOperation{
		OpType: "FOT_UPDATE_TTL",
		Params: string(paramsBytes),
	}
	return operation, nil
}

func generateRules(hashkeyPattern string, hashkeyMatch string,
	sortkeyPattern string, sortkeyMatch string,
	startTimestamp int64, stopTimestamp int64) ([]compactionRule, error) {
	var res []compactionRule
	var err error
	if hashkeyPattern != "" {
		var rule *compactionRule
		if rule, err = generateKeyRule("FRT_HASHKEY_PATTERN", hashkeyPattern, hashkeyMatch); err != nil {
			return nil, err
		}
		res = append(res, *rule)
	}

	if sortkeyPattern != "" {
		var rule *compactionRule
		if rule, err = generateKeyRule("FRT_SORTKEY_PATTERN", sortkeyPattern, sortkeyMatch); err != nil {
			return nil, err
		}
		res = append(res, *rule)
	}

	if startTimestamp >= 0 && stopTimestamp >= 0 {
		res = append(res, generateTimeRangeRule(startTimestamp, stopTimestamp))
	}
	return res, nil
}

func generateKeyRule(ruleType string, pattern string, match string) (*compactionRule, error) {
	var params keyRuleParams
	params.Pattern = pattern
	switch match {
	case "anywhere":
		params.MatchType = "SMT_MATCH_ANYWHERE"
	case "prefix":
		params.MatchType = "SMT_MATCH_PREFIX"
	case "postfix":
		params.MatchType = "SMT_MATCH_POSTFIX"
	default:
		return nil, fmt.Errorf("invalid match type {%s}", match)
	}

	paramsBytes, _ := json.Marshal(params)
	var rule = &compactionRule{
		RuleType: ruleType,
		Params:   string(paramsBytes),
	}
	return rule, nil
}

func generateTimeRangeRule(startTimestamp int64, stopTimestamp int64) compactionRule {
	var params timeRangeRuleParams
	params.StartTimestamp = uint64(startTimestamp)
	params.StopTimestamp = uint64(stopTimestamp)
	paramsBytes, _ := json.Marshal(params)

	return compactionRule{
		RuleType: "FRT_EXPIRE_TIME_RANGE",
		Params:   string(paramsBytes),
	}
}
