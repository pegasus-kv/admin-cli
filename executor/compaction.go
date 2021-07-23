package executor

import (
	"encoding/json"
	"fmt"
)

const userSpecifiedCompaction = "user_specified_compaction"

func SetCompaction(client *Client, tableName string,
	operationType string, updateTTLType string, timeValue uint,
	hashkeyPattern string, hashkeyMatch string,
	sortkeyPattern string, sortkeyMatch string,
	startTTL int64, stopTTL int64) error {
	json, err := generateCompactionEnv(client, tableName,
		operationType, updateTTLType, timeValue,
		hashkeyPattern, hashkeyMatch,
		sortkeyPattern, sortkeyMatch,
		startTTL, stopTTL)
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
	Value           uint   `json:"value"`
}
type compactionOperations struct {
	Ops []compactionOperation `json:"ops"`
}
type keyRuleParams struct {
	Pattern   string `json:"pattern"`
	MatchType string `json:"match_type"`
}
type timeRangeRuleParams struct {
	StartTTL uint32 `json:"start_ttl"`
	StopTTL  uint32 `json:"stop_ttl"`
}

func generateCompactionEnv(client *Client, tableName string,
	operationType string, updateTTLType string, timeValue uint,
	hashkeyPattern string, hashkeyMatch string,
	sortkeyPattern string, sortkeyMatch string,
	startTTL int64, stopTTL int64) (string, error) {
	var err error
	var operation = &compactionOperation{}
	switch operationType {
	case "delete":
		operation.OpType = "COT_DELETE"
	case "update-ttl":
		if operation, err = generateUpdateTTLOperation(updateTTLType, timeValue); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("invalid operation type {%s}", operationType)
	}

	if operation.Rules, err = generateRules(hashkeyPattern, hashkeyMatch,
		sortkeyPattern, sortkeyMatch, startTTL, stopTTL); err != nil {
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

var updateTTLTypeMapping = map[string]string{
	"from_now":     "UTOT_FROM_NOW",
	"from_current": "UTOT_FROM_CURRENT",
	"timestamp":    "UTOT_TIMESTAMP",
}

func generateUpdateTTLOperation(updateTTLType string, timeValue uint) (*compactionOperation, error) {
	var params updateTTLParams
	params.Value = timeValue
	ok := false
	if params.UpdateTTLOpType, ok = updateTTLTypeMapping[updateTTLType]; !ok {
		return nil, fmt.Errorf("not support the type: %s", updateTTLType)
	}

	paramsBytes, _ := json.Marshal(params)
	return &compactionOperation{
		OpType: "COT_UPDATE_TTL",
		Params: string(paramsBytes),
	}, nil
}

func generateRules(hashkeyPattern string, hashkeyMatch string,
	sortkeyPattern string, sortkeyMatch string,
	startTTL int64, stopTTL int64) ([]compactionRule, error) {
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

	if startTTL >= 0 && stopTTL >= 0 {
		res = append(res, generateTTLRangeRule(startTTL, stopTTL))
	}
	return res, nil
}

var ruleTypeMapping = map[string]string{
	"anywhere": "SMT_MATCH_ANYWHERE",
	"prefix":   "SMT_MATCH_PREFIX",
	"postfix":  "SMT_MATCH_POSTFIX",
}

func generateKeyRule(ruleType string, pattern string, match string) (*compactionRule, error) {
	var params keyRuleParams
	params.Pattern = pattern
	ok := false
	if params.MatchType, ok = ruleTypeMapping[ruleType]; !ok {
		return nil, fmt.Errorf("invalid match type {%s}", match)
	}

	paramsBytes, _ := json.Marshal(params)
	return &compactionRule{
		RuleType: ruleType,
		Params:   string(paramsBytes),
	}, nil
}

func generateTTLRangeRule(startTTL int64, stopTTL int64) compactionRule {
	var params timeRangeRuleParams
	params.StartTTL = uint32(startTTL)
	params.StopTTL = uint32(stopTTL)
	paramsBytes, _ := json.Marshal(params)

	return compactionRule{
		RuleType: "FRT_TTL_RANGE",
		Params:   string(paramsBytes),
	}
}
