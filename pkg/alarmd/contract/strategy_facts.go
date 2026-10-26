package contract

import (
	"bytes"
	"crypto/md5" // The platform's comparison is MD5, not a security primitive.
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// StrategyFactsSchemaV1 names the one form of StrategyFactsV1 this build
// writes and reads.
const StrategyFactsSchemaV1 = "v1"

// StrategyFactsV1 is what an event says about the strategy configuration it
// was decided under, for a later reader to tell whether the strategy has
// changed in a way that orphans the alert the event opened: the platform
// closes an alert when its strategy's metrics change, and closes a no-data
// alert when its filter conditions change (bk-monitor alert manager,
// CloseStatusChecker.check_strategy_changed). The alert link copies an
// alert's extra_data from its opening event and never changes it, so the
// facts on an alert are those of the revision that raised it.
//
// Both values are derived from the strategy's first item's query_configs,
// in their order, as the platform reads them:
//
//   - MetricDigest is the hex MD5 of the JSON array of the query configs'
//     metric_id strings, in query order, encoded without HTML escaping. The
//     platform compares the metric ids as an ordered list, and two digests
//     are equal exactly when the lists are.
//   - ConditionMD5 is the platform's get_agg_condition_md5 of the query
//     config's agg_condition (AggConditionMD5) when the item has one query
//     config. With several, it is the hex MD5 of their AggConditionMD5s
//     joined by "," in query order: the platform compares the conditions
//     query by query and closes on any difference, which a digest of the
//     ordered list states the same way.
type StrategyFactsV1 struct {
	Schema       string `json:"schema"`
	MetricDigest string `json:"metric_digest"`
	ConditionMD5 string `json:"condition_md5"`
}

// DeriveStrategyFactsV1 derives the facts from an item's query_configs. A
// query config without a metric_id string, or whose agg_condition is not a
// list of objects, has no facts: the platform's own comparison cannot read
// it either.
func DeriveStrategyFactsV1(queryConfigs []json.RawMessage) (*StrategyFactsV1, error) {
	metricIDs := make([]string, 0, len(queryConfigs))
	conditions := make([]string, 0, len(queryConfigs))
	for index, raw := range queryConfigs {
		var query struct {
			MetricID     *string         `json:"metric_id"`
			AggCondition json.RawMessage `json:"agg_condition"`
		}
		if err := json.Unmarshal(raw, &query); err != nil {
			return nil, fmt.Errorf("strategy facts: query config %d: %w", index, err)
		}
		if query.MetricID == nil {
			return nil, fmt.Errorf("strategy facts: query config %d names no metric_id", index)
		}
		metricIDs = append(metricIDs, *query.MetricID)
		condition := query.AggCondition
		if condition == nil {
			// The platform reads a missing agg_condition as an empty one.
			condition = json.RawMessage("[]")
		}
		digest, err := AggConditionMD5(condition)
		if err != nil {
			return nil, fmt.Errorf("strategy facts: query config %d: %w", index, err)
		}
		conditions = append(conditions, digest)
	}
	var metrics bytes.Buffer
	encoder := json.NewEncoder(&metrics)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(metricIDs); err != nil {
		return nil, err
	}
	facts := &StrategyFactsV1{Schema: StrategyFactsSchemaV1,
		MetricDigest: md5Hex(strings.TrimSuffix(metrics.String(), "\n"))}
	if len(conditions) == 1 {
		facts.ConditionMD5 = conditions[0]
	} else {
		facts.ConditionMD5 = md5Hex(strings.Join(conditions, ","))
	}
	return facts, nil
}

// AggConditionMD5 is the platform's get_agg_condition_md5 (bk-monitor
// alarm_backends/service/alert/manager/checker/close.py): of each condition
// only key, value, method and condition are kept, the first condition's
// connector is dropped - it is always AND and does not count as a change -
// and the list is digested by bkmonitor.utils.common_utils.count_md5
// (pythonCountMD5, the same implementation the alert fingerprint uses).
func AggConditionMD5(raw json.RawMessage) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var conditions []any
	if err := decoder.Decode(&conditions); err != nil || conditions == nil {
		return "", errors.New("agg_condition is not a list")
	}
	kept := make([]any, 0, len(conditions))
	for index, condition := range conditions {
		fields, ok := condition.(map[string]any)
		if !ok {
			return "", fmt.Errorf("agg_condition %d is not an object", index)
		}
		target := make(map[string]any, 4)
		for _, key := range []string{"key", "value", "method", "condition"} {
			if value, present := fields[key]; present {
				target[key] = value
			}
		}
		if index == 0 {
			delete(target, "condition")
		}
		kept = append(kept, target)
	}
	return pythonCountMD5(kept)
}

func md5Hex(text string) string {
	sum := md5.Sum([]byte(text))
	return hex.EncodeToString(sum[:])
}
