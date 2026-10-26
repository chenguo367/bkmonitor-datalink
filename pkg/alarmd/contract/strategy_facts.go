package contract

import (
	"bytes"
	"crypto/md5" // The platform's comparison is MD5, not a security primitive.
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// StrategyFactsSchemaV2 names the one form of StrategyFactsV2 this build
// writes and reads.
const StrategyFactsSchemaV2 = "v2"

// StrategyFactsV2 is what an event says about the strategy configuration it
// was decided under, for a later reader to tell whether the strategy has
// changed in a way that orphans the alert the event opened: the platform
// closes an alert when its strategy's metrics or grouping dimensions
// change, and closes a no-data alert when its filter conditions change
// (bk-monitor alert manager, CloseStatusChecker.check_strategy_changed).
// The alert link copies an alert's extra_data from its opening event and
// never changes it, so the facts on an alert are those of the revision that
// raised it.
//
// The values are derived from the strategy's first item, as the platform
// reads it:
//
//   - MetricDigest is the hex MD5 of the JSON array of the query configs'
//     metric_id strings, in query order, encoded without HTML escaping. The
//     platform compares the metric ids as an ordered list, and two digests
//     are equal exactly when the lists are. A PromQL query published with
//     an empty metric_id (data_source_label prometheus, data_type_label
//     time_series) counts under the id the platform derives from its text
//     (PromQLMetricID).
//   - ConditionMD5 is the platform's get_agg_condition_md5 of the query
//     config's agg_condition (AggConditionMD5) when the item has one query
//     config. With several, it is the hex MD5 of their AggConditionMD5s
//     joined by "," in query order: the platform compares the conditions
//     query by query and closes on any difference, which a digest of the
//     ordered list states the same way.
//   - DimensionDigest is the GroupingDigest of the dimensions the event's
//     series is grouped by, the key set its alert fingerprint is built from:
//     the Plan's output identity fields for a threshold event, and the
//     no-data configuration's agg_dimension for a no-data event. Empty when
//     the grouping is every dimension the series carries (dynamic
//     dimensions), which no fixed set states.
//   - ConditionInjected says some query's agg_condition may hold a
//     condition the publisher added and the user did not write
//     (InjectedConditionKeys): ConditionMD5 then moves with the publisher's
//     own state as well as with the user's edits, and a reader must not
//     take a change of it for an edit.
//   - NoDataDimensionDigest is on a Plan only, beside DimensionDigest: the
//     GroupingDigest of its no-data agg_dimension, empty when the Plan
//     detects no no-data. ForEvent puts the one an event is grouped by in
//     DimensionDigest and leaves this out, so an event carries one grouping.
type StrategyFactsV2 struct {
	Schema                string `json:"schema"`
	MetricDigest          string `json:"metric_digest"`
	ConditionMD5          string `json:"condition_md5"`
	DimensionDigest       string `json:"dimension_digest,omitempty"`
	ConditionInjected     bool   `json:"condition_injected,omitempty"`
	NoDataDimensionDigest string `json:"no_data_dimension_digest,omitempty"`
}

// InjectedConditionKeys are the condition keys the strategy publisher adds
// to a query's agg_condition on its own, which no marker in the published
// item tells apart from a condition the user wrote (the publisher's
// strategy cache projector):
//
//   - device_type: the file system type exclusion, from the deployment's
//     dynamic file_system_type_ignore list, intersected with every OR branch
//     of a bk_monitor time_series system.disk query
//     (_with_file_system_type_ignore, :234-247, applied at :468-475);
//   - bk_biz_id: the business condition of a strategy spanning businesses,
//     ANDed into every OR branch (_business_condition and
//     _with_business_condition, :204-224, applied at :456-460).
//
// A query config carrying the publisher's metric materialization snapshot
// (metric_snapshot, from the publisher's metric resolver) has the
// metric row's object_filter and expanded member conditions merged into its
// agg_condition the same way, under whatever keys the row names; it counts
// as injected too.
var InjectedConditionKeys = []string{"device_type", "bk_biz_id"}

// PromQLMetricID is the platform's get_metric_id for a prometheus
// time_series query (bk-monitor bkmonitor/strategy/new_strategy.py:116-162):
// the PromQL text, or past 128 characters its first 125 and "...",
// characters counted as Python counts them, by code point.
func PromQLMetricID(promql string) string {
	runes := []rune(promql)
	if len(runes) > 128 {
		return string(runes[:125]) + "..."
	}
	return promql
}

// ForEvent is the facts an event of the Plan carries: its grouping is the
// no-data one for a no-data event and the Plan's own for every other.
func (facts StrategyFactsV2) ForEvent(noData bool) StrategyFactsV2 {
	if noData {
		facts.DimensionDigest = facts.NoDataDimensionDigest
	}
	facts.NoDataDimensionDigest = ""
	return facts
}

// StrategyGrouping is what a Plan's series are grouped by: its output
// identity fields (Fields, or every dimension when Dynamic), and its no-data
// configuration's agg_dimension when it detects no-data.
type StrategyGrouping struct {
	Fields       []string
	Dynamic      bool
	NoData       bool
	NoDataFields []string
}

// GroupingDigest is the hex MD5 of the JSON array, encoded without HTML
// escaping, of the grouping keys with a "tags." prefix removed - the
// fingerprint reads a key and its tags. form as one
// (ProjectMonitorTarget) - without repeats and sorted by byte. Two digests
// are equal exactly when the key sets are, whatever order the strategy
// lists them in, as the platform compares them (as sets).
func GroupingDigest(fields []string) string {
	seen := make(map[string]struct{}, len(fields))
	keys := make([]string, 0, len(fields))
	for _, field := range fields {
		key := strings.TrimPrefix(field, "tags.")
		if _, repeated := seen[key]; repeated {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return md5Hex(jsonText(keys))
}

// DeriveStrategyFactsV2 derives the facts from an item's query_configs and
// its Plan's grouping. A query config without a metric_id string, or whose
// agg_condition is not a list of objects, has no facts: the platform's own
// comparison cannot read it either.
func DeriveStrategyFactsV2(queryConfigs []json.RawMessage, grouping StrategyGrouping) (*StrategyFactsV2, error) {
	metricIDs := make([]string, 0, len(queryConfigs))
	conditions := make([]string, 0, len(queryConfigs))
	injected := false
	for index, raw := range queryConfigs {
		var query struct {
			MetricID        *string         `json:"metric_id"`
			DataSourceLabel string          `json:"data_source_label"`
			DataTypeLabel   string          `json:"data_type_label"`
			PromQL          string          `json:"promql"`
			AggCondition    json.RawMessage `json:"agg_condition"`
			MetricSnapshot  json.RawMessage `json:"metric_snapshot"`
		}
		if err := json.Unmarshal(raw, &query); err != nil {
			return nil, fmt.Errorf("strategy facts: query config %d: %w", index, err)
		}
		if query.MetricID == nil {
			return nil, fmt.Errorf("strategy facts: query config %d names no metric_id", index)
		}
		metricID := *query.MetricID
		if metricID == "" && query.DataSourceLabel == "prometheus" && query.DataTypeLabel == "time_series" {
			metricID = PromQLMetricID(query.PromQL)
		}
		metricIDs = append(metricIDs, metricID)
		if conditionInjected(query.AggCondition, query.MetricSnapshot) {
			injected = true
		}
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
	facts := &StrategyFactsV2{Schema: StrategyFactsSchemaV2, MetricDigest: md5Hex(jsonText(metricIDs)), ConditionInjected: injected}
	if len(conditions) == 1 {
		facts.ConditionMD5 = conditions[0]
	} else {
		facts.ConditionMD5 = md5Hex(strings.Join(conditions, ","))
	}
	if !grouping.Dynamic {
		facts.DimensionDigest = GroupingDigest(grouping.Fields)
	}
	if grouping.NoData {
		facts.NoDataDimensionDigest = GroupingDigest(grouping.NoDataFields)
	}
	return facts, nil
}

// conditionInjected reports whether a query's conditions may hold one the
// publisher added (InjectedConditionKeys, or a metric materialization
// snapshot beside them).
func conditionInjected(conditions, snapshot json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(snapshot, &fields) == nil && len(fields) > 0 {
		return true
	}
	var keyed []struct {
		Key string `json:"key"`
	}
	_ = json.Unmarshal(conditions, &keyed)
	for _, condition := range keyed {
		if slices.Contains(InjectedConditionKeys, condition.Key) {
			return true
		}
	}
	return false
}

// jsonText is a string list as a JSON array, without HTML escaping.
func jsonText(values []string) string {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(values)
	return strings.TrimSuffix(buffer.String(), "\n")
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
