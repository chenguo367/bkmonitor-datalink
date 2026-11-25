// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// An item detected over several inputs (query_output_config's
// response_contract multi_detect/v1): each input an expression over the
// item's query configs, detected by its own thresholds, the inputs joined on
// the dimensions they declare in common. Each input is a query of its own -
// the provider's named outputs give only one output an expression of its
// own - and they share one Slot window: every input reads the same
// aggregation interval, step and delay, or the item is refused.

// multiDetectConfig is the item's query_output_config under multi_detect/v1.
// Fields are read raw, as legacyQueryOutputConfig's are, so that a value of
// the wrong type is refused rather than read as empty.
type multiDetectConfig struct {
	ResponseContract json.RawMessage   `json:"response_contract"`
	CommonDimensions json.RawMessage   `json:"common_dimensions"`
	Inputs           []json.RawMessage `json:"inputs"`
}

type multiDetectInputConfig struct {
	Ref         json.RawMessage `json:"ref"`
	DisplayName json.RawMessage `json:"display_name"`
	Expression  json.RawMessage `json:"expression"`
	Interval    json.RawMessage `json:"interval"`
	Unit        json.RawMessage `json:"unit"`
}

// MultiDetectInput is one input of an item detected over several: the name
// its algorithms bind it by (input_ref), the name a reader is shown - the
// reference name when the writer gave none - the unit of its value, and its
// own query.
type MultiDetectInput struct {
	Ref         string
	DisplayName string
	Unit        string
	Facts       execution.QueryPlanFacts
}

// MultiDetect is an item detected over several inputs: the dimensions its
// inputs are joined on, sorted, and each input in the order it was written.
// The first input is the Plan's PRIMARY: its query names the Query Group.
type MultiDetect struct {
	CommonDimensions []string
	Inputs           []MultiDetectInput
	// fallbacks are the agg_method fallbacks compiling the inputs ran, each
	// noted on the Plan as the single query's are.
	fallbacks []aggMethodFallback
}

// MultiDetectQueryCompiler compiles an item detected over several inputs.
type MultiDetectQueryCompiler interface {
	CompileMultiDetect(context.Context, PrimaryQuerySource) (MultiDetect, error)
}

// multiDetectDatasetName is the named input an input after the first of an
// item detected over several is read as: its reference name, beside the
// first input's "primary".
func multiDetectDatasetName(ref string) execution.DatasetName {
	return execution.DatasetName("input:" + ref)
}

// queryOutputContractOf is the response contract an item's
// query_output_config names, empty for an item without one. A config that
// does not decode, or names its contract with something other than a
// string, is no contract this reads; the named outputs compile refuses it
// by name.
func queryOutputContractOf(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var config struct {
		ResponseContract json.RawMessage `json:"response_contract"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		return ""
	}
	contract, err := strippedConfigString(config.ResponseContract)
	if err != nil {
		return ""
	}
	return contract
}

// CompileMultiDetect compiles each input of an item detected over several
// into its own query: the query configs its expression names, and its
// expression as their merge. The configs each input reads must aggregate by
// exactly the common dimensions and at the input's interval; every input
// must read at the same step, alignment and delay, and every config must be
// read by some input.
func (compiler *LegacyPrimaryQueryCompiler) CompileMultiDetect(_ context.Context, source PrimaryQuerySource) (MultiDetect, error) {
	var config multiDetectConfig
	if err := json.Unmarshal(source.QueryOutputConfig, &config); err != nil {
		return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", err)
	}
	contract, err := strippedConfigString(config.ResponseContract)
	if err != nil || contract != execution.MultiDetectContract {
		return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", fmt.Errorf("response contract %q", contract))
	}
	if len(source.Functions) != 0 {
		// An item's functions apply to the one expression it detects; under
		// multi_detect/v1 each input is its own expression, and a function
		// is written in the input's expression.
		return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID",
			errors.New("item functions are not read under multi_detect/v1: write them in each input's expression"))
	}
	common, err := multiDetectCommonDimensions(config.CommonDimensions)
	if err != nil {
		return MultiDetect{}, err
	}
	if len(config.Inputs) < execution.MinMultiDetectInputs || len(config.Inputs) > execution.MaxMultiDetectInputs {
		return MultiDetect{}, queryConfigRejected("MULTI_DETECT_INPUT_COUNT", fmt.Errorf("%d inputs, want %d to %d",
			len(config.Inputs), execution.MinMultiDetectInputs, execution.MaxMultiDetectInputs))
	}
	type queryConfig struct {
		raw     json.RawMessage
		decoded legacyQueryConfig
		read    bool
	}
	configs := make([]*queryConfig, 0, len(source.QueryConfigs))
	byAlias := make(map[string]*queryConfig, len(source.QueryConfigs))
	for index, raw := range source.QueryConfigs {
		decoded, err := decodeLegacyQueryConfig(raw)
		if err != nil {
			return MultiDetect{}, atQueryConfig(queryConfigRejected("QUERY_CONFIG_INVALID", err), index)
		}
		alias := strings.ToLower(decoded.Alias)
		if alias == "" || byAlias[alias] != nil {
			return MultiDetect{}, atQueryConfig(queryConfigRejected("MULTI_DETECT_CONFIG_INVALID",
				fmt.Errorf("query config alias %q is empty or written twice", decoded.Alias)), index)
		}
		config := &queryConfig{raw: raw, decoded: decoded}
		configs = append(configs, config)
		byAlias[alias] = config
	}
	result := MultiDetect{CommonDimensions: common, Inputs: make([]MultiDetectInput, 0, len(config.Inputs))}
	seen := make(map[string]bool, len(config.Inputs))
	for _, entry := range config.Inputs {
		var input multiDetectInputConfig
		if err := json.Unmarshal(entry, &input); err != nil {
			return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", err)
		}
		ref, refErr := strippedConfigString(input.Ref)
		display, displayErr := strippedConfigString(input.DisplayName)
		expression, expressionErr := strippedConfigString(input.Expression)
		unit, unitErr := strippedConfigString(input.Unit)
		if err := errors.Join(refErr, displayErr, expressionErr, unitErr); err != nil {
			return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", err)
		}
		if ref == "" || len(ref) > maxNamedOutputRefLength || !namedOutputRefName.MatchString(ref) || seen[ref] {
			return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", fmt.Errorf("input reference %q", ref))
		}
		seen[ref] = true
		if expression == "" {
			return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", fmt.Errorf("input %q has no expression", ref))
		}
		var interval int64
		if err := json.Unmarshal(input.Interval, &interval); err != nil || interval <= 0 {
			return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", fmt.Errorf("input %q interval %s", ref, input.Interval))
		}
		read := make([]json.RawMessage, 0, len(configs))
		for _, config := range configs {
			if !expressionNames(expression, strings.ToLower(config.decoded.Alias)) {
				continue
			}
			if config.decoded.AggInterval != interval {
				return MultiDetect{}, queryConfigRejected("MULTI_DETECT_INTERVAL_MISMATCH", fmt.Errorf(
					"input %q reads query %q at agg_interval %d, the input's interval is %d", ref, config.decoded.Alias, config.decoded.AggInterval, interval))
			}
			if !slices.Equal(canonicalDimensionStrings(config.decoded.AggDimensions), common) {
				return MultiDetect{}, queryConfigRejected("MULTI_DETECT_DIMENSIONS_MISMATCH", fmt.Errorf(
					"input %q reads query %q aggregated by %v, the common dimensions are %v", ref, config.decoded.Alias,
					canonicalDimensionStrings(config.decoded.AggDimensions), common))
			}
			config.read = true
			read = append(read, config.raw)
		}
		if len(read) == 0 {
			return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", fmt.Errorf("input %q names no query config", ref))
		}
		if only := byAliasOf(byAlias, expression); len(read) == 1 && only != nil && only.decoded.DataSourceLabel == "prometheus" &&
			strings.ToLower(strings.TrimFunc(expression, pythonSpace)) != strings.ToLower(only.decoded.Alias) {
			// One PromQL query is run as its own PromQL, its alias's
			// expression not read: an input over one PromQL query is the
			// query itself, and anything more is written in its promql.
			return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", fmt.Errorf(
				"input %q is an expression over one PromQL query: write it in the query's promql", ref))
		}
		own := source
		own.Expression, own.QueryConfigs, own.QueryOutputConfig = expression, read, nil
		var decoded []legacyQueryConfig
		facts, err := compiler.compilePrimaryQueryConfigs(own, &decoded, &result.fallbacks)
		if err != nil {
			return MultiDetect{}, err
		}
		if display == "" {
			display = ref
		}
		result.Inputs = append(result.Inputs, MultiDetectInput{Ref: ref, DisplayName: display, Unit: unit, Facts: facts})
	}
	for _, config := range configs {
		if !config.read {
			return MultiDetect{}, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID",
				fmt.Errorf("query config %q is read by no input", config.decoded.Alias))
		}
	}
	first := result.Inputs[0].Facts
	for _, input := range result.Inputs[1:] {
		if !sameMultiDetectStep(first, input.Facts) {
			return MultiDetect{}, queryConfigRejected("MULTI_DETECT_INTERVAL_MISMATCH", fmt.Errorf(
				"input %q reads at another step, alignment or delay than input %q", input.Ref, result.Inputs[0].Ref))
		}
	}
	return result, nil
}

// byAliasOf is the one query config an expression names, nil when it names
// none or more than one.
func byAliasOf[T any](byAlias map[string]*T, expression string) *T {
	var found *T
	for alias, config := range byAlias {
		if expressionNames(expression, alias) {
			if found != nil {
				return nil
			}
			found = config
		}
	}
	return found
}

// multiDetectCommonDimensions is the common dimensions as written: a list of
// distinct, non-empty strings, at least one, returned sorted.
func multiDetectCommonDimensions(raw json.RawMessage) ([]string, error) {
	var written []string
	if len(raw) == 0 || string(raw) == "null" {
		return nil, queryConfigRejected("MULTI_DETECT_DIMENSIONS_MISMATCH", errors.New("no common dimensions are declared"))
	}
	if err := json.Unmarshal(raw, &written); err != nil {
		return nil, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", fmt.Errorf("common dimensions: %w", err))
	}
	common := canonicalDimensionStrings(written)
	if len(common) == 0 {
		return nil, queryConfigRejected("MULTI_DETECT_DIMENSIONS_MISMATCH", errors.New("no common dimensions are declared"))
	}
	if len(common) != len(written) {
		return nil, queryConfigRejected("MULTI_DETECT_CONFIG_INVALID", fmt.Errorf("common dimensions %v hold an empty or repeated name", written))
	}
	return common, nil
}

// expressionNames reports whether an expression names a query by its alias:
// the alias as a whole identifier, letters, digits and underscores, compared
// without case - the compiler reads a merge's references lowercased.
func expressionNames(expression, alias string) bool {
	lower := strings.ToLower(expression)
	identifier := func(c byte) bool {
		return c == '_' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
	}
	for index := 0; index < len(lower); {
		if !identifier(lower[index]) {
			index++
			continue
		}
		end := index
		for end < len(lower) && identifier(lower[end]) {
			end++
		}
		if lower[index:end] == alias {
			return true
		}
		index = end
	}
	return false
}

// sameMultiDetectStep is whether two inputs read on one grid: the same
// bucket width and request step, alignment, time alignment, downsampling,
// time zone and delay, and the same end - both asked it a second earlier, or
// neither (execution.RequestEndTrimEligible). Inputs that agree here and
// share one Slot window ask the provider the same start, end and step.
func sameMultiDetectStep(a, b execution.QueryPlanFacts) bool {
	return a.StepMillis == b.StepMillis && a.QueryStepMillis == b.QueryStepMillis && a.AlignmentMillis == b.AlignmentMillis &&
		a.NotTimeAlign == b.NotTimeAlign && a.DownSampleRange == b.DownSampleRange && a.Timezone == b.Timezone &&
		a.QueryDelaySeconds == b.QueryDelaySeconds &&
		execution.RequestEndTrimEligible(a) == execution.RequestEndTrimEligible(b)
}
