// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/detect"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/legacyoutput"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/targetplan"
)

// LegacySourceReadPaths is every key of a strategy document this build reads,
// as a dotted path with arrays left out (items.query_configs.filter_dict).
// The operator evidence's source view shows these values as written and any
// other key by its shape only; its test holds the view's allowlist to this
// list, so a key this build starts reading cannot stay hidden from the view
// that a refused strategy is read and replayed from.
//
// It is the union of legacySourceReads, which names every place the
// document, or the frozen copy of it a plan carries to its outputs, is read:
// in this package and in the others that read it. A struct is walked by its
// json tags. A struct declared inside a function, a value kept raw and
// decoded later, and a key read from a map by name are what a walk cannot
// find, and are registered there by hand. A new place that reads the
// document has to be registered there too, or the view can hide what it
// reads.
func LegacySourceReadPaths() []string {
	paths := map[string]bool{}
	for _, read := range legacySourceReads() {
		if read.path != "" {
			paths[read.path] = true
		}
		switch decoded := read.decoded.(type) {
		case reflect.Type:
			walkLegacySource(decoded, read.path, paths)
		case []string:
			for _, key := range decoded {
				paths[joinSourcePath(read.path, key)] = true
			}
		}
	}
	out := make([]string, 0, len(paths))
	for path := range paths {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

// legacySourceRead is one place a strategy document is read: the path of the
// value read, empty for the whole document, and what the value is decoded
// into - a struct to walk, or the keys read from it, each a dotted path below
// the value.
type legacySourceRead struct {
	path    string
	decoded any
}

func legacySourceReads() []legacySourceRead {
	reads := []legacySourceRead{
		// The catalog's decode of the document, and of the values it keeps
		// raw to decode later.
		{"", reflect.TypeOf(legacyStrategy{})},
		{"items.query_configs", reflect.TypeOf(legacyQueryConfig{})},
		{"items.query_configs", reflect.TypeOf(legacyQueryConfigIdentity{})},
		{"items.functions", reflect.TypeOf(legacyFunction{})},
		{"items.no_data_config", reflect.TypeOf(legacyNoDataConfig{})},
		{"items.target", reflect.TypeOf(legacyTargetCondition{})},
		{"items.target.value", reflect.TypeOf(legacyTargetValue{})},
		{"detects.recovery_config", reflect.TypeOf(legacyRecovery{})},

		// Structs declared inside functions of this package, and keys it
		// reads by name, each under the function that reads it.
		//
		// The source adapter's identity read.
		{"", []string{"id", "bk_biz_id", "bk_tenant_id", "space_uid", "is_global_strategy"}},
		// decodeLegacyStrategy, for a document without a strategy_revision.
		{"", []string{"update_time"}},
		// compileTargetPlanDocument.
		{"items", []string{"target_plan", "query_configs"}},
		// itemUnit; frozenSubjectFacts; itemInterval and decodeLegacyQueryConfig.
		{"items.query_configs", []string{"unit", "result_table_id", "agg_interval"}},
		// thresholdConfig; compileAlgorithmConfig for SimpleRingRatio.
		{"items.algorithms.config", []string{"method", "threshold", "floor", "ceil"}},
		// orderedTargetValues.
		{"items.target", []string{"value"}},
		// objectModelInstanceKey.
		{"items.target.value", []string{defaultObjectModelField, defaultObjectModelInstField}},
		// isAlwaysActiveUptime.
		{"detects.trigger_config.uptime", []string{"calendars", "active_calendars", "time_ranges.start", "time_ranges.end"}},

		// What the compiler decodes the values the catalog hands it raw into.
		{"items.algorithms.config", strategy.TraditionalComparisonKeys()},
		{"detects.trigger_config.uptime", strategy.UptimeSource()},
		{"effective_time_snapshot", strategy.EffectiveTimeSnapshotSource()},
		{"effective_time_snapshot.calendars.items.repeat", strategy.EffectiveTimeRepeatSource()},
		// The target plan protocol, which its decoder holds a document to
		// key by key.
		{"items.target_plan", targetplan.DocumentKeys()},

		// The frozen copy of the document a plan carries: the frozen
		// output's validation, the threshold processor's decode of it and
		// the keys it reads from each threshold condition by name
		// (parseThresholdAlgorithm), and the legacy output converter's
		// decodes below.
		{"", []string{"id", "bk_biz_id", "update_time"}}, // contract.LegacyOutputContext.Validate
		{"", detect.LegacyStrategySource()},
		{"items.algorithms.config", []string{"method", "threshold"}},
	}
	for _, source := range legacyoutput.FrozenStrategySources() {
		reads = append(reads, legacySourceRead{"", source})
	}
	return reads
}

func joinSourcePath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

var rawMessageType = reflect.TypeOf(json.RawMessage(nil))

func walkLegacySource(kind reflect.Type, prefix string, paths map[string]bool) {
	for kind.Kind() == reflect.Pointer || kind.Kind() == reflect.Slice {
		if kind == rawMessageType {
			return
		}
		kind = kind.Elem()
	}
	if kind.Kind() != reflect.Struct {
		return
	}
	for index := 0; index < kind.NumField(); index++ {
		field := kind.Field(index)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if !field.IsExported() || name == "" || name == "-" {
			continue
		}
		path := joinSourcePath(prefix, name)
		paths[path] = true
		walkLegacySource(field.Type, path, paths)
	}
}
