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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// LegacySourceReadPaths is every key of a strategy document this build reads,
// as a dotted path with arrays left out (items.query_configs.filter_dict).
// The operator evidence's source view shows these values as written and any
// other key by its shape only; its test holds the view's allowlist to this
// list, so a key this build starts reading cannot stay hidden from the view
// that a refused strategy is read and replayed from.
//
// It walks the decoding structs by their json tags. A value kept raw and
// decoded later is listed by what it is decoded into, from
// legacySourceRawDecodes; a raw field decoded somewhere new has to be added
// there, which is the one part of this a reflection cannot find.
func LegacySourceReadPaths() []string {
	paths := map[string]bool{}
	// Read beside the decoding structs, by the source adapter and the
	// target-plan reader.
	for _, path := range []string{"bk_tenant_id", "space_uid", "is_global_strategy", "items.target_plan"} {
		paths[path] = true
	}
	walkLegacySource(reflect.TypeOf(legacyStrategy{}), "", paths)
	for path, decoded := range legacySourceRawDecodes() {
		paths[path] = true
		switch decoded := decoded.(type) {
		case reflect.Type:
			walkLegacySource(decoded, path, paths)
		case []string:
			for _, key := range decoded {
				paths[path+"."+key] = true
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

// legacySourceRawDecodes is what each value the structs keep raw is decoded
// into: a struct to walk, or the keys read from it.
func legacySourceRawDecodes() map[string]any {
	return map[string]any{
		"items.query_configs":           reflect.TypeOf(legacyQueryConfig{}),
		"items.functions":               reflect.TypeOf(legacyFunction{}),
		"items.no_data_config":          reflect.TypeOf(legacyNoDataConfig{}),
		"items.target":                  reflect.TypeOf(legacyTargetCondition{}),
		"items.target.value":            reflect.TypeOf(legacyTargetValue{}),
		"items.algorithms.config":       strategy.TraditionalComparisonKeys(),
		"detects.recovery_config":       reflect.TypeOf(legacyRecovery{}),
		"detects.trigger_config.uptime": strategy.UptimeKeys(),
	}
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
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		paths[path] = true
		walkLegacySource(field.Type, path, paths)
	}
}
