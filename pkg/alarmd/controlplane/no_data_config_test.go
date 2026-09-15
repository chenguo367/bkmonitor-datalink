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
	"testing"
)

// The item's setting is read off the strategy cache exactly as the backend
// reads it. Each case names the read site it mirrors, so a change here has to
// argue with that site rather than with a number in this file.
// A number this section states as text must not cost the strategy its
// threshold detection.
//
// The store keeps no_data_config as a bare DictField with no field-level
// validation, so "5" is as legal there as 5, and the backend reads it with
// int() either way. A numeric Go type on this field does not disable no-data
// when it meets the string - it fails Decode for the whole strategy document,
// and the item's thresholds stop being detected with it. This ran red before
// the field became json.Number, with the decoder naming no_data_config while
// the strategy that vanished had nothing to do with no-data.
func TestAStatedNumberDoesNotCostTheStrategyItsOtherDetection(t *testing.T) {
	document := json.RawMessage(`{"id":1,"bk_biz_id":2,"update_time":1700000000,"items":[{"id":11,"query_md5":"m","expression":"a",` +
		`"query_configs":[{}],"algorithms":[{"level":1,"type":"Threshold"}],` +
		`"no_data_config":{"is_enabled":true,"continuous":"5"}}]}`)
	strategy, err := decodeLegacyStrategy(document)
	if err != nil {
		t.Fatalf("decode strategy = %v; a quoted no-data number took the whole strategy with it", err)
	}
	if len(strategy.Items) != 1 || len(strategy.Items[0].Algorithms) != 1 {
		t.Fatalf("decoded items = %+v, want the item and its algorithm intact", strategy.Items)
	}
	config, err := frozenNoDataConfig(strategy.Items[0])
	if err != nil || config == nil || config.Continuous != 5 {
		t.Fatalf("frozenNoDataConfig() = %+v, %v, want continuous 5", config, err)
	}

	// A value that is not a number at all stays inside the section too. The
	// backend raises on int("many") and loses that item's no-data detection;
	// losing the strategy's thresholds with it is the part that must not
	// happen. json.Number would refuse this one at the document level, which
	// is why the field is raw.
	malformed := json.RawMessage(`{"id":1,"bk_biz_id":2,"update_time":1700000000,"items":[{"id":11,` +
		`"query_md5":"m","expression":"a","query_configs":[{}],` +
		`"algorithms":[{"level":1,"type":"Threshold"}],` +
		`"no_data_config":{"is_enabled":true,"continuous":"many"}}]}`)
	decoded, err := decodeLegacyStrategy(malformed)
	if err != nil {
		t.Fatalf("decode strategy = %v; a malformed no-data number took the whole strategy with it", err)
	}
	if _, err := frozenNoDataConfig(decoded.Items[0]); err == nil {
		t.Fatal("frozenNoDataConfig() accepted a continuous that is not a number")
	}

	// Typing the numbers alone was not enough. Every position in this section
	// is open, so each one that stayed typed kept the whole blast radius: these
	// two failed Decode for the document until the section itself became raw.
	// wantItemError says whether the item itself is then refused. Surviving the
	// document decode is only half: a shape the section should not hold has to
	// be refused by name, or it arrives in a Plan as a dimension no series can
	// carry and the item detects nothing while looking configured.
	for name, shape := range map[string]struct {
		section       string
		wantItemError bool
	}{
		"is_enabled as text":         {section: `{"is_enabled":"true","continuous":5}`},
		"agg_dimension of numbers":   {section: `{"is_enabled":true,"continuous":5,"agg_dimension":[1]}`, wantItemError: true},
		"the section is not a table": {section: `"enabled"`, wantItemError: true},
	} {
		section := shape.section
		t.Run(name, func(t *testing.T) {
			document := json.RawMessage(`{"id":1,"bk_biz_id":2,"update_time":1700000000,"items":[{"id":11,` +
				`"query_md5":"m","expression":"a","query_configs":[{}],` +
				`"algorithms":[{"level":1,"type":"Threshold"}],"no_data_config":` + section + `}]}`)
			decoded, err := decodeLegacyStrategy(document)
			if err != nil {
				t.Fatalf("decode strategy = %v; the shape took the whole strategy with it", err)
			}
			if len(decoded.Items) != 1 || len(decoded.Items[0].Algorithms) != 1 {
				t.Fatalf("decoded items = %+v, want the item and its algorithm intact", decoded.Items)
			}
			config, err := frozenNoDataConfig(decoded.Items[0])
			if shape.wantItemError {
				if err == nil {
					t.Fatalf("frozenNoDataConfig() = %+v, want the shape refused by name", config)
				}
				return
			}
			if err != nil {
				t.Fatalf("frozenNoDataConfig() error = %v", err)
			}
		})
	}
}

func TestFrozenNoDataConfigFollowsTheBackendReadSites(t *testing.T) {
	for name, test := range map[string]struct {
		payload    string
		wantAbsent bool
		wantError  bool
		continuous uint32
		level      uint32
		dimensions int
	}{
		// item.py takes .get("no_data_config", {}); an item without the
		// section detects nothing.
		"section absent": {payload: `{"id": 1}`, wantAbsent: true},
		// strategy.py selects on `cfg and cfg.get("is_enabled")`.
		"disabled": {
			payload: `{"id": 1, "no_data_config": {"is_enabled": false, "continuous": 5}}`, wantAbsent: true,
		},
		"enabled with everything stated": {
			payload: `{"id": 1, "no_data_config": {"is_enabled": true, "continuous": 3,` +
				` "agg_dimension": ["bk_target_ip"], "level": 1}}`,
			continuous: 3, level: 1, dimensions: 1,
		},
		// mixins/nodata.py reads .get("level", NO_DATA_LEVEL).
		"level omitted takes the read-side default": {
			payload:    `{"id": 1, "no_data_config": {"is_enabled": true, "continuous": 5}}`,
			continuous: 5, level: 2,
		},
		// Empty agg_dimension is the whole-item setting, not a gap.
		"empty agg_dimension is a setting": {
			payload:    `{"id": 1, "no_data_config": {"is_enabled": true, "continuous": 2, "agg_dimension": []}}`,
			continuous: 2, level: 2,
		},
		// strategy.py subscripts continuous, int(cfg["continuous"]), beside the
		// level it defaults. An item omitting it raises there and detects
		// nothing, so supplying a default here would invent detection.
		"continuous omitted is refused": {
			payload:   `{"id": 1, "no_data_config": {"is_enabled": true, "agg_dimension": ["host"]}}`,
			wantError: true,
		},
		"continuous zero is refused": {
			payload:   `{"id": 1, "no_data_config": {"is_enabled": true, "continuous": 0}}`,
			wantError: true,
		},
		// The store keeps this section as a bare dict, and the backend reads
		// every number out of it with int(). A quoted number is what that
		// tolerates and what this has to tolerate.
		"quoted numbers are read as numbers": {
			payload:    `{"id": 1, "no_data_config": {"is_enabled": true, "continuous": "3", "level": "1"}}`,
			continuous: 3, level: 1,
		},
		// int() truncates a float toward zero.
		"a float is truncated": {
			payload:    `{"id": 1, "no_data_config": {"is_enabled": true, "continuous": 5.9}}`,
			continuous: 5, level: 2,
		},
		"text that is not a number is refused": {
			payload:   `{"id": 1, "no_data_config": {"is_enabled": true, "continuous": "many"}}`,
			wantError: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var item legacyItem
			if err := json.Unmarshal([]byte(test.payload), &item); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			config, err := frozenNoDataConfig(item)
			if test.wantError {
				if err == nil {
					t.Fatalf("frozenNoDataConfig() = %+v, want an error", config)
				}
				return
			}
			if err != nil {
				t.Fatalf("frozenNoDataConfig() error = %v", err)
			}
			if test.wantAbsent {
				if config != nil {
					t.Fatalf("frozenNoDataConfig() = %+v, want no section", config)
				}
				return
			}
			if config == nil {
				t.Fatal("frozenNoDataConfig() = nil, want the item's setting")
			}
			if config.Continuous != test.continuous || config.Level != test.level ||
				len(config.AggDimension) != test.dimensions {
				t.Fatalf("frozenNoDataConfig() = %+v, want continuous=%d level=%d dimensions=%d",
					config, test.continuous, test.level, test.dimensions)
			}
		})
	}
}
