// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package contract

import (
	"encoding/json"
	"strings"
	"testing"
)

// A nil section is the disabled item, which every Plan that does not detect
// no-data carries. It is not an error, and Validate must not treat it as one -
// the caller would otherwise have to know to skip the call.
func TestNoDataConfigNilValidates(t *testing.T) {
	var config *NoDataConfigV1
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() on an absent section = %v", err)
	}
}

// Empty agg_dimension is Python's model default and means "one group for the
// whole item". It must validate, because rejecting it would refuse the most
// common setting there is.
func TestNoDataConfigAcceptsEmptyAggDimension(t *testing.T) {
	config := &NoDataConfigV1{Continuous: 5, Level: 2}
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() on the whole-item setting = %v", err)
	}
}

func TestNoDataConfigRejectsSettingsThatCannotDecide(t *testing.T) {
	for name, config := range map[string]*NoDataConfigV1{
		"zero continuous":   {Continuous: 0, Level: 2},
		"level below one":   {Continuous: 1, Level: 0},
		"level above three": {Continuous: 1, Level: 4},
		"empty dimension":   {Continuous: 1, Level: 2, AggDimension: []string{""}},
		"padded dimension":  {Continuous: 1, Level: 2, AggDimension: []string{" host"}},
		"repeated dimension": {
			Continuous: 1, Level: 2, AggDimension: []string{"host", "host"},
		},
		"dimension names the tag": {
			Continuous: 1, Level: 2, AggDimension: []string{NoDataDimensionTag},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := config.Validate(); err == nil {
				t.Fatalf("Validate() accepted %+v", config)
			}
		})
	}
}

// The section is absent from a Plan's JSON when the item does not detect
// no-data, rather than present and empty. A reader that sees "no_data" in the
// wire form is reading an item that detects it.
func TestPlanOmitsTheNoDataSectionWhenAbsent(t *testing.T) {
	payload, err := json.Marshal(EvaluationPlanV2{PlanID: "1001"})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if got := string(payload); strings.Contains(got, `"no_data"`) {
		t.Fatalf("Plan without no-data detection carries the section: %s", got)
	}

	enabled := EvaluationPlanV2{PlanID: "1001", NoData: &NoDataConfigV1{Continuous: 5, Level: 2}}
	payload, err = json.Marshal(enabled)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if got := string(payload); !strings.Contains(got, `"no_data"`) {
		t.Fatalf("Plan with no-data detection omits the section: %s", got)
	}
}
