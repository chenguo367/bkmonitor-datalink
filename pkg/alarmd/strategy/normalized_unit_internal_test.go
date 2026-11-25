// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package strategy

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
)

// The unit a normalized value is in is a unit the platform defines: the
// first step of the same steps as its own unit, so a reader formats it by
// the platform's unit table - never the normalizer's "base". A unit of no
// scale is itself.
func TestANormalizedValueIsInTheFirstStepOfItsUnit(t *testing.T) {
	payload, err := os.ReadFile("testdata/bkmonitor_units.json")
	if err != nil {
		t.Fatal(err)
	}
	var platform platformUnits
	if err := json.Unmarshal(payload, &platform); err != nil {
		t.Fatal(err)
	}
	for id, own := range platform.Units {
		base := normalizedUnitID(id)
		want, defined := platform.Units[base]
		if !defined {
			t.Errorf("%q normalizes to %q, which the platform does not define", id, base)
			continue
		}
		if want.DefaultIndex != 0 || !slices.Equal(want.Suffixes, own.Suffixes) {
			t.Errorf("%q normalizes to %q, written in step %d of %v, want the first of %v", id, base, want.DefaultIndex, want.Suffixes, own.Suffixes)
		}
	}
	// The measure, which the steps do not tell apart.
	for id, want := range map[string]string{
		"kbytes": "bytes", "bits": "bits", "deckbytes": "decbytes", "MBs": "Bps", "Mbits": "bps", "Kbits": "bps",
		"ms": "ns", "percentunit": "percent", "percent": "percent", "cps": "cps", "none": "none", "": "",
		"celsius": "celsius", "group||mbytes": "bytes",
	} {
		if got := normalizedUnitID(id); got != want {
			t.Errorf("%q normalizes to %q, want %q", id, got, want)
		}
	}
	normalizer, _, ok := compileUnitNormalizer("mbytes", "Gi")
	if !ok || normalizer.ValueUnit() != "bytes" || normalizer.TargetUnit() != "base" {
		t.Fatalf("an mbytes normalizer: value unit %q, target %q, want bytes beside its own base", normalizer.ValueUnit(), normalizer.TargetUnit())
	}
}
