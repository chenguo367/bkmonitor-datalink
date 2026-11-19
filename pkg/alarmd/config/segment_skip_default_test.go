// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package config

import (
	"strings"
	"testing"
)

// The segment skip is on by default: a configuration that does not state
// phase_two.control.segment_skip - the chart renders none - asks for it, as
// Default does; segment_skip: false stops it, and true states the default.
func TestTheSegmentSkipIsOnUnlessStatedOff(t *testing.T) {
	if !Default().PhaseTwo.Control.SegmentSkip {
		t.Fatal("Default() does not ask for the segment skip")
	}
	fixture := validGoAccessRuntimeConfigYAML("skip-worker")
	if strings.Contains(fixture, "segment_skip") {
		t.Fatal("the fixture states segment_skip: the absent case would not be read")
	}
	const control = "    strategy_cache_prefix: alarm-config\n"
	if !strings.Contains(fixture, control) {
		t.Fatal("the fixture no longer has the control block's strategy_cache_prefix line")
	}
	for _, test := range []struct {
		name, line string
		want       bool
	}{
		{"absent", "", true},
		{"stated off", "    segment_skip: false\n", false},
		{"stated on", "    segment_skip: true\n", true},
	} {
		loaded, err := LoadWithRoles(writeConfig(t, strings.Replace(fixture, control, control+test.line, 1)), nil)
		if err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		if loaded.PhaseTwo.Control.SegmentSkip != test.want {
			t.Fatalf("%s: segment_skip loaded %t, want %t", test.name, loaded.PhaseTwo.Control.SegmentSkip, test.want)
		}
	}
}
