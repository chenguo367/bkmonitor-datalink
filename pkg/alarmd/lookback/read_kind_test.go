// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A sample's planned rungs are asked as rechecks, and its deep recheck -
// read once after them at the deepest rung, the fixture probing a group's
// first sample - as the deep recheck.
func TestASamplesRungsAreAskedAsRechecksAndItsProbeAsItsDeepRecheck(t *testing.T) {
	f := newFixture(t)
	same := func(slot int64) []*execution.Dataset { return []*execution.Dataset{point(slot, "1")} }
	f.classSample(0, []*execution.Dataset{point(f.clock.now().Unix(), "1")}, same)
	kinds := f.readKinds()
	if len(kinds) < 2 || kinds[len(kinds)-1] != execution.LookbackReadDeepRecheck {
		t.Fatalf("reads %v, want its rungs and then its deep recheck", kinds)
	}
	for _, kind := range kinds[:len(kinds)-1] {
		if kind != execution.LookbackReadRecheck {
			t.Fatalf("reads %v, want each rung before the deep recheck asked as a recheck", kinds)
		}
	}
}
