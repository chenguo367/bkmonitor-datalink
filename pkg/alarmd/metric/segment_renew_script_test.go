// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// Each renewal script that ran is counted once, at its own run time: at 2.5
// ms exactly it is within the budget's bucket, a microsecond past it is not;
// the 2.4 ms bucket holds what ran in 2.4 ms and not what ran in 2.401. The
// count is the scripts, and a round with none counts nothing.
func TestTheRenewalScriptHistogramCountsEachScriptAtItsOwnTime(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	observe := func(scripts ...time.Duration) {
		facts := &observability.SegmentShadowFacts{}
		for _, elapsed := range scripts {
			facts.RenewScript(elapsed)
		}
		r.Observe(context.Background(), observability.Observation{Component: observability.ComponentState,
			Stage: observability.StageMutationCompared, Result: observability.ResultSuccess, SegmentShadow: facts})
	}
	observe(400*time.Microsecond, 2400*time.Microsecond, 2401*time.Microsecond, 2500*time.Microsecond, 2501*time.Microsecond)
	observe()
	families, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	cumulative := map[float64]uint64{}
	var count uint64
	for _, family := range families {
		if family.GetName() != "alarmd_state_segment_renew_script_seconds" {
			continue
		}
		histogram := family.GetMetric()[0].GetHistogram()
		count = histogram.GetSampleCount()
		for _, bucket := range histogram.GetBucket() {
			cumulative[bucket.GetUpperBound()] = bucket.GetCumulativeCount()
		}
	}
	for bound, want := range map[float64]uint64{0.0005: 1, 0.0022: 1, 0.0024: 2, 0.0025: 4, 0.005: 5} {
		if cumulative[bound] != want {
			t.Errorf("le=%v holds %d, want %d (all %v)", bound, cumulative[bound], want, cumulative)
		}
	}
	if count != 5 {
		t.Fatalf("%d scripts counted, want the five that ran", count)
	}
}
