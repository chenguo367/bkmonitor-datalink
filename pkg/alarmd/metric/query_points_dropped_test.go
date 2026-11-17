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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// Every reason is built at zero, so a scrape reads none let go rather than
// nothing known, and each counts its own points.
func TestThePointsDroppedAreCountedByReasonFromZero(t *testing.T) {
	recorder := NewRecorder(BuildInfo{})
	dropped := recorder.phaseTwo.flow.pointsDropped
	if got := testutil.CollectAndCount(dropped); got != len(observability.PointDropReasons) {
		t.Fatalf("%d series before any drop, want one a reason", got)
	}
	recorder.RecordQueryPointsDropped(observability.PointDrops{OutsideRangeBefore: 2, OutsideRangeAfter: 1})
	recorder.RecordQueryPointsDropped(observability.PointDrops{OutsideRangeBefore: 1})
	for reason, want := range map[string]float64{
		observability.PointDroppedOutsideRangeBefore: 3, observability.PointDroppedOutsideRangeAfter: 1, observability.PointDroppedRowMissingColumns: 0,
	} {
		if got := testutil.ToFloat64(dropped.WithLabelValues(reason)); got != want {
			t.Fatalf("%s = %v, want %v", reason, got, want)
		}
	}
	var none *Recorder
	none.RecordQueryPointsDropped(observability.PointDrops{OutsideRangeBefore: 1})
}
