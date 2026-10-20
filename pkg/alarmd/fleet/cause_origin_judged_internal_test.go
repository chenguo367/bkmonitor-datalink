// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A reason alarmd judged reads derived on the row, a reason the round met
// reads physical, and a stored guard's reason reads carried whatever it is.
func TestARowsCauseOriginFollowsWhoseTheReasonIs(t *testing.T) {
	carried := &observability.CarriedReasonFacts{OriginSlot: 1_700_000_040, OriginCause: "HISTORY_WARMING"}
	for _, test := range []struct {
		reason  string
		filed   bool
		carried *observability.CarriedReasonFacts
		want    string
	}{
		{"EFFECTIVE_TIME_INACTIVE", false, nil, CauseOriginDerived},
		{"HISTORY_WARMING", false, nil, CauseOriginDerived},
		{"QUERY_TIMEOUT", false, nil, observability.ReasonOriginPhysical},
		{"HISTORY_WARMING", false, carried, observability.ReasonOriginCarried},
		{"GAP_GUARD_WARMING", true, carried, CauseOriginDerived},
	} {
		if got := causeOriginOf(test.reason, test.filed, test.carried); got == nil || got.Origin != test.want {
			t.Errorf("%s filed=%t carried=%t: origin %+v, want %s", test.reason, test.filed, test.carried != nil, got, test.want)
		}
	}
}

// The overview carries the object list's cause reasons, all and by origin,
// as /api/objects counts them: the read every channel has answers whose each
// degraded round is without a route to the object list.
func TestTheOverviewCarriesTheCauseReasonsByOrigin(t *testing.T) {
	rows := []Anomaly{
		{QueryGroup: "qg-321", Kind: KindDegradedRun, CauseReason: "EFFECTIVE_TIME_INACTIVE", CauseOrigin: &CauseOrigin{Origin: CauseOriginDerived}},
		{QueryGroup: "qg-322", Kind: KindDegradedRun, CauseReason: "QUERY_TIMEOUT", CauseOrigin: &CauseOrigin{Origin: observability.ReasonOriginPhysical}},
		{QueryGroup: "qg-323", Kind: KindDegradedRun, CauseReason: "QUERY_TIMEOUT", CauseOrigin: &CauseOrigin{Origin: observability.ReasonOriginCarried}},
	}
	summary := summarize(rows, now)
	got := overviewCauseReasonsOf(summary)
	if !reflect.DeepEqual(got.ByCauseReason, summary.ByCauseReason) || !reflect.DeepEqual(got.ByCauseReasonPhysical, summary.ByCauseReasonPhysical) ||
		!reflect.DeepEqual(got.ByCauseReasonCarried, summary.ByCauseReasonCarried) || !reflect.DeepEqual(got.ByCauseReasonDerived, summary.ByCauseReasonDerived) ||
		!reflect.DeepEqual(got.ByCauseReasonUnknown, summary.ByCauseReasonUnknown) {
		t.Fatalf("overview cause reasons %+v, want the summary's %+v", got, summary)
	}
	if len(got.ByCauseReasonDerived.Top) != 1 || got.ByCauseReasonDerived.Top[0].Value != "EFFECTIVE_TIME_INACTIVE" {
		t.Fatalf("derived = %+v, want the out-of-hours row", got.ByCauseReasonDerived)
	}
}
