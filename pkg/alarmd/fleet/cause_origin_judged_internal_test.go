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
