// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// The real family's name - what its pages' renewals are staggered by and its
// sampled day placed by - is the one it was before there were two families,
// byte for byte; the no-data family's is it with the family after (014
// next9 ⑥ 2.2).
func TestTheRealFamilysNameIsTheOneItWasAndTheNoDataFamilysFollowsIt(t *testing.T) {
	real := shadowKey(shadowDue, "g", segment.FamilyReal)
	noData := shadowKey(shadowDue, "g", segment.FamilyNoData)
	if real != (segment.PlanKey{Plan: shadowDue.Identity, Generation: "g"}) || noData.Family != segment.FamilyNoData {
		t.Fatalf("keys %+v and %+v, want the real one the key it always was", real, noData)
	}
	if name := segmentPlanName(real); name != "tenant/2/9/g" {
		t.Fatalf("the real family's name %q, want the one it was", name)
	}
	if name := segmentPlanName(noData); name != "tenant/2/9/g/no_data" {
		t.Fatalf("the no-data family's name %q, want the real one's with its family after", name)
	}
}

// A Slot leaves its no-data family's members only under the no-data word as
// well as the skip word; its real members under the skip word alone.
func TestTheNoDataFamilyIsLeftOnlyUnderItsOwnWordBesideTheSkipWord(t *testing.T) {
	for _, test := range []struct {
		leaves, noData   bool
		real, noDataLeft bool
	}{
		{leaves: false, noData: false},
		{leaves: false, noData: true},
		{leaves: true, noData: false, real: true},
		{leaves: true, noData: true, real: true, noDataLeft: true},
	} {
		slot := &segmentSlot{leaves: test.leaves, skipNoData: test.noData}
		if slot.familyLeaves(segment.FamilyReal) != test.real || slot.familyLeaves(segment.FamilyNoData) != test.noDataLeft {
			t.Fatalf("%+v: leaves real %t, no-data %t", test, slot.familyLeaves(segment.FamilyReal), slot.familyLeaves(segment.FamilyNoData))
		}
		round := &segmentShadowRound{slot: slot, skip: test.leaves, skipNoData: test.leaves && test.noData}
		if round.familySkips(segment.FamilyReal) != test.real || round.familySkips(segment.FamilyNoData) != test.noDataLeft {
			t.Fatalf("%+v: round skips real %t, no-data %t", test, round.familySkips(segment.FamilyReal), round.familySkips(segment.FamilyNoData))
		}
	}
}
