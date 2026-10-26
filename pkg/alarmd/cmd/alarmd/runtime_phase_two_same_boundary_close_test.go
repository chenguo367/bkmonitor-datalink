// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"strconv"
	"testing"
	"time"
)

// A strategy edited out of its Query Group by an activation at or before the
// boundary its open Segment starts at (inventory row L43b, same-boundary
// cutover): in the same second as revision 1, or on a Leader clock stepped
// back. Revision 2 drops the device grouping, which moves the strategy to
// another Query Group, and moves its effective window later. At a time
// revision 1 is out of its window and revision 2 is in its own, nothing is
// closed, from the copy's listing or from the index set; once revision 2's
// window ends, the alert is closed under revision 2.

// sameBoundaryEdits are the two ways an edit reaches the boundary revision
// 1's Segment starts at.
var sameBoundaryEdits = []struct {
	name   string
	editAt time.Duration
}{
	{"same second", 0},
	{"clock stepped back", -3 * time.Second},
}

func TestASameBoundaryEditClosesNothingUnderTheOldRevision(t *testing.T) {
	for _, edit := range sameBoundaryEdits {
		t.Run(edit.name, func(t *testing.T) {
			fixture, old, origin := moveStrategyOutOfItsGroup(t, 30*time.Minute, edit.editAt)
			// Revision 1's window ended at origin+20m; revision 2's runs from
			// origin+20m to origin+50m.
			fixture.life.clock.Store(origin.Add(30 * time.Minute).UnixMilli())
			fixture.heldAcrossSteps(old)
			if closes := fixture.closes(); len(closes) != 0 {
				t.Fatalf("the strategy is in effect under revision 2, yet %+v was closed", closes)
			}
			// Revision 2's own window ends: the alert is closed, under
			// revision 2.
			fixture.life.clock.Store(origin.Add(60 * time.Minute).UnixMilli())
			fixture.waitFor("the inactive close closes A1", func() bool {
				fixture.copy.RequestCalibration()
				return len(fixture.closes()) > 0
			}, func() string { return "no close" })
			wantClosesUnderRevision(t, fixture.closes(), 2)
		})
	}
}

// The same through the index-set fallback: the link's reconcile answers
// nothing, so the set's members stand in for the strategy's alerts.
func TestASameBoundaryEditClosesNothingFromTheSetUnderTheOldRevision(t *testing.T) {
	for _, edit := range sameBoundaryEdits {
		t.Run(edit.name, func(t *testing.T) {
			fixture, old, origin := moveStrategyOutOfItsGroup(t, 90*time.Minute, edit.editAt)
			key := closePathsKey()
			fixture.console.fail(true)
			fixture.waitFor("the copy is refused a calibration", func() bool {
				fixture.copy.RequestCalibration()
				return fixture.console.refusedToCopy() > 0
			}, func() string { return "no refused reconcile" })
			// Revision 1's window ended at origin+20m; revision 2's runs from
			// origin+80m to origin+110m. The last listing is past the age it
			// is acted on, and the set stands in.
			fixture.life.clock.Store(origin.Add(95 * time.Minute).UnixMilli())
			fixture.waitFor("the set stands in for the strategy's alerts", func() bool {
				fixture.copy.RequestCalibration()
				members, ok := fixture.copy.UnlistedMembers(key)
				return ok && len(members) == 1 && members[0] == closePathsA1
			}, func() string {
				members, ok := fixture.copy.UnlistedMembers(key)
				return strconv.FormatBool(ok) + " " + strconv.Itoa(len(members))
			})
			fixture.heldAcrossSteps(old)
			if closes := fixture.closes(); len(closes) != 0 {
				t.Fatalf("the strategy is in effect under revision 2, yet %+v was closed from the set", closes)
			}
			// Revision 2's own window ends: the member is closed from the
			// set, under revision 2.
			fixture.life.clock.Store(origin.Add(120 * time.Minute).UnixMilli())
			fixture.waitFor("the inactive close closes the set's member", func() bool {
				return fixture.maintenanceCounts()[closeOutcomeIndexSetAcked] > 0
			}, func() string { return "no index-set close" })
			wantClosesUnderRevision(t, fixture.closes(), 2)
		})
	}
}
