// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// The recovery gate of a trusted copy whose set has no current calibration.
// A healthy series says RECOVERY every cycle, and the gate is what keeps it
// from the link once its alert is closed. This process's record of what it
// sent never refreshes while no complete calibration comes, so without more
// the gate would pass a flapping series' RECOVERY every cycle for good after
// the link closed the alert. Once this process's RECOVERY of it was taken a
// grace ago (the calibration interval) with no ABNORMAL since, and the
// set's own read does not carry it, that record stops answering it held.
// Only that record: an alert a reconcile named missing from the set is the
// link's own word that it is active, and no time lets it go. The set's read
// carrying it, a later ABNORMAL, or a current calibration naming it keep it
// passing.
func TestARecoverySentAGraceAgoLetsTheGateGoOfWhatNeverRefreshes(t *testing.T) {
	const m = "m9"
	grace := indexOptions(&clock{}).ReconcileInterval
	failing := func() (Reconciliation, error) { return Reconciliation{}, errors.New("reconcile: link answered 503") }
	cutNamingMissing := func() (Reconciliation, error) {
		return Reconciliation{EventSourceID: "source", Truncated: true, Members: []string{m}, Missing: []string{m},
			Alerts: []Alert{{AlertID: "a9", EventSourceID: "source", Fingerprint: m}}}, nil
	}
	cutListingNothing := func() (Reconciliation, error) { return Reconciliation{EventSourceID: "source", Truncated: true}, nil }
	completeNamingMissing := func() (Reconciliation, error) {
		return Reconciliation{EventSourceID: "source", Members: []string{m}, Missing: []string{m},
			Alerts: []Alert{{AlertID: "a9", EventSourceID: "source", Fingerprint: m}}}, nil
	}
	for _, c := range []struct {
		name    string
		members []string
		// setup runs before the RECOVERY is taken.
		setup func(*unlistedFixture)
		// later runs once the grace has passed, before the last answer.
		later func(*testing.T, *unlistedFixture)
		// steps is how many rounds past the grace are answered, 2 unless
		// set.
		steps int
		want  bool
	}{
		{name: "sent by this process, the set read without it", setup: func(f *unlistedFixture) {
			f.reconcile = failing
			f.round(0)
			f.cache.Acknowledged([]contract.TriggerEventV1{abnormal(keyA, m)})
		}, want: false},
		{name: "named missing by a cut reconcile, the set read without it", setup: func(f *unlistedFixture) {
			f.reconcile = cutNamingMissing
			f.round(0)
			f.reconcile = cutListingNothing
		}, steps: 6, want: true},
		{name: "sent by this process, the set read carrying it", members: []string{m}, setup: func(f *unlistedFixture) {
			f.reconcile = failing
			f.round(0)
			f.cache.Acknowledged([]contract.TriggerEventV1{abnormal(keyA, m)})
		}, want: true},
		{name: "an ABNORMAL after the recovery", setup: func(f *unlistedFixture) {
			f.reconcile = failing
			f.round(0)
			f.cache.Acknowledged([]contract.TriggerEventV1{abnormal(keyA, m)})
		}, later: func(t *testing.T, f *unlistedFixture) {
			f.cache.Acknowledged([]contract.TriggerEventV1{abnormal(keyA, m)})
		}, want: true},
		{name: "named missing by a current calibration", setup: func(f *unlistedFixture) {
			f.reconcile = completeNamingMissing
			f.round(0)
		}, want: true},
		// A complete calibration after the RECOVERY that still names the
		// alert active gives it another grace: once that calibration is past
		// its age, what it named missing answers held again.
		{name: "named missing by a complete calibration after the recovery, then failing", setup: func(f *unlistedFixture) {
			f.reconcile = failing
			f.round(0)
			f.cache.Acknowledged([]contract.TriggerEventV1{abnormal(keyA, m)})
		}, later: func(t *testing.T, f *unlistedFixture) {
			f.reconcile = completeNamingMissing
			// Past the wait between two reconcile attempts of a key.
			f.round(3 * time.Minute)
			if !f.cache.Snapshot(keyA).Calibrated {
				t.Fatalf("the complete calibration did not land (snapshot %+v)", f.cache.Snapshot(keyA))
			}
			f.reconcile = failing
		}, steps: 4, want: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newUnlistedFixture(t)
			f.members = c.members
			c.setup(f)
			if !f.cache.Contains(keyA.TenantID, keyA.StrategyID, m) {
				t.Fatalf("before the RECOVERY the gate holds %s (snapshot %+v)", m, f.cache.Snapshot(keyA))
			}
			f.cache.Acknowledged([]contract.TriggerEventV1{recovery(keyA, m)})
			// Within the grace the RECOVERY goes on passing: the alert may
			// stand at another Level, whose own RECOVERY is still to come.
			f.round(time.Hour + time.Minute)
			if !f.cache.Contains(keyA.TenantID, keyA.StrategyID, m) {
				t.Fatalf("within the grace the gate holds %s (snapshot %+v)", m, f.cache.Snapshot(keyA))
			}
			for elapsed := time.Hour + time.Minute; elapsed <= grace; elapsed += time.Hour + time.Minute {
				f.round(time.Hour + time.Minute)
			}
			if c.later != nil {
				c.later(t, f)
			}
			steps := c.steps
			if steps == 0 {
				steps = 2
			}
			for step := 1; step <= steps; step++ {
				f.round(time.Hour + time.Minute)
				if got := f.cache.Contains(keyA.TenantID, keyA.StrategyID, m); got != c.want {
					t.Fatalf("step %d past the grace the gate answers %v, want %v (snapshot %+v)", step, got, c.want, f.cache.Snapshot(keyA))
				}
			}
		})
	}
}

// A current calibration answers for what it names whatever the grace, and
// what it named missing goes on answering held past its age: only another
// answer of the link lifts it, never time.
func TestACurrentCalibrationAnswersPastTheRecoveryGrace(t *testing.T) {
	const m = "m9"
	options := indexOptions(&clock{})
	f := newUnlistedFixture(t)
	f.members = nil
	f.reconcile = func() (Reconciliation, error) {
		return Reconciliation{EventSourceID: "source", Members: []string{m}, Missing: []string{m},
			Alerts: []Alert{{AlertID: "a9", EventSourceID: "source", Fingerprint: m}}}, nil
	}
	f.round(0)
	f.reconcile = func() (Reconciliation, error) { return Reconciliation{}, errors.New("reconcile: link answered 503") }
	f.cache.Acknowledged([]contract.TriggerEventV1{recovery(keyA, m)})
	f.round(options.ReconcileInterval + time.Minute)
	if !f.cache.Contains(keyA.TenantID, keyA.StrategyID, m) {
		t.Fatalf("past the grace, within the calibration's age, the gate holds %s (snapshot %+v)", m, f.cache.Snapshot(keyA))
	}
	for step := 0; step < 3; step++ {
		f.round(options.CalibrationMaxAge)
		if !f.cache.Contains(keyA.TenantID, keyA.StrategyID, m) {
			t.Fatalf("past the calibration's age (step %d) the gate let go of %s, which the link named active (snapshot %+v)", step, m, f.cache.Snapshot(keyA))
		}
	}
}

// Without a RECOVERY the broker acknowledged, nothing is let go: an alert
// this process opened, with no calibration and the set read without it,
// answers held however long that lasts. Only an acknowledged RECOVERY a grace
// ago starts the release.
func TestNoAcknowledgedRecoveryNoRelease(t *testing.T) {
	const m = "m9"
	grace := indexOptions(&clock{}).ReconcileInterval
	f := newUnlistedFixture(t)
	f.members = nil
	f.reconcile = func() (Reconciliation, error) { return Reconciliation{}, errors.New("reconcile: link answered 503") }
	f.round(0)
	f.cache.Acknowledged([]contract.TriggerEventV1{abnormal(keyA, m)})
	for elapsed := time.Duration(0); elapsed <= 3*grace; elapsed += time.Hour + time.Minute {
		f.round(time.Hour + time.Minute)
		if !f.cache.Contains(keyA.TenantID, keyA.StrategyID, m) {
			t.Fatalf("%v with no acknowledged RECOVERY the gate let go of %s (snapshot %+v)", elapsed, m, f.cache.Snapshot(keyA))
		}
	}
}

// A current calibration answers for what this process sent after it began:
// an ABNORMAL after the calibration's start, then a RECOVERY a grace and
// more ago, the calibration still within its age, keeps answering held.
// The grace lets go of this process's record only where no current
// calibration answers.
func TestAnAbnormalAfterACurrentCalibrationIsHeldPastTheRecoveryGrace(t *testing.T) {
	const m = "m9"
	options := indexOptions(&clock{})
	f := newUnlistedFixture(t)
	f.members = nil
	f.reconcile = func() (Reconciliation, error) { return Reconciliation{EventSourceID: "source"}, nil }
	f.round(0)
	if !f.cache.Snapshot(keyA).Calibrated {
		t.Fatalf("the calibration did not land (snapshot %+v)", f.cache.Snapshot(keyA))
	}
	f.c.advance(time.Minute)
	f.cache.Acknowledged([]contract.TriggerEventV1{abnormal(keyA, m)})
	f.c.advance(time.Minute)
	f.cache.Acknowledged([]contract.TriggerEventV1{recovery(keyA, m)})
	// No round: nothing calibrates again, and the one that landed stays
	// current until its age.
	f.c.advance(options.ReconcileInterval + 30*time.Minute)
	if !f.cache.Snapshot(keyA).Calibrated {
		t.Fatalf("the calibration is past its age (snapshot %+v)", f.cache.Snapshot(keyA))
	}
	if !f.cache.Contains(keyA.TenantID, keyA.StrategyID, m) {
		t.Fatalf("past the grace, within the calibration's age, the gate let go of %s sent after it began (snapshot %+v)", m, f.cache.Snapshot(keyA))
	}
}
