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
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// missingUnlisted counts the inactive close's evidence_unavailable lines that
// name alerts a reconcile named missing from the index set.
func (fixture *closePathsFixture) missingUnlisted() int {
	life := fixture.life
	life.observationsMu.Lock()
	defer life.observationsMu.Unlock()
	n := 0
	for _, observation := range life.observations {
		if observation.Stage == observability.StageEffectiveTimeMaintenance && string(observation.ReasonCode) == closeOutcomeEvidenceUnavailable &&
			observation.Trace.StrategyID == lifecycleStrategyID && errors.Is(observation.Err, errIndexSetMissingUnlisted) {
			n++
		}
	}
	return n
}

// An alert active in the link's store and missing from the strategy's index
// set - a gap the link's repair has not filled, which its reconcile names
// missing_redis. While a reconcile lists the strategy's alerts, the inactive
// close closes it with the set's member. Once the reconcile fails past a
// calibration's age, the set stands in: its member is closed from it, and
// the missing alert is not - the set does not carry it, and nothing would
// take it out of what the copy remembers while the reconcile fails, so a
// close of it would go out again every step after the link had closed it.
// It is not lost either: each step counts the Plan evidence_unavailable,
// naming the alerts a reconcile named missing.
func TestAnAlertMissingFromTheIndexSetIsClosedListedAndCountedUnlisted(t *testing.T) {
	fixture := startClosePathsFixture(t)
	alert := closePathsAlert{revision: 1, family: "metric_algorithm", severity: "critical", dimensions: thresholdDimensions(true)}
	fixture.console.set(true, []closePathsRow{
		{fingerprint: closePathsA1, status: "matched", listed: true},
		{fingerprint: closePathsA2, status: "missing_redis", listed: true},
	}, map[string]closePathsAlert{closePathsA1: alert, closePathsA2: alert})
	fixture.setMembers(closePathsA1)
	both := sortedStrings(closePathsA1, closePathsA2)
	fixture.waitFor("the copy lists both alerts", func() bool {
		got, listed := fixture.listed()
		return listed && sameStrings(got, both)
	}, func() string {
		got, listed := fixture.listed()
		return strconv.FormatBool(listed) + " " + strconv.Itoa(len(got))
	})

	// Listed: outside the window, both are closed, the missing one too.
	fixture.life.clock.Store(fixture.start.Add(30 * time.Minute).UnixMilli())
	fixture.waitFor("the inactive close closes the listed alerts", func() bool {
		return fixture.maintenanceCounts()[closeOutcomeAcked] >= 2
	}, func() string {
		return "close_acked=" + strconv.FormatUint(fixture.maintenanceCounts()[closeOutcomeAcked], 10)
	})
	listed := fixture.closes()
	if split := byReason(listed); len(listed) != 2 || !sameStrings(fingerprintsOf(split[linkdoutput.CloseReasonInactive]), both) {
		t.Fatalf("listed, the inactive close sent %+v, want A1 and A2", listed)
	}
	if counts := fixture.maintenanceCounts(); counts[closeOutcomeIndexSetAcked] != 0 || fixture.missingUnlisted() != 0 {
		t.Fatalf("listed, the inactive close counted %v and %d missing lines, want neither the set nor the missing word", counts, fixture.missingUnlisted())
	}

	// Unlisted: the set stands in for its own member only.
	fixture.console.fail(true)
	fixture.waitFor("the copy is refused a calibration", func() bool {
		fixture.copy.RequestCalibration()
		return fixture.console.refusedToCopy() > 0
	}, func() string { return "no refused reconcile" })
	fixture.life.clock.Store(fixture.life.clock.Load() + (61 * time.Minute).Milliseconds())
	fixture.waitFor("the set's member is closed and the missing alert counted on three steps", func() bool {
		return fixture.maintenanceCounts()[closeOutcomeIndexSetAcked] >= 1 && fixture.missingUnlisted() >= 3
	}, func() string {
		return "index_set_close_acked=" + strconv.FormatUint(fixture.maintenanceCounts()[closeOutcomeIndexSetAcked], 10) +
			" missing lines=" + strconv.Itoa(fixture.missingUnlisted())
	})
	unlisted := fixture.closes()[len(listed):]
	if len(unlisted) != 1 || unlisted[0].Fingerprint != closePathsA1 || unlisted[0].AlertInstanceID != closePathsA1 {
		t.Fatalf("unlisted, the inactive close sent %+v, want A1 alone, from the set", unlisted)
	}
}
