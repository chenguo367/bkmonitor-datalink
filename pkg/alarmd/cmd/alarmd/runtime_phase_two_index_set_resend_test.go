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
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"
)

// standInClosedOnce opens the bundle with A1 in the strategy's set and A2
// named missing_redis, lets the reconcile fail past a calibration's age with
// the strategy out of its window, and waits until the set has stood in and
// A1 was closed from it, once. A2 is never closed from the set and is
// counted evidence_unavailable on every step, which is how the case knows
// a step ran.
func standInClosedOnce(t *testing.T) *closePathsFixture {
	t.Helper()
	fixture := startClosePathsFixture(t)
	alert := closePathsAlert{revision: 1, family: "metric_algorithm", severity: "critical", dimensions: thresholdDimensions(true)}
	fixture.console.set(true, []closePathsRow{
		{fingerprint: closePathsA1, status: "matched", listed: true},
		{fingerprint: closePathsA2, status: "missing_redis", listed: true},
	}, map[string]closePathsAlert{closePathsA1: alert, closePathsA2: alert})
	fixture.setMembers(closePathsA1)
	fixture.waitFor("the copy lists the strategy's alerts", func() bool {
		_, listed := fixture.listed()
		return listed
	}, func() string { return "not listed" })
	fixture.console.fail(true)
	fixture.waitFor("the copy is refused a calibration", func() bool {
		fixture.copy.RequestCalibration()
		return fixture.console.refusedToCopy() > 0
	}, func() string { return "no refused reconcile" })
	// Out of the window, which ends 20 minutes after start, and past the
	// age of the calibration taken at start.
	fixture.life.clock.Store(fixture.start.Add(61 * time.Minute).UnixMilli())
	fixture.waitFor("the set's member is closed from the set", func() bool {
		return fixture.maintenanceCounts()[closeOutcomeIndexSetAcked] >= 1
	}, func() string { return "no index-set close" })
	if closes := fixture.closes(); len(closes) != 1 || closes[0].Fingerprint != closePathsA1 {
		t.Fatalf("the set stood in with %+v, want A1 alone", closes)
	}
	return fixture
}

// stepsRun waits until the inactive close has run n more steps.
func (fixture *closePathsFixture) stepsRun(n int) {
	fixture.t.Helper()
	from := fixture.missingUnlisted()
	fixture.waitFor(strconv.Itoa(n)+" more steps", func() bool {
		return fixture.missingUnlisted() >= from+n
	}, func() string { return strconv.Itoa(fixture.missingUnlisted()-from) + " steps" })
}

// A close addressed from the index set is not sent again until a read of
// the set lands after it. Live, a restart closed 48 members from the set
// and sent all 48 again a minute later: the record of the send was let go
// after exactly a minute, and the copy had not read the set since, so it
// still listed them. Here no read can land after the send - the set's key
// no longer reads as a set - and two steps more than a minute apart each,
// inside the set's stale bound, send nothing again.
func TestAnIndexSetCloseIsNotSentAgainBeforeTheSetIsReadAgain(t *testing.T) {
	fixture := standInClosedOnce(t)
	key := lifecycleLinkPrefix + ":" + closePathsTenant + ":" + lifecycleStrategyID
	ctx := context.Background()
	if err := fixture.life.redis.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.life.redis.Set(ctx, key, "not a set", 0).Err(); err != nil {
		t.Fatal(err)
	}
	read := fixture.copy.Snapshot(closePathsKey()).IndexReadAt
	stale := fixture.copy.Snapshot(closePathsKey()).StaleAfter
	for step := 1; step <= 2; step++ {
		fixture.life.clock.Store(fixture.life.clock.Load() + (61 * time.Second).Milliseconds())
		if now := time.UnixMilli(fixture.life.clock.Load()); now.Sub(read) >= stale {
			t.Fatalf("step %d is past the set's stale bound %s, where the set no longer stands in", step, stale)
		}
		fixture.stepsRun(2)
		if closes := fixture.closes(); len(closes) != 1 {
			t.Fatalf("%d minute(s) on, with no read of the set since, the inactive close sent %+v, want A1 once", step, closes)
		}
		// The hold is counted, not silent, and once per send held, not
		// once per step.
		if held := fixture.maintenanceCounts()[closeOutcomeAlreadySent]; held != 1 {
			t.Fatalf("%d minute(s) on, close_already_sent = %d, want the one send held counted once", step, held)
		}
		if after := fixture.copy.Snapshot(closePathsKey()).IndexReadAt; !after.Equal(read) {
			t.Fatalf("a read of the set landed at %s, after the one at %s; the case needs none", after, read)
		}
	}
}

// readAfter has the link say the set changed, as it does on writing it,
// and waits until the copy has read the set after before.
func (fixture *closePathsFixture) readAfter(before time.Time) time.Time {
	fixture.t.Helper()
	notice, _ := json.Marshal(map[string]string{"bk_tenant_id": closePathsTenant, "strategy_id": lifecycleStrategyID})
	if err := fixture.life.redis.Publish(context.Background(), lifecycleLinkPrefix+":changes", notice).Err(); err != nil {
		fixture.t.Fatal(err)
	}
	fixture.waitFor("the copy reads the set again", func() bool {
		return fixture.copy.Snapshot(closePathsKey()).IndexReadAt.After(before)
	}, func() string { return fixture.copy.Snapshot(closePathsKey()).IndexReadAt.String() })
	return fixture.copy.Snapshot(closePathsKey()).IndexReadAt
}

// Reads of the set that land after the send and still carry the member are
// no evidence the alert is still open: the set's copy lags the link's
// processing of the close by over a minute. Live, releasing on such a read
// sent each of a strategy's 138 members three times, about 38 s apart.
// Three reads 38 s apart, each still carrying A1, send nothing again; the
// hold is counted once. Then a read without A1 lets the send go and sends
// nothing.
func TestAnIndexSetCloseIsHeldThroughReadsThatStillCarryTheMemberInTheBundle(t *testing.T) {
	fixture := standInClosedOnce(t)
	read := fixture.copy.Snapshot(closePathsKey()).IndexReadAt
	for n := 1; n <= 3; n++ {
		fixture.life.clock.Store(fixture.life.clock.Load() + (38 * time.Second).Milliseconds())
		read = fixture.readAfter(read)
		fixture.stepsRun(2)
		if closes := fixture.closes(); len(closes) != 1 {
			t.Fatalf("after read %d still carrying A1, the inactive close sent %+v, want A1 once", n, closes)
		}
	}
	if held := fixture.maintenanceCounts()[closeOutcomeAlreadySent]; held != 1 {
		t.Fatalf("close_already_sent = %d, want the one send held counted once", held)
	}
	fixture.setMembers()
	fixture.life.clock.Store(fixture.life.clock.Load() + (38 * time.Second).Milliseconds())
	fixture.readAfter(read)
	fixture.stepsRun(2)
	if closes := fixture.closes(); len(closes) != 1 {
		t.Fatalf("after a read without A1, the inactive close sent %+v, want A1 once", closes)
	}
}
