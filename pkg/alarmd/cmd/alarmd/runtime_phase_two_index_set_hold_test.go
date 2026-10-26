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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

// indexSetHoldFixture is an inactive Plan whose reconcile fails and whose
// set stands in with our member, its members changeable between reads, at
// 18:00, outside the 09:00-17:00 window for longer than the set's stale
// bound (three times an hour's interval plus an hour's cycle here).
type indexSetHoldFixture struct {
	*maintenanceTestFixture
	mu      sync.Mutex
	members []string
}

func newIndexSetHoldFixture(t *testing.T) *indexSetHoldFixture {
	t.Helper()
	f := &indexSetHoldFixture{members: []string{ourMember}}
	f.maintenanceTestFixture = newMaintenanceTestFixtureFrom(t, maintenanceReadySnapshot, maintenanceTime(18, 0), maintenanceTestIndex{trusted: true,
		reconcileErr: errors.New("reconcile: link answered 503"), membersOf: func() []string {
			f.mu.Lock()
			defer f.mu.Unlock()
			return append([]string(nil), f.members...)
		}})
	return f
}

func (f *indexSetHoldFixture) setMembers(members ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.members = members
}

func (f *indexSetHoldFixture) key() openalerts.StrategyKey {
	return openalerts.StrategyKey{TenantID: "tenant-a", StrategyID: "123"}
}

// readAgain moves the clock past the copy's read interval and has the copy
// read the set, then runs two steps.
func (f *indexSetHoldFixture) readAgain(t *testing.T, d time.Duration) {
	t.Helper()
	f.advance(d)
	f.m.cache.Refresh(context.Background())
	if read := f.m.cache.Snapshot(f.key()).IndexReadAt; !read.Equal(f.now()) {
		t.Fatalf("the set was last read at %s, want a read at %s", read, f.now())
	}
	f.m.step(context.Background())
	f.m.step(context.Background())
}

// A close addressed from the index set is held through reads of the set
// that land after the send and still carry the member: the set's copy lags
// the link's processing of the close by over a minute, and live each member
// was sent three times, about 38 s apart, on those reads. Three such reads
// and six steps send nothing again, and the hold is counted once - per send
// held, not per step.
func TestAnIndexSetCloseIsHeldThroughReadsThatStillCarryTheMember(t *testing.T) {
	f := newIndexSetHoldFixture(t)
	f.m.step(context.Background())
	if sent := sentCloses(f.writer); len(sent) != 1 || sent[0].Fingerprint != ourMember {
		t.Fatalf("sent %+v, want our member closed from the set", sent)
	}
	for read := 1; read <= 3; read++ {
		f.readAgain(t, 61*time.Minute)
		if sent := sentCloses(f.writer); len(sent) != 1 {
			t.Fatalf("after read %d still carrying the member, sent %d closes, want the one", read, len(sent))
		}
	}
	if held := f.m.counts[closeOutcomeAlreadySent]; held != 1 {
		t.Fatalf("close_already_sent = %d over six steps, want the one send held counted once", held)
	}
}

// A read after the send that no longer carries the member lets the send go
// and sends nothing: the set does not name it any more.
func TestAnIndexSetCloseIsForgottenOnceAReadNoLongerCarriesTheMember(t *testing.T) {
	f := newIndexSetHoldFixture(t)
	f.m.step(context.Background())
	if len(sentCloses(f.writer)) != 1 || len(f.m.sent) != 1 {
		t.Fatalf("sent %+v, held %d, want one close sent and held", sentCloses(f.writer), len(f.m.sent))
	}
	f.setMembers()
	f.readAgain(t, 61*time.Minute)
	if len(sentCloses(f.writer)) != 1 || len(f.m.sent) != 0 {
		t.Fatalf("sent %d, held %v, want nothing more sent and the send let go", len(sentCloses(f.writer)), f.m.sent)
	}
}

// Past the set's stale bound since the send, a set read fresh and still
// carrying the member closes it exactly once more, and holds that send in
// turn: at most one repeat per stale bound.
func TestAnIndexSetCloseIsSentOnceMorePastTheSetsStaleBound(t *testing.T) {
	f := newIndexSetHoldFixture(t)
	f.m.step(context.Background())
	sentAt := f.now()
	stale := f.m.cache.Snapshot(f.key()).StaleAfter
	if stale < 3*time.Hour {
		t.Fatalf("the set's stale bound is %s, want the fixture's 6h", stale)
	}
	for f.now().Sub(sentAt) < stale {
		f.readAgain(t, 61*time.Minute)
		if f.now().Sub(sentAt) < stale && len(sentCloses(f.writer)) != 1 {
			t.Fatalf("%s after the send, inside the stale bound %s, sent %d closes", f.now().Sub(sentAt), stale, len(sentCloses(f.writer)))
		}
	}
	if sent := sentCloses(f.writer); len(sent) != 2 || sent[1].Fingerprint != ourMember {
		t.Fatalf("past the stale bound, sent %+v, want our member once more", sent)
	}
	f.readAgain(t, 61*time.Minute)
	if len(sentCloses(f.writer)) != 2 || f.m.counts[closeOutcomeAlreadySent] != 2 {
		t.Fatalf("sent %d, close_already_sent %d; want the repeat held in turn and each send held counted once",
			len(sentCloses(f.writer)), f.m.counts[closeOutcomeAlreadySent])
	}
}

// A send of a strategy whose Plan this process no longer tracks - its
// Query Group released here - is let go at the next step.
func TestAHeldSendIsForgottenWhenItsPlanIsReleased(t *testing.T) {
	f := newIndexSetHoldFixture(t)
	f.m.step(context.Background())
	if len(f.m.sent) != 1 {
		t.Fatalf("held %d, want the send held", len(f.m.sent))
	}
	f.m.bundle.mu.Lock()
	f.m.bundle.runners = nil
	f.m.bundle.mu.Unlock()
	f.m.step(context.Background())
	if len(f.m.sent) != 0 {
		t.Fatalf("held %v after the Plan was released, want nothing", f.m.sent)
	}
}

// At the bound of the memory of sends a close is not sent: one the memory
// could not take would go out again on every step, silently. With room
// for two, of three members two are sent and the third is held, counted
// once for the step; once a send is let go - its member gone from a read -
// the third is sent.
func TestACloseTheMemoryOfSendsHasNoRoomForWaitsCounted(t *testing.T) {
	third := strings.Repeat("d", 32)
	f := newIndexSetHoldFixture(t)
	f.m.capacity.LocalEntries = 2
	f.setMembers(ourMember, foreignMember, third)
	f.readAgain(t, 61*time.Minute)
	sent := sentCloses(f.writer)
	if len(sent) != 2 || sent[0].Fingerprint != ourMember || sent[1].Fingerprint != foreignMember {
		t.Fatalf("sent %+v, want the two the memory has room for", sent)
	}
	if full := f.m.counts[closeOutcomeSentMemoryFull]; full != 2 {
		t.Fatalf("sent_memory_full = %d over two steps, want the Plan counted once each step", full)
	}
	f.setMembers(ourMember, third)
	f.readAgain(t, 61*time.Minute)
	if sent := sentCloses(f.writer); len(sent) != 3 || sent[2].Fingerprint != third {
		t.Fatalf("sent %+v, want the third sent once a send was let go", sent)
	}
}

// Listed closes share the memory of sends and its bound: with room for
// two, of three listed alerts two are sent and the third waits, counted.
// Once the link has processed the two and a calibration no longer lists
// them, their sends are let go and the third is sent.
func TestAListedCloseTheMemoryOfSendsHasNoRoomForWaitsCounted(t *testing.T) {
	alerts := []openalerts.Alert{
		{AlertID: "a1", EventSourceID: "native", Severity: "critical", Fingerprint: strings.Repeat("a", 32)},
		{AlertID: "a2", EventSourceID: "native", Severity: "critical", Fingerprint: strings.Repeat("b", 32)},
		{AlertID: "a3", EventSourceID: "native", Severity: "critical", Fingerprint: strings.Repeat("c", 32)},
	}
	var mu sync.Mutex
	listed := alerts
	f := newMaintenanceTestFixtureFrom(t, maintenanceReadySnapshot, maintenanceTime(18, 0), maintenanceTestIndex{alerts: alerts,
		alertsOf: func() []openalerts.Alert {
			mu.Lock()
			defer mu.Unlock()
			return append([]openalerts.Alert(nil), listed...)
		}})
	f.m.capacity.LocalEntries = 2
	f.m.step(context.Background())
	if sent := sentCloses(f.writer); len(sent) != 2 || f.m.counts[closeOutcomeSentMemoryFull] != 1 {
		t.Fatalf("sent %+v, counts %v; want two sent and the third counted waiting", sent, f.m.counts)
	}
	mu.Lock()
	listed = alerts[2:]
	mu.Unlock()
	f.advance(61 * time.Minute)
	f.m.cache.Refresh(context.Background())
	f.m.step(context.Background())
	if sent := sentCloses(f.writer); len(sent) != 3 || sent[2].AlertInstanceID != "a3" {
		t.Fatalf("sent %+v, want the third sent once the first sends were let go", sent)
	}
}
