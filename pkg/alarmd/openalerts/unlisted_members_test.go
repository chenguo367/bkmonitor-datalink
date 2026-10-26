// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"context"
	"errors"
	"testing"
	"time"
)

// unlistedFixture is a copy of one strategy whose set reads members and
// whose reconcile answers reconcile, both changeable between rounds.
type unlistedFixture struct {
	c         *clock
	cache     *Cache
	facts     *factsStub
	members   []string
	readErr   error
	reconcile func() (Reconciliation, error)
}

func newUnlistedFixture(t *testing.T) *unlistedFixture {
	t.Helper()
	f := &unlistedFixture{c: &clock{at: time.Unix(1700000000, 0)}, facts: confirmedFacts(), members: []string{"m1", "m2"}}
	f.reconcile = func() (Reconciliation, error) { return Reconciliation{}, errors.New("reconcile: link answered 503") }
	options := indexOptions(f.c)
	options.Facts = f.facts
	options.Source = setReaderFunc(func(context.Context, StrategyKey) ([]string, error) {
		if f.readErr != nil {
			return nil, f.readErr
		}
		return f.members, nil
	})
	options.Reconciler = reconcilerFunc(func(context.Context, StrategyKey) (Reconciliation, error) { return f.reconcile() })
	f.cache = mustIndex(t, options)
	if err := f.cache.SetTracked([]StrategyKey{keyA}); err != nil {
		t.Fatal(err)
	}
	return f
}

// round moves the clock by d and runs one round of reads and calibrations.
func (f *unlistedFixture) round(d time.Duration) {
	f.c.advance(d)
	f.cache.RequestReconcile(keyA)
	f.cache.Refresh(context.Background())
}

// The index set's members stand in for a strategy's alerts only where the
// link's reconcile gave no usable rows for it and the set's read answers by
// OpenCounts' rule, for a copy whose sets the Console has confirmed. A set
// never read, refused for its size or stale, an untrusted copy, and a
// strategy whose reconcile gave rows - complete or truncated - answer
// nothing, so no close is addressed from them.
func TestUnlistedMembersStandInOnlyWhereTheReconcileGaveNoRows(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(*unlistedFixture)
		want  []string
	}{
		{name: "the reconcile failing, the set read fresh", setup: func(f *unlistedFixture) { f.round(0) }, want: []string{"m1", "m2"}},
		{name: "a truncated reconcile that listed nothing", setup: func(f *unlistedFixture) {
			f.reconcile = func() (Reconciliation, error) { return Reconciliation{EventSourceID: "source", Truncated: true}, nil }
			f.round(0)
		}, want: []string{"m1", "m2"}},
		{name: "the set never read", setup: func(f *unlistedFixture) {
			f.readErr = errors.New("read: connection refused")
			f.round(0)
		}},
		{name: "the set refused for its size", setup: func(f *unlistedFixture) {
			f.round(0)
			f.readErr = ErrSetTooLarge
			f.round(2 * time.Hour)
		}},
		{name: "the set's read stale", setup: func(f *unlistedFixture) {
			f.round(0)
			f.readErr = errors.New("read: connection refused")
			f.round(staleBoundFor(1) + time.Minute)
		}},
		{name: "the sets not confirmed", setup: func(f *unlistedFixture) {
			f.round(0)
			f.facts.mu.Lock()
			f.facts.keyed = false
			f.facts.mu.Unlock()
		}},
		{name: "a complete reconcile with rows", setup: func(f *unlistedFixture) {
			f.reconcile = func() (Reconciliation, error) {
				return Reconciliation{EventSourceID: "source", Members: []string{"m1"}, Alerts: []Alert{{AlertID: "a1", EventSourceID: "source", Fingerprint: "m1"}}}, nil
			}
			f.round(0)
		}},
		{name: "a complete reconcile with no alert", setup: func(f *unlistedFixture) {
			f.members = nil
			f.reconcile = func() (Reconciliation, error) { return Reconciliation{EventSourceID: "source"}, nil }
			f.round(0)
		}},
		{name: "a truncated reconcile that listed an alert", setup: func(f *unlistedFixture) {
			f.reconcile = func() (Reconciliation, error) {
				return Reconciliation{EventSourceID: "source", Truncated: true, Members: []string{"m1"}, Alerts: []Alert{{AlertID: "a1", EventSourceID: "source", Fingerprint: "m1"}}}, nil
			}
			f.round(0)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newUnlistedFixture(t)
			c.setup(f)
			members, ok := f.cache.UnlistedMembers(keyA)
			count, countOK := f.cache.UnlistedMemberCount(keyA)
			held, heldOK := f.cache.UnlistedHolds(keyA, "m1")
			if c.want == nil {
				if ok || countOK || heldOK || members != nil || held {
					t.Fatalf("answered %v %v / %d %v / %v %v; want nothing (snapshot %+v)", members, ok, count, countOK, held, heldOK, f.cache.Snapshot(keyA))
				}
				return
			}
			if !ok || len(members) != len(c.want) || members[0] != c.want[0] || members[1] != c.want[1] || !countOK || count != 2 || !heldOK || !held {
				t.Fatalf("answered %v %v / %d %v / %v %v; want %v (snapshot %+v)", members, ok, count, countOK, held, heldOK, c.want, f.cache.Snapshot(keyA))
			}
			if other, otherOK := f.cache.UnlistedHolds(keyA, "elsewhere"); other || !otherOK {
				t.Fatalf("UnlistedHolds(elsewhere) = %v, %v; want a fingerprint the set does not carry", other, otherOK)
			}
		})
	}
}

// The stand-in follows the set's own reads. An alert a cut reconcile named
// missing from the set, still active then, is not carried on once the set
// is read again: after the link closes it, nothing would take it out, and
// every step would close it again (seen live, once a minute for an hour).
// A set read that still carries a fingerprint keeps answering it.
func TestUnlistedMembersFollowTheSetsReadsNotACutReconcilesMissing(t *testing.T) {
	cut := func(f *unlistedFixture) {
		f.reconcile = func() (Reconciliation, error) {
			return Reconciliation{EventSourceID: "source", Truncated: true, Members: []string{"m3"}, Missing: []string{"m3"},
				Alerts: []Alert{{AlertID: "a3", EventSourceID: "source", Fingerprint: "m3"}}}, nil
		}
		f.round(0)
		f.reconcile = func() (Reconciliation, error) { return Reconciliation{EventSourceID: "source", Truncated: true}, nil }
	}
	t.Run("the set read empty", func(t *testing.T) {
		f := newUnlistedFixture(t)
		f.members = nil
		cut(f)
		for step := 1; step <= 3; step++ {
			// Past IndexInterval: each step reads the set again.
			f.round(time.Hour + time.Minute)
			members, ok := f.cache.UnlistedMembers(keyA)
			held, heldOK := f.cache.UnlistedHolds(keyA, "m3")
			if !ok || len(members) != 0 || !heldOK || held {
				t.Fatalf("step %d answered %v %v / %v %v; want an empty set answering nothing (snapshot %+v)", step, members, ok, held, heldOK, f.cache.Snapshot(keyA))
			}
		}
	})
	t.Run("the set read still carrying it", func(t *testing.T) {
		f := newUnlistedFixture(t)
		f.members = []string{"m3"}
		cut(f)
		f.round(time.Hour + time.Minute)
		members, ok := f.cache.UnlistedMembers(keyA)
		held, heldOK := f.cache.UnlistedHolds(keyA, "m3")
		if !ok || len(members) != 1 || members[0] != "m3" || !heldOK || !held {
			t.Fatalf("answered %v %v / %v %v; want [m3] (snapshot %+v)", members, ok, held, heldOK, f.cache.Snapshot(keyA))
		}
	})
}

// What the stand-in leaves out is counted, not lost: a fingerprint a cut
// reconcile named missing from the set, which the set's last read does not
// carry, answers in UnlistedMissing for as long as the set stands in; one
// the set's read carries is a member and not counted twice. Where the
// reconcile's rows answer, UnlistedMissing answers nothing, as
// UnlistedMembers does: the listing closes a missing alert itself.
func TestUnlistedMissingCountsWhatTheStandInLeavesOut(t *testing.T) {
	named := func(f *unlistedFixture) {
		f.reconcile = func() (Reconciliation, error) {
			return Reconciliation{EventSourceID: "source", Truncated: true, Members: []string{"m3"}, Missing: []string{"m3"},
				Alerts: []Alert{{AlertID: "a3", EventSourceID: "source", Fingerprint: "m3"}}}, nil
		}
		f.round(0)
	}
	t.Run("listed by the cut answer", func(t *testing.T) {
		f := newUnlistedFixture(t)
		f.members = nil
		named(f)
		if count, ok := f.cache.UnlistedMissing(keyA); ok || count != 0 {
			t.Fatalf("UnlistedMissing = %d, %v while the cut answer lists the alert; want nothing", count, ok)
		}
		if alerts, listed := f.cache.ListActiveAlerts(keyA); !listed || len(alerts) != 1 || alerts[0].Fingerprint != "m3" {
			t.Fatalf("ListActiveAlerts = %+v, %v; want m3 listed", alerts, listed)
		}
	})
	t.Run("the set read without it", func(t *testing.T) {
		f := newUnlistedFixture(t)
		f.members = nil
		named(f)
		f.reconcile = func() (Reconciliation, error) { return Reconciliation{EventSourceID: "source", Truncated: true}, nil }
		for step := 1; step <= 3; step++ {
			f.round(time.Hour + time.Minute)
			count, ok := f.cache.UnlistedMissing(keyA)
			members, membersOK := f.cache.UnlistedMembers(keyA)
			if !ok || count != 1 || !membersOK || len(members) != 0 {
				t.Fatalf("step %d: UnlistedMissing = %d, %v, members %v %v; want m3 counted and not a member (snapshot %+v)",
					step, count, ok, members, membersOK, f.cache.Snapshot(keyA))
			}
		}
	})
	t.Run("the set read carrying it", func(t *testing.T) {
		f := newUnlistedFixture(t)
		f.members = []string{"m3"}
		named(f)
		f.reconcile = func() (Reconciliation, error) { return Reconciliation{EventSourceID: "source", Truncated: true}, nil }
		f.round(time.Hour + time.Minute)
		if count, ok := f.cache.UnlistedMissing(keyA); !ok || count != 0 {
			t.Fatalf("UnlistedMissing = %d, %v; want 0, the set's member is closed from the set", count, ok)
		}
	})
}
