// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package openalerts

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// truncatedFixture is a trusted copy of keyA whose first calibration is
// complete and lists only "theirs", with "ours" opened by this process
// afterwards; answer is what every later calibration returns.
type truncatedFixture struct {
	c       *clock
	cache   *Cache
	options IndexOptions
	answer  Reconciliation
}

func newTruncatedFixture(t *testing.T) *truncatedFixture {
	t.Helper()
	f := &truncatedFixture{c: &clock{at: time.Unix(1700000000, 0)}}
	f.options = indexOptions(f.c)
	f.options.Facts = confirmedFacts()
	f.options.Source = setReaderFunc(func(context.Context, StrategyKey) ([]string, error) { return []string{"theirs"}, nil })
	calls := 0
	f.options.Reconciler = reconcilerFunc(func(context.Context, StrategyKey) (Reconciliation, error) {
		calls++
		if calls == 1 {
			return Reconciliation{EventSourceID: "source", Members: []string{"theirs"},
				Alerts: []Alert{{AlertID: "t", EventSourceID: "source", Fingerprint: "theirs"}}}, nil
		}
		return f.answer, nil
	})
	f.cache = mustIndex(t, f.options)
	if err := f.cache.SetTracked([]StrategyKey{keyA}); err != nil {
		t.Fatal(err)
	}
	f.cache.Refresh(context.Background())
	f.cache.Acknowledged([]contract.TriggerEventV1{abnormal(keyA, "ours")})
	return f
}

// recalibrate moves the clock by d and has the next round calibrate keyA.
func (f *truncatedFixture) recalibrate(d time.Duration) {
	f.c.advance(d)
	f.cache.indexChanged(keyA)
	f.cache.RequestReconcile(keyA)
	f.cache.Refresh(context.Background())
}

// listedOnly is a cut answer that lists one alert of ours, "listed", and
// neither "theirs" nor "ours".
func listedOnly() Reconciliation {
	return Reconciliation{EventSourceID: "source", Truncated: true, Members: []string{"listed"},
		Alerts: []Alert{{AlertID: "l", EventSourceID: "source", Fingerprint: "listed"}}}
}

// A cut calibration lists what it received for the closes to act on, and
// takes nothing it does not list as gone: the alert this process opened stays
// in its record, where the same answer complete would prune it.
func TestACutCalibrationListsWhatItReadAndPrunesNothing(t *testing.T) {
	for _, tc := range []struct {
		name      string
		truncated bool
		wantOwned int
	}{
		{"cut: kept", true, 1},
		{"complete: pruned", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTruncatedFixture(t)
			f.answer = listedOnly()
			f.answer.Truncated = tc.truncated
			f.recalibrate(f.options.LocalRetention + time.Second)
			stats := f.cache.Stats()
			if stats.OwnOpen != tc.wantOwned {
				t.Fatalf("own open %d, want %d (departures %v)", stats.OwnOpen, tc.wantOwned, stats.OwnOpenDepartures)
			}
			wantTruncated, wantAuthoritative := uint64(0), uint64(2)
			if tc.truncated {
				wantTruncated, wantAuthoritative = 1, 1
			}
			if stats.Refreshes["truncated"] != wantTruncated || stats.Refreshes["authoritative"] != wantAuthoritative {
				t.Fatalf("refreshes %v, want truncated %d and authoritative %d", stats.Refreshes, wantTruncated, wantAuthoritative)
			}
			alerts := f.cache.ActiveAlerts(keyA)
			if len(alerts) != 1 || alerts[0].AlertID != "l" {
				t.Fatalf("active alerts %+v, want the one the answer listed", alerts)
			}
		})
	}
}

// Once the complete calibration has aged out, a cut one still lets the
// closes act: it lists its alerts, and judges held for what it lists. What it
// does not list is not judged at all - not "not held" - so nothing that acts
// only on judged answers ends or forgets it. A complete calibration of the
// same rows would judge both.
func TestACutCalibrationJudgesOnlyWhatItLists(t *testing.T) {
	f := newTruncatedFixture(t)
	f.answer = listedOnly()
	f.recalibrate(f.options.CalibrationMaxAge + time.Second)
	if held, judged := f.cache.Holds(keyA, "listed"); !held || !judged {
		t.Fatalf("Holds(listed) = %v, %v; want a judged member", held, judged)
	}
	for _, fingerprint := range []string{"ours", "elsewhere"} {
		if held, judged := f.cache.Holds(keyA, fingerprint); held || judged {
			t.Fatalf("Holds(%s) = %v, %v; a cut answer judged what it did not list", fingerprint, held, judged)
		}
	}
	if alerts := f.cache.ActiveAlerts(keyA); len(alerts) != 1 || alerts[0].AlertID != "l" {
		t.Fatalf("active alerts %+v, want the listed one", alerts)
	}

	// The same rows complete: the set is the link's word again, both ways.
	f.answer.Truncated = false
	f.recalibrate(f.options.LocalRetention + time.Second)
	if held, judged := f.cache.Holds(keyA, "elsewhere"); held || !judged {
		t.Fatalf("after a complete calibration Holds(elsewhere) = %v, %v; want a judged non-member", held, judged)
	}

	// A cut answer past its own age lists nothing and judges nothing.
	g := newTruncatedFixture(t)
	g.answer = listedOnly()
	g.recalibrate(g.options.CalibrationMaxAge + time.Second)
	g.c.advance(g.options.CalibrationMaxAge + time.Second)
	if alerts := g.cache.ActiveAlerts(keyA); alerts != nil {
		t.Fatalf("an aged cut calibration still listed %+v", alerts)
	}
	if _, judged := g.cache.Holds(keyA, "listed"); judged {
		t.Fatal("an aged cut calibration still judged")
	}
}

// A cut calibration only adds: a fingerprint the set already carried stays,
// one it lists missing from the link's set still answers held, and one an
// earlier calibration suppressed and this one lists active is no longer
// suppressed.
func TestACutCalibrationOnlyAddsToTheSets(t *testing.T) {
	f := newTruncatedFixture(t)
	f.answer = Reconciliation{EventSourceID: "source", Members: []string{"theirs"}, Suppressed: []string{"revived"}}
	f.recalibrate(f.options.LocalRetention + time.Second)
	f.answer = Reconciliation{EventSourceID: "source", Truncated: true, Members: []string{"revived", "gone-from-set"},
		Missing: []string{"gone-from-set"}, Alerts: []Alert{
			{AlertID: "r", EventSourceID: "source", Fingerprint: "revived"},
			{AlertID: "g", EventSourceID: "source", Fingerprint: "gone-from-set"}}}
	f.recalibrate(f.options.CalibrationMaxAge + time.Second)
	f.cache.mu.Lock()
	entry := f.cache.index.entries[keyA]
	_, theirs := entry.index["theirs"]
	_, suppressed := entry.suppressed["revived"]
	f.cache.mu.Unlock()
	if !theirs || suppressed {
		t.Fatalf("theirs kept %v, revived still suppressed %v; a cut answer took something out or left a listed alert hidden", theirs, suppressed)
	}
	for _, fingerprint := range []string{"revived", "gone-from-set"} {
		if held, judged := f.cache.Holds(keyA, fingerprint); !held || !judged {
			t.Fatalf("Holds(%s) = %v, %v; want a judged member", fingerprint, held, judged)
		}
	}
}
