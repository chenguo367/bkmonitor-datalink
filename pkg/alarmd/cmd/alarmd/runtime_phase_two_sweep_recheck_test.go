// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A retired Query Group whose record a live lease still holds is read again
// by name each round until the lease lapses, not found again by walking the
// key space: on a deployment of half a million keys one retirement with its
// lease live walked the store eight or nine rounds in a row, about a thousand
// SCAN pages each (015 O2). Through the production bundle on a real Redis:
// the term's first round sweeps the key space and finds the record held; the
// rounds after it read that one record (scanned 1, not the store's three)
// until the lease lapses and the record is reclaimed; the sweep counter says
// owed for those rounds and the page counter does not move with them.
func TestARecordALiveLeaseHoldsIsReadAgainByNameNotByWalkingTheStore(t *testing.T) {
	var recordKey, leaseKey string
	fixture := startCutoverFixtureWith(t, nil, observability.Discard(observability.ComponentRuntime), func(ctx context.Context, client *redis.Client, cfg config.Config) {
		// A Query Group nothing publishes any more, its record in the
		// store's key shape and its ownership held by a lease that is live
		// on Redis's clock.
		retired := "retired-query-group-held"
		tag := sha256.Sum256([]byte(retired))
		base := productionPhaseTwoPrefix(cfg.Redis.StatePrefix, "ownership") + ":{" + hex.EncodeToString(tag[:]) + "}"
		recordKey, leaseKey = base+":assignment", base+":ownership"
		now, err := client.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.HSet(ctx, recordKey, "query_group", retired, "desired_worker_id", "worker-of-a-retired-replica", "record_revision", "3").Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.HSet(ctx, leaseKey, "owner_id", "worker-of-a-retired-replica", "owner_epoch", "4",
			"deadline_ms", strconv.FormatInt(now.Add(time.Hour).UnixMilli(), 10)).Err(); err != nil {
			t.Fatal(err)
		}
	})
	ctx := context.Background()
	sweeps := func() []observability.AssignmentSweepFacts {
		var out []observability.AssignmentSweepFacts
		for _, observation := range fixture.observed() {
			if observation.Stage == observability.StageAssignmentSwept && observation.Result == observability.ResultSuccess && observation.AssignmentSweep != nil {
				out = append(out, *observation.AssignmentSweep)
			}
		}
		return out
	}
	// The bundle's own first round swept; the rounds after it are run here
	// the way the reconcile tick runs them, on the bundle's Query Groups.
	waitUntil(t, "the term's first sweep", func() bool { return len(sweeps()) >= 1 })
	fixture.bundle.mu.RLock()
	groups := append([]execution.QueryGroupIdentity(nil), fixture.bundle.queryGroups...)
	fixture.bundle.mu.RUnlock()
	round := func() {
		t.Helper()
		if err := fixture.production.PublishAssignments(ctx, groups, fixture.now()); err != nil {
			t.Fatalf("PublishAssignments() error = %v", err)
		}
	}
	for range 4 {
		round()
	}
	observed := sweeps()
	if len(observed) != 5 {
		t.Fatalf("sweeps = %d after the term's first round and four more, want one a round: each round owes the held record", len(observed))
	}
	first := observed[0]
	if first.Scanned != 3 || first.Retired != 1 || first.HeldByLease != 1 || first.Reclaimed != 0 {
		t.Fatalf("the term's first sweep = %+v, want the store walked (scanned 3: two live records and the held one), the held one left", first)
	}
	for index, sweep := range observed[1:] {
		if sweep.Scanned != 1 || sweep.HeldByLease != 1 || sweep.Reclaimed != 0 {
			t.Fatalf("round %d after the first sweep = %+v, want the held record read again by name (scanned 1), still held: "+
				"a round that walks the store again for one held record costs the key space's pages every round its lease lives",
				index+1, sweep)
		}
	}
	if exists, err := fixture.redisClient.Exists(ctx, recordKey).Result(); err != nil || exists != 1 {
		t.Fatalf("the held record is gone (exists=%d err=%v) before its lease lapsed", exists, err)
	}
	pages := counterValue(t, fixture.recorder, "alarmd_ownership_assignment_sweep_scan_pages_total", nil)
	if term := counterValue(t, fixture.recorder, "alarmd_ownership_assignment_sweeps_total", map[string]string{"trigger": "term", "result": "success"}); term != 1 || pages < 1 {
		t.Fatalf("sweeps{term,success} = %v, scan pages = %v, want the one full sweep of the term and its pages", term, pages)
	}
	if owed := counterValue(t, fixture.recorder, "alarmd_ownership_assignment_sweeps_total", map[string]string{"trigger": "owed", "result": "success"}); owed < 4 {
		t.Fatalf("sweeps{owed,success} = %v, want the rounds that read the held record again counted owed", owed)
	}

	// The lease lapses on Redis's clock: the next round's recheck reclaims
	// the record, still without a page, and the rounds after it stop.
	now, err := fixture.redisClient.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.redisClient.HSet(ctx, leaseKey, "deadline_ms", strconv.FormatInt(now.Add(-time.Second).UnixMilli(), 10)).Err(); err != nil {
		t.Fatal(err)
	}
	round()
	if exists, err := fixture.redisClient.Exists(ctx, recordKey).Result(); err != nil || exists != 0 {
		t.Fatalf("the round after the lease lapsed left the record (exists=%d err=%v), want it reclaimed", exists, err)
	}
	reclaimed := sweeps()
	last := reclaimed[len(reclaimed)-1]
	if last.Scanned != 1 || last.Reclaimed != 1 || last.HeldByLease != 0 {
		t.Fatalf("the sweep that reclaimed the record = %+v, want the recheck by name (scanned 1) reclaiming it", last)
	}
	if after := counterValue(t, fixture.recorder, "alarmd_ownership_assignment_sweep_scan_pages_total", nil); after != pages {
		t.Fatalf("scan pages %v -> %v across the rechecks, want no page: the record was read by name", pages, after)
	}
	settled := len(sweeps())
	for range 5 {
		round()
	}
	if extra := len(sweeps()) - settled; extra != 0 {
		t.Fatalf("%d sweeps ran after the held record was reclaimed, want none: nothing is owed and the set did not change", extra)
	}

	// What each sweep took - the term's walk and every recheck after it - is
	// on its assignment_swept line and in the fleet's last sweep: each one
	// made its round trips, so none of them took 0.
	for index, observation := range fixture.observed() {
		if observation.Stage == observability.StageAssignmentSwept && observation.Result == observability.ResultSuccess &&
			(!observation.DurationKnown || observation.Duration <= 0) {
			t.Fatalf("assignment_swept observation %d says it took %v (known %v), want what the sweep took",
				index, observation.Duration, observation.DurationKnown)
		}
	}
	fixture.production.mu.Lock()
	published := fixture.production.lastAssignmentSweep
	fixture.production.mu.Unlock()
	if published == nil || published.DurationSeconds <= 0 {
		t.Fatalf("the fleet's last sweep = %+v, want what the recheck that reclaimed the record took", published)
	}
}
