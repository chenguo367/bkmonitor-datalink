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
	"fmt"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// startLocalLeaseFixture is the cutover fixture with the view stream on the
// real clock - the fixture's clock jumps a minute a Slot, and a stream that
// judged the Leader's silence by it would read each jump as a minute
// without a word (StreamClock) - after a second round of its Leader. The
// first, at its start, restated every record's segment skip word and sent
// them without the record's own fields; a Leader runs a round every few
// seconds, and the next reads the records the first wrote.
func startLocalLeaseFixture(t *testing.T) *cutoverStallFixture {
	t.Helper()
	fixture := startLocalLeaseFixtureWith(t, nil)
	readRecordsAgain(t, fixture)
	return fixture
}

// readRecordsAgain runs a round of the fixture's Leader and waits for the
// worker to install what it published, the Query Group's entry with the
// record's own fields.
func readRecordsAgain(t *testing.T, fixture *cutoverStallFixture) {
	t.Helper()
	stream, client := fixture.production.dependencies.ViewStream, fixture.bundle.dependencies.ViewClient
	if err := fixture.production.PublishAssignments(context.Background(), fixture.bundle.queryGroups, fixture.now()); err != nil {
		t.Fatal(err)
	}
	published := stream.Stats().Revision
	testwait.Until(t, "the worker installing the Leader's round", func() bool {
		view, ok := client.Installed()
		return ok && view.Version.Revision == published
	})
	if entry, named := client.Entry(fixture.queryGroup); !named || !entry.Assignment.Complete() {
		t.Fatalf("after the Leader's round the view entry is %+v (named %t), want the record's own fields", entry.Assignment, named)
	}
}

func startLocalLeaseFixtureWith(t *testing.T, configure func(*config.Config)) *cutoverStallFixture {
	t.Helper()
	return openCutoverFixture(t, configure, observability.Discard(observability.ComponentRuntime), nil, nil, func(external *phaseTwoProductionExternalDependencies) {
		external.StreamClock = time.Now
	})
}

// moveToNextSlot moves the fixture's clock to the first round of the Slot
// its Progress record names next, as the fixture runs its own Slots.
func moveToNextSlot(fixture *cutoverStallFixture) {
	next := fixture.progress(context.Background()).NextSlot
	fixture.clock.Store(firstRoundAtMillis(fixture.cfg, fixture.queryGroup, int64(next), 60))
}

// runSteadyRound runs the fixture's Runner for its next Slot, the clock
// moved to it, and reports what the round sent of each family read.
func runSteadyRound(t *testing.T, fixture *cutoverStallFixture, round int, read map[string]func() float64) (map[string]float64, bool) {
	t.Helper()
	moveToNextSlot(fixture)
	before := map[string]float64{}
	for name, reading := range read {
		before[name] = reading()
	}
	sent := len(fixture.events.snapshot())
	result, attempted, err := fixture.runner.RunOne(context.Background())
	if err != nil || !attempted || !result.Completed {
		t.Fatalf("round %d: RunOne = (%+v, %t, %v), want a completed Slot", round, result, attempted, err)
	}
	delta := map[string]float64{}
	for name, reading := range read {
		delta[name] = reading() - before[name]
	}
	return delta, len(fixture.events.snapshot()) == sent
}

// localChecksSaid is every cell of the Runner's ownership check counter that
// is not zero, for a failure to say why the attempts went to the store.
func localChecksSaid(reading scriptReading) string {
	said := ""
	for _, cell := range ownership.OwnershipCheckCells() {
		if count := reading.counter("alarmd_ownership_local_check_total", map[string]string{"outcome": cell[0], "reason": cell[1]}); count != 0 {
			said += fmt.Sprintf(" %s/%s=%v", cell[0], cell[1], count)
		}
	}
	if said == "" {
		return " (none counted)"
	}
	return said
}

func localCheckReading(reading scriptReading, outcome, reason string) func() float64 {
	return func() float64 {
		return reading.counter("alarmd_ownership_local_check_total", map[string]string{"outcome": outcome, "reason": reason})
	}
}

// A steady round - its Slot due, nothing sent, nothing written that the
// fence does not guard itself - of a Query Group this worker holds under its
// session asks the store nothing about its ownership, while the view it has
// installed is fresh and names it desired: the Runner's fence check is
// answered from the session and the view (015 R1). It opened every attempt
// with one, a tenth to a quarter of the Redis CPU of a fleet of small Query
// Groups.
//
// The production bundle against Redis, its control leader publishing the
// view the worker installs; the rounds run one by one on the fixture's
// clock, so every script the round sends is the round's.
func TestASteadyRoundAsksTheStoreNothingAboutItsOwnership(t *testing.T) {
	fixture := startLocalLeaseFixture(t)
	reading := scriptReading{t, fixture.recorder}
	read := map[string]func() float64{
		"checks": func() float64 {
			return reading.counter("alarmd_redis_script_total", map[string]string{"script": "ownership_check_fence"})
		},
		"local": localCheckReading(reading, "local_pass", "local_pass"),
	}
	steady := 0
	for round := 1; round <= 8; round++ {
		delta, quiet := runSteadyRound(t, fixture, round, read)
		// The first rounds settle the Query Group's markers; a round that
		// sent events wrote what the fence check before it guards.
		if round <= 3 || !quiet {
			continue
		}
		steady++
		if delta["checks"] != 0 || delta["local"] != 1 {
			t.Errorf("steady round %d sent %v ownership fence checks and admitted %v locally, want none and one: "+
				"the session and the view answer the Runner; checks so far:%s", round, delta["checks"], delta["local"], localChecksSaid(reading))
		}
	}
	if steady < 3 {
		t.Fatalf("fixture: %d steady rounds of 5, want three at least", steady)
	}
}

// A write whose own fence refuses the lease the view admitted - another
// owner holds it, and the view has not said so yet - ends the round as an
// ownership rejection, and the next attempt checks its fence with the
// store, whose answer ends the session's admission as it always has: one
// round of a view behind the store, not one a round until the view catches
// up (015 R1, section 4).
func TestARoundAfterAWritesFenceRefusedTheLeaseAsksTheStore(t *testing.T) {
	fixture := startLocalLeaseFixture(t)
	ctx := context.Background()
	reading := scriptReading{t, fixture.recorder}
	checks := func() float64 {
		return reading.counter("alarmd_redis_script_total", map[string]string{"script": "ownership_check_fence"})
	}
	local := map[string]func() float64{"local": localCheckReading(reading, "local_pass", "local_pass")}
	admitted := false
	for round := 1; round <= 8 && !admitted; round++ {
		delta, quiet := runSteadyRound(t, fixture, round, local)
		admitted = round > 3 && quiet && delta["local"] == 1
	}
	if !admitted {
		t.Fatalf("fixture: no round was admitted locally, so none can be refused after one; checks:%s", localChecksSaid(reading))
	}
	// Another owner's token on the lease: the store's word, not the view's.
	keys, err := fixture.redisClient.Keys(ctx, "*{"+ownership.ControlHashTag(fixture.queryGroup)+"}:ownership").Result()
	if err != nil || len(keys) != 1 {
		t.Fatalf("ownership keys of %s = %v (%v), want one", fixture.queryGroup, keys, err)
	}
	if err := fixture.redisClient.HSet(ctx, keys[0], "lease_token", "another-owners-token").Err(); err != nil {
		t.Fatal(err)
	}
	refusedLocally := localCheckReading(reading, "fallback_redis", "fence_refused")
	moveToNextSlot(fixture)
	before, beforeRefused := checks(), refusedLocally()
	if _, attempted, err := fixture.runner.RunOne(ctx); attempted || !errors.Is(err, ownership.ErrStaleFence) {
		t.Fatalf("the round the write's fence refused: attempted=%t err=%v, want the ownership rejection", attempted, err)
	}
	if asked := checks() - before; asked != 0 {
		t.Fatalf("the round the view admitted asked the store %v fence checks before its write, want none", asked)
	}
	before = checks()
	_, attempted, err := fixture.runner.RunOne(ctx)
	if attempted || !errors.Is(err, ownership.ErrStaleFence) {
		t.Fatalf("the round after it: attempted=%t err=%v, want the store's refusal", attempted, err)
	}
	if asked, refused := checks()-before, refusedLocally()-beforeRefused; asked != 1 || refused != 1 {
		t.Fatalf("the round after it asked the store %v fence checks under fence_refused %v times, want one of each", asked, refused)
	}
	if fixture.session.Alive(fixture.now()) {
		t.Fatal("the session still admits work after the store refused its fence")
	}
}

// A record the Leader's round wrote after reading it is published, through
// the Leader's own path to the worker's installed view, without the record's
// own fields: they are the read record's, not the one the store holds, and
// the local check would admit against them. The worker's next attempt checks
// its fence with the store until the next round reads the record (015 R1).
// A move is told by its owner (viewAssignmentOf); this is the record
// rewritten in place, its owner the record's own - a restated segment skip
// word, which TestARoundThatRewroteItsRecordsSendsThemWithoutTheirFieldsUntilTheNextRead
// runs through the Leader's whole round.
func TestARoundAfterTheLeaderRewroteItsRecordAsksTheStore(t *testing.T) {
	fixture := startLocalLeaseFixture(t)
	ctx := context.Background()
	reading := scriptReading{t, fixture.recorder}
	checks := func() float64 {
		return reading.counter("alarmd_redis_script_total", map[string]string{"script": "ownership_check_fence"})
	}
	local := map[string]func() float64{"local": localCheckReading(reading, "local_pass", "local_pass")}
	admitted := false
	for round := 1; round <= 8 && !admitted; round++ {
		delta, quiet := runSteadyRound(t, fixture, round, local)
		admitted = round > 3 && quiet && delta["local"] == 1
	}
	if !admitted {
		t.Fatalf("fixture: no round was admitted locally, so none can be sent to the store after one; checks:%s", localChecksSaid(reading))
	}

	production := fixture.production
	worker := fixture.cfg.PhaseTwo.Worker.ID
	records := map[execution.QueryGroupIdentity]ownership.AssignmentRecord{}
	for _, identity := range fixture.bundle.queryGroups {
		record, err := production.dependencies.Store.ReadAssignment(ctx, identity)
		if err != nil {
			t.Fatal(err)
		}
		records[identity] = record
	}
	if records[fixture.queryGroup].DesiredWorkerID != worker {
		t.Fatalf("the record read names %s, want this worker %s", records[fixture.queryGroup].DesiredWorkerID, worker)
	}
	registration, found, err := production.dependencies.Store.(viewStreamRegistry).ReadWorker(ctx, worker)
	if err != nil || !found || !registration.Declares(ownership.CapabilityViewAssignment) {
		t.Fatalf("this worker's registration = %+v (found %t, %v), want it declaring %s", registration, found, err, ownership.CapabilityViewAssignment)
	}
	authority, err := production.ensureControlAuthority(ctx, fixture.now())
	if err != nil {
		t.Fatal(err)
	}
	stream, client := production.dependencies.ViewStream, fixture.bundle.dependencies.ViewClient
	before := stream.Stats().Revision
	// Rewritten in place: it ends where the record read says, and the store
	// holds the record the round wrote.
	production.publishViewRewritten(ctx, authority, records, map[execution.QueryGroupIdentity]string{fixture.queryGroup: worker},
		map[execution.QueryGroupIdentity]bool{fixture.queryGroup: true}, []ownership.WorkerRegistration{registration}, true)
	published := stream.Stats().Revision
	testwait.Until(t, "the worker installing the Leader's publication", func() bool {
		view, ok := client.Installed()
		return ok && view.Version.Revision == published
	})
	entry, named := client.Entry(fixture.queryGroup)
	if !named || entry.Assignment.DesiredWorkerID != worker || entry.Assignment.Complete() || entry.Assignment.AssignmentGeneration != 0 {
		t.Fatalf("after a rewrite within the round (revision %d to %d) the view entry is %+v (named %t), "+
			"want this worker named without the read record's own fields", before, published, entry.Assignment, named)
	}

	missing := localCheckReading(reading, "fallback_redis", "view_fields_missing")
	read := map[string]func() float64{"checks": checks, "local": local["local"], "missing": missing}
	delta, _ := runSteadyRound(t, fixture, 9, read)
	if delta["checks"] != 1 || delta["missing"] != 1 || delta["local"] != 0 {
		t.Fatalf("the round after the rewrite sent %v fence checks, %v under view_fields_missing and %v admitted locally, "+
			"want the store asked once under view_fields_missing", delta["checks"], delta["missing"], delta["local"])
	}
}

// A round that rewrote its records after reading them - here the first with
// the segment skip asked for, restating every record's word - publishes them
// without the record's own fields: the view would name the read record's
// words and generation beside the store's new ones, and a Runner admitted
// from it would follow a skip word the store no longer holds. Its attempts
// check their fence with the store until the next round reads the records
// it wrote; that round's view carries them, and the attempts are admitted
// locally again. Both rounds are the Leader's own (PublishAssignments).
func TestARoundThatRewroteItsRecordsSendsThemWithoutTheirFieldsUntilTheNextRead(t *testing.T) {
	fixture := startLocalLeaseFixtureWith(t, func(cfg *config.Config) { cfg.PhaseTwo.Control.SegmentSkip = true })
	reading := scriptReading{t, fixture.recorder}
	client := fixture.bundle.dependencies.ViewClient
	entry, named := client.Entry(fixture.queryGroup)
	if !named || entry.Assignment.Complete() {
		t.Fatalf("after the round that restated the skip words the view entry is %+v (named %t), want it without the record's own fields",
			entry.Assignment, named)
	}
	read := map[string]func() float64{
		"local": localCheckReading(reading, "local_pass", "local_pass"), "missing": localCheckReading(reading, "fallback_redis", "view_fields_missing"),
	}
	for round := 1; round <= 2; round++ {
		if delta, _ := runSteadyRound(t, fixture, round, read); delta["local"] != 0 || delta["missing"] != 1 {
			t.Fatalf("round %d after the rewrite admitted %v locally and sent %v to the store under view_fields_missing, want none and one; checks:%s",
				round, delta["local"], delta["missing"], localChecksSaid(reading))
		}
	}
	readRecordsAgain(t, fixture)
	if entry, _ := client.Entry(fixture.queryGroup); entry.Assignment.SegmentSkip == 0 {
		t.Fatalf("after the round that read them the view entry is %+v, want the record's own fields with its skip word", entry.Assignment)
	}
	admitted := false
	for round := 3; round <= 8 && !admitted; round++ {
		delta, quiet := runSteadyRound(t, fixture, round, read)
		admitted = quiet && delta["local"] == 1
	}
	if !admitted {
		t.Fatalf("no round after the records were read again was admitted locally; checks:%s", localChecksSaid(reading))
	}
}
