package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/readhold"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

type runtimeTestReadHoldControl struct {
	values map[execution.QueryGroupIdentity][]byte
	reads  [][]execution.QueryGroupIdentity
	writes int
}

func (c *runtimeTestReadHoldControl) ReadControlBatch(_ context.Context, groups []execution.QueryGroupIdentity, _ string) ([]ownership.ControlRead, error) {
	c.reads = append(c.reads, append([]execution.QueryGroupIdentity(nil), groups...))
	result := make([]ownership.ControlRead, len(groups))
	for i, qg := range groups {
		raw, found := c.values[qg]
		result[i] = ownership.ControlRead{Raw: append([]byte(nil), raw...), Missing: !found}
	}
	return result, nil
}
func (c *runtimeTestReadHoldControl) FencedCompareAndSet(_ context.Context, r ownership.FencedCASRequest) (ownership.FencedCASStatus, error) {
	c.writes++
	raw, found := c.values[r.Fence.QueryGroup]
	if r.ExpectedMissing == found || !bytes.Equal(r.Expected, raw) {
		return ownership.FencedCASConflict, nil
	}
	c.values[r.Fence.QueryGroup] = append([]byte(nil), r.Value...)
	return ownership.FencedCASApplied, nil
}
func runtimeTestHolds(t *testing.T) (*productionReadHolds, *runtimeTestReadHoldControl, *time.Time) {
	t.Helper()
	at := time.Unix(1800, 0)
	store := &runtimeTestReadHoldControl{values: map[execution.QueryGroupIdentity][]byte{}}
	h := &productionReadHolds{now: func() time.Time { return at }, groups: map[execution.QueryGroupIdentity]*productionReadHoldGroup{}}
	h.cfg.PhaseTwo.Worker.ID = "worker"
	var err error
	h.controller, err = readhold.NewController(readhold.Options{Control: store, Prefix: "schedule", MaxHold: 10 * time.Minute, Now: h.now, Owner: h.owner})
	if err != nil {
		t.Fatal(err)
	}
	return h, store, &at
}
func runtimeTestHoldSchedule(t *testing.T, qg execution.QueryGroupIdentity) execution.FrozenQueryGroupSchedule {
	t.Helper()
	spec := execution.ScheduleSpec{EvaluationIntervalSeconds: 60, Timezone: "UTC", CompletionDeadlineOffsetSeconds: 55}
	rev, _ := execution.DerivePlanScheduleRevision(spec)
	plan := execution.PlanIdentity{TenantID: "tenant", BusinessID: "business", StrategyID: "strategy"}
	plans := []execution.FrozenPlanSchedule{{Identity: plan, Spec: spec, ScheduleRevision: rev}}
	groupRev, _ := execution.DeriveQueryGroupScheduleRevision(plans)
	return execution.FrozenQueryGroupSchedule{Segment: execution.ScheduleSegmentFact{Publication: execution.SnapshotPublicationRef{PublicationEpoch: 1, SnapshotRevision: "snapshot"}, QueryGroup: qg, QueryRevision: "query", ScheduleRevision: groupRev, Start: 60}, Plans: plans}
}
func TestRuntimeOwnedRestoreIsolatesBadGroups(t *testing.T) {
	h, c, _ := runtimeTestHolds(t)
	raw, _ := json.Marshal(readhold.Record{SinceSlot: 1, HoldMillis: 120000})
	c.values["good"] = raw
	c.values["bad"] = []byte(`{"hold_ms":-1,"since_slot":1}`)
	h.restore(context.Background(), []execution.QueryGroupIdentity{"bad", "good", "zero"})
	// The corrupt record restores its group marked, so the next write
	// replaces it -- it would otherwise keep the group unrestored for a week.
	if bad := h.controller.Inspect("bad"); !bad.Loaded || !bad.Corrupt || !h.controller.Inspect("good").Loaded ||
		h.controller.Inspect("good").Corrupt || !h.controller.Inspect("zero").Loaded {
		t.Fatal("a corrupt group invalidated good batch entries, or was not restored marked")
	}
	h.restore(context.Background(), []execution.QueryGroupIdentity{"bad", "good", "zero"})
	if len(c.reads) != 1 {
		t.Fatalf("restored groups were reread: %+v", c.reads)
	}
}
func TestRuntimePreparedZeroHoldDoesNotReadOrCreateKey(t *testing.T) {
	h, c, at := runtimeTestHolds(t)
	qg := execution.QueryGroupIdentity("zero")
	schedule := runtimeTestHoldSchedule(t, qg)
	session, err := ownership.OpenSession(context.Background(), &fakePhaseTwoOwnershipStore{}, qg, "worker", *at, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	h.bind(qg, session)
	h.groups[qg].prepared = schedule.Segment
	spec := readhold.GroupSpec{QueryGroup: qg, Plans: []readhold.PlanRef{{Key: schedule.Plans[0].Key(), Route: "source/metric"}}, SettlingWait: 30 * time.Second, HoldLimit: 10 * time.Minute}
	if err := h.controller.Configure(spec); err != nil {
		t.Fatal(err)
	}
	h.restore(context.Background(), []execution.QueryGroupIdentity{qg})
	lease, _ := session.Current()
	for _, slot := range []execution.EvaluationTime{1200, 1260, 1320} {
		if got, err := h.SlotReadHold(context.Background(), schedule, slot, lease.Fence); err != nil || got != 0 {
			t.Fatalf("prepared h0 read: %s %v", got, err)
		}
	}
	if page := h.groupPage(nil, "", 200); len(page.Groups) != 1 || !page.Groups[0].HoldKnown || page.Groups[0].ReadHoldMillis != 0 {
		t.Fatalf("successfully seeded h0 was not known: %+v", page)
	}
	if len(c.reads) != 1 || c.writes != 0 || len(c.values) != 0 {
		t.Fatalf("h0 normal path made storage calls: reads=%d writes=%d keys=%d", len(c.reads), c.writes, len(c.values))
	}
	*at = at.Add(2 * time.Minute)
	if _, err := h.owner(qg); !errors.Is(err, ownership.ErrStaleFence) {
		t.Fatalf("expired owner still admitted: %v", err)
	}
	h.forget(qg)
	if len(h.groups) != 0 || h.controller.Inspect(qg).Loaded {
		t.Fatal("release kept the owner's controller record")
	}
}
func TestRuntimeLookbackPagerUsesOnlyOwnedMemory(t *testing.T) {
	h, c, at := runtimeTestHolds(t)
	raw, _ := json.Marshal(readhold.Record{SinceSlot: 1, HoldMillis: 120000})
	c.values["good"] = raw
	c.values["bad"] = []byte(`{"hold_ms":-1,"since_slot":1}`)
	for _, qg := range []execution.QueryGroupIdentity{"bad", "good", "zero"} {
		session, err := ownership.OpenSession(context.Background(), &fakePhaseTwoOwnershipStore{}, qg, "worker", *at, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		h.bind(qg, session)
	}
	h.restore(context.Background(), []execution.QueryGroupIdentity{"bad", "good", "zero"})
	handler := withLookbackAPI(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }), nil, lookbackStanding{}, h, h.now)
	request := func(method, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w
	}
	first := request(http.MethodGet, "/api/lookback?limit=1")
	if first.Code != 200 {
		t.Fatalf("GET status %d", first.Code)
	}
	var page lookbackAPIReading
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Total != 3 || page.Next != "bad" || len(page.Groups) != 1 || page.Groups[0].HoldKnown {
		t.Fatalf("bad hold was reported as known: %+v", page)
	}
	second := request(http.MethodGet, "/api/lookback?limit=1&after=bad")
	if err := json.Unmarshal(second.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Groups[0].QueryGroup != "good" || !page.Groups[0].HoldKnown || page.Groups[0].ReadHoldMillis != 120000 {
		t.Fatalf("good page: %+v", page)
	}
	if len(c.reads) != 1 || c.writes != 0 {
		t.Fatal("pager issued storage calls")
	}
	if request(http.MethodGet, "/api/lookback?after="+strings.Repeat("x", 257)).Code != 400 {
		t.Fatal("unbounded cursor accepted")
	}
	op := cliLookbackOperation(nil, lookbackStanding{}, h)
	outcome := op.Run(context.Background(), obchannel.Params{"limit": json.Number("1")})
	if outcome.Complete || len(outcome.Next) != 1 {
		t.Fatal("CLI truncated page gave no next call")
	}
	following := op.Run(context.Background(), outcome.Next[0].Params).Value.(cliLookbackReading)
	if following.ReadHolds.Groups[0].QueryGroup != "good" {
		t.Fatal("CLI cursor reread first page")
	}
	if page := h.groupPage(nil, "good", 200); len(page.Groups) != 1 || page.Groups[0].HoldKnown {
		t.Fatalf("missing, unseeded h0 was reported as known: %+v", page)
	}
	*at = at.Add(2 * time.Minute)
	if h.groupPage(nil, "", 200).Total != 0 || len(h.fleetFacts()) != 0 {
		t.Fatal("expired owner reported a current hold")
	}
	if request(http.MethodPost, "/api/lookback").Code != 405 || request(http.MethodGet, "/api/lookback?limit=201").Code != 400 || request(http.MethodGet, "/other").Code != 418 {
		t.Fatal("method, budget, or route forwarding changed")
	}
}

// A retired group that retires without closing its hold is counted where
// the read hold's counters are exported, and every predecessor reason and
// clamp source is exported at zero beside it.
func TestARetireCloseFailureIsExportedWithTheReadHoldCounters(t *testing.T) {
	h, _, _ := runtimeTestHolds(t)
	h.links = map[string]uint64{}
	engine, err := lookback.New(lookback.Options{Now: h.now,
		Recheck: func(context.Context, execution.PhysicalQuerySpec, execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
			return execution.ProviderCompletion{}, nil
		},
		Permit: func() (func(), <-chan struct{}, string) { return func() {}, nil, "" },
		Owns:   func(execution.QueryGroupIdentity) bool { return true }, Owned: func() int { return 1 }})
	if err != nil {
		t.Fatal(err)
	}
	h.RetireCloseFailed(errors.New("closing failed"))
	stats := productionLookbackStats(engine, h)
	if stats.ReadHoldRetireCloseFailed != 1 {
		t.Fatalf("retire close failures exported %d, want 1", stats.ReadHoldRetireCloseFailed)
	}
	if len(stats.ReadHoldPredecessors) != len(readhold.PredecessorReasons)+len(readhold.LinkSkipReasons) || len(stats.ReadHoldClamped) != len(readhold.ClampSources) {
		t.Fatalf("not every reason and source exported: %v %v", stats.ReadHoldPredecessors, stats.ReadHoldClamped)
	}
}

// The read hold counts by source are the group page's own facts: a corrupt
// or unseeded record is a hold not yet known, a known one is counted held
// when more than none and at its limit when marked so, and the largest
// known one is kept. A group the lookback has not seen is under other; a
// group this process no longer holds is not counted.
func TestRuntimeReadHoldGroupsCountWhatTheGroupPageKnows(t *testing.T) {
	h, c, at := runtimeTestHolds(t)
	held, _ := json.Marshal(readhold.Record{SinceSlot: 1, HoldMillis: 120000})
	limited, _ := json.Marshal(readhold.Record{SinceSlot: 1, HoldMillis: 600000, AtLimit: true, LimitMillis: 600000})
	// A change chosen and not yet frozen: the hold the next Slot is frozen with.
	pending := int64(90000)
	raised, _ := json.Marshal(readhold.Record{SinceSlot: 1, HoldMillis: 0, PendingHoldMillis: &pending})
	c.values["good"], c.values["limited"], c.values["raised"] = held, limited, raised
	c.values["bad"] = []byte(`{"hold_ms":-1,"since_slot":1}`)
	groups := []execution.QueryGroupIdentity{"bad", "good", "limited", "raised", "zero"}
	for _, qg := range groups {
		session, err := ownership.OpenSession(context.Background(), &fakePhaseTwoOwnershipStore{}, qg, "worker", *at, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		h.bind(qg, session)
	}
	h.restore(context.Background(), groups)
	counts := h.groupsBySource(nil)
	want := map[string]lookback.ReadHoldGroups{lookback.SourceOther: {Held: 3, AtLimit: 1, Unknown: 2, MaxMillis: 600000, MaxKnown: true}}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("counts %+v, want %+v", counts, want)
	}
	page := h.groupPage(nil, "", 200)
	known := 0
	for _, row := range page.Groups {
		if row.HoldKnown {
			known++
		}
	}
	if entry := counts[lookback.SourceOther]; len(page.Groups)-known != entry.Unknown {
		t.Fatalf("counts %+v disagree with the group page %+v", entry, page)
	}
	// One group's hold, as an overdue episode reads it, by the same rule.
	for qg, want := range map[string]struct {
		millis int64
		known  bool
	}{"good": {120000, true}, "limited": {600000, true}, "raised": {90000, true}, "bad": {0, false}, "zero": {0, false}} {
		if millis, known := h.holdOf(qg); millis != want.millis || known != want.known {
			t.Fatalf("hold of %s %d %t, want %d %t", qg, millis, known, want.millis, want.known)
		}
	}
	*at = at.Add(2 * time.Minute)
	if counts := h.groupsBySource(nil); counts != nil {
		t.Fatalf("groups no longer held were counted: %+v", counts)
	}
}
