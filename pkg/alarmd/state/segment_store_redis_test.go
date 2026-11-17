// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// countingBackend is a Redis backend counting its MGETs; lose makes every
// segment write it carries come back unknown after it was sent, as a reply
// lost on the way back.
type countingBackend struct {
	*RedisBackend
	mgets atomic.Int64
	lose  atomic.Bool
	// failMGet makes every MGET fail, as a read cut off.
	failMGet atomic.Bool
	// renewChunks is the key counts of the renewal scripts it sent; slow,
	// when set, makes every renewal script answer it ran past its budget.
	renewMu     sync.Mutex
	renewChunks []int
	slow        atomic.Bool
	// refuse, when set, is the result every segment write it carries comes
	// back with after it was sent: a fence refusing a later chunk of a write
	// whose earlier chunks landed.
	refuse atomic.Value
}

func (backend *countingBackend) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	backend.mgets.Add(1)
	if backend.failMGet.Load() {
		return nil, errors.New("read cut off")
	}
	return backend.RedisBackend.MGet(ctx, keys)
}

func (backend *countingBackend) RenewSegments(ctx context.Context, guard *FenceGuard, chunks []segmentRenewChunk) []segmentRenewOutcome {
	backend.renewMu.Lock()
	for _, chunk := range chunks {
		backend.renewChunks = append(backend.renewChunks, len(chunk.keys))
	}
	backend.renewMu.Unlock()
	outcomes := backend.RedisBackend.RenewSegments(ctx, guard, chunks)
	if backend.slow.Load() {
		for index := range outcomes {
			outcomes[index].elapsed = segment.RenewScriptBudget + time.Millisecond
		}
	}
	return outcomes
}

func (backend *countingBackend) WriteSegments(ctx context.Context, guard *FenceGuard, plans []segmentPlanWrite) []segmentPlanOutcome {
	outcomes := backend.RedisBackend.WriteSegments(ctx, guard, plans)
	if refused, _ := backend.refuse.Load().(string); refused != "" {
		for index := range outcomes {
			outcomes[index] = segmentPlanOutcome{result: refused}
		}
	}
	if backend.lose.Load() {
		for index := range outcomes {
			outcomes[index] = segmentPlanOutcome{result: segment.WriteUnknown}
		}
	}
	return outcomes
}

// strategyRouter routes each strategy to its own target.
type strategyRouter map[string]StorageTarget

func (router strategyRouter) Route(_, strategy string) (StorageTarget, error) {
	return router[strategy], nil
}
func (router strategyRouter) Targets() []StorageTarget {
	targets := make([]StorageTarget, 0, len(router))
	for _, target := range router {
		targets = append(targets, target)
	}
	return targets
}

// segmentRedis is one redis-server with two databases, each a storage
// target, and the fence's records in both.
type segmentRedis struct {
	targets [2]*countingBackend
	clients [2]*redis.Client
}

func newSegmentRedis(t *testing.T) *segmentRedis {
	t.Helper()
	executable := redistest.Server(t)
	address := reserveTCPAddress(t)
	startRedisServer(t, executable, address)
	rig := &segmentRedis{}
	for db := range rig.targets {
		backend, err := NewRedisBackend(RedisBackendOptions{Address: address, DB: db, DialTimeout: time.Second,
			ReadTimeout: time.Second, WriteTimeout: time.Second, PoolSize: 2})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = backend.Close() })
		waitRedisReady(t, backend)
		rig.targets[db] = &countingBackend{RedisBackend: backend}
		rig.clients[db] = redis.NewClient(&redis.Options{Addr: address, DB: db})
		t.Cleanup(func() { _ = rig.clients[db].Close() })
		keys := testFenceKeys()
		fence := testApplyFence().Fence
		ctx := context.Background()
		rig.clients[db].HSet(ctx, keys.AssignmentKey, "desired_worker_id", fence.OwnerID)
		rig.clients[db].HSet(ctx, keys.OwnershipKey, "execution_disposition", "ACTIVE", "owner_id", fence.OwnerID,
			"owner_epoch", "3", "lease_token", fence.LeaseToken, "deadline_ms", "99999999999999")
	}
	return rig
}

func (rig *segmentRedis) store(t *testing.T, budget int64) *ExecutionStore {
	t.Helper()
	return rig.storeAt(t, budget, time.Now)
}

// productionStore is a store with the State lifetimes in production's
// relation - the replay age, the restart margin and the shortest lifetime
// all ten minutes (config.go) - reading the time from now.
func (rig *segmentRedis) productionStore(t *testing.T, budget int64, now func() time.Time) *ExecutionStore {
	t.Helper()
	router := strategyRouter{"9": {Name: "db0", Backend: rig.targets[0]}, "10": {Name: "db1", Backend: rig.targets[1]}}
	store, err := NewExecutionStore(ExecutionStoreOptions{Prefix: "alarmd", Router: router, MaxValueBytes: 1 << 20,
		MaxItemsPerCall: 8192, MinTTL: 10 * time.Minute, MaxTTL: time.Hour, RestartMargin: 10 * time.Minute,
		FenceKeys: fixedFenceKeys{testFenceKeys()}, StateCacheBudgetBytes: budget, StateCacheReplayAge: 10 * time.Minute,
		Now: now})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// storeAt is a store reading the time from now.
func (rig *segmentRedis) storeAt(t *testing.T, budget int64, now func() time.Time) *ExecutionStore {
	t.Helper()
	router := strategyRouter{"9": {Name: "db0", Backend: rig.targets[0]}, "10": {Name: "db1", Backend: rig.targets[1]}}
	store, err := NewExecutionStore(ExecutionStoreOptions{Prefix: "alarmd", Router: router, MaxValueBytes: 1 << 20,
		MaxItemsPerCall: 8192, MinTTL: 10 * time.Minute, MaxTTL: time.Hour, RestartMargin: time.Minute,
		FenceKeys: fixedFenceKeys{testFenceKeys()}, StateCacheBudgetBytes: budget, StateCacheReplayAge: time.Second,
		Now: now})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func segmentPlanOf(strategy string) segment.PlanKey {
	identity := stateIdentityV2()
	identity.Plan.StrategyID = strategy
	return segment.PlanKey{Plan: identity.Plan, Generation: identity.StateGeneration}
}

func segmentLifetime() segment.Lifetime {
	return segment.Lifetime{Retention: testRetention(), Interval: time.Minute}
}

func markBytes(t *testing.T, pages int, position int64) []byte {
	t.Helper()
	renewed := make([]segment.Renewal, pages)
	for index := range renewed {
		renewed[index] = segment.Renewal{At: 1, TTL: 60_000}
	}
	raw, err := segment.EncodeMark(segment.Mark{Position: position, Pages: pages, Renewed: renewed, Sizes: make([]int, pages)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func planWrite(t *testing.T, key segment.PlanKey, pages map[int][]byte, count, prior int, position int64) segment.PlanWrite {
	t.Helper()
	return segment.PlanWrite{Key: key, Lifetime: segmentLifetime(), Pages: pages, Mark: markBytes(t, count, position),
		PageCount: count, PriorCount: prior}
}

func writeSegments(t *testing.T, store *ExecutionStore, fence execution.StateApplyFence, plans ...segment.PlanWrite) []string {
	t.Helper()
	result, err := store.WriteSegments(context.Background(), segment.WriteRequest{Contract: frozenRef(), Fence: fence, Plans: plans})
	if err != nil {
		t.Fatal(err)
	}
	return result.Plans
}

func readSegments(t *testing.T, store *ExecutionStore, fence execution.OwnerFence, keys ...segment.PlanKey) []segment.Stored {
	t.Helper()
	request := segment.ReadRequest{Contract: frozenRef(), OwnerFence: fence}
	for _, key := range keys {
		request.Plans = append(request.Plans, segment.ReadPlan{Key: key, Lifetime: segmentLifetime()})
	}
	result, err := store.ReadSegments(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return result.Plans
}

// A Plan's mark and pages have names of their own: every page of it from
// zero to past ten, the mark, its State and its gap marker are each another
// key - 1 and 10 among them, whose numbers right padded would meet.
func TestASegmentsKeysAreTheirOwnPageByPage(t *testing.T) {
	plan := segmentPlanOf("9")
	seen := map[string]string{}
	add := func(name, key string) {
		if previous, clash := seen[key]; clash {
			t.Fatalf("%s and %s share the key %s", name, previous, key)
		}
		seen[key] = name
	}
	mark, err := SegmentMarkKeyV1("alarmd", plan)
	if err != nil {
		t.Fatal(err)
	}
	add("mark", mark)
	for page := 0; page <= 12; page++ {
		key, err := SegmentPageKeyV1("alarmd", plan, page)
		if err != nil {
			t.Fatal(err)
		}
		add(fmt.Sprintf("page %d", page), key)
		if isSegmentMarkKey(key) {
			t.Fatalf("page %d's key %s reads as a mark's", page, key)
		}
	}
	if !isSegmentMarkKey(mark) {
		t.Fatalf("the mark's key %s does not read as a mark's", mark)
	}
	state, _ := RuntimeStateKeyV3("alarmd", stateIdentityV2())
	add("state", state)
	gap, _ := PlanGapKeyV2("alarmd", execution.PlanGapIdentity{Plan: plan.Plan, StateGeneration: plan.Generation})
	add("gap", gap)
	if _, err := SegmentPageKeyV1("alarmd", plan, -1); err == nil {
		t.Fatal("a negative page was named")
	}
}

// A Plan's segments are written on the target its State is routed to, by its
// tenant and strategy: one write of two Plans routed apart leaves each on its
// own target and nothing of either on the other.
func TestAPlansSegmentsLandOnTheTargetItsStateIsRoutedTo(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 0)
	nine, ten := segmentPlanOf("9"), segmentPlanOf("10")
	results := writeSegments(t, store, testApplyFence(), planWrite(t, nine, map[int][]byte{0: []byte("nine")}, 1, 0, 60),
		planWrite(t, ten, map[int][]byte{0: []byte("ten")}, 1, 0, 60))
	if results[0] != segment.WriteApplied || results[1] != segment.WriteApplied {
		t.Fatalf("WriteSegments() = %v", results)
	}
	ctx := context.Background()
	for index, key := range []segment.PlanKey{nine, ten} {
		mark, _ := SegmentMarkKeyV1("alarmd", key)
		page, _ := SegmentPageKeyV1("alarmd", key, 0)
		state, _ := RuntimeStateKeyV3("alarmd", execution.StateKeyIdentity{Plan: key.Plan, StateGeneration: key.Generation,
			SeriesIdentityDigest: stateIdentityV2().SeriesIdentityDigest})
		target, _ := store.options.Router.Route(key.Plan.TenantID, key.Plan.StrategyID)
		if target.Backend != rig.targets[index] {
			t.Fatalf("strategy %s's State %s is not routed to database %d", key.Plan.StrategyID, state, index)
		}
		if rig.clients[index].Exists(ctx, mark, page).Val() != 2 || rig.clients[1-index].Exists(ctx, mark, page).Val() != 0 {
			t.Fatalf("strategy %s's segments are not on its State's target alone", key.Plan.StrategyID)
		}
	}
}

// A write a stale fence refuses leaves no key; one under the owner's fence
// writes its pages - cut into scripts by their bytes - and then the mark,
// renews the pages it did not write and removes those past its count. A page
// it kept alive that was already gone lets the Plan's cache go: its next
// read is from Redis, counted renew_missing, and the page gone is no drift.
func TestASegmentWriteIsFencedAndCutIntoScriptsTheMarkLast(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 1<<22)
	plan := segmentPlanOf("9")
	ctx := context.Background()
	stale := testApplyFence()
	stale.Fence.OwnerEpoch = 99
	if results := writeSegments(t, store, stale, planWrite(t, plan, map[int][]byte{0: []byte("p0")}, 1, 0, 60)); results[0] != segment.WriteStaleOwner {
		t.Fatalf("a stale fence's write = %v, want stale_owner", results)
	}
	if keys := rig.clients[0].Keys(ctx, "alarmd:segment-*").Val(); len(keys) != 0 {
		t.Fatalf("a refused write left %v", keys)
	}
	// What a refused write left is not known to the writer: the Plan is
	// read from Redis until it is read again, whatever it writes between.
	if stored := readSegments(t, store, testApplyFence().Fence, plan); stored[0].Reason != segment.ReadUnknownResult || stored[0].Mark != nil {
		t.Fatalf("after a refused write: %+v, want read from Redis, nothing there", stored[0])
	}
	big := bytes.Repeat([]byte("x"), 120<<10)
	first := map[int][]byte{0: big, 1: big, 2: big, 3: []byte("p3")}
	chunks, _, err := store.segmentChunks(planWrite(t, plan, first, 4, 0, 60), "mark")
	if err != nil || len(chunks) != 3 || chunks[2].mark == nil || chunks[0].mark != nil {
		t.Fatalf("four pages of 360 KiB cut into %d scripts, the mark on the last: %+v, %v", len(chunks), chunks, err)
	}
	if results := writeSegments(t, store, testApplyFence(), planWrite(t, plan, first, 4, 0, 60)); results[0] != segment.WriteApplied {
		t.Fatalf("the first write = %v", results)
	}
	page := func(number int) string {
		key, _ := SegmentPageKeyV1("alarmd", plan, number)
		return key
	}
	rig.clients[0].PExpire(ctx, page(1), 2*time.Second) // shortened behind it, to see the next write renew it
	second := map[int][]byte{0: []byte("p0'")}
	if results := writeSegments(t, store, testApplyFence(), planWrite(t, plan, second, 2, 4, 120)); results[0] != segment.WriteApplied {
		t.Fatalf("the second write = %v", results)
	}
	if got := rig.clients[0].Get(ctx, page(0)).Val(); got != "p0'" {
		t.Fatalf("page 0 = %q", got)
	}
	if ttl := rig.clients[0].PTTL(ctx, page(1)).Val(); ttl < time.Minute {
		t.Fatalf("the page not written was left with %v, want renewed with the mark", ttl)
	}
	if rig.clients[0].Exists(ctx, page(2), page(3)).Val() != 0 {
		t.Fatal("the pages past the count were not removed")
	}
	mark, _ := SegmentMarkKeyV1("alarmd", plan)
	if raw := rig.clients[0].Get(ctx, mark).Val(); !bytes.Equal([]byte(raw), markBytes(t, 2, 120)) {
		t.Fatal("the mark is not the second write's")
	}
	if stored := readSegments(t, store, testApplyFence().Fence, plan); stored[0].Source != segment.SourceCache {
		t.Fatalf("after an applied write: %+v, want served from the cache", stored[0])
	}
	rig.clients[0].Del(ctx, page(1))
	if results := writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: []byte("p0''")}, 2, 2, 180)); results[0] != segment.WriteApplied {
		t.Fatalf("the third write = %v", results)
	}
	stored := readSegments(t, store, testApplyFence().Fence, plan)
	if stored[0].Reason != segment.ReadRenewMissing || stored[0].Pages[1] != nil {
		t.Fatalf("after the mark found page 1 gone: %+v, want read from Redis for it", stored[0])
	}
	if drift := store.SegmentStats().Drift; drift != 0 {
		t.Fatalf("drift %d, want none for a page its write reported gone", drift)
	}
}

// A Slot's segments are read from the cache under the same fence, and from
// Redis otherwise, each read counted by why: cold, the fence changed, a write
// in doubt let it go; the Slot's cold Plans of one target are read in one
// MGET of marks and one of pages; a key read from Redis other than the cache
// held it is drift, unless a write in doubt tried to leave it so.
func TestSegmentReadsAreServedFromTheCacheAndCountedByWhyNot(t *testing.T) {
	rig := newSegmentRedis(t)
	writer := rig.store(t, 1<<20)
	ctx := context.Background()
	plans := []segment.PlanKey{segmentPlanOf("9"), segmentPlanOf("9"), segmentPlanOf("9")}
	plans[1].Generation, plans[2].Generation = "generation-b", "generation-c"
	var writes []segment.PlanWrite
	for _, plan := range plans {
		writes = append(writes, planWrite(t, plan, map[int][]byte{0: []byte("a"), 1: []byte("b")}, 2, 0, 60))
	}
	if results := writeSegments(t, writer, testApplyFence(), writes...); results[2] != segment.WriteApplied {
		t.Fatalf("write: %v", results)
	}
	cold := rig.store(t, 1<<20)
	before := rig.targets[0].mgets.Load()
	stored := readSegments(t, cold, testApplyFence().Fence, plans...)
	if mgets := rig.targets[0].mgets.Load() - before; mgets != 2 {
		t.Fatalf("three cold Plans read in %d MGETs, want the marks' and the pages'", mgets)
	}
	for _, plan := range stored {
		if plan.Source != segment.SourceRedis || plan.Reason != segment.ReadCold || len(plan.Pages) != 2 || string(plan.Pages[1]) != "b" {
			t.Fatalf("cold read %+v", plan)
		}
	}
	before = rig.targets[0].mgets.Load()
	if again := readSegments(t, cold, testApplyFence().Fence, plans...); again[0].Source != segment.SourceCache || rig.targets[0].mgets.Load() != before {
		t.Fatalf("the second read: %+v, want served from what the first kept", again[0])
	}
	reads := func(store *ExecutionStore, source, reason string) uint64 {
		return store.SegmentStats().Reads[[2]string{source, reason}]
	}
	if got := reads(cold, segment.SourceRedis, segment.ReadCold); got != 3 {
		t.Fatalf("cold reads counted %d, want 3", got)
	}
	moved := testApplyFence().Fence
	moved.OwnerEpoch = 4
	if stored := readSegments(t, writer, moved, plans[0]); stored[0].Reason != segment.ReadFenceChanged {
		t.Fatalf("under another fence: %+v, want read for the fence", stored[0])
	}
	// A write whose reply is lost lands; the read after it finds what it
	// tried to leave, and that is no drift.
	rig.targets[0].lose.Store(true)
	if results := writeSegments(t, writer, testApplyFence(), planWrite(t, plans[1], map[int][]byte{1: []byte("b'")}, 2, 2, 120)); results[0] != segment.WriteUnknown {
		t.Fatalf("a write with its reply lost = %v", results)
	}
	rig.targets[0].lose.Store(false)
	stored = readSegments(t, writer, testApplyFence().Fence, plans[1])
	if stored[0].Reason != segment.ReadUnknownResult || string(stored[0].Pages[1]) != "b'" {
		t.Fatalf("after a write in doubt: %+v, want read from Redis", stored[0])
	}
	if drift := writer.SegmentStats().Drift; drift != 0 {
		t.Fatalf("drift %d after a write in doubt that landed, want none", drift)
	}
	page, _ := SegmentPageKeyV1("alarmd", plans[2], 1)
	rig.clients[0].Set(ctx, page, "written behind", time.Minute)
	writer.InvalidateSegments(frozenRef().Slot.QueryGroup, plans[2], segment.ReadRenewMissing)
	if stored := readSegments(t, writer, testApplyFence().Fence, plans[2]); string(stored[0].Pages[1]) != "written behind" {
		t.Fatalf("after its cache was let go: %+v, want read from Redis", stored[0])
	}
	if drift := writer.SegmentStats().Drift; drift != 1 {
		t.Fatalf("drift %d, want the one page written behind the owner", drift)
	}
	if stats := writer.SegmentStats(); stats.Plans != 3 || stats.Pages != 6 || stats.Bytes <= 0 {
		t.Fatalf("the cache holds %+v of the segments, want three Plans of two pages", stats)
	}
	writer.DropStateCacheQueryGroup(frozenRef().Slot.QueryGroup)
	if stats := writer.SegmentStats(); stats.Plans != 0 || stats.Pages != 0 {
		t.Fatalf("after the Query Group's release the cache holds %+v", stats)
	}
}

// A Plan with no segments yet is read once: its absence is kept, and the
// rounds after read it from the cache until its first write.
func TestAPlanWithNoSegmentsIsReadOnce(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 1<<20)
	plan := segmentPlanOf("9")
	if stored := readSegments(t, store, testApplyFence().Fence, plan); stored[0].Mark != nil || stored[0].Reason != segment.ReadCold {
		t.Fatalf("first read %+v", stored[0])
	}
	before := rig.targets[0].mgets.Load()
	if stored := readSegments(t, store, testApplyFence().Fence, plan); stored[0].Source != segment.SourceCache || stored[0].Mark != nil {
		t.Fatalf("second read %+v, want its absence from the cache", stored[0])
	}
	if rig.targets[0].mgets.Load() != before {
		t.Fatal("the second read went to Redis")
	}
}

// A cache given back to the pool lets the marks go as evicted: their next
// read is counted evicted, not cold.
func TestASegmentLetGoForTheRoomIsReadAsEvicted(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 1<<20)
	plan := segmentPlanOf("9")
	writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: []byte("a")}, 1, 0, 60))
	store.cache.YieldRetainedBytes(1 << 30)
	if stored := readSegments(t, store, testApplyFence().Fence, plan); stored[0].Reason != segment.ReadEvicted {
		t.Fatalf("after the cache gave its room back: %+v, want evicted", stored[0])
	}
}

// A server that lost its scripts takes a write again with the script's
// text, the Plan's chunks whole and in order.
func TestASegmentWriteSurvivesTheScriptsFlushed(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 0)
	plan := segmentPlanOf("9")
	writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: []byte("a")}, 1, 0, 60))
	rig.clients[0].ScriptFlush(context.Background())
	if results := writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: []byte("b")}, 1, 1, 120)); results[0] != segment.WriteApplied {
		t.Fatalf("after SCRIPT FLUSH = %v", results)
	}
	page, _ := SegmentPageKeyV1("alarmd", plan, 0)
	if got := rig.clients[0].Get(context.Background(), page).Val(); got != "b" {
		t.Fatalf("page 0 = %q", got)
	}
}

// A Plan's segments untouched for two of its intervals - a Plan no longer
// due, deleted, or moved to a new generation - leave the cache with the
// sweep, mark and pages; and its next read, were there one, is counted
// evicted, not cold.
func TestASegmentUntouchedForTwoIntervalsLeavesAndIsReadAsEvicted(t *testing.T) {
	rig := newSegmentRedis(t)
	var clock atomic.Int64
	clock.Store(time.Unix(1_800_000_000, 0).UnixNano())
	store := rig.storeAt(t, 1<<20, func() time.Time { return time.Unix(0, clock.Load()) })
	plan, other := segmentPlanOf("9"), segmentPlanOf("9")
	other.Generation = "generation-b"
	writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: []byte("a")}, 1, 0, 60),
		planWrite(t, other, map[int][]byte{0: []byte("b")}, 1, 0, 60))
	if stats := store.SegmentStats(); stats.Plans != 2 || stats.Pages != 2 {
		t.Fatalf("after the write the cache holds %+v", stats)
	}
	// One interval on, the other Plan is read: both stay.
	clock.Add(int64(time.Minute + time.Second))
	readSegments(t, store, testApplyFence().Fence, other)
	if stats := store.SegmentStats(); stats.Plans != 2 {
		t.Fatalf("within two intervals the cache holds %+v", stats)
	}
	// Two intervals past the first Plan's last touch, the read of the other
	// sweeps it out, mark and page.
	clock.Add(int64(time.Minute))
	if stored := readSegments(t, store, testApplyFence().Fence, other); stored[0].Source != segment.SourceCache {
		t.Fatalf("the Plan read every round: %+v, want it still served from the cache", stored[0])
	}
	if stats := store.SegmentStats(); stats.Plans != 1 || stats.Pages != 1 {
		t.Fatalf("past two intervals untouched the cache holds %+v, want the other Plan alone", stats)
	}
	if stored := readSegments(t, store, testApplyFence().Fence, plan); stored[0].Reason != segment.ReadEvicted {
		t.Fatalf("the Plan swept out read again: %+v, want evicted", stored[0])
	}
}

// A steady Plan's pages are not rewritten round after round, its mark is:
// the write renews the pages it keeps, in Redis and in the cache alike, so
// the Plan is served from the cache past the life a page was cached with,
// for two of those lives, with no read from Redis and no drift.
func TestASteadyPlansPagesStayCachedPastTheirLifeWhileItsMarkIsWritten(t *testing.T) {
	rig := newSegmentRedis(t)
	var clock atomic.Int64
	clock.Store(time.Unix(1_800_000_000, 0).UnixNano())
	store := rig.productionStore(t, 1<<20, func() time.Time { return time.Unix(0, clock.Load()) })
	_, life, ok := store.segmentLife(segmentLifetime())
	if !ok || life <= 0 {
		t.Fatalf("a cached segment's life %v, want one", life)
	}
	plan := segmentPlanOf("9")
	writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: []byte("a"), 1: []byte("b")}, 2, 0, 60))
	rounds := int(2*life/time.Minute) + 2
	for round := 1; round <= rounds; round++ {
		clock.Add(int64(time.Minute))
		if stored := readSegments(t, store, testApplyFence().Fence, plan); stored[0].Source != segment.SourceCache {
			t.Fatalf("round %d, %v past the first write (a life is %v): %+v, want served from the cache", round,
				time.Duration(round)*time.Minute, life, stored[0])
		}
		if results := writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{}, 2, 2, int64(60+60*round))); results[0] != segment.WriteApplied {
			t.Fatalf("round %d's mark: %v", round, results)
		}
	}
	if stats := store.SegmentStats(); stats.Drift != 0 || stats.Reads[[2]string{segment.SourceRedis, segment.ReadEvicted}] != 0 {
		t.Fatalf("after %d rounds: %+v, want no drift and nothing evicted", rounds, stats)
	}
}

// A write the fence refuses part way - a later chunk refused after the
// earlier ones landed, CONTENT_MOVED under a lease still held - lets the
// Plan's cache go: the next read under the same fence is from Redis, and
// what it finds of the write is no drift.
func TestASegmentWriteTheFenceRefusesPartWayLetsThePlansCacheGo(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 1<<20)
	plan := segmentPlanOf("9")
	writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: []byte("a")}, 1, 0, 60))
	for _, refused := range []string{segment.WriteContentMoved, segment.WriteStaleOwner} {
		rig.targets[0].refuse.Store(refused)
		page := []byte("landed before the refusal: " + refused)
		if results := writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: page}, 1, 1, 120)); results[0] != refused {
			t.Fatalf("a refused write = %v, want %s", results, refused)
		}
		rig.targets[0].refuse.Store("")
		stored := readSegments(t, store, testApplyFence().Fence, plan)
		if stored[0].Source != segment.SourceRedis || !bytes.Equal(stored[0].Pages[0], page) {
			t.Fatalf("after a write refused %s part way: %+v, want read from Redis", refused, stored[0])
		}
	}
	if drift := store.SegmentStats().Drift; drift != 0 {
		t.Fatalf("drift %d, want none for what the refused write left", drift)
	}
}

// A Slot's cold Plans of one target are read in one MGET of their marks and
// pages cut by the bytes the marks say they are, not by a page's largest
// size: twenty single-page Plans, two MGETs.
func TestASlotsColdSinglePagePlansAreReadInTwoMGETs(t *testing.T) {
	rig := newSegmentRedis(t)
	writer := rig.store(t, 0)
	var keys []segment.PlanKey
	var writes []segment.PlanWrite
	for index := 0; index < 20; index++ {
		key := segmentPlanOf("9")
		key.Generation = execution.StateGeneration(fmt.Sprintf("generation-%02d", index))
		keys = append(keys, key)
		writes = append(writes, planWrite(t, key, map[int][]byte{0: []byte(fmt.Sprint("page of ", index))}, 1, 0, 60))
	}
	writeSegments(t, writer, testApplyFence(), writes...)
	cold := rig.store(t, 1<<20)
	before := rig.targets[0].mgets.Load()
	stored := readSegments(t, cold, testApplyFence().Fence, keys...)
	if mgets := rig.targets[0].mgets.Load() - before; mgets != 2 {
		t.Fatalf("twenty cold single-page Plans read in %d MGETs, want two", mgets)
	}
	for index, plan := range stored {
		if string(plan.Pages[0]) != fmt.Sprint("page of ", index) {
			t.Fatalf("Plan %d read %q", index, plan.Pages[0])
		}
	}
}

// Pages are cut into MGETs by the bytes their marks name: fifteen cold
// Plans of a 120 KiB page each, past what one read carries, take two MGETs
// of pages beside the one of marks.
func TestColdPagesAreCutIntoMGETsByTheBytesTheirMarksName(t *testing.T) {
	rig := newSegmentRedis(t)
	writer := rig.store(t, 0)
	var keys []segment.PlanKey
	var writes []segment.PlanWrite
	for index := 0; index < 15; index++ {
		key := segmentPlanOf("9")
		key.Generation = execution.StateGeneration(fmt.Sprintf("generation-%02d", index))
		keys = append(keys, key)
		write := planWrite(t, key, map[int][]byte{0: bytes.Repeat([]byte{byte('a' + index)}, 120<<10)}, 1, 0, 60)
		renewed := []segment.Renewal{{At: 1, TTL: 60_000}}
		mark, err := segment.EncodeMark(segment.Mark{Position: 60, Pages: 1, Renewed: renewed, Sizes: []int{120 << 10}})
		if err != nil {
			t.Fatal(err)
		}
		write.Mark = mark
		writes = append(writes, write)
	}
	writeSegments(t, writer, testApplyFence(), writes...)
	cold := rig.store(t, 0)
	before := rig.targets[0].mgets.Load()
	stored := readSegments(t, cold, testApplyFence().Fence, keys...)
	if mgets := rig.targets[0].mgets.Load() - before; mgets != 3 {
		t.Fatalf("fifteen cold Plans of 1.8 MiB of pages read in %d MGETs, want the marks' and two of pages", mgets)
	}
	if len(stored[14].Pages[0]) != 120<<10 {
		t.Fatalf("the last Plan's page read %d bytes", len(stored[14].Pages[0]))
	}
}

// A segment write with no fence that holds for its Slot is refused as a
// State write is, counted failed, and leaves nothing.
func TestASegmentWriteWithoutAFenceIsRefused(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 1<<20)
	plan := segmentPlanOf("9")
	result, err := store.WriteSegments(context.Background(), segment.WriteRequest{Contract: frozenRef(),
		Plans: []segment.PlanWrite{planWrite(t, plan, map[int][]byte{0: []byte("a")}, 1, 0, 60)}})
	if err == nil || result.Plans[0] != segment.WriteFailed {
		t.Fatalf("a write with no fence = %v, %v, want refused, failed", result.Plans, err)
	}
	if keys := rig.clients[0].Keys(context.Background(), "alarmd:segment-*").Val(); len(keys) != 0 {
		t.Fatalf("a write with no fence left %v", keys)
	}
	if failed := store.SegmentStats().Writes[segment.WriteFailed]; failed != 1 {
		t.Fatalf("failed writes %d, want 1", failed)
	}
}

// A Plan whose cache a write in doubt let go and that is never read again -
// deleted, moved to a new generation - has its note swept past
// segmentNoteLife: the note does not outlive the Plan in the cache.
func TestAWriteInDoubtsNoteIsSweptOnceThePlanIsGone(t *testing.T) {
	rig := newSegmentRedis(t)
	var clock atomic.Int64
	clock.Store(time.Unix(1_800_000_000, 0).UnixNano())
	store := rig.storeAt(t, 1<<20, func() time.Time { return time.Unix(0, clock.Load()) })
	plan := segmentPlanOf("9")
	writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: []byte("a")}, 1, 0, 60))
	rig.targets[0].lose.Store(true)
	writeSegments(t, store, testApplyFence(), planWrite(t, plan, map[int][]byte{0: []byte("b")}, 1, 1, 120))
	rig.targets[0].lose.Store(false)
	mark, _ := SegmentMarkKeyV1("alarmd", plan)
	if _, noted := store.cache.segmentInvalidOf(frozenRef().Slot.QueryGroup, mark); !noted {
		t.Fatal("the write in doubt left no note")
	}
	// Its entries leave untouched first, its group then holding none; the
	// note goes with a later sweep of the group past segmentNoteLife.
	group := frozenRef().Slot.QueryGroup
	clock.Add(int64(3 * time.Minute))
	store.cache.sweep(group)
	if stats := store.SegmentStats(); stats.Plans != 0 || stats.Pages != 0 {
		t.Fatalf("the untouched Plan's entries stayed: %+v", stats)
	}
	clock.Add(int64(segmentNoteLife))
	store.cache.sweep(group)
	if _, noted := store.cache.segmentInvalidOf(group, mark); noted {
		t.Fatal("the note of a Plan no read cleared outlived segmentNoteLife")
	}
}

// A read that fails leaves the Slot with no segments at all: every Plan of
// the request is counted failed, the one served from the cache among them.
func TestAFailedReadCountsEveryPlanOfTheRequest(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 1<<20)
	cached, first, second := segmentPlanOf("9"), segmentPlanOf("9"), segmentPlanOf("9")
	first.Generation, second.Generation = "generation-b", "generation-c"
	writeSegments(t, store, testApplyFence(), planWrite(t, cached, map[int][]byte{0: []byte("a")}, 1, 0, 60))
	rig.targets[0].failMGet.Store(true)
	request := segment.ReadRequest{Contract: frozenRef(), OwnerFence: testApplyFence().Fence}
	for _, key := range []segment.PlanKey{cached, first, second} {
		request.Plans = append(request.Plans, segment.ReadPlan{Key: key, Lifetime: segmentLifetime()})
	}
	if _, err := store.ReadSegments(context.Background(), request); err == nil {
		t.Fatal("a read cut off read")
	}
	stats := store.SegmentStats()
	if failed := stats.Reads[[2]string{segment.SourceRedis, segment.ReadFailed}]; failed != 3 || stats.Reads[[2]string{segment.SourceCache, segment.ReadHit}] != 0 {
		t.Fatalf("a failed read counted %v, want all three Plans failed and no hit", stats.Reads)
	}
}

// A daily Plan whose write the fence refused part way is read from Redis
// at its next round, a day on: its note stays while its cached mark could
// still be served - the entry lives for days and each round touches it -
// and goes only once the mark is out of the cache.
func TestADailyPlansRefusedWriteIsReadFromRedisADayOn(t *testing.T) {
	rig := newSegmentRedis(t)
	var clock atomic.Int64
	clock.Store(time.Unix(1_800_000_000, 0).UnixNano())
	router := strategyRouter{"9": {Name: "db0", Backend: rig.targets[0]}}
	store, err := NewExecutionStore(ExecutionStoreOptions{Prefix: "alarmd", Router: router, MaxValueBytes: 1 << 20,
		MaxItemsPerCall: 8192, MinTTL: 10 * time.Minute, MaxTTL: 30 * 24 * time.Hour, RestartMargin: 10 * time.Minute,
		FenceKeys: fixedFenceKeys{testFenceKeys()}, StateCacheBudgetBytes: 1 << 20, StateCacheReplayAge: 10 * time.Minute,
		Now: func() time.Time { return time.Unix(0, clock.Load()) }})
	if err != nil {
		t.Fatal(err)
	}
	daily := segment.Lifetime{Retention: []execution.StateRetentionRequirement{{LevelID: 1, RetentionPoints: 5,
		EvaluationInterval: 24 * time.Hour}}, Interval: 24 * time.Hour}
	plan := segmentPlanOf("9")
	write := func(page string, prior int, position int64) []string {
		t.Helper()
		pw := planWrite(t, plan, map[int][]byte{0: []byte(page)}, 1, prior, position)
		pw.Lifetime = daily
		return writeSegments(t, store, testApplyFence(), pw)
	}
	read := func() segment.Stored {
		t.Helper()
		result, err := store.ReadSegments(context.Background(), segment.ReadRequest{Contract: frozenRef(),
			OwnerFence: testApplyFence().Fence, Plans: []segment.ReadPlan{{Key: plan, Lifetime: daily}}})
		if err != nil {
			t.Fatal(err)
		}
		return result.Plans[0]
	}
	write("day 0", 0, 86400)
	if stored := read(); stored.Source != segment.SourceCache {
		t.Fatalf("the round after the write: %+v, want served from the cache", stored)
	}
	rig.targets[0].refuse.Store(segment.WriteContentMoved)
	if results := write("day 1, landed before the refusal", 1, 172800); results[0] != segment.WriteContentMoved {
		t.Fatalf("the refused write = %v", results)
	}
	rig.targets[0].refuse.Store("")
	clock.Add(int64(24 * time.Hour))
	stored := read()
	if stored.Source != segment.SourceRedis || stored.Reason != segment.ReadUnknownResult || string(stored.Pages[0]) != "day 1, landed before the refusal" {
		t.Fatalf("a day after the refused write: %+v, want read from Redis for it", stored)
	}
}

// memberKeys writes count members' State keys of plan with a short life and
// says their digests and keys.
func memberKeys(t *testing.T, rig *segmentRedis, db int, plan segment.PlanKey, from, count int) ([]execution.SeriesIdentityDigest, []string) {
	t.Helper()
	var digests []execution.SeriesIdentityDigest
	var keys []string
	for index := from; index < from+count; index++ {
		identity := stateIdentityV2()
		identity.Plan, identity.StateGeneration = plan.Plan, plan.Generation
		identity.SeriesIdentityDigest = execution.SeriesIdentityDigest(fmt.Sprintf("%064x", index+1))
		key, err := RuntimeStateKeyV3("alarmd", identity)
		if err != nil {
			t.Fatal(err)
		}
		rig.clients[db].Set(context.Background(), key, "state", 30*time.Second)
		digests, keys = append(digests, identity.SeriesIdentityDigest), append(keys, key)
	}
	return digests, keys
}

func renewSegments(t *testing.T, store *ExecutionStore, fence execution.StateApplyFence, plans ...segment.PlanRenewal) []segment.PlanRenewed {
	t.Helper()
	result, err := store.RenewSegments(context.Background(), segment.RenewRequest{Contract: frozenRef(), Fence: fence, Plans: plans})
	if err != nil {
		t.Fatal(err)
	}
	return result.Plans
}

// A Slot's due pages are renewed on their Plan's own target, the members'
// State keys given the Plan's lifetime in scripts of at most RenewKeys keys:
// 600 members of two pages, three scripts; a page renewed once every script
// carrying it answered. A key already gone is counted, not made again, and
// lets the Plan's cache go; a stale fence renews nothing and no page.
func TestDuePagesAreRenewedInScriptsOfAtMostRenewKeysKeys(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 1<<20)
	plan, other := segmentPlanOf("9"), segmentPlanOf("10")
	ctx := context.Background()
	first, firstKeys := memberKeys(t, rig, 0, plan, 0, 300)
	second, secondKeys := memberKeys(t, rig, 0, plan, 300, 300)
	elsewhere, elsewhereKeys := memberKeys(t, rig, 1, other, 0, 5)
	gone := execution.SeriesIdentityDigest(fmt.Sprintf("%064x", 9999))
	renewed := renewSegments(t, store, testApplyFence(),
		segment.PlanRenewal{Key: plan, Lifetime: segmentLifetime(), Pages: []segment.PageRenewal{
			{Page: 0, Members: first}, {Page: 1, Members: append(append([]execution.SeriesIdentityDigest{}, second...), gone)}}},
		segment.PlanRenewal{Key: other, Lifetime: segmentLifetime(), Pages: []segment.PageRenewal{{Page: 0, Members: elsewhere}}})
	if !renewed[0].Renewed[0] || !renewed[0].Renewed[1] || !renewed[1].Renewed[0] || renewed[0].Missing != 1 || renewed[0].OverBudget != 0 {
		t.Fatalf("renewals %+v, want every page renewed, the one key gone counted", renewed)
	}
	rig.targets[0].renewMu.Lock()
	chunks := append([]int(nil), rig.targets[0].renewChunks...)
	rig.targets[0].renewMu.Unlock()
	if len(chunks) != 3 || chunks[0] != segment.RenewKeys || chunks[2] != 601-2*segment.RenewKeys {
		t.Fatalf("601 member keys in scripts of %v, want %d, %d and the rest", chunks, segment.RenewKeys, segment.RenewKeys)
	}
	ttl, _, _ := store.segmentLife(segmentLifetime())
	for _, key := range append(append(append([]string{}, firstKeys...), secondKeys...), elsewhereKeys[0]) {
		client := rig.clients[0]
		if key == elsewhereKeys[0] {
			client = rig.clients[1]
		}
		if left := client.PTTL(ctx, key).Val(); left < ttl-time.Minute {
			t.Fatalf("%s left %v, want renewed to the Plan's %v", key, left, ttl)
		}
	}
	if exists := rig.clients[0].Exists(ctx, elsewhereKeys...).Val(); exists != 0 {
		t.Fatal("the other Plan's keys were renewed on the first target")
	}
	mark, _ := SegmentMarkKeyV1("alarmd", plan)
	if invalid, noted := store.cache.segmentInvalidOf(frozenRef().Slot.QueryGroup, mark); !noted || invalid.reason != segment.ReadRenewMissing {
		t.Fatalf("a renewal that found a key gone left the Plan's cache: %+v %v", invalid, noted)
	}
	stale := testApplyFence()
	stale.Fence.OwnerEpoch = 99
	rig.clients[0].PExpire(ctx, firstKeys[0], 30*time.Second)
	refused := renewSegments(t, store, stale, segment.PlanRenewal{Key: plan, Lifetime: segmentLifetime(),
		Pages: []segment.PageRenewal{{Page: 0, Members: first}}})
	if refused[0].Renewed[0] {
		t.Fatal("a stale fence's renewal renewed its page")
	}
	if left := rig.clients[0].PTTL(ctx, firstKeys[0]).Val(); left > time.Minute {
		t.Fatalf("a stale fence's renewal gave a key %v", left)
	}
}

// A renewal script that runs past its budget by the server's clock is
// counted over_budget, and the page is renewed all the same; one the
// server had no script for is sent again with its text.
func TestARenewalScriptPastItsBudgetIsCountedAndOneUncachedIsSentAgain(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 0)
	plan := segmentPlanOf("9")
	members, _ := memberKeys(t, rig, 0, plan, 0, 3)
	rig.clients[0].ScriptFlush(context.Background())
	rig.targets[0].slow.Store(true)
	renewed := renewSegments(t, store, testApplyFence(), segment.PlanRenewal{Key: plan, Lifetime: segmentLifetime(),
		Pages: []segment.PageRenewal{{Page: 0, Members: members}}})
	if !renewed[0].Renewed[0] || renewed[0].OverBudget != 1 {
		t.Fatalf("a slow renewal after SCRIPT FLUSH: %+v, want renewed and one over budget", renewed[0])
	}
}

// A renewal without a fence that holds for its Slot renews nothing.
func TestASegmentRenewalWithoutAFenceIsRefused(t *testing.T) {
	rig := newSegmentRedis(t)
	store := rig.store(t, 0)
	plan := segmentPlanOf("9")
	members, _ := memberKeys(t, rig, 0, plan, 0, 1)
	result, err := store.RenewSegments(context.Background(), segment.RenewRequest{Contract: frozenRef(),
		Plans: []segment.PlanRenewal{{Key: plan, Lifetime: segmentLifetime(), Pages: []segment.PageRenewal{{Page: 0, Members: members}}}}})
	if err == nil || result.Plans[0].Renewed[0] {
		t.Fatalf("a renewal with no fence = %+v, %v, want refused", result.Plans, err)
	}
}
