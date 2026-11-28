// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// storeOutageFixture is a runtime client on a Sentinel whose master accepts
// and never answers, opened the way production opens it, and one whose master
// refuses every connection, with the Runtime State store over each.
type storeOutageFixture struct {
	hung, refusing redis.UniversalClient
	connection     config.RedisConnectionConfig
}

func newStoreOutageFixture(t *testing.T) storeOutageFixture {
	t.Helper()
	recorder := metric.NewRecorder(metric.BuildInfo{})
	hungMaster := respServer(t, func(args []string) []byte {
		if strings.EqualFold(args[0], "ping") {
			return []byte("+PONG\r\n")
		}
		return nil
	})
	connection := func(master string) config.RedisConnectionConfig {
		return config.RedisConnectionConfig{Mode: config.RedisModeSentinel, SentinelAddress: []string{respServer(t, sentinelAnswering("mymaster", master))},
			MasterName: "mymaster", DialTimeout: config.Duration(3 * time.Second), ReadTimeout: config.Duration(3 * time.Second),
			WriteTimeout: config.Duration(3 * time.Second)}
	}
	fixture := storeOutageFixture{connection: connection(hungMaster)}
	hung, err := openProductionRedisOptionsWithHook(context.Background(), productionRedisOptions(fixture.connection), recorder.RedisHook("runtime"))
	if err != nil {
		t.Fatalf("open the runtime client: %v", err)
	}
	t.Cleanup(func() { _ = hung.Close() })
	// A master that refuses cannot answer the open's ping; the client is
	// the same one with the same hook.
	refusing := redis.NewUniversalClient(productionRedisOptions(connection(closedAddress(t))))
	refusing.AddHook(recorder.RedisHook("runtime"))
	t.Cleanup(func() { _ = refusing.Close() })
	fixture.hung, fixture.refusing = hung, refusing
	return fixture
}

// gapStore is the Runtime State store over client, its calls made for caller.
func (fixture storeOutageFixture) gapStore(t *testing.T, client redis.UniversalClient, caller string) *state.ExecutionStore {
	t.Helper()
	backend, err := state.NewRedisBackendWithClient(productionRedisAddress(fixture.connection), redisForCaller(client, caller))
	if err != nil {
		t.Fatal(err)
	}
	router, err := state.NewFixedRouter("runtime", backend)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.NewExecutionStore(state.ExecutionStoreOptions{Prefix: "alarmd", Router: router, MaxValueBytes: 4096, MaxItemsPerCall: 4,
		MinTTL: time.Minute, MaxTTL: 30 * 24 * time.Hour, RestartMargin: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// gapPreflight reads one Plan's gap marker as the worker's preflight does:
// the store turns a read that failed into an unavailable marker, and the
// worker's own check of the Slot's time (worker/gap_reservation.go) is what
// stops it, with the Slot's deadline and nothing more.
func gapPreflight(ctx context.Context, store *state.ExecutionStore) error {
	err := store.LoadGapsInto(ctx, execution.GapLoadRequest{
		Contract: execution.FrozenExecutionContractRef{Slot: execution.SlotIdentity{QueryGroup: "q", EvaluationTime: 60}, SnapshotRevision: "snapshot",
			QueryRevision: "query", ScheduleRevision: "schedule", ScheduleSegmentStart: 60, DuePlanSetDigest: "plans"},
		Items: []execution.PlanGapLoadItem{{
			Identity:     execution.PlanGapIdentity{Plan: execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "9"}, StateGeneration: "generation"},
			ApplyVersion: execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: 60, SlotDigest: "slot"}, ScheduleRevision: "plan-r1",
			Retention: []execution.StateRetentionRequirement{{LevelID: 1, RetentionPoints: 5, EvaluationInterval: time.Minute}},
		}},
	}, func(execution.GapGuardSnapshot) error { return ctx.Err() })
	if err == nil {
		// The read's deadline and the context's timer are not one event: a
		// Slot that read on is stopped by the next check of its time.
		<-ctx.Done()
		err = ctx.Err()
	}
	return fmt.Errorf("complete Slot: %w", fmt.Errorf("alarmd worker: gap preflight: %w", err))
}

// completeAttempt runs one attempt of a Slot through the production executor
// with timeout to run in, and is its completion row.
func completeAttempt(t *testing.T, timeout time.Duration, attempt func(ctx context.Context) error) observability.Observation {
	t.Helper()
	var completed *observability.Observation
	executor := observedProductionSlotExecutor{
		next: slotExecutorFunc(func(ctx context.Context, _ execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
			return execution.SlotExecutionResult{}, attempt(ctx)
		}),
		observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			if observation.Stage == observability.StageSlotCompleted {
				normalized := observability.NormalizeObservation(observation)
				completed = &normalized
			}
		}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout) // fixed-wait: the attempt's time, which a hung store call runs out
	defer cancel()
	if _, err := executor.Execute(ctx, gapGuardSlotRequest(execution.PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "9"})); err == nil {
		t.Fatal("the attempt did not fail")
	}
	if completed == nil {
		t.Fatal("no slot_completed row")
	}
	return *completed
}

// A Slot whose gap preflight reads a Runtime State store whose master stops
// answering fails as REDIS_UNAVAILABLE, through the client production opens,
// the Runtime State job's clone of it, the state store's own read and the
// production executor; its completion row says the call ran out the time it
// was issued with. The store turned the read into a status and the worker
// stopped on the Slot's deadline, so the Slot's error says nothing of the
// store: the attempt's witness does. The same read made for another job is
// not this deployment's store, and stays internal_unknown.
func TestAGapPreflightOnAHungMasterFailsTheSlotAsRedisUnavailable(t *testing.T) {
	fixture := newStoreOutageFixture(t)
	row := completeAttempt(t, 300*time.Millisecond, func(ctx context.Context) error {
		return gapPreflight(ctx, fixture.gapStore(t, fixture.hung, redisfailure.CallerRuntimeState))
	})
	if row.ReasonCode != observability.ReasonCode(contract.ReasonRedisUnavailable) {
		t.Fatalf("reason = %s, want %s", row.ReasonCode, contract.ReasonRedisUnavailable)
	}
	call := row.StoreCall
	if call == nil || call.Client != "runtime" || call.Caller != redisfailure.CallerRuntimeState || call.Reason != redisfailure.Timeout {
		t.Fatalf("store call = %+v, want the runtime client's Runtime State call, timed out", call)
	}
	if !call.LeftKnown || call.Left <= 0 || call.Left > 300*time.Millisecond || call.Waited < call.Left {
		t.Fatalf("waited %s with %s left (known %v), want a call that ran out its time", call.Waited, call.Left, call.LeftKnown)
	}

	other := completeAttempt(t, 300*time.Millisecond, func(ctx context.Context) error {
		return gapPreflight(ctx, fixture.gapStore(t, fixture.hung, redisfailure.CallerDiagnosticRead))
	})
	if other.ReasonCode != observability.ReasonInternalUnknown || other.StoreCall != nil {
		t.Fatalf("another job's read: reason %s store call %+v, want %s and none", other.ReasonCode, other.StoreCall, observability.ReasonInternalUnknown)
	}
}

// What the witness keeps does not depend on which of the attempt's
// goroutines ran last: one call hangs until the attempt's time runs out while
// another is refused at once beside it, and a third, made on a context the
// deadline does not reach, is refused after it; the Slot is the store's, with
// the call that ran out its time on the row.
func TestAConcurrentRefusedCallDoesNotDisplaceTheCallThatRanOutItsTime(t *testing.T) {
	fixture := newStoreOutageFixture(t)
	job := redisfailure.CallerRuntimeState
	row := completeAttempt(t, 300*time.Millisecond, func(ctx context.Context) error {
		var wait sync.WaitGroup
		refused := make(chan error, 2)
		wait.Add(2)
		go func() {
			defer wait.Done()
			_ = redisForCaller(fixture.hung, job).Get(ctx, "k").Err()
			refused <- redisForCaller(fixture.refusing, job).Get(context.WithoutCancel(ctx), "k").Err()
		}()
		go func() {
			defer wait.Done()
			refused <- redisForCaller(fixture.refusing, job).Get(ctx, "k").Err()
		}()
		wait.Wait()
		close(refused)
		for err := range refused {
			if call := redisfailure.CallErrorOf(err); call == nil || call.Reason != redisfailure.ConnectionRefused || call.RanOutItsTime() {
				t.Errorf("a refused call came back %+v, want refused with time left", call)
			}
		}
		<-ctx.Done()
		return fmt.Errorf("complete Slot: %w", ctx.Err())
	})
	if row.ReasonCode != observability.ReasonCode(contract.ReasonRedisUnavailable) || row.StoreCall == nil || row.StoreCall.Reason != redisfailure.Timeout {
		t.Fatalf("reason %s store call %+v, want %s with the call that ran out its time", row.ReasonCode, row.StoreCall, contract.ReasonRedisUnavailable)
	}
}

// A call that ran out its time names the Slot only when that wait was at least
// half of the attempt. One that ran out a short time of its own before the
// Slot went on and spent the rest, and one issued with the last moments of a
// Slot whose time went elsewhere, are only the last thing that waited: the
// Slot stays internal_unknown, the call's facts on its row.
func TestACallThatWaitedLessThanHalfTheAttemptDoesNotNameTheSlot(t *testing.T) {
	fixture := newStoreOutageFixture(t)
	job := redisfailure.CallerRuntimeState
	ownShortTime := completeAttempt(t, 400*time.Millisecond, func(ctx context.Context) error {
		shorter, cancel := context.WithTimeout(ctx, 100*time.Millisecond) // fixed-wait: the call's own time, a quarter of the attempt's
		defer cancel()
		_ = redisForCaller(fixture.hung, job).Get(shorter, "k").Err()
		<-ctx.Done()
		return fmt.Errorf("complete Slot: %w", ctx.Err())
	})
	if ownShortTime.ReasonCode != observability.ReasonInternalUnknown || ownShortTime.StoreCall == nil || ownShortTime.StoreCall.Left > 100*time.Millisecond {
		t.Fatalf("a call that ran out its own short time: reason %s store call %+v, want %s with that call", ownShortTime.ReasonCode, ownShortTime.StoreCall, observability.ReasonInternalUnknown)
	}
	lastMoments := completeAttempt(t, 400*time.Millisecond, func(ctx context.Context) error {
		elsewhere, cancel := context.WithTimeout(ctx, 350*time.Millisecond) // fixed-wait: the Slot's time spent elsewhere before its last call
		defer cancel()
		<-elsewhere.Done()
		_ = redisForCaller(fixture.hung, job).Get(ctx, "k").Err()
		<-ctx.Done()
		return fmt.Errorf("complete Slot: %w", ctx.Err())
	})
	if lastMoments.ReasonCode != observability.ReasonInternalUnknown || lastMoments.StoreCall == nil || !lastMoments.StoreCall.LeftKnown || lastMoments.StoreCall.Left > 100*time.Millisecond {
		t.Fatalf("a call issued with the last moments: reason %s store call %+v, want %s with that call", lastMoments.ReasonCode, lastMoments.StoreCall, observability.ReasonInternalUnknown)
	}
}

// A witness is its attempt's: the attempt after one whose store call ran out
// its time, spending its own time elsewhere, is internal_unknown; and so is
// an attempt whose store refused it at once and whose time then ran out
// elsewhere, because a call that failed with time left is not what ran the
// Slot out of time.
func TestAnAttemptWhoseTimeRanOutElsewhereIsNotTheStores(t *testing.T) {
	fixture := newStoreOutageFixture(t)
	first := completeAttempt(t, 300*time.Millisecond, func(ctx context.Context) error {
		return gapPreflight(ctx, fixture.gapStore(t, fixture.hung, redisfailure.CallerRuntimeState))
	})
	if first.ReasonCode != observability.ReasonCode(contract.ReasonRedisUnavailable) {
		t.Fatalf("first attempt: reason %s, want %s", first.ReasonCode, contract.ReasonRedisUnavailable)
	}
	elsewhere := func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("complete Slot: %w", ctx.Err())
	}
	if next := completeAttempt(t, 100*time.Millisecond, elsewhere); next.ReasonCode != observability.ReasonInternalUnknown || next.StoreCall != nil {
		t.Fatalf("the next attempt: reason %s store call %+v, want %s and none", next.ReasonCode, next.StoreCall, observability.ReasonInternalUnknown)
	}
	refusedFirst := completeAttempt(t, 200*time.Millisecond, func(ctx context.Context) error {
		if call := redisfailure.CallErrorOf(redisForCaller(fixture.refusing, redisfailure.CallerRuntimeState).Get(ctx, "k").Err()); call == nil {
			t.Error("the refused call came back unmarked")
		}
		return elsewhere(ctx)
	})
	if refusedFirst.ReasonCode != observability.ReasonInternalUnknown || refusedFirst.StoreCall != nil {
		t.Fatalf("refused, then out of time elsewhere: reason %s store call %+v, want %s and none", refusedFirst.ReasonCode, refusedFirst.StoreCall, observability.ReasonInternalUnknown)
	}
	// The witness speaks for a Slot that ended on its deadline and nothing
	// more: one that failed for something else nobody named, after a call of
	// its ran out its own shorter time, is not the store's.
	unnamed := completeAttempt(t, time.Second, func(ctx context.Context) error {
		hungCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond) // fixed-wait: the hung call's own time, shorter than the attempt's
		defer cancel()
		_ = redisForCaller(fixture.hung, redisfailure.CallerRuntimeState).Get(hungCtx, "k").Err()
		return fmt.Errorf("complete Slot: something nobody named")
	})
	if unnamed.ReasonCode != observability.ReasonInternalUnknown || unnamed.StoreCall != nil {
		t.Fatalf("an unnamed failure after a call ran out its own time: reason %s store call %+v, want %s and none", unnamed.ReasonCode, unnamed.StoreCall, observability.ReasonInternalUnknown)
	}
}

// A Slot whose error carries its own store call that did not answer is the
// store's whatever the call's reason, the row carrying that call; an error
// rebuilt as text has lost it and stays internal_unknown.
func TestASlotWhoseErrorCarriesItsStoreCallIsTheStores(t *testing.T) {
	fixture := newStoreOutageFixture(t)
	refused := func(ctx context.Context) error {
		return redisForCaller(fixture.refusing, redisfailure.CallerRuntimeState).Get(ctx, "k").Err()
	}
	row := completeAttempt(t, time.Second, func(ctx context.Context) error {
		return fmt.Errorf("complete Slot: %w", refused(ctx))
	})
	if row.ReasonCode != observability.ReasonCode(contract.ReasonRedisUnavailable) || row.StoreCall == nil || row.StoreCall.Reason != redisfailure.ConnectionRefused {
		t.Fatalf("reason %s store call %+v, want %s with the refused call", row.ReasonCode, row.StoreCall, contract.ReasonRedisUnavailable)
	}
	// Another job's call on the same client, in the error, is not this
	// deployment's store: the row carries the call and the Slot stays unnamed.
	otherJob := completeAttempt(t, time.Second, func(ctx context.Context) error {
		return fmt.Errorf("complete Slot: %w", redisForCaller(fixture.refusing, redisfailure.CallerDiagnosticRead).Get(ctx, "k").Err())
	})
	if otherJob.ReasonCode != observability.ReasonInternalUnknown || otherJob.StoreCall == nil || otherJob.StoreCall.Caller != redisfailure.CallerDiagnosticRead {
		t.Fatalf("another job's refused call: reason %s store call %+v, want %s with that call", otherJob.ReasonCode, otherJob.StoreCall, observability.ReasonInternalUnknown)
	}
	rebuilt := completeAttempt(t, time.Second, func(ctx context.Context) error {
		return fmt.Errorf("complete Slot: %v", refused(ctx))
	})
	if rebuilt.ReasonCode != observability.ReasonInternalUnknown || rebuilt.StoreCall != nil {
		t.Fatalf("an error rebuilt as text: reason %s store call %+v, want %s and none", rebuilt.ReasonCode, rebuilt.StoreCall, observability.ReasonInternalUnknown)
	}
}

// A Slot a rule before names keeps that name though its error also carries
// a store call that did not answer: the store's word is for a Slot nothing
// else named. A Slot whose stable segments did not read is SEGMENT_READ_FAILED
// whatever made the read fail.
func TestASlotARuleBeforeNamesKeepsItsNameOverTheStoreCall(t *testing.T) {
	call := &redisfailure.CallError{Client: "runtime", Caller: redisfailure.CallerRuntimeState, Reason: redisfailure.Timeout, Err: context.DeadlineExceeded}
	err := fmt.Errorf("complete Slot: %w", fmt.Errorf("%w: %w", worker.ErrSegmentsUnread, call))
	storeCall, named := slotStoreCall(err, nil, time.Second)
	if !named {
		t.Fatal("a call that failed with time left does not name the Slot")
	}
	if reason, _ := slotFailureReason(err, storeCall); reason != observability.ReasonCode(contract.ReasonSegmentReadFailed) {
		t.Fatalf("reason = %s, want the segment read's %s", reason, contract.ReasonSegmentReadFailed)
	}
	if storeCallFacts(storeCall) == nil {
		t.Fatal("the row lost the store call's facts")
	}
}

// closedAddress is an address nothing listens on: a master that refuses.
func closedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

// respServer answers each RESP command with what answer returns for it; a
// nil answer leaves the command unanswered, as a server that hangs does.
func respServer(t *testing.T, answer func(args []string) []byte) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			go func() {
				reader := bufio.NewReader(conn)
				for {
					args, err := readRESPCommand(reader)
					if err != nil {
						return
					}
					if reply := answer(args); reply != nil {
						if _, err := conn.Write(reply); err != nil {
							return
						}
					}
				}
			}()
		}
	}()
	return listener.Addr().String()
}

// readRESPCommand reads one command a client sends: an array of bulk strings.
func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("not a command: %q", line)
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count < 1 {
		return nil, fmt.Errorf("command length %q", line)
	}
	args := make([]string, 0, count)
	for range count {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "$")))
		if err != nil || size < 0 {
			return nil, fmt.Errorf("bulk length %q", header)
		}
		body := make([]byte, size+2)
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, err
		}
		args = append(args, string(body[:size]))
	}
	return args, nil
}

// sentinelAnswering is a Sentinel that names master as the master of name and
// knows no other Sentinel.
func sentinelAnswering(name, master string) func([]string) []byte {
	host, port, _ := net.SplitHostPort(master)
	bulk := func(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }
	return func(args []string) []byte {
		switch {
		case len(args) == 3 && strings.EqualFold(args[0], "sentinel") && strings.EqualFold(args[1], "get-master-addr-by-name") && args[2] == name:
			return []byte("*2\r\n" + bulk(host) + bulk(port))
		case len(args) == 3 && strings.EqualFold(args[0], "sentinel") && strings.EqualFold(args[1], "sentinels"):
			return []byte("*0\r\n")
		case strings.EqualFold(args[0], "subscribe"):
			var reply strings.Builder
			for index, channel := range args[1:] {
				reply.WriteString("*3\r\n" + bulk("subscribe") + bulk(channel) + ":" + strconv.Itoa(index+1) + "\r\n")
			}
			return []byte(reply.String())
		case strings.EqualFold(args[0], "ping"):
			return []byte("*2\r\n" + bulk("pong") + bulk(""))
		}
		return []byte("-ERR unknown command\r\n")
	}
}
