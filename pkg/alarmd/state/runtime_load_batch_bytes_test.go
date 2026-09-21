// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package state

import (
	"context"
	"errors"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func mustExpect(t *testing.T, store *ExecutionStore, group execution.QueryGroupIdentity) uint64 {
	t.Helper()
	size, learned := store.expectedValueBytes(group)
	if !learned {
		t.Fatalf("no record size learned for %q", group)
	}
	return size
}

func sizedStore() *ExecutionStore {
	return &ExecutionStore{options: ExecutionStoreOptions{MaxValueBytes: 512 << 10}}
}

// A preflight batch is bounded by what it is expected to move, not by key count
// alone.
//
// The apply side has had a byte bound since it was written, and its own comment
// gives the reason: with MaxValueBytes up to 512 KiB an item bound alone lets
// one call carry 128 MiB. The read side had only the item bound, so a Query
// Group whose records had grown to 345 KiB each asked for 86 MB in one MGET -
// which does not fit a 3 s read timeout, failed identically on all four
// attempts, and took the whole Slot with it, 99 times in a day.
func TestAPreflightBatchIsBoundedByWhatItWillMove(t *testing.T) {
	store := sizedStore()
	const group = execution.QueryGroupIdentity("qg-heavy")

	// Nothing read for this Query Group yet. The bound is the only one that
	// holds whatever its records turn out to be: the batch budget over the
	// largest value the store accepts.
	first := store.runtimeLoadBatchLimit(group)
	if want := int(runtimeLoadBatchBytes / (512 << 10)); first != want {
		t.Fatalf("first batch limit = %d, want %d - the only shape-independent bound there is", first, want)
	}
	if uint64(first)*(512<<10) > runtimeLoadBatchBytes {
		t.Fatal("the first batch for an unknown Query Group can exceed the batch budget")
	}

	// The shape that produced this decision: records of about 345 KiB.
	store.observeValueBytes(group, 249, 249*345*1024)
	limit := store.runtimeLoadBatchLimit(group)
	if expected := uint64(limit) * mustExpect(t, store, group); expected > runtimeLoadBatchBytes {
		t.Fatalf("a batch of %d records of %d bytes is %d, over the %d bound",
			limit, mustExpect(t, store, group), expected, runtimeLoadBatchBytes)
	}

	// Ordinary records get the full item bound once they are known: the bound
	// is what the batch will move, so learning is what buys back the batch size
	// the safe first call gave up.
	small := sizedStore()
	small.observeValueBytes("qg-small", 1000, 1000*2048)
	if limit := small.runtimeLoadBatchLimit("qg-small"); limit != runtimeLoadBatchItems {
		t.Fatalf("batch limit = %d for 2 KiB records, want the full item bound %d", limit, runtimeLoadBatchItems)
	}

	// A single record over the whole batch budget is still read, alone.
	huge := sizedStore()
	huge.observeValueBytes("qg-huge", 1, runtimeLoadBatchBytes*2)
	if limit := huge.runtimeLoadBatchLimit("qg-huge"); limit != 1 {
		t.Fatalf("batch limit = %d for a record larger than the batch budget, want 1", limit)
	}
}

// What one Query Group's records weigh says nothing about another's, so the
// size is learned per Query Group.
//
// Record size is a property of one strategy's retention and Level count. A
// replica holding two thousand ordinary objects of a few KiB and one object of
// 345 KiB averages to a few KiB, so a process-wide figure hands the largest
// batch to the one object that needs the smallest - the bound is defeated by
// exactly the population it exists for, and it looks like it is working.
func TestTheRecordSizeIsLearnedPerQueryGroup(t *testing.T) {
	store := sizedStore()
	const ordinary, heavy = execution.QueryGroupIdentity("qg-ordinary"), execution.QueryGroupIdentity("qg-heavy")

	// Twenty ordinary reads, as a busy replica produces between two Slots of
	// the heavy object.
	for range 20 {
		store.observeValueBytes(ordinary, 256, 256*3*1024)
	}
	store.observeValueBytes(heavy, 249, 249*345*1024)

	// Interleaved again, which is what a real replica does.
	for range 20 {
		store.observeValueBytes(ordinary, 256, 256*3*1024)
	}

	if limit := store.runtimeLoadBatchLimit(ordinary); limit != runtimeLoadBatchItems {
		t.Fatalf("ordinary batch limit = %d, want the full item bound: the heavy object must not shrink "+
			"everyone else's batches either", limit)
	}
	heavyLimit := store.runtimeLoadBatchLimit(heavy)
	if moved := uint64(heavyLimit) * mustExpect(t, store, heavy); moved > runtimeLoadBatchBytes {
		t.Fatalf("heavy batch of %d keys moves %d, over the %d bound; twenty ordinary reads in between "+
			"must not raise what this Query Group is allowed to ask for", heavyLimit, moved, runtimeLoadBatchBytes)
	}
	if heavyLimit >= runtimeLoadBatchItems {
		t.Fatalf("heavy batch limit = %d, the full item bound - which is the 86 MB call, unchanged", heavyLimit)
	}
}

// A batch that did not come back teaches nothing.
//
// A failed read has loaded zero bytes. Folded in, it lowers the estimate and
// the next batch is allowed to be at least as large, so the read that was
// already too big to finish is reissued at the same size - and every failure
// makes the estimate smaller. An instrument that learns "smaller" from the
// event it exists to catch reports the reverse of the truth exactly when
// consulted.
//
// Stated through the load path rather than against the accounting function,
// because the distinction lives at the call: "came back empty" is a reading
// about records that are not there, and "did not come back" is no reading at
// all, and the two are told apart by the error. Written against the function,
// the case cannot see which of them is in force.
func TestAFailedBatchTeachesNothing(t *testing.T) {
	backend := newPipelineMemoryBackend()
	store := newBatchStore(t, backend, nil)
	group := frozenRef().Slot.QueryGroup

	mutations := seriesMutations(t, 4, applyVersion(), 0)
	request := execution.StatePreflightRequest{Contract: frozenRef(), Items: preflightItems(mutations)}
	if _, err := store.LoadRuntime(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	learned, ok := store.expectedValueBytes(group)
	if !ok {
		t.Fatal("a read that came back taught nothing")
	}

	backend.failMGet = errors.New("i/o timeout")
	if _, err := store.LoadRuntime(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	after, _ := store.expectedValueBytes(group)
	if after != learned {
		t.Fatalf("a failed batch moved the estimate from %d to %d; every timeout would let the next "+
			"batch be at least as large, which is the wrong direction from the only evidence there is",
			learned, after)
	}
}

// The learned size follows a strategy whose records just grew, because that is
// when the bound matters and a lifetime mean is slowest then.
func TestTheLearnedRecordSizeFollowsAStrategyThatGrew(t *testing.T) {
	store := sizedStore()
	const group = execution.QueryGroupIdentity("qg")
	for range 20 {
		store.observeValueBytes(group, 100, 100*4096)
	}
	small := mustExpect(t, store, group)
	if small == 0 {
		t.Fatal("nothing learned from twenty batches")
	}
	for range 20 {
		store.observeValueBytes(group, 100, 100*345*1024)
	}
	if grown := mustExpect(t, store, group); grown <= small*10 {
		t.Fatalf("expected bytes went from %d to %d after the records grew 86x; a bound that lags this "+
			"far behind is the item bound with extra steps", small, grown)
	}
}

// The table is bounded, and running past the bound costs one safe batch per
// Query Group rather than unbounded memory.
func TestTheLearnedSizeTableIsBounded(t *testing.T) {
	store := sizedStore()
	for index := range runtimeValueSizeGroups + 64 {
		store.observeValueBytes(execution.QueryGroupIdentity(string(rune('a'+index%26))+string(rune(index))), 10, 10*4096)
	}
	store.valueSizes.mu.RLock()
	size := len(store.valueSizes.bytes)
	store.valueSizes.mu.RUnlock()
	if size > runtimeValueSizeGroups {
		t.Fatalf("learned sizes for %d Query Groups, over the %d bound", size, runtimeValueSizeGroups)
	}
}
