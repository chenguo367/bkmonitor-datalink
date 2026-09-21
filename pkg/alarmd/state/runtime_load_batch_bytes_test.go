// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package state

import "testing"

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
	store := &ExecutionStore{}

	// Nothing read yet: the item bound is all there is, which is the exposure
	// that existed before this and now lasts one call.
	if limit := store.runtimeLoadBatchLimit(); limit != runtimeLoadBatchItems {
		t.Fatalf("first batch limit = %d, want the item bound %d", limit, runtimeLoadBatchItems)
	}

	// The shape that produced this decision: 249 records of about 345 KiB.
	store.observeValueBytes(249, 249*345*1024)
	limit := store.runtimeLoadBatchLimit()
	if limit >= runtimeLoadBatchItems {
		t.Fatalf("batch limit = %d against records of %d bytes; the item bound alone is what let one call "+
			"ask for 86 MB", limit, store.expectedValueBytes())
	}
	if expected := uint64(limit) * store.expectedValueBytes(); expected > runtimeLoadBatchBytes {
		t.Fatalf("a batch of %d records of %d bytes is %d, over the %d bound",
			limit, store.expectedValueBytes(), expected, runtimeLoadBatchBytes)
	}

	// Small records are not punished for the big ones: the bound is what the
	// batch will move, so an ordinary Query Group keeps the full item bound.
	small := &ExecutionStore{}
	small.observeValueBytes(1000, 1000*2048)
	if limit := small.runtimeLoadBatchLimit(); limit != runtimeLoadBatchItems {
		t.Fatalf("batch limit = %d for 2 KiB records, want the full item bound: sizing every batch against "+
			"the 512 KiB ceiling would cut batches for records nowhere near it", limit)
	}

	// A single record over the whole batch budget is still read, alone. The
	// store accepts records up to MaxValueBytes, so refusing one here would
	// stop a Plan that writes successfully.
	huge := &ExecutionStore{}
	huge.observeValueBytes(1, runtimeLoadBatchBytes*2)
	if limit := huge.runtimeLoadBatchLimit(); limit != 1 {
		t.Fatalf("batch limit = %d for a record larger than the batch budget, want 1", limit)
	}

	// A reading that says nothing changes nothing.
	steady := store.expectedValueBytes()
	store.observeValueBytes(0, 1<<20)
	store.observeValueBytes(10, -1)
	if store.expectedValueBytes() != steady {
		t.Fatalf("expected bytes moved to %d on an empty batch, from %d", store.expectedValueBytes(), steady)
	}
}

// The learned size follows a strategy whose records just grew, because that is
// when the bound matters and a plain lifetime mean would take the longest to
// notice.
func TestTheLearnedRecordSizeFollowsAStrategyThatGrew(t *testing.T) {
	store := &ExecutionStore{}
	for range 20 {
		store.observeValueBytes(100, 100*4096)
	}
	small := store.expectedValueBytes()
	if small == 0 {
		t.Fatal("nothing learned from twenty batches")
	}
	for range 20 {
		store.observeValueBytes(100, 100*345*1024)
	}
	grown := store.expectedValueBytes()
	if grown <= small*10 {
		t.Fatalf("expected bytes went from %d to %d after the records grew 86x; a bound that lags this far "+
			"behind is the item bound with extra steps", small, grown)
	}
}
