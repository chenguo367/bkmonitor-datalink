// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/progress"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// commitLostStore is the Progress store with its next commit, once armed,
// lost before it reaches the record, as a store that did not answer loses
// it: the attempt ends between its apply and its commit.
type commitLostStore struct {
	*progress.Store
	armed atomic.Bool
}

func (store *commitLostStore) CommitProgress(ctx context.Context, request execution.ProgressCommitRequest) (execution.ProgressCommitResult, error) {
	if store.armed.CompareAndSwap(true, false) {
		return execution.ProgressCommitResult{}, errors.New("injected: the Progress commit did not reach the store")
	}
	return store.Store.CommitProgress(ctx, request)
}

// appliedMarks is the Slot-applied mark store production wires, over the
// rig's Redis.
func appliedMarks(t *testing.T, address string) *state.SlotAppliedMarkStore {
	t.Helper()
	backend, err := state.NewRedisBackend(state.RedisBackendOptions{Address: address, DB: 0,
		DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, PoolSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	router, err := state.NewFixedRouter("progress-anchor-marks", backend)
	if err != nil {
		t.Fatal(err)
	}
	marks, err := state.NewSlotAppliedMarkStore("alarmd-progress-anchor", router)
	if err != nil {
		t.Fatal(err)
	}
	return marks
}

// After every commit the Slot the record's completed facts derive as next
// is the cursor it stores: the two are one fact, read the same by the
// commit, the BeginSlot and the slot source (ContinuityAnchor).
func assertAnchorAtCursor(t *testing.T, store *progress.Store, after string) execution.ScheduleProgress {
	t.Helper()
	loaded, err := store.LoadProgress(context.Background(), execution.ProgressIdentity{QueryGroup: frozenContract().Slot.QueryGroup})
	if err != nil || loaded.Progress == nil {
		t.Fatalf("%s: the record read (%+v, %v)", after, loaded, err)
	}
	record := *loaded.Progress
	if completed, anchored := record.ContinuityAnchor(); anchored {
		// The rig's Slots are a minute apart (g3aFixedSlotResolver).
		if next := completed + 60; next != record.NextSlot {
			t.Errorf("%s: the record's completed facts derive %d as next, it stores %d (last full %d, gap %+v, last completion %+v)",
				after, next, record.NextSlot, record.LastFullSlot, record.CurrentOrRecentGap, record.LastCompletion)
		}
	}
	return record
}

// A Slot an attempt evaluated and applied whole and could not write down -
// the attempt ending between its apply and its commit, as a crash or a
// store that did not answer ends it - is finished query-free once its
// replay window passed, and is no gap: that completion moves neither the
// last full Slot nor the recent gap. The record's cursor moves on. The
// next Slot begins at the cursor and commits there, and so does every Slot
// after it.
//
// It used to be refused for good: the commit derived the next Slot from the
// last full Slot and the gap, one Slot behind the cursor, and every attempt
// at the next Slot was a conflict, the record never written again - two
// strategies of a test deployment stood seventeen and twenty hours without
// a detection, through a restart onto another build.
//
// Through Execute, wired as production wires the fence, the Progress store
// over the ownership store's fenced compare-and-set, the State, the gap
// markers and the Slot-applied marks in Redis.
func TestASlotFinishedQueryFreeAfterItsAttemptAppliedItWholeLeavesTheNextSlotCommittable(t *testing.T) {
	var commits *commitLostStore
	fixed := newFixedCostRig(t, 4, "alarmd-progress-anchor", func(fixed *fixedCostRig, ports *worker.Ports) {
		commits = &commitLostStore{Store: fixed.progress}
		ports.Progress, ports.ExecutionEvidence = commits, appliedMarks(t, fixed.address)
	})

	// A Slot written down whole: the record's last full Slot.
	full := fixed.round(t, false, nil, nil)
	if full.err != nil || !full.result.Completed {
		t.Fatalf("fixture: the first Slot: result=%+v err=%v", full.result, full.err)
	}
	before := assertAnchorAtCursor(t, fixed.progress, "the first Slot")
	if before.LastFullSlot != fixed.at {
		t.Fatalf("fixture: the first Slot left the last full Slot at %d, want %d", before.LastFullSlot, fixed.at)
	}

	// The next Slot evaluated, applied, and lost its commit.
	commits.armed.Store(true)
	lost := fixed.round(t, false, nil, nil)
	if lost.err == nil {
		t.Fatalf("fixture: the Slot whose commit was lost completed: %+v", lost.result)
	}
	applied := fixed.at

	// Its replay window passed: finished without a query, the mark saying
	// an earlier attempt applied every Plan.
	fixed.ports.finalizationMode = execution.FinalizationGapSkipped
	finished := fixed.round(t, true, nil, nil)
	fixed.ports.finalizationMode = ""
	if finished.err != nil || !finished.result.Completed || finished.result.CompletionKind != execution.CompletionGapSkipped {
		t.Fatalf("fixture: the query-free finish: result=%+v err=%v", finished.result, finished.err)
	}
	record := assertAnchorAtCursor(t, fixed.progress, "the query-free finish of a Slot applied whole")
	// The record a test deployment's two strategies stood on: the cursor on,
	// the last full Slot and the gap where they were.
	if record.NextSlot != applied+60 || record.LastFullSlot != before.LastFullSlot || record.LastCompletion == nil ||
		record.LastCompletion.Slot != applied || record.LastCompletionKind != execution.CompletionGapSkipped {
		t.Fatalf("fixture: the query-free finish left %+v, want the cursor at %d, the last full Slot at %d and the last "+
			"completion GAP_SKIPPED at %d", record, applied+60, before.LastFullSlot, applied)
	}
	// No gap: the mark said the attempt before applied every Plan.
	if gap := record.CurrentOrRecentGap; gap != nil && gap.Kind == execution.CompletionGapSkipped {
		t.Fatalf("fixture: the query-free finish recorded the gap %+v: the attempt before was not read as applied whole", *gap)
	}

	// The Slot after it, and the one after that.
	for round := 0; round < 2; round++ {
		next := fixed.round(t, false, nil, nil)
		if next.err != nil || !next.result.Completed {
			t.Fatalf("the Slot %d after the query-free finish: result=%+v err=%v", fixed.at, next.result, next.err)
		}
		written := assertAnchorAtCursor(t, fixed.progress, "a Slot after the query-free finish")
		if written.NextSlot != fixed.at+60 || written.LastCompletion == nil || written.LastCompletion.Slot != fixed.at {
			t.Fatalf("the Slot %d committed %+v, want its completion and the cursor after it", fixed.at, written)
		}
	}
}
