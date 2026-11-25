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
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// Every write of every Slot attempt this package's tests run, watched at the
// ports (014 ⑩ §7 cases 2 and 6): sent as beforeWrite cleared it - a write
// whose script does not run the owner fence (a gap statement, an event, the
// no-data memory and its ends) cleared as one, the fence confirmed in its
// step - and after the attempt's BeginSlot, when it put it off. The frozen-
// State renewal alone is not watched: it only lengthens a key's life. A
// write not sent so is written down here, and the package's tests fail on
// it, naming the write and where it was sent from.
func TestMain(m *testing.M) {
	worker.WatchPorts(watchWrites)
	code := m.Run()
	if found := unclearedWrites.take(); len(found) > 0 {
		fmt.Fprintf(os.Stderr, "%d writes of Slot attempts were not sent as beforeWrite cleared them (014 ⑩):\n", len(found))
		for _, write := range found {
			fmt.Fprintln(os.Stderr, write)
		}
		code = 1
	}
	os.Exit(code)
}

type writesFound struct {
	mu     sync.Mutex
	writes []string
}

var unclearedWrites writesFound

func (found *writesFound) take() []string {
	found.mu.Lock()
	defer found.mu.Unlock()
	return found.writes
}

// checkWrite is the watch of one write, named by what it writes; unfenced is
// a write whose script does not run the fence, cleared needs no clearance.
func checkWrite(ctx context.Context, name string, unfenced, needsClearance bool) {
	attempt, cleared, clearedUnfenced, begun := worker.WriteCleared(ctx)
	if !attempt {
		return
	}
	var problems []string
	if needsClearance && !cleared {
		problems = append(problems, "not cleared by beforeWrite")
	}
	if needsClearance && cleared && unfenced && !clearedUnfenced {
		problems = append(problems, "cleared as a fenced write, its script runs no fence")
	}
	if !begun {
		problems = append(problems, "sent before the attempt's BeginSlot")
	}
	if len(problems) == 0 {
		return
	}
	pcs := make([]uintptr, 12)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs)])
	var from []string
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "/pkg/alarmd/worker.") {
			from = append(from, fmt.Sprintf("%s:%d", frame.Function[strings.LastIndex(frame.Function, "/")+1:], frame.Line))
		}
		if !more || len(from) == 3 {
			break
		}
	}
	unclearedWrites.mu.Lock()
	unclearedWrites.writes = append(unclearedWrites.writes, fmt.Sprintf("  %s: %s, from %s", name, strings.Join(problems, "; "), strings.Join(from, " < ")))
	unclearedWrites.mu.Unlock()
}

func watchWrites(ports worker.Ports) worker.Ports {
	ports.GapGuard = clearedGaps{ports.GapGuard}
	ports.Events = clearedEvents{ports.Events}
	ports.NoData = watchNoData(ports.NoData)
	if ports.NoDataEnds != nil {
		ports.NoDataEnds = clearedNoDataEnds{ports.NoDataEnds}
	}
	ports.State = watchState(ports.State)
	if ports.ExecutionEvidence != nil {
		ports.ExecutionEvidence = clearedEvidence{ports.ExecutionEvidence}
	}
	return ports
}

type clearedGaps struct{ execution.GapGuardStore }

func (store clearedGaps) ApplyGap(ctx context.Context, request execution.GapGuardApplyRequest) (execution.GapGuardApplyResult, error) {
	checkWrite(ctx, "gap statement", true, true)
	return store.GapGuardStore.ApplyGap(ctx, request)
}

type clearedEvents struct{ execution.EventSink }

func (sink clearedEvents) WriteBatch(ctx context.Context, events []contract.TriggerEventV1) error {
	checkWrite(ctx, "events", true, true)
	return sink.EventSink.WriteBatch(ctx, events)
}

type clearedNoData struct{ execution.PlanNoDataStore }

func (store clearedNoData) ApplyNoData(ctx context.Context, request execution.NoDataApplyRequest) (execution.NoDataApplyResult, error) {
	checkWrite(ctx, "no-data memory", true, true)
	return store.PlanNoDataStore.ApplyNoData(ctx, request)
}

// clearedStagingNoData is the no-data store that stages, as the one it
// watches does: a staging key is the attempt's own, so it is a write the
// fence need not stand before, but one the BeginSlot must.
type clearedStagingNoData struct {
	clearedNoData
	stager execution.PlanNoDataStager
}

func (store clearedStagingNoData) StageNoData(ctx context.Context, request execution.NoDataStageRequest) (execution.NoDataStageResult, error) {
	checkWrite(ctx, "no-data staging", false, true)
	return store.stager.StageNoData(ctx, request)
}

func (store clearedStagingNoData) DiscardNoDataStage(ctx context.Context, discard execution.NoDataStageDiscard) error {
	return store.stager.DiscardNoDataStage(ctx, discard)
}

func watchNoData(store execution.PlanNoDataStore) execution.PlanNoDataStore {
	watched := clearedNoData{store}
	if stager, ok := store.(execution.PlanNoDataStager); ok {
		return clearedStagingNoData{clearedNoData: watched, stager: stager}
	}
	return watched
}

type clearedNoDataEnds struct{ execution.NoDataEndIndex }

func (index clearedNoDataEnds) ApplyNoDataEnds(ctx context.Context, write execution.NoDataEndWrite) error {
	checkWrite(ctx, "no-data ends", true, true)
	return index.NoDataEndIndex.ApplyNoDataEnds(ctx, write)
}

// The State store, as fenced and as segmented as the one it watches: a store
// that takes the fence in its script is a fenced write, one that does not is
// an unfenced one, and the segments' writes and renewals are fenced writes.
type clearedState struct{ execution.StateStore }

func (store clearedState) ApplyRuntime(ctx context.Context, request execution.StateApplyRequest) (execution.StateApplyResult, error) {
	checkWrite(ctx, "State (unfenced store)", true, true)
	return store.StateStore.ApplyRuntime(ctx, request)
}

type clearedFencedState struct {
	clearedState
	fenced execution.FencedStateStore
}

func (store clearedFencedState) ApplyRuntimeFenced(ctx context.Context, request execution.StateApplyRequest,
	fence execution.StateApplyFence) (execution.StateApplyResult, error) {
	checkWrite(ctx, "State", false, true)
	return store.fenced.ApplyRuntimeFenced(ctx, request, fence)
}

type clearedSegments struct{ segments segment.Store }

func (watch clearedSegments) ReadSegments(ctx context.Context, request segment.ReadRequest) (segment.ReadResult, error) {
	return watch.segments.ReadSegments(ctx, request)
}

func (watch clearedSegments) WriteSegments(ctx context.Context, request segment.WriteRequest) (segment.WriteResult, error) {
	checkWrite(ctx, "segments", false, true)
	return watch.segments.WriteSegments(ctx, request)
}

func (watch clearedSegments) RenewSegments(ctx context.Context, request segment.RenewRequest) (segment.RenewResult, error) {
	checkWrite(ctx, "segment renewal", false, true)
	return watch.segments.RenewSegments(ctx, request)
}

type clearedSegmentedState struct {
	clearedState
	clearedSegments
}

type clearedFencedSegmentedState struct {
	clearedFencedState
	clearedSegments
}

func watchState(store execution.StateStore) execution.StateStore {
	watched := clearedState{store}
	fenced, isFenced := store.(execution.FencedStateStore)
	segments, isSegmented := store.(segment.Store)
	switch {
	case isFenced && isSegmented:
		return clearedFencedSegmentedState{clearedFencedState{watched, fenced}, clearedSegments{segments}}
	case isFenced:
		return clearedFencedState{watched, fenced}
	case isSegmented:
		return clearedSegmentedState{watched, clearedSegments{segments}}
	}
	return watched
}

// clearedEvidence is the execution evidence mark (U5): after the BeginSlot,
// as every write is; written only by an attempt that wrote State, it is no
// write beforeWrite clears.
type clearedEvidence struct {
	execution.SlotExecutionEvidenceStore
}

func (store clearedEvidence) Record(ctx context.Context, slot execution.SlotIdentity, duePlans []execution.PlanIdentity,
	applied []execution.PlanIdentity, keepUntil time.Time, now time.Time) error {
	checkWrite(ctx, "execution evidence", false, false)
	return store.SlotExecutionEvidenceStore.Record(ctx, slot, duePlans, applied, keepUntil, now)
}
