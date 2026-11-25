// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// answeringRegardlessQuery is a provider that answers once told, whether or
// not the read's context has ended by then: a read that cannot tell an end
// it was given from the end of its answer.
type answeringRegardlessQuery struct{ answer chan struct{} }

func (query answeringRegardlessQuery) Execute(context.Context, execution.QueryExecutionRequest, execution.QueryExecutionConsumer) (execution.QueryExecutionCompletion, error) {
	<-query.answer
	return execution.QueryExecutionCompletion{AllRequiredCompleted: true}, nil
}

// A read ahead whose read answered with its context already ended is not
// the slice's read: what it answered then may be short of it. It is refused,
// counted context_ended, and the slice read again on the Slot's goroutine.
// The same read with its context live is replayed.
func TestAReadAheadThatEndedWithItsContextIsRefused(t *testing.T) {
	for _, ended := range []bool{true, false} {
		stream := newWindowedFixture(t).stream
		query := answeringRegardlessQuery{answer: make(chan struct{})}
		stream.coordinator.ports.Query = query
		stream.slices = &sliceReading{}
		parent, cancel := context.WithCancel(context.Background())
		next := inputslice.Leaf{ID: 2}
		stream.startSliceReadAheads(parent, execution.QueryExecutionRequest{}, []inputslice.Leaf{{ID: 1}, next}, 2)
		if ended {
			cancel()
		}
		close(query.answer)
		<-stream.aheads[0].done
		_, err, replayed := stream.replayReadAhead(context.Background(), next)
		stream.letGoReadAhead()
		cancel()
		if replayed == ended || err != nil {
			t.Fatalf("context ended %v: replayed %v err %v, want it replayed only with its context live", ended, replayed, err)
		}
		if got := stream.readAheadEnds[readAheadContextEnded]; (got == 1) != ended {
			t.Fatalf("context ended %v: %d refused as context_ended", ended, got)
		}
	}
}

// The series a Slot bound to a due Plan beyond those it took up are counted
// by the Plan's family - none, for a Plan of no family the algorithm
// counters name - and a Slot that took up every one counts nothing.
func TestTheSeriesBoundAndNotTakenUpAreCountedByFamily(t *testing.T) {
	plan := execution.PlanIdentity{StrategyID: "7"}
	bound := 5
	stream := &streamedExecution{
		slotStream: &slotStream{
			header: execution.InternalExecutionHeader{DuePlans: []execution.DuePlan{{Identity: plan}}},
		},
		planSeriesCounts: map[execution.PlanIdentity]*int{plan: &bound},
	}
	for range 3 {
		stream.noteTakenUp(plan)
	}
	none := len(observability.SeriesNotEvaluatedFamilies) - 1
	if observability.SeriesNotEvaluatedFamilies[none] != observability.SeriesNotEvaluatedNoFamily {
		t.Fatal("none is not the last family")
	}
	want := [observability.SeriesNotEvaluatedFamilyCount]uint64{}
	want[none] = 2
	if got := stream.seriesNotEvaluated(); got != want {
		t.Fatalf("not taken up %v, want the 2 of 5 under none", got)
	}
	for range 2 {
		stream.noteTakenUp(plan)
	}
	if got := stream.seriesNotEvaluated(); got != ([observability.SeriesNotEvaluatedFamilyCount]uint64{}) {
		t.Fatalf("not taken up %v with every series taken up, want nothing", got)
	}
}

// A Slot failed for a read ahead that ended with its context ended says so
// by its own name, on the query line and as the Slot's failure: the query
// failure the Slot's failure reason reads (a delivery category's code), not
// internal_unknown with every other failure.
func TestAReadAheadEndedWithItsContextFailsTheSlotByItsOwnName(t *testing.T) {
	err := fmt.Errorf("alarmd worker: query: %w", ErrReadAheadContextEnded)
	var failure interface{ QueryFailure() (string, string) }
	if !errors.As(err, &failure) || !errors.Is(err, ErrReadAheadContextEnded) {
		t.Fatalf("%v carries no query failure", err)
	}
	if category, code := failure.QueryFailure(); category != observability.QueryFailureCategoryDelivery || code != contract.ReasonReadAheadContextEnded {
		t.Fatalf("query failure %s %s, want delivery %s", category, code, contract.ReasonReadAheadContextEnded)
	}
	if reason := observability.NormalizeReason(observability.ReasonCode(contract.ReasonReadAheadContextEnded), observability.ResultFailed); reason != observability.ReasonCode(contract.ReasonReadAheadContextEnded) {
		t.Fatalf("the reason normalizes to %s, want it kept", reason)
	}
}
