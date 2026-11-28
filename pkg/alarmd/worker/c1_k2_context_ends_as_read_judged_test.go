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
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/worker"
)

// A Slot whose context ends as the dispatch judges a read it took back from
// a consumer - taken before the end, judged after it - ends as one stream
// does where its context ends at that point of the same slices: by the
// lowest slice a consumer has. A read above the lowest taken, the lowest
// still read: the lowest's read fails on the context, as one stream's read
// of that slice does when the context ends in it. The lowest's own read
// taken, back whole: its judgement fails on the completion's deadline the
// context ended, as one stream's does when the context ends after that read
// came back - not judged again as a read that failed. Each cell holds the
// other consumer's read until the context ends, so the read the dispatch
// takes is the one the cell names; one stream reads one slice at a time.
func TestASlotWhoseContextEndsAsTheDispatchJudgesAReadEndsByItsLowestSliceAsOneStreamDoes(t *testing.T) {
	request := windowedRequest()
	// Ended at the test's deadline, for every read held at once.
	expired := testwait.Context(t)
	setup := func(t *testing.T, permits int) (*slicedFixture, []string) {
		fixture := newSlicedFixture(t, spreadHosts(24, 4<<10), sliceableFacts(), 200)
		fixture.queryPermits, fixture.counts = permits, &slotCounts{}
		fixture.coordinator = fixture.build(t, worker.Ports{Activation: fixture.ports, State: fixture.state, GapGuard: fixture.ports,
			Events: fixture.ports}, 200)
		worker.RememberSliceShapeForTest(fixture.coordinator, request.Contract.Slot.QueryGroup, request.Contract.QueryRevision,
			[]string{"host"}, inputslice.Shape{}, 12, 1, 24)
		// The slices the Slot starts in, in the order they are handed out,
		// the dispatch reading the first.
		slices, held := worker.RestoredSlices(fixture.coordinator, request)
		if !held || len(slices) < 4 {
			t.Fatalf("fixture: the Slot starts in %d slices (held %v), want several", len(slices), held)
		}
		var numbered []string
		for _, slice := range slices {
			numbered = append(numbered, slice.Digest())
		}
		return fixture, numbered
	}
	holdUntilEnded := func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case <-expired.Done():
		}
	}
	execute := func(t *testing.T, fixture *slicedFixture, ctx context.Context) error {
		ended := make(chan error, 1)
		go func() {
			_, err := fixture.coordinator.Execute(ctx, request)
			ended <- err
		}()
		var err error
		select {
		case err = <-ended:
		case <-expired.Done():
			t.Fatal("the Slot did not end")
		}
		testwait.Until(t, "every consumer of the ended Slot ended", func() bool {
			return !goroutinesIn("(*sliceConsumer).run")
		})
		return err
	}
	for _, cell := range []struct {
		name string
		// taken is the slice whose read the dispatch takes as the context
		// ends, and held the other consumer's, read until it ends.
		taken, held int
		// oneStream ends one stream's context where the cell's ends: in the
		// lowest slice's read, or once that read came back.
		oneStream func(source *slicedSource, lowest string, cancel context.CancelFunc)
		// want says what the end is: of one stream, the fixture checked.
		want string
	}{
		{"a read above the lowest slice, the lowest still read", 2, 1,
			func(source *slicedSource, lowest string, cancel context.CancelFunc) {
				source.hold = func(ctx context.Context, slice execution.InputSlice) {
					if slice.Digest() == lowest {
						cancel()
						holdUntilEnded(ctx)
					}
				}
			}, "alarmd worker: query: "},
		{"the lowest slice's own read, back whole", 1, 2,
			func(source *slicedSource, lowest string, cancel context.CancelFunc) {
				source.afterRead = func(slice execution.InputSlice) {
					if slice.Digest() == lowest {
						cancel()
					}
				}
			}, "alarmd worker: completion deadline: "},
	} {
		t.Run(cell.name, func(t *testing.T) {
			// One stream, two permits: no read ahead, the slices read one at a
			// time in their order.
			worker.SetSliceConsumersForTest(t, 1)
			fixture, numbered := setup(t, 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cell.oneStream(fixture.source, numbered[1], cancel)
			want := execute(t, fixture, ctx)
			if want == nil || !strings.HasPrefix(want.Error(), cell.want) || ctx.Err() == nil {
				t.Fatalf("fixture: one stream whose context ended in its second slice ended with %v, want %q", want, cell.want)
			}
			wantNotCommitted := fixture.counts.notCommitted

			// Two consumers, sixteen permits: the slices numbered 1 and 2
			// handed to them at once.
			worker.SetSliceConsumersByCoresForTest(t, 4)
			fixture, numbered = setup(t, 16)
			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			held := numbered[cell.held]
			fixture.source.hold = func(ctx context.Context, slice execution.InputSlice) {
				if slice.Digest() == held {
					holdUntilEnded(ctx)
				}
			}
			var took atomic.Bool
			taken := numbered[cell.taken]
			worker.SetSliceReadTakenForTest(t, func(slice execution.InputSlice) {
				if slice.Digest() == taken && took.CompareAndSwap(false, true) {
					cancel()
				}
			})
			err := execute(t, fixture, ctx)
			if !took.Load() {
				t.Fatalf("fixture: the dispatch never took the read of slice %d back; the Slot ended with %v", cell.taken, err)
			}
			if err == nil || err.Error() != want.Error() {
				t.Fatalf("two consumers whose context ended as the dispatch judged slice %d's read, slice %d still read, ended with %v, want %v as one stream",
					cell.taken, cell.held, err, want)
			}
			if !reflect.DeepEqual(fixture.counts.notCommitted, wantNotCommitted) {
				t.Fatalf("two consumers were counted not committed as %v, want %v as one stream", fixture.counts.notCommitted, wantNotCommitted)
			}
		})
	}
}
