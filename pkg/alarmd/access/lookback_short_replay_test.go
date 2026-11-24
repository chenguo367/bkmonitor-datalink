// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package access

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
)

// passingProvider answers as fakeProvider does, its read taking took on the
// clock pass moves.
type passingProvider struct {
	fakeProvider
	took time.Duration
	pass func(time.Duration)
}

func (provider *passingProvider) Execute(ctx context.Context, attempt execution.QueryAttempt, sink execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
	provider.pass(provider.took)
	return provider.fakeProvider.Execute(ctx, attempt, sink)
}

// A replay short of time - distance one, age zero: the normal round in its
// place, sent to replay before its deadline because its read would not fit -
// that reads whole is timed into its Query Group's reading from its permit
// on, as that normal round's first read would have been (014 next9a, the
// boundary of B' and E). It is counted among no first reads or their bytes
// and taken as no sample. One that reads whole in longer than that normal
// round's whole budget - its readiness to the query deadline its read is cut
// at, the live one, which it was diverted against - is not timed: no normal
// round could have read it. A replay past its deadline is not timed. Before,
// no replay was: a group diverted round after round by its reading had the
// reading age, and ran as a normal round on the fallback.
//
// The Slot is ready a second after its evaluation, its consumer due 30 s
// after it, under a frozen reserve of 5 s: its normal round has 24 s under
// the frozen reserve; under a live reserve of 8 s, 21 s; of 2 s, 27 s. A read
// of 23 s is in the first and not the second, one of 26 s in the third and
// not the first.
func TestAReplayShortOfTimeThatReadsWholeIsTimedFromItsPermit(t *testing.T) {
	slot, frozen := frozenExecution(t)
	consumer := frozen.Requirements[0].Consumers[0]
	if due := consumer.ConsumerDeadlineUnixMilli - int64(slot.Slot.EvaluationTime)*1000; due != 30_000 ||
		consumer.DownstreamExecutionReserveMilliSec != 5_000 {
		t.Fatalf("fixture: the consumer is due %d ms after the Slot under a reserve of %d ms", due, consumer.DownstreamExecutionReserveMilliSec)
	}
	for _, c := range []struct {
		name  string
		age   int64
		live  time.Duration
		took  time.Duration
		timed bool
	}{
		{"short of time", 0, 0, 3 * time.Second, true},
		{"short of time, slower than its normal round", 0, 0, 25 * time.Second, false},
		{"short of time, slower than its normal round under a live reserve past the frozen one", 0, 8 * time.Second, 23 * time.Second, false},
		{"short of time, within its normal round under a live reserve short of the frozen one", 0, 2 * time.Second, 26 * time.Second, true},
		{"past its deadline", 5_000, 0, 3 * time.Second, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			ref, frozen := frozenExecution(t)
			var mu sync.Mutex
			at := time.UnixMilli(int64(ref.Slot.EvaluationTime)*1000 + 10_000)
			now := func() time.Time {
				mu.Lock()
				defer mu.Unlock()
				return at
			}
			pass := func(took time.Duration) {
				mu.Lock()
				at = at.Add(took)
				mu.Unlock()
			}
			engine, err := lookback.New(lookback.Options{Now: now, UnspreadFirstSamples: true,
				Recheck: func(context.Context, execution.PhysicalQuerySpec, execution.ProviderSeriesSink) (execution.ProviderCompletion, error) {
					return execution.ProviderCompletion{}, nil
				},
				Permit: func() (func(), <-chan struct{}, string) { return func() {}, nil, "" }, Owns: func(execution.QueryGroupIdentity) bool { return true },
				Owned: func() int { return 1 }})
			if err != nil {
				t.Fatal(err)
			}
			// The permit takes 4 s to come, the read its time after it.
			permits := &recordingQueryPermits{onAcquire: func() { pass(4 * time.Second) }}
			config := Config{MinReadyDelay: time.Second, Lookback: engine, Now: now}
			if c.live > 0 {
				config.LiveReserve = &execution.LiveReserve{}
				config.LiveReserve.Set(c.live)
			}
			source, err := NewSource(staticFrozenPlan{plan: frozen}, &passingProvider{took: c.took, pass: pass}, permits, config)
			if err != nil {
				t.Fatal(err)
			}
			source.wait = func(context.Context, time.Duration) error { return nil }
			if _, err := source.Execute(context.Background(), execution.QueryExecutionRequest{Contract: ref, Operation: execution.OperationReplay,
				AttemptNo: 1, ReplayDistance: 1, ReplayAgeMillis: c.age}, &recordingConsumer{}); err != nil {
				t.Fatal(err)
			}
			if len(permits.attempts) != 1 {
				t.Fatalf("fixture: %d permits asked, want the replay's one", len(permits.attempts))
			}
			got, measured := engine.QueryDuration(ref.Slot.QueryGroup)
			if c.timed && (!measured || got != c.took) {
				t.Fatalf("the group's reading is %v (measured %t), want the read's %v from its permit", got, measured, c.took)
			}
			if !c.timed && measured {
				t.Fatalf("the replay that took %v left the group a reading of %v, want none", c.took, got)
			}
			// The bound of the lookback's own reads again is a normal round's
			// request reading; the replay took none.
			if bound, ok := engine.RereadBound(ref.Slot.QueryGroup); ok {
				t.Fatalf("the replay gave the group a reread bound of %v, want none", bound)
			}
			stats := engine.Stats()
			for name, counted := range stats.Sources {
				if counted.FirstReads != 0 || counted.FirstReadBytes != 0 || counted.Samples[lookback.OutcomeCaptured] != 0 {
					t.Fatalf("%s counted %d first reads of %d bytes and samples %v, want the replay counted nowhere", name,
						counted.FirstReads, counted.FirstReadBytes, counted.Samples)
				}
			}
			if stats.Pending != 0 {
				t.Fatalf("%d samples in flight, want none", stats.Pending)
			}
		})
	}
}
