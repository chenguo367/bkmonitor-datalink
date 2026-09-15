// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// What the gate saves is the round trip, which is a different saving from what
// the script saves.
//
// The script's threshold means a load usually writes no new expiry; the command
// goes out either way and the process waits for the answer. Measured, that ask
// is the whole cost: 38.6 EVAL/s at 7.1ms each on around 2,100 Plans. So this
// asserts the backend was not called -- a test that read the returned value, or
// that the key was not renewed, would pass just as well against the version
// that sends the command and is told no.
func TestASecondLoadInsideTheIntervalDoesNotReachTheBackend(t *testing.T) {
	backend := &casMemoryBackend{values: make(map[string][]byte), remaining: make(map[string]time.Duration)}
	store := generationStore(t, backend)
	item := gapLoadItem("generation", nil)
	key, err := PlanGapKeyV2("alarmd", item.Identity)
	if err != nil {
		t.Fatal(err)
	}
	backend.values[key] = []byte("{}")
	backend.remaining[key] = GenerationScopedFloor

	load := func() {
		t.Helper()
		if _, err := store.LoadGaps(context.Background(), execution.GapLoadRequest{
			Contract: frozenRef(), Items: []execution.PlanGapLoadItem{item},
		}); err != nil {
			t.Fatal(err)
		}
	}

	load()
	if len(backend.renewals) != 1 {
		t.Fatalf("first load made %d renewal calls, want the one that establishes the key's life",
			len(backend.renewals))
	}
	// Every Slot for the next six hours.
	for round := 0; round < 200; round++ {
		load()
	}
	if len(backend.renewals) != 1 {
		t.Fatalf("renewal calls after 201 loads = %d, want 1: the script decides inside Redis, so every "+
			"call that reaches it has already spent the round trip this gate exists to save",
			len(backend.renewals))
	}
	// The load itself still happens: the gate is about the renewal, not about
	// reading the record, and a gate that skipped the read would stop the Plan
	// from being evaluated at all.
	if backend.reads < 201 {
		t.Fatalf("reads = %d, want one per load: the record is still read every Slot", backend.reads)
	}
}

// Once the interval is up, the ask goes out again.
//
// A gate that never reopened would be the leak this whole mechanism exists to
// close, arriving by a different route and with the same symptom: the keys work
// until they do not.
func TestTheAskGoesOutAgainOnceTheIntervalHasPassed(t *testing.T) {
	gate := newRenewalGate()
	clock := time.Unix(1_700_000_000, 0).UTC()
	gate.now = func() time.Time { return clock }
	interval := RenewalAskInterval(GenerationScopedFloor)

	if !gate.Ask("k", interval) {
		t.Fatal("a key the gate has never seen was not asked about")
	}
	gate.Answered("k")
	if gate.Ask("k", interval) {
		t.Fatal("the key was asked about again immediately")
	}
	clock = clock.Add(interval - time.Second)
	if gate.Ask("k", interval) {
		t.Fatal("the key was asked about a second before the interval was up")
	}
	clock = clock.Add(time.Second)
	if !gate.Ask("k", interval) {
		t.Fatalf("the key was not asked about after %s; nothing else renews it", interval)
	}
}

// The interval is half of what an answered ask guarantees.
//
// After an ask that returned, the key has at least the threshold left: either
// the script renewed it to a full life, or it declined because at least the
// threshold remained. Spending all of that on skipping would arrive at the next
// ask with nothing left. This is the arithmetic, stated where it can fail.
func TestTheAskIntervalSpendsHalfOfWhatAnAnswerGuarantees(t *testing.T) {
	for _, ttl := range []time.Duration{GenerationScopedFloor, 30 * 24 * time.Hour, 2 * time.Hour} {
		guaranteed := GenerationScopedRenewalThreshold(ttl)
		interval := RenewalAskInterval(ttl)
		if interval <= 0 {
			t.Fatalf("interval for a %s life is %s; a non-positive interval asks every time", ttl, interval)
		}
		if interval*2 > guaranteed {
			t.Fatalf("at a %s life the gate skips %s but an answered ask only guarantees %s",
				ttl, interval, guaranteed)
		}
		// And it does not put the script's own branches out of reach: at the
		// interval the key still has more than the threshold, so the decline
		// branch is reached, and by the third ask it is below and the renew
		// branch is reached. A gate wide enough to leave only one of them alive
		// would have deleted the thing it was tuned against.
		if ttl-interval < guaranteed {
			t.Fatalf("at a %s life the first ask after the interval already has less than the threshold "+
				"left, so the script never declines and the gate is doing the script's job", ttl)
		}
		if ttl-3*interval >= guaranteed {
			t.Fatalf("at a %s life three intervals still leave more than the threshold, so the renew "+
				"branch is not reached in the rounds this bound covers", ttl)
		}
	}
}

// A failed ask is not an answer.
//
// The gate spends a guarantee that only a returned ask provides. Recording a
// call that errored would skip on the strength of an answer nobody got, and the
// keys it skipped are the ones whose store was in trouble.
func TestAFailedAskIsNotRecorded(t *testing.T) {
	backend := &failingRenewalBackend{casMemoryBackend: casMemoryBackend{
		values: make(map[string][]byte), remaining: make(map[string]time.Duration),
	}}
	store := generationStore(t, &backend.casMemoryBackend)
	store.renewals = newRenewalGate()
	key, err := PlanGapKeyV2("alarmd", gapLoadItem("generation", nil).Identity)
	if err != nil {
		t.Fatal(err)
	}
	backend.values[key] = []byte("{}")

	for round := 0; round < 3; round++ {
		if err := RenewGenerationKey(context.Background(), StorageTarget{Backend: backend}, key, nil,
			time.Minute, time.Minute, 30*24*time.Hour, store.renewals); err == nil {
			t.Fatal("a failing backend renewed without error")
		}
	}
	if backend.attempts != 3 {
		t.Fatalf("attempts = %d, want one per load: a call that failed leaves nothing to skip on",
			backend.attempts)
	}
}

// A backend that cannot renew says so on every load, not once per interval.
//
// That error is what stops a Slot running against a store where generation keys
// leak. Behind the gate it would be reported on the first load and then hidden
// for six hours at a time, which restores the leak and the silence together.
func TestACapabilityMissIsReportedOnEveryLoad(t *testing.T) {
	gate := newRenewalGate()
	for round := 0; round < 3; round++ {
		err := RenewGenerationKey(context.Background(), StorageTarget{Backend: lifetimelessBackend{}},
			"key", nil, time.Minute, time.Minute, 30*24*time.Hour, gate)
		if !errors.Is(err, ErrLifetimeUnsupported) {
			t.Fatalf("round %d returned %v, want the capability miss on every load", round, err)
		}
	}
}

// Running out of room forgets everything rather than choosing.
//
// The only consequence is a round of asking. A gate that decided which keys to
// keep would have to be right about which Plans are still owned, which it has
// no way to know and no way to be caught getting wrong.
func TestAFullGateForgetsEverythingAndAsksAgain(t *testing.T) {
	gate := newRenewalGate()
	gate.capacity = 4
	interval := RenewalAskInterval(GenerationScopedFloor)
	for index := 0; index < 4; index++ {
		gate.Answered(string(rune('a' + index)))
	}
	if gate.Ask("a", interval) {
		t.Fatal("a key inside the interval was asked about before the gate was full")
	}
	gate.Answered("e")
	if !gate.Ask("a", interval) {
		t.Fatal("a full gate kept an entry; it must forget everything so the next load asks")
	}
	if len(gate.asked) != 1 {
		t.Fatalf("entries after overflow = %d, want only the one that caused it", len(gate.asked))
	}
}

// failingRenewalBackend renews nothing and counts the attempts.
type failingRenewalBackend struct {
	casMemoryBackend
	attempts int
}

func (backend *failingRenewalBackend) RenewIfBelow(
	_ context.Context, _ string, _, _ time.Duration,
) (bool, error) {
	backend.attempts++
	return false, errors.New("state: renewal failed")
}

// lifetimelessBackend is a routed backend with no lifetime support at all.
type lifetimelessBackend struct{}

func (lifetimelessBackend) MGet(_ context.Context, keys []string) ([][]byte, error) {
	return make([][]byte, len(keys)), nil
}

func (lifetimelessBackend) SetMany(_ context.Context, _ []BackendWrite) error {
	return errors.New("state: not supported")
}
