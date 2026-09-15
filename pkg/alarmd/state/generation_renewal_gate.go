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
	"sync"
	"time"
)

// RenewalAskInterval is how long a process may go without asking Redis about a
// key it has already asked about.
//
// The whole interval rests on one guarantee: an ask that returns without an
// error leaves the key with at least the renewal threshold remaining. If the
// script renewed, the key has a full ttl; if it declined, it declined because
// at least the threshold was left. So after an ask, remaining >= threshold in
// both outcomes, and that is what may be spent on skipping.
//
// Half of it is spent, not all of it. Skipping for the whole threshold would
// consume the entire guarantee and arrive at the next ask with nothing left --
// a key whose remaining life was exactly the threshold would expire on the
// boundary. Half leaves as much margin as it spends.
//
// It does not make the renewal script's branches unreachable, which would be
// the sign the gate had been set too wide: at a 24h life the ask is every 6h
// and the threshold is 12h, so an ask declines twice and renews on the third.
func RenewalAskInterval(ttl time.Duration) time.Duration {
	return GenerationScopedRenewalThreshold(ttl) / 2
}

// renewalGate is what a process remembers about the keys it has renewed, so
// that a load inside the interval spends no round trip.
//
// Renewal hangs on the load, and a Plan loads its generation-scoped key every
// Slot: at a one-minute period that is one EVAL per Plan per minute, forever,
// to decide something that changes twice a day. Measured on a deployment with
// around 2,100 Plans it was 38.6 EVAL/s at 7.1ms each, which is 0.27 seconds
// of waiting on Redis every second. The script was written to make the write
// cheap and the ask was assumed to be small beside the Slot's own reads; it is
// the asking that costs.
//
// Everything about it fails toward asking. An ask that returned an error is
// not recorded, a key the gate has forgotten is asked about, and a gate that
// runs out of room forgets everything rather than choosing what to keep. The
// cost of asking is a round trip; the cost of wrongly skipping is a key that
// expires while a Plan is still being evaluated.
type renewalGate struct {
	mu       sync.Mutex
	asked    map[string]time.Time
	capacity int
	now      func() time.Time
}

// renewalGateCapacity bounds what one process remembers.
//
// A worker holds one entry per generation-scoped key of every Plan it owns,
// and a Plan whose execution content changes leaves its old generation's entry
// behind -- nothing loads that key again, so nothing renews or removes it. The
// number is well past the Plans one worker owns on the deployments this runs
// on, and reaching it costs a round of asking rather than a wrong answer.
const renewalGateCapacity = 50000

func newRenewalGate() *renewalGate {
	return &renewalGate{asked: make(map[string]time.Time), capacity: renewalGateCapacity, now: time.Now}
}

// Ask reports whether key should be asked about now. A nil gate always asks,
// so a store built without one behaves exactly as it did before the gate
// existed.
func (gate *renewalGate) Ask(key string, interval time.Duration) bool {
	if gate == nil || interval <= 0 {
		return true
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	last, known := gate.asked[key]
	if !known {
		return true
	}
	return !gate.now().Before(last.Add(interval))
}

// Answered records that an ask came back without an error, which is what makes
// the guarantee this gate spends. A failed ask is not recorded by its caller,
// so the next load asks again.
func (gate *renewalGate) Answered(key string) {
	if gate == nil {
		return
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if len(gate.asked) >= gate.capacity {
		// Forgetting everything, rather than evicting the oldest. The only
		// consequence is a round of asking, and a gate that picks which keys
		// to keep would have to be right about which Plans are still owned --
		// a judgement it has no way to make and no way to be caught making
		// wrongly.
		gate.asked = make(map[string]time.Time, gate.capacity)
	}
	gate.asked[key] = gate.now()
}
