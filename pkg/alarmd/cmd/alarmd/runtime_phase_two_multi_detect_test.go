// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// The word a worker declares is the word the algorithm's registry capability
// is gated by, and this binary declares it.
func TestTheMultiDetectCapabilityIsOneWordAndThisWorkerDeclaresIt(t *testing.T) {
	if ownership.CapabilityMultiDetect != strategy.MultiDetectCapability {
		t.Fatalf("workers declare %q, the algorithm is gated by %q", ownership.CapabilityMultiDetect, strategy.MultiDetectCapability)
	}
	registration, err := phaseTwoWorkerRegistration(validGoAccessRuntimeConfig(), ownership.WorkerReady, time.Unix(1_700_000_000, 0), nil, nil, viewStreamIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	if !registration.Declares(ownership.CapabilityMultiDetect) {
		t.Fatalf("this worker declares %v, want %s among them", registration.Capabilities, ownership.CapabilityMultiDetect)
	}
}

// The gate admits only a ready set that all declares the capability, names
// the workers that do not, and before any ownership round reads the registry
// once rather than take no ready set; a registry it cannot read admits
// nothing and is read again on the next round.
func TestTheMultiDetectGateAdmitsOnlyAReadySetThatAllDeclaresIt(t *testing.T) {
	declares := ownership.WorkerRegistration{WorkerID: "worker-new", Capabilities: []string{ownership.CapabilityMultiDetect}}
	old := ownership.WorkerRegistration{WorkerID: "worker-old"}
	reads := 0
	failing := true
	gate := &multiDetectGate{now: time.Now, list: func(context.Context, time.Time) ([]ownership.WorkerRegistration, ownership.ControlReadStats, error) {
		reads++
		if failing {
			return nil, ownership.ControlReadStats{}, errors.New("registry unreachable")
		}
		return []ownership.WorkerRegistration{declares}, ownership.ControlReadStats{}, nil
	}}
	if got := gate.admission(context.Background()); got.Admitted || got.Known || reads != 1 {
		t.Fatalf("an unreadable registry: %+v after %d reads, want nothing admitted or known", got, reads)
	}
	failing = false
	if got := gate.admission(context.Background()); !got.Admitted || !got.Known || reads != 2 {
		t.Fatalf("the registry read before any ownership round: %+v after %d reads, want it admitted", got, reads)
	}
	gate.record([]ownership.WorkerRegistration{declares, old})
	if got := gate.admission(context.Background()); got.Admitted || !got.Known || !slices.Equal(got.Unaware, []string{"worker-old"}) || reads != 2 {
		t.Fatalf("a round with an old worker ready: %+v after %d reads, want it held naming worker-old", got, reads)
	}
	gate.record(nil)
	if got := gate.admission(context.Background()); got.Admitted || !got.Known {
		t.Fatalf("a round with no worker ready: %+v, want nothing admitted", got)
	}
}

// A record older than a Leader's ownership rounds come is from before this
// process last lost the Leader's place: it is read again from the registry,
// not taken. Released, then the Leader's place elsewhere while a worker of
// the release before joined: back, the first source round holds the item.
func TestAGateRecordFromBeforeTheLastTermIsReadAgain(t *testing.T) {
	declares := ownership.WorkerRegistration{WorkerID: "worker-new", Capabilities: []string{ownership.CapabilityMultiDetect}}
	old := ownership.WorkerRegistration{WorkerID: "worker-old"}
	clock := time.Unix(1_700_000_000, 0)
	ready := []ownership.WorkerRegistration{declares}
	gate := &multiDetectGate{now: func() time.Time { return clock }, fresh: time.Minute,
		list: func(context.Context, time.Time) ([]ownership.WorkerRegistration, ownership.ControlReadStats, error) {
			return ready, ownership.ControlReadStats{}, nil
		}}
	gate.record(ready)
	if got := gate.admission(context.Background()); !got.Admitted {
		t.Fatalf("this term's ownership round admitted: %+v, want it taken", got)
	}
	ready = []ownership.WorkerRegistration{declares, old}
	clock = clock.Add(30 * time.Second)
	if got := gate.admission(context.Background()); !got.Admitted {
		t.Fatalf("a record within its freshness: %+v, want it taken as recorded", got)
	}
	clock = clock.Add(10 * time.Minute)
	if got := gate.admission(context.Background()); got.Admitted || !slices.Equal(got.Unaware, []string{"worker-old"}) {
		t.Fatalf("back after another Leader's rounds: %+v, want the registry read again and the item held for worker-old", got)
	}
}

// The item held for a worker that cannot run it keeps its open alerts: the
// absent close closes the alerts of a strategy refused only for a
// capability no configuration brings back, and this one comes back with
// the fleet.
func TestAHeldItemsAlertsAreNotClosedAsUnsupported(t *testing.T) {
	if closing := capabilityRefusals(map[string]string{"1001": "MULTI_DETECT_NOT_ENABLED"}); len(closing) != 0 {
		t.Fatalf("closing %v, want the held item's alerts kept", closing)
	}
}
