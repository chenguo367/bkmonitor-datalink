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
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// A release past what the process's pool holds stops at zero and is said,
// once a release, by the count's kind, with what it gave back and what the
// count held (RESOURCE_POOL_OVER_RELEASE): wrapped, the count would refuse
// every Slot of the process until it restarts. A release within
// what is held says nothing. Every count of the pool is released so: the
// side effects', the series and retained bytes', and the loaded gap facts'.
func TestAReleasePastWhatThePoolHoldsStopsAtZeroAndIsSaid(t *testing.T) {
	var said []observability.Observation
	co := &SlotExecutionCoordinator{ports: Ports{Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
		said = append(said, observation)
	})}}
	pool := &co.reservations
	pool.states, pool.events, pool.gaps, pool.series, pool.retainedBytes, pool.gapFacts = 2, 1, 0, 5, 100, 3

	co.releaseEffects(effectCounts{states: 1, events: 1})
	co.releaseProvisional(5, 40)
	if len(said) != 0 {
		t.Fatalf("releases within what the pool holds said %+v, want nothing", said)
	}
	if pool.states != 1 || pool.events != 0 || pool.series != 0 || pool.retainedBytes != 60 {
		t.Fatalf("the pool holds %d States, %d events, %d series and %d bytes, want 1, 0, 0 and 60",
			pool.states, pool.events, pool.series, pool.retainedBytes)
	}

	co.releaseEffects(effectCounts{states: 3, events: 1, gaps: 2})
	co.releaseProvisional(1, 61)
	// A Slot's stream let go with more gap facts than the pool holds.
	stream := &streamedExecution{slotStream: &slotStream{coordinator: co, began: true, gapFacts: 4}}
	stream.releaseProvisional()
	held := [...]uint64{pool.states, pool.events, pool.gaps, pool.series, pool.retainedBytes, pool.gapFacts}
	if held != [6]uint64{} {
		t.Fatalf("the pool holds %v once released past what it held, want every count stopped at zero", held)
	}
	want := []observability.PoolOverReleaseFacts{
		{Kind: observability.PoolOverReleaseStates, Asked: 3, Held: 1},
		{Kind: observability.PoolOverReleaseEvents, Asked: 1, Held: 0},
		{Kind: observability.PoolOverReleaseGaps, Asked: 2, Held: 0},
		{Kind: observability.PoolOverReleaseSeries, Asked: 1, Held: 0},
		{Kind: observability.PoolOverReleaseRetainedBytes, Asked: 61, Held: 60},
		{Kind: observability.PoolOverReleaseGapFacts, Asked: 4, Held: 3},
	}
	var got []observability.PoolOverReleaseFacts
	for _, observation := range said {
		if observation.Component != observability.ComponentResource || observation.Stage != observability.StagePoolOverRelease ||
			observation.ReasonCode != observability.ReasonCode(contract.ReasonResourcePoolOverRelease) || observation.PoolOverRelease == nil {
			t.Fatalf("a release past zero said %+v, want a %s line with its facts", observation, contract.ReasonResourcePoolOverRelease)
		}
		got = append(got, *observation.PoolOverRelease)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the releases past zero said %+v, want %+v", got, want)
	}
}
