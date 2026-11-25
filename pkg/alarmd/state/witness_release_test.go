// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The keys a landed window read let go of their witnesses through the store
// (ReleaseRuntimeWitnesses, 014 next9 ④ §7 item 4): the ones a write took
// are not counted again, the others are counted as landed with the bytes the
// cache held them at; another Slot's are left alone; and what an attempt
// leaves at its end is counted apart, as ended.
func TestAWindowsKeysLetGoOfTheirWitnessesCountedAsLanded(t *testing.T) {
	ctx := context.Background()
	store := newBatchStore(t, newPipelineMemoryBackend(), nil)
	mutations := seriesMutations(t, 4, applyVersion(), 0)
	slot := frozenRef().Slot
	if _, err := store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: frozenRef(), Items: preflightItems(mutations)}); err != nil {
		t.Fatal(err)
	}
	later := slot
	later.EvaluationTime += 60
	other := slot
	other.QueryGroup += "-other"
	otherRef := frozenRef()
	otherRef.Slot = other
	if _, err := store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: otherRef, Items: preflightItems(mutations[:1])}); err != nil {
		t.Fatal(err)
	}
	// The window's write took the first key's.
	if _, taken := store.witnesses.take(slot, mutations[0].Identity); !taken {
		t.Fatal("fixture: the written key found no witness")
	}
	var unwritten uint64
	keys := make([]execution.StateKeyIdentity, 0, len(mutations))
	store.witnesses.mu.Lock()
	for index, mutation := range mutations {
		keys = append(keys, mutation.Identity)
		if index > 0 {
			unwritten += uint64(runtimeWitnessBytes(mutation.Identity, store.witnesses.groups[slot.QueryGroup].items[mutation.Identity]))
		}
	}
	store.witnesses.mu.Unlock()
	store.ReleaseRuntimeWitnesses(later, keys)
	if stats := store.RuntimeWitnessStats(); stats.Items != len(mutations) || stats.Landed.Items != 0 {
		t.Fatalf("a later Slot's release: %+v, want the three unwritten and the other Query Group's one kept", stats)
	}
	store.ReleaseRuntimeWitnesses(slot, keys)
	stats := store.RuntimeWitnessStats()
	if stats.Items != 1 || stats.Landed != (RuntimeWitnessRelease{Items: 3, Bytes: unwritten}) || stats.Ended != (RuntimeWitnessRelease{}) {
		t.Fatalf("after the window landed: %+v, want the other Query Group's one witness left and the three unwritten counted landed at %d B",
			stats, unwritten)
	}
	store.ReleaseRuntimeWitnesses(slot, keys)
	if again := store.RuntimeWitnessStats(); again.Landed != stats.Landed {
		t.Fatalf("a second release counted %+v, want the first's %+v", again.Landed, stats.Landed)
	}
	if _, err := store.LoadRuntime(ctx, execution.StatePreflightRequest{Contract: frozenRef(), Items: preflightItems(mutations[2:])}); err != nil {
		t.Fatal(err)
	}
	store.ReleaseSlotWitnesses(slot)
	if ended := store.RuntimeWitnessStats(); ended.Items != 1 || ended.Ended.Items != 2 || ended.Ended.Bytes == 0 || ended.Landed != stats.Landed {
		t.Fatalf("after the attempt ended: %+v, want the two read after the landing counted ended, apart", ended)
	}
}
