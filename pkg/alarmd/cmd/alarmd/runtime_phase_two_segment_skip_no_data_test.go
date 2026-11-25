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
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// noDataSkipWorker is a ready worker declaring the segment skip contract,
// and the no-data family's when noData is set.
func noDataSkipWorker(id string, noData bool, now time.Time) ownership.WorkerRegistration {
	registration := segmentSkipWorker(id, true, now)
	if noData {
		registration.Capabilities = append(registration.Capabilities, ownership.CapabilitySegmentSkipNoData)
	}
	return registration
}

func noDataSkipWords(store *rebalanceOwnershipStore, groups []execution.QueryGroupIdentity) []uint64 {
	words := make([]uint64, 0, len(groups))
	for _, queryGroup := range groups {
		words = append(words, store.assignments[queryGroup].SegmentSkipNoData)
	}
	return words
}

// The Leader sets the no-data family's word (014 next9 ⑥) only beside the
// segment skip word, and only while every ready worker declares the no-data
// family's contract as well: a replica that declares the skip alone holds
// it, the skip word set all the same; that replica gone, the word is the
// epoch of the view the Leader publishes; one joining again takes it back,
// the skip word left; and turning the skip off takes both back - the stop
// the values have for both is the one key.
func TestTheLeaderSetsTheNoDataWordOnlyBesideTheSkipWordForAFleetThatDeclaresBoth(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	groups := []execution.QueryGroupIdentity{"query-group-1", "query-group-2"}
	store := &rebalanceOwnershipStore{
		fakePhaseTwoOwnershipStore: &fakePhaseTwoOwnershipStore{now: now, renewed: make(chan struct{}), leaderEpoch: 9},
		workers:                    []ownership.WorkerRegistration{noDataSkipWorker("worker-1", true, now), noDataSkipWorker("worker-2", false, now)},
		assignments:                map[execution.QueryGroupIdentity]ownership.AssignmentRecord{},
	}
	for _, queryGroup := range groups {
		store.assignments[queryGroup] = ownership.AssignmentRecord{
			QueryGroup: queryGroup, DesiredWorkerID: "worker-1", AssignmentGeneration: 1, RecordRevision: 1,
			ControlEpoch: 1, PlacementReason: ownership.PlacementRendezvous, AssignedAt: now,
		}
	}
	var observations []observability.Observation
	production, server := segmentSkipLeader(t, store, true, now, &observations)

	// worker-2 declares the skip, not the no-data family's.
	if err := production.PublishAssignments(context.Background(), groups, now); err != nil {
		t.Fatal(err)
	}
	viewEpoch := server.Stats().ControlEpoch
	if viewEpoch == 0 {
		t.Fatal("the Leader publishes no view")
	}
	if words := segmentSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{viewEpoch, viewEpoch}) {
		t.Fatalf("round 1 skip words %v, want the view's epoch %d on both", words, viewEpoch)
	}
	if words := noDataSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{0, 0}) {
		t.Fatalf("round 1 no-data words %v, want none while worker-2 does not declare it", words)
	}
	if facts := lastSegmentSkip(t, observations); !facts.Changed {
		t.Fatalf("round 1 = %+v, want the gate news", facts)
	}

	// The roll completes: both words set.
	store.workers[1] = noDataSkipWorker("worker-2", true, now)
	at := now.Add(40 * time.Second)
	if err := production.PublishAssignments(context.Background(), groups, at); err != nil {
		t.Fatal(err)
	}
	if words := noDataSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{viewEpoch, viewEpoch}) {
		t.Fatalf("round 2 no-data words %v, want the view's epoch %d on both", words, viewEpoch)
	}
	if facts := lastSegmentSkip(t, observations); facts.Set != 2 || !facts.Changed {
		t.Fatalf("round 2 = %+v, want both records restated and the gate news", facts)
	}
	before := store.published
	if err := production.PublishAssignments(context.Background(), groups, at.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if store.published != before {
		t.Fatalf("a round with nothing to restate published %d decisions", store.published-before)
	}

	// A replica declaring the skip alone joins: the no-data word is taken
	// back, the skip word kept.
	store.workers = append(store.workers, noDataSkipWorker("worker-3", false, now))
	if err := production.PublishAssignments(context.Background(), groups, at.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if words := noDataSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{0, 0}) {
		t.Fatalf("after worker-3 joined the no-data words are %v, want none", words)
	}
	if words := segmentSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{viewEpoch, viewEpoch}) {
		t.Fatalf("after worker-3 joined the skip words are %v, want them kept", words)
	}

	// The roll back to a fleet that declares both, then the values turn the
	// skip off: both words go.
	store.workers = store.workers[:2]
	if err := production.PublishAssignments(context.Background(), groups, at.Add(15*time.Second)); err != nil {
		t.Fatal(err)
	}
	if words := noDataSkipWords(store, groups); !reflect.DeepEqual(words, []uint64{viewEpoch, viewEpoch}) {
		t.Fatalf("back to a fleet declaring both, the no-data words are %v", words)
	}
	production.dependencies.SegmentSkip = false
	if err := production.PublishAssignments(context.Background(), groups, at.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if skip, noData := segmentSkipWords(store, groups), noDataSkipWords(store, groups); !reflect.DeepEqual(skip, []uint64{0, 0}) ||
		!reflect.DeepEqual(noData, []uint64{0, 0}) {
		t.Fatalf("with the skip off the words are %v and %v, want none", skip, noData)
	}
}

// This build declares the no-data family's contract beside the segment
// skip's.
func TestThisBuildDeclaresTheNoDataFamilysSkipContract(t *testing.T) {
	cfg := validGoAccessRuntimeConfig()
	registration, err := phaseTwoWorkerRegistration(cfg, ownership.WorkerReady, time.Unix(1_700_000_000, 0), nil, nil, viewStreamIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	if !registration.Declares(ownership.CapabilitySegmentSkipNoData) || !registration.Declares(ownership.CapabilitySegmentSkip) {
		t.Fatalf("registration declares %v, want both skip contracts", registration.Capabilities)
	}
}
