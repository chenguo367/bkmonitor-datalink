// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package state

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func TestSameSlotGapExtensionPreservesScopesAndChecksCAS(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "extension", true: "concurrent writer"}[concurrent], func(t *testing.T) {
			backend := &casMemoryBackend{values: make(map[string][]byte)}
			router, _ := NewFixedRouter("test", backend)
			store, err := NewExecutionStore(ExecutionStoreOptions{Prefix: "alarmd", Router: router, MaxValueBytes: 4096, MaxItemsPerCall: 4, MinTTL: time.Minute, MaxTTL: time.Hour, RestartMargin: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			identity := execution.PlanGapIdentity{Plan: stateIdentityV2().Plan, StateGeneration: "generation"}
			build := func(scopes []execution.GapScopeMutation, rev uint64) execution.PlanGapMutation {
				m, e := execution.BuildPlanGapMutation(execution.PlanGapMutation{Identity: identity, ExpectedMarkerRevision: rev, ApplyVersion: applyVersion(), ScheduleRevision: "plan-r1", Scopes: scopes})
				if e != nil {
					t.Fatal(e)
				}
				return m
			}
			level := execution.GapScopeMutation{Scope: execution.GapScope{HasLevel: true, LevelID: 1}, Kind: execution.GapOpen, ReasonCode: execution.ReasonCode(contract.ReasonHistoryGapped), RequiredFullSlots: 3}
			otherLevel := level
			otherLevel.Scope.LevelID = 2
			otherLevel.RequiredFullSlots = 4
			old := build([]execution.GapScopeMutation{level, otherLevel}, 0)
			apply := func(m execution.PlanGapMutation) execution.GapGuardApplyStatus {
				r, e := store.ApplyGap(context.Background(), execution.GapGuardApplyRequest{Contract: frozenRef(), Items: []execution.PlanGapMutation{m}})
				if e != nil {
					t.Fatal(e)
				}
				return r.Items[0].Status
			}
			if s := apply(old); s != execution.GapGuardApplied {
				t.Fatalf("seed=%s", s)
			}
			load := func() execution.GapGuardSnapshot {
				r, e := store.LoadGaps(context.Background(), execution.GapLoadRequest{Contract: frozenRef(), Items: []execution.PlanGapLoadItem{{Identity: identity, ApplyVersion: applyVersion(), ScheduleRevision: "plan-r1"}}})
				if e != nil {
					t.Fatal(e)
				}
				return r.Items[0]
			}
			before := load()
			extend := build([]execution.GapScopeMutation{{Scope: execution.GapScope{}, Kind: execution.GapOpen, ReasonCode: execution.ReasonCode(contract.ReasonSnapshotUnavailable), RequiredFullSlots: 9}}, before.MarkerRevision)
			staleRevision := extend
			staleRevision.ExpectedMarkerRevision = 0
			if s := apply(staleRevision); s != execution.GapGuardConflict || !reflect.DeepEqual(before, load()) {
				t.Fatal("extension bypassed marker revision")
			}
			weaken := level
			weaken.RequiredFullSlots--
			if s := apply(build([]execution.GapScopeMutation{weaken}, before.MarkerRevision)); s != execution.GapGuardConflict || !reflect.DeepEqual(before, load()) {
				t.Fatal("same-Slot weakening was accepted")
			}
			wrongSchedule := extend
			wrongSchedule.ScheduleRevision = "plan-r2"
			wrongSchedule.MutationDigest = ""
			wrongSchedule, err = execution.BuildPlanGapMutation(wrongSchedule)
			if err != nil {
				t.Fatal(err)
			}
			if s := apply(wrongSchedule); s != execution.GapGuardConflict || !reflect.DeepEqual(before, load()) {
				t.Fatal("extension crossed schedule revision")
			}
			backend.conflict = concurrent
			got := apply(extend)
			if concurrent {
				if got != execution.GapGuardConflict || !reflect.DeepEqual(before, load()) {
					t.Fatal("CAS conflict did not preserve marker")
				}
				return
			}
			if got != execution.GapGuardApplied {
				t.Fatalf("same Slot extension=%s, want APPLIED", got)
			}
			after := load()
			if after.MarkerRevision != before.MarkerRevision+1 || len(after.Scopes) != 3 {
				t.Fatalf("extension=%+v", after)
			}
			for _, original := range before.Scopes {
				var found bool
				for _, s := range after.Scopes {
					if s.Scope == original.Scope {
						found = true
						if s != original {
							t.Fatalf("level scope changed: %+v", s)
						}
					}
				}
				if !found {
					t.Fatal("existing scope lost")
				}
			}
			if s := apply(extend); s != execution.GapGuardAlreadyApplied {
				t.Fatalf("redo=%s", s)
			}
		})
	}
}
