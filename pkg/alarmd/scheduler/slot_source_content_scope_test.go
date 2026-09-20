// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// decision-016 batch 2: a Slot declares the content it runs under -- the
// ObjectDigest of the Segment it was frozen from -- so every fenced write it
// makes can be refused by name once the Assignment record has moved the
// Query Group to other content. The declaration has to come from the Segment
// on every path a Slot is built on, and a retry has to declare what the
// first attempt froze, not what the live Segment says now.

func TestAFrozenSlotDeclaresItsSegmentsContent(t *testing.T) {
	schedule := schedulerSchedule(t, 60, 60, nil, "snapshot-1", 1)
	schedule.Segment.ObjectDigest = "qg-object-a"
	catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	source := newProductionSlotSourceForTest(t, catalog, missingProgress(), time.Unix(200, 0))

	slot, due, _, err := source.Next(context.Background(), "query-group-1")
	if err != nil || !due {
		t.Fatalf("Next() due=%v error=%v", due, err)
	}
	if slot.Dispatch.ContentScope != "qg-object-a" {
		t.Fatalf("frozen Slot content scope = %q, want the Segment's object digest", slot.Dispatch.ContentScope)
	}
}

func TestASegmentWithoutContentAddressingDeclaresNothing(t *testing.T) {
	schedule := schedulerSchedule(t, 60, 60, nil, "snapshot-1", 1)
	catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	source := newProductionSlotSourceForTest(t, catalog, missingProgress(), time.Unix(200, 0))

	slot, due, _, err := source.Next(context.Background(), "query-group-1")
	if err != nil || !due {
		t.Fatalf("Next() due=%v error=%v", due, err)
	}
	if slot.Dispatch.ContentScope != "" {
		t.Fatalf("content scope = %q for a Segment that names no content, want empty: the fence then compares what it always did", slot.Dispatch.ContentScope)
	}
}

// A retry rebuilt from the persisted projection declares the content the
// first attempt was begun under, even when the live Segment has since moved
// to other content: the retry retries that attempt.
func TestARetryFromTheProjectionDeclaresTheContentItWasBegunUnder(t *testing.T) {
	schedule := schedulerSchedule(t, 60, 60, nil, "snapshot-1", 1)
	schedule.Segment.ObjectDigest = "qg-object-a"
	catalog := &fakeSlotCatalog{t: t, schedules: []execution.FrozenQueryGroupSchedule{schedule}}
	first := newProductionSlotSourceForTest(t, catalog, missingProgress(), time.Unix(200, 0))
	slot, due, _, err := first.Next(context.Background(), "query-group-1")
	if err != nil || !due || slot.Dispatch.ContentScope != "qg-object-a" {
		t.Fatalf("Next(first) = (%+v, %t, %v)", slot, due, err)
	}
	request := execution.SlotExecutionRequest{
		Contract: slot.Contract, DuePlanTargets: slot.DuePlanTargets.Clone(),
		EarliestQueryDeadlineUnixMilli: slot.EarliestQueryDeadlineUnixMilli, KeepUntilUnixMilli: slot.KeepUntilUnixMilli,
		ContentScope: slot.Dispatch.ContentScope,
	}
	projection := request.UnfinishedProjection()
	if projection.ContentScope != "qg-object-a" {
		t.Fatalf("projection content scope = %q, want the request's", projection.ContentScope)
	}
	load := foundProgress(slot.ExpectedNextSlot, 0)
	load.Progress.UnfinishedSlot = &projection
	catalog.freezeErr = controlplane.ErrSnapshotUnavailable
	// The live Segment has moved on; the retry must not pick that up.
	catalog.schedules[0].Segment.ObjectDigest = "qg-object-b"
	restarted := newProductionSlotSourceForTest(t, catalog, load, time.Unix(200, 0))
	restored, due, _, err := restarted.Next(context.Background(), "query-group-1")
	if err != nil || !due || restored.Contract != slot.Contract {
		t.Fatalf("Next(restarted) = (%+v, %t, %v)", restored, due, err)
	}
	if restored.Dispatch.ContentScope != "qg-object-a" {
		t.Fatalf("retry content scope = %q, want the content the attempt was begun under, qg-object-a", restored.Dispatch.ContentScope)
	}
}

// The projection's content is metadata, like a Segment's digest: a
// projection begun by a binary that did not write it is still the same
// unfinished Slot, so a rollout does not refuse every retry in flight.
func TestTheContentScopeTakesNoPartInProjectionIdentity(t *testing.T) {
	base := execution.UnfinishedSlotProjection{
		Contract: execution.FrozenExecutionContractRef{
			Slot:             execution.SlotIdentity{QueryGroup: "query-group-1", EvaluationTime: 60},
			SnapshotRevision: "snapshot-1", QueryRevision: "query-1", ScheduleRevision: "schedule-1", ScheduleSegmentStart: 60,
			DuePlanSetDigest: "due-1",
		},
		DuePlanTargets:                 execution.FrozenDuePlanTargets{DuePlanSetDigest: "due-1", Plans: []execution.PlanIdentity{planIdentity("1")}},
		EarliestQueryDeadlineUnixMilli: 61_000, KeepUntilUnixMilli: 700_000,
	}
	declared := base
	declared.ContentScope = "qg-object-a"
	if !base.Equal(declared) || !declared.Equal(base) {
		t.Fatal("a projection with a content scope and the same projection without one must be the same unfinished Slot")
	}
}

// The Runner hands the executor the Session as the attempt's lease
// authority, so the output sink can ask how long the lease has left before
// it starts a batch. Without this the sink finds no authority and admits
// every batch against no lease at all.
func TestTheRunnerHandsTheExecutorItsLeaseAuthority(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	deadline := now.Add(25 * time.Second)
	fence := execution.OwnerFence{QueryGroup: "query-group-1", OwnerID: "worker-1", OwnerEpoch: 1, LeaseToken: "token-1"}
	session := &fakeSession{fence: fence, deadline: deadline}
	source := &fakeSlotSource{slot: frozenSlot("query-group-1")}
	executor := &authorityRecordingExecutor{}
	runner, err := NewRunner("query-group-1", session, source, executor, NewFlightCoordinator(), func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}
	if _, _, err := runner.RunOne(context.Background()); err != nil {
		t.Fatalf("RunOne() error = %v", err)
	}
	if executor.seen == nil || !executor.seen.Deadline().Equal(deadline) {
		t.Fatalf("executor saw lease authority %v, want the Session's deadline %v", executor.seen, deadline)
	}
}

type authorityRecordingExecutor struct{ seen execution.LeaseAuthority }

func (executor *authorityRecordingExecutor) Execute(ctx context.Context, _ execution.SlotExecutionRequest) (execution.SlotExecutionResult, error) {
	executor.seen, _ = execution.LeaseAuthorityFromContext(ctx)
	return execution.SlotExecutionResult{Completed: true, Result: observability.ResultSuccess, ReasonCode: observability.ReasonNone}, nil
}
