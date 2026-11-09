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
	"unsafe"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// noDataReservationPhase is the phase a pool refusal of a no-data memory
// reports.
const noDataReservationPhase = "normal_no_data"

// What a whole memory's write builds for each group, beside the group's key
// and its stored value: the store's field - a name, the key behind a
// two-byte prefix, and a value - and the pair twice more as the script's
// arguments, in the store's list and in the client's command, each one boxed.
// A group with no absence is stored as one byte; an absence as its three
// timestamps in JSON, ten digits each.
const (
	noDataFieldBytes        = 16 + 24
	noDataFieldPrefixBytes  = 2
	noDataScriptArgBytes    = 2*2*16 + 16 + 24
	noDataPresentValueBytes = 1
	noDataAbsentValueBytes  = 80
	// noDataGroupBuildBytes is what a first write holds a group beyond the
	// parts above, measured rather than named: the statement's build and the
	// client's command. TestMeasureTheFirstNoDataWriteAgainstTheHeap read a
	// live peak of 495-498 bytes a group at 64-byte keys against 302 counted
	// by the parts, at 100,000 and 500,000 groups (11-10).
	noDataGroupBuildBytes = 196
	// A map of string to int64: a header and groups of eight 24-byte slots
	// and their control bytes, filled to seven eighths.
	noDataCloseMapHeaderBytes = 48
	noDataCloseMapGroupBytes  = 8 + 8*(16+8)
	noDataCloseMapGroupFill   = 7
)

var (
	noDataGroupDeltaBytes   = uint64(unsafe.Sizeof(execution.NoDataGroupDelta{}))
	noDataGroupAbsenceBytes = uint64(unsafe.Sizeof(execution.NoDataGroupAbsence{}))
	noDataSnapshotBytes     = uint64(unsafe.Sizeof(execution.NoDataMemorySnapshot{}))
	noDataGroupMemoryBytes  = uint64(unsafe.Sizeof(execution.NoDataGroupMemory{}))
)

// noDataFirstWriteBytes is what a write of a whole memory holds in the worker
// while it is written: the statement's groups, and per group the field the
// store builds and the arguments that carry it. A statement that changes
// some groups is the size of those, held by the round that built them; one
// that replaces the record carries every group the Plan remembers, which is
// what this counts it for.
func noDataFirstWriteBytes(mutation execution.PlanNoDataMutation) uint64 {
	total := uint64(cap(mutation.Set)) * noDataGroupDeltaBytes
	for _, group := range mutation.Set {
		key := uint64(len(group.GroupKey))
		total += 2*key + noDataFieldBytes + noDataFieldPrefixBytes + noDataScriptArgBytes + noDataGroupBuildBytes
		if group.Absent != nil {
			total += noDataGroupAbsenceBytes + noDataAbsentValueBytes
		} else {
			total += noDataPresentValueBytes
		}
	}
	return total
}

// noDataMemoryBytes is what one Plan's loaded memory holds for the Slot: the
// snapshot, its groups and their keys, and its manual closes.
func noDataMemoryBytes(snapshot execution.NoDataMemorySnapshot) uint64 {
	total := noDataSnapshotBytes + uint64(cap(snapshot.Groups))*noDataGroupMemoryBytes
	for _, group := range snapshot.Groups {
		total += uint64(len(group.GroupKey))
	}
	if snapshot.ManualCloses != nil {
		groups := max((uint64(len(snapshot.ManualCloses))+noDataCloseMapGroupFill-1)/noDataCloseMapGroupFill, 1)
		total += noDataCloseMapHeaderBytes + groups*noDataCloseMapGroupBytes
		for key := range snapshot.ManualCloses {
			total += uint64(len(key))
		}
	}
	return total
}

// noDataBytesRefusal is a Plan's memory refused for what it holds against the
// share, as the store's refusals are reported.
func noDataBytesRefusal(identity execution.PlanNoDataIdentity, groups int, bytes, share uint64) execution.NoDataApplyItemResult {
	return execution.NoDataApplyItemResult{Identity: identity, Status: execution.NoDataRejected,
		ReasonCode: execution.ReasonCode(contract.ReasonStateBudgetExceeded),
		Size: &execution.NoDataRecordSize{Record: execution.NoDataRecordBytes, Groups: groups, Limit: int(min(share, uint64(^uint(0)>>1))),
			LimitSource: execution.NoDataLimitShare, Bytes: bytes}}
}

// holdNoDataFirstWrites reserves what the writes of whole memories hold
// against the retained pool, for the length of the write, and says which
// mutations go on to the store. A whole memory past the share one Query
// Group's Slot may hold never fits, whatever else is running: that Plan's
// memory alone is refused, named and counted, and nothing else about the
// Slot changes - its threshold results were already sent. What is within the
// share but finds no room in the pool is the Slot's RESOURCE_HARD_STOP, and
// the Slot is retried. The caller gives back what was held. A coordinator
// with no retained budget - only a test builds one - holds nothing.
func (coordinator *SlotExecutionCoordinator) holdNoDataFirstWrites(
	ctx context.Context, request execution.SlotExecutionRequest, mutations []execution.PlanNoDataMutation,
) ([]execution.PlanNoDataMutation, uint64, error) {
	if coordinator.budget.MaxRetainedBytes == 0 {
		return mutations, 0, nil
	}
	share := coordinator.qgShareBytes()
	kept := make([]execution.PlanNoDataMutation, 0, len(mutations))
	var held uint64
	for _, mutation := range mutations {
		if !mutation.ReplacesWholeRecord() {
			kept = append(kept, mutation)
			continue
		}
		bytes := noDataFirstWriteBytes(mutation)
		if share > 0 && bytes > share {
			coordinator.observeNoDataMemoryRefusal(ctx, request, noDataBytesRefusal(mutation.Identity, len(mutation.Set), bytes, share))
			continue
		}
		held += bytes
		kept = append(kept, mutation)
	}
	if held > 0 && !coordinator.tryAcquireRetained(held) {
		err := budgetRejection(observability.CapacityBudgetRetainedBytes, noDataReservationPhase, RetainedReserved(coordinator), held,
			coordinator.budget.MaxRetainedBytes, nil, nil)
		coordinator.observeCapacityRejection(ctx, request.Operation, observability.CapacityBudgetRetainedBytes, err)
		return nil, 0, err
	}
	return kept, held, nil
}

// reserveNoDataMemory reserves what the Slot's loaded no-data memories hold,
// for as long as the Slot holds them. A memory that would take this Slot past
// its share is refused for this Slot - its Plan's no-data detection skips the
// round, SKIPPED_SLOT_BUDGET, and its memory is neither judged nor written -
// while its threshold detection goes on; what is within the share and finds
// no room in the pool refuses the Slot, RESOURCE_HARD_STOP. A coordinator
// with no retained budget - only a test builds one - holds nothing.
func (stream *streamedExecution) reserveNoDataMemory(ctx context.Context) error {
	if stream.coordinator.budget.MaxRetainedBytes == 0 {
		return nil
	}
	share := stream.coordinator.qgShareBytes()
	var held uint64
	for _, snapshot := range stream.noData.Items {
		bytes := noDataMemoryBytes(snapshot)
		if share > 0 && stream.retainedTotal()+held+bytes > share {
			if stream.noDataRefused == nil {
				stream.noDataRefused = make(map[execution.PlanNoDataIdentity]execution.NoDataApplyItemResult)
			}
			stream.noDataRefused[snapshot.Identity] = noDataBytesRefusal(snapshot.Identity, len(snapshot.Groups), bytes, share)
			continue
		}
		held += bytes
	}
	if held == 0 {
		return nil
	}
	if err := stream.reserveProvisionalAt(ctx, 0, held, stream.reservationPhase(noDataReservationPhase)); err != nil {
		return err
	}
	stream.retainBytes(retainPhaseState, held)
	return nil
}
