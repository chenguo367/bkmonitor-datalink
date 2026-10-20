// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// namedOutputCompletion is a completion whose one query aligned records
// records of two outputs.
func namedOutputCompletion(records int) execution.QueryExecutionCompletion {
	table := &execution.NamedOutputRecords{Refs: []string{"A", "C"}, Records: map[string][]execution.NamedOutputValue{}}
	for record := 0; record < records; record++ {
		table.Records[fmt.Sprintf("%064x", record)] = []execution.NamedOutputValue{
			{Value: json.RawMessage("1"), State: execution.NamedOutputSuccess}, {Value: json.RawMessage("11"), State: execution.NamedOutputSuccess}}
	}
	return execution.QueryExecutionCompletion{PhysicalQueries: []execution.PhysicalQueryCompletion{{NamedOutputs: table}}}
}

func namedOutputStream(maxRetained, own uint64) (*SlotExecutionCoordinator, *streamedExecution) {
	co := &SlotExecutionCoordinator{budget: ProvisionalBudget{
		MaxSeries: 10, MaxRetainedBytes: maxRetained, MaxStateMutations: 10, MaxEvents: 10, MaxGapMutations: 10,
	}}
	co.reservations.retainedBytes = own
	return co, &streamedExecution{coordinator: co, began: true, retainedByPhase: retainedSeed(own)}
}

// The named outputs a Slot's queries aligned are input it retains: charged
// to it when the completion arrives, at what the table holds, to the input
// phase and the pool, and given back with the Slot. A completion of queries
// that asked for none is charged nothing.
func TestANamedOutputTableIsChargedToTheSlotAndReleasedWithIt(t *testing.T) {
	completion := namedOutputCompletion(10)
	retained := namedOutputRecordsRetainedBytes(completion)
	if retained == 0 || retained != completion.PhysicalQueries[0].NamedOutputs.RetainedBytes() {
		t.Fatalf("a table of 10 records is charged %d", retained)
	}
	co, stream := namedOutputStream(1<<20, 100)
	if err := stream.retainNamedOutputs(context.Background(), completion); err != nil {
		t.Fatal(err)
	}
	if stream.retainedByPhase[retainPhaseInput] != 100+retained || co.reservations.retainedBytes != 100+retained {
		t.Fatalf("input %d pool %d, want %d", stream.retainedByPhase[retainPhaseInput], co.reservations.retainedBytes, 100+retained)
	}
	stream.releaseProvisional()
	if co.reservations.retainedBytes != 0 {
		t.Fatalf("%d bytes still reserved after the Slot", co.reservations.retainedBytes)
	}
	co, stream = namedOutputStream(1<<20, 100)
	if err := stream.retainNamedOutputs(context.Background(), execution.QueryExecutionCompletion{
		PhysicalQueries: []execution.PhysicalQueryCompletion{{}}}); err != nil || co.reservations.retainedBytes != 100 {
		t.Fatalf("a completion without named outputs was charged: %v, pool %d", err, co.reservations.retainedBytes)
	}
}

// A table that takes the Slot past the share one Query Group may hold is
// refused as a share refusal of the Slot's own total - the same one a
// series over the share gets, which the scheduler stops re-reading - and
// reserves nothing.
func TestANamedOutputTablePastTheShareIsRefused(t *testing.T) {
	completion := namedOutputCompletion(10)
	retained := namedOutputRecordsRetainedBytes(completion)
	// The share is three quarters of the pool: room for the Slot's own 100
	// bytes and not for the table beside them.
	co, stream := namedOutputStream((100+retained)/3*4-4, 100)
	err := stream.retainNamedOutputs(context.Background(), completion)
	refusal, deterministic := execution.ShareRefusalOf(fmt.Errorf("alarmd worker: complete Slot: %w", err))
	if err == nil || !deterministic || refusal.RequestedBytes != retained || refusal.OwnUsedBytes != 100 {
		t.Fatalf("refusal %+v deterministic %v error %v", refusal, deterministic, err)
	}
	if co.reservations.retainedBytes != 100 || stream.retainedByPhase[retainPhaseInput] != 100 {
		t.Fatalf("a refused table reserved: pool %d input %d", co.reservations.retainedBytes, stream.retainedByPhase[retainPhaseInput])
	}
}

// A Slot says how much it may still retain under its share - the bound the
// access source puts on aligning named outputs - and nothing once it is at
// or past it; a pool with no share bounds nothing.
func TestASlotSaysWhatItsShareHasLeft(t *testing.T) {
	_, stream := namedOutputStream(1000, 100)
	if left, bounded := stream.RetainedShareLeftBytes(); !bounded || left != 650 {
		t.Fatalf("left %d bounded %v, want 650 of a 750 share", left, bounded)
	}
	_, stream = namedOutputStream(1000, 900)
	if left, bounded := stream.RetainedShareLeftBytes(); !bounded || left != 0 {
		t.Fatalf("a Slot past its share has %d left (bounded %v)", left, bounded)
	}
	_, stream = namedOutputStream(0, 0)
	if _, bounded := stream.RetainedShareLeftBytes(); bounded {
		t.Fatal("a pool with no share bounds the alignment")
	}
}
