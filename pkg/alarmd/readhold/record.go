// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package readhold keeps each Query Group's read hold: how much later than
// its schedule says a Slot of it is read and due, for a Query Group whose
// whole window is read before its data has arrived. A Slot carries the hold
// it was frozen with in its contract (execution.FrozenExecutionContractRef).
package readhold

import (
	"encoding/json"
	"errors"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Namespace is the control namespace of a Query Group's read hold record,
// beside its Progress under the schedule prefix.
const Namespace = "read_hold"

// Record is a Query Group's read hold as its owner keeps it: the hold its
// Slots are frozen with now and the first Slot frozen with it, and the hold
// before that and its first Slot. A Query Group with no record has never
// had a hold other than none within the record's lifetime: every change is
// written, a return to none too.
type Record struct {
	HoldMillis int64                    `json:"hold_ms"`
	SinceSlot  execution.EvaluationTime `json:"since_slot"`
	// PreviousHoldMillis and PreviousSinceSlot are the hold before and its
	// first Slot. A record begun from no hold has none before it from Slot
	// 1 on: every Slot before SinceSlot was frozen with none.
	PreviousHoldMillis int64                    `json:"previous_hold_ms,omitempty"`
	PreviousSinceSlot  execution.EvaluationTime `json:"previous_since_slot,omitempty"`
}

// ErrRecordInvalid is a record that does not decode or holds out of range.
var ErrRecordInvalid = errors.New("alarmd readhold: invalid read hold record")

// Decode reads a record. Fields it does not know are ignored: a later
// build may keep more beside the holds.
func Decode(raw []byte) (Record, error) {
	var record Record
	if err := json.Unmarshal(raw, &record); err != nil {
		return Record{}, ErrRecordInvalid
	}
	if !inRange(record.HoldMillis) || !inRange(record.PreviousHoldMillis) || record.SinceSlot <= 0 ||
		record.PreviousSinceSlot < 0 || record.PreviousSinceSlot > record.SinceSlot {
		return Record{}, ErrRecordInvalid
	}
	return record, nil
}

// HoldAt is the hold the Slot at slot was frozen with, and false when the
// record no longer says: a Slot before the hold before this one.
func (record Record) HoldAt(slot execution.EvaluationTime) (int64, bool) {
	switch {
	case slot >= record.SinceSlot:
		return record.HoldMillis, true
	case record.PreviousSinceSlot > 0 && slot >= record.PreviousSinceSlot:
		return record.PreviousHoldMillis, true
	}
	return 0, false
}

func inRange(hold int64) bool {
	return hold >= 0 && hold <= execution.MaxReadHoldMillis
}
