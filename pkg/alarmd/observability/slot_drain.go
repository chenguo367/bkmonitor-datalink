// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package observability

// StageHandoverDrained names one planned handover's wait for the moved Query
// Group's Slot before its lease is released (SlotDrain, site handover). A
// stop's wait for every Slot is carried on the shutdown line instead (site
// shutdown), which is written once per process.
const StageHandoverDrained = "handover_drained"

// StageOutputUnapplied names one Slot whose events the broker acknowledged
// and whose State write the ownership store then refused (ReasonCode is the
// refusal): the next owner redoes the Slot from Progress and sends the same
// events again. Written by the Slot that holds both facts, once per Slot.
const StageOutputUnapplied = "output_unapplied"

// The places a drain is waited for, closed.
const (
	SlotDrainSiteHandover = "handover"
	SlotDrainSiteShutdown = "shutdown"
)

// The drain outcomes, closed.
const (
	// SlotDrainIdle: nothing was in flight; the release did not wait.
	SlotDrainIdle = "idle"
	// SlotDrainFinished: everything in flight returned inside the bound.
	SlotDrainFinished = "finished"
	// SlotDrainLeaseEnded: a handover's lease stopped renewing first -- its
	// grace ran out or the store refused it -- so the Slot's later writes are
	// refused and the next owner can redo it.
	SlotDrainLeaseEnded = "lease_ended"
	// SlotDrainDeadline: a stop's drain deadline passed and what was still
	// running was cancelled.
	SlotDrainDeadline = "deadline"
	// SlotDrainStopped: a handover was still waiting when the process began
	// to stop; the stop's own drain covers the Slot.
	SlotDrainStopped = "stopped"
)

// SlotDrainOutcomes is every outcome, for closed label sets.
var SlotDrainOutcomes = []string{SlotDrainIdle, SlotDrainFinished, SlotDrainLeaseEnded, SlotDrainDeadline, SlotDrainStopped}

// SlotDrainFacts is one wait for in-flight Slots before a lease or a process
// let go (design 02 §6.2, §6.6). A Slot that outlives the wait can have had
// its events acknowledged and its State refused, and the next owner sends
// those events again: lease_ended and deadline are where that can happen,
// and output_unapplied_total counts it when it does.
type SlotDrainFacts struct {
	Site    string `json:"site"`
	Outcome string `json:"outcome"`
	// Waited is how many Slots were in flight when the wait began, and
	// Cancelled how many were still running when a stop's deadline passed.
	Waited    int   `json:"waited"`
	Cancelled int   `json:"cancelled"`
	WaitMS    int64 `json:"wait_ms"`
}
