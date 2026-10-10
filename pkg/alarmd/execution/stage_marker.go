// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package execution

import (
	"context"
	"sync/atomic"
	"time"
)

// The stages a running Slot can be found in, closed: where the Runner and the
// coordinator have got to, so that an execution found past its deadline
// names where it is stuck (design 02 section 6.5). A stage is entered, not
// completed; the last one entered is where an execution that never returns
// stopped.
const (
	SlotStageSource   = "source"
	SlotStageExecute  = "execute"
	SlotStageQuery    = "query"
	SlotStageEvaluate = "evaluate"
	SlotStageOutput   = "output"
	SlotStageState    = "state"
	SlotStageProgress = "progress"
)

// SlotStages is every stage, in the order a Slot passes them.
var SlotStages = []string{
	SlotStageSource, SlotStageExecute, SlotStageQuery, SlotStageEvaluate, SlotStageOutput, SlotStageState, SlotStageProgress,
}

// StageMarker holds the stage a Slot last entered and the deadline of the
// Slot it is running. One per Runner; read from another goroutine, so both
// are atomic.
type StageMarker struct {
	stage    atomic.Value
	deadline atomic.Int64
}

// Deadline is the deadline of the Slot being executed, zero before one is
// frozen (the source stage has none).
func (marker *StageMarker) Deadline() time.Time {
	if marker == nil {
		return time.Time{}
	}
	if millis := marker.deadline.Load(); millis > 0 {
		return time.UnixMilli(millis)
	}
	return time.Time{}
}

// Begin enters stage for a Slot whose deadline is deadlineMillis (zero for
// none known).
func (marker *StageMarker) Begin(stage string, deadlineMillis int64) {
	if marker != nil {
		marker.deadline.Store(deadlineMillis)
		marker.stage.Store(stage)
	}
}

// Stage is the stage last entered, empty before any.
func (marker *StageMarker) Stage() string {
	if marker == nil {
		return ""
	}
	stage, _ := marker.stage.Load().(string)
	return stage
}

// Enter records stage as the one the Slot is in now.
func (marker *StageMarker) Enter(stage string) {
	if marker != nil {
		marker.stage.Store(stage)
	}
}

type stageMarkerKey struct{}

// WithStageMarker carries marker to the code a Slot runs, for MarkStage.
func WithStageMarker(ctx context.Context, marker *StageMarker) context.Context {
	if marker == nil {
		return ctx
	}
	return context.WithValue(ctx, stageMarkerKey{}, marker)
}

// MarkStage records that the Slot running under ctx has entered stage; a
// context without a marker records nothing.
func MarkStage(ctx context.Context, stage string) {
	if marker, ok := ctx.Value(stageMarkerKey{}).(*StageMarker); ok {
		marker.Enter(stage)
	}
}
