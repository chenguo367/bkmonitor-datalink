// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package nodata

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// SlotInput is one Slot's no-data evidence for one Plan, as the worker holds it
// before any of it has been turned into a decision.
//
// The series are their dimensions and nothing else. Absence is decided from
// which groups reported, never from what they reported: a group that sent a
// value this item would call anomalous is present, and a group that sent
// nothing is absent, and no value anywhere changes either.
type SlotInput struct {
	Plan           *contract.EvaluationPlanV2
	EvaluationTime int64
	PeriodSeconds  int64
	Completeness   execution.Completeness
	// Series is one entry per series the Slot saw, holding that series'
	// dimensions with values already as text.
	Series []map[string]string
	// KnownHosts is the set of "address|cloud" keys the CMDB index confirmed
	// for this business, for a target roster to be intersected with.
	KnownHosts map[string]struct{}
	// OutOfBusiness names the groups whose host resolved to another business.
	OutOfBusiness map[string]struct{}
	Memory        map[string]GroupMemory
	// RosterVersion names the derivation the roster came from, carried into the
	// facts so a reader can tell one round's expected set from another's.
	RosterVersion string
}

// EvaluateSlot turns one Slot's evidence into the no-data decision for it.
//
// It is the seam the worker calls: everything above it is state and wiring,
// everything below it is the projection, the roster and the absence rules. It
// reads no clock, no store and no CMDB index, so a Slot that is retried reaches
// the same decision from the same evidence - which is the property the whole
// evaluation is built on, and the one that stops being true the moment any of
// this is decided inside the worker instead.
//
// A Plan that does not detect no-data returns the zero result and no error. The
// caller does not have to ask twice, and a Plan that gains the section later
// starts being evaluated without the caller changing.
func EvaluateSlot(input SlotInput) (AbsenceResult, bool, error) {
	if input.Plan == nil || input.Plan.NoData == nil {
		return AbsenceResult{}, false, nil
	}
	config := input.Plan.NoData
	tally := ProjectSeries(input.Series, config.AggDimension)
	roster, err := BuildRoster(RosterRequest{
		AggDimension: config.AggDimension,
		Scope:        input.Plan.TargetScope,
		KnownHosts:   input.KnownHosts,
		Memory:       input.Memory,
	})
	if err != nil {
		return AbsenceResult{}, false, err
	}
	roster.Version = input.RosterVersion
	return Evaluate(AbsenceInput{
		EvaluationTime: input.EvaluationTime,
		PeriodSeconds:  input.PeriodSeconds,
		Completeness:   input.Completeness,
		Present:        tally.Groups,
		Dropped:        tally.Dropped,
		Roster:         roster,
		Memory:         input.Memory,
		OutOfBusiness:  input.OutOfBusiness,
	}), true, nil
}
