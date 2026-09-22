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
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// stateResultWithHistory is one series' pending mutation carrying a retained
// window, with every string as long as the caller asks. The strings inside a
// real window are a Plan level constant and a copy of the loaded history's
// own record identity, so every point shares one backing array with every
// other point of every other series.
func stateResultWithHistory(points, levels, stringLength int) []execution.StateEvaluation {
	fingerprint := strings.Repeat("f", stringLength)
	recordID := strings.Repeat("c", stringLength)
	history := make([]execution.StateHistoryPoint, 0, points)
	for index := 0; index < points; index++ {
		facts := make([]execution.StateLevelFact, 0, levels)
		for level := 0; level < levels; level++ {
			facts = append(facts, execution.StateLevelFact{
				LevelID: uint32(level + 1), DetectFingerprint: fingerprint, Result: "NORMAL",
			})
		}
		history = append(history, execution.StateHistoryPoint{
			RecordID: recordID, SourceTime: int64(index) * 60, Levels: facts,
		})
	}
	return []execution.StateEvaluation{{Mutation: execution.StateMutation{Points: history}}}
}

// A Slot is charged for the history it allocated, and the strings inside that
// history are not part of it.
//
// The two windows here differ only in how long their strings are. Every one of
// those strings is shared: DetectFingerprint is a Plan level constant, and
// RecordID is copied out of the loaded history as a header. A Slot holding the
// window holds the point array and the fact arrays; lengthening the strings
// allocates nothing, so it must not cost anything either.
//
// Before this, it cost everything. A two-level Plan retaining 1469 points was
// charged 989,739 bytes for one mutation, of which 564 KiB was string content
// shared with the loaded history and 376 KiB was the arrays. Two hundred of
// those put a Slot against a pool limit its real memory was nowhere near, and
// the refusal is RESOURCE_HARD_STOP - detection stopped over memory nothing
// held.
func TestAPendingStateHistoryIsChargedForItsArraysNotItsSharedStrings(t *testing.T) {
	const points, levels = 1469, 2
	short := retainedStateResultBytes(stateResultWithHistory(points, levels, 1))
	long := retainedStateResultBytes(stateResultWithHistory(points, levels, 64))
	if short != long {
		t.Fatalf("a window of %d-byte strings costs %d and one of 64-byte strings costs %d: the charge follows "+
			"string content that every point and every series shares one copy of", 1, short, long)
	}

	// And it still follows what was allocated. A window twice as long is two
	// arrays twice the size, so a charge that stopped moving with the window
	// would be as wrong in the other direction - and would pass the assertion
	// above without measuring anything.
	twice := retainedStateResultBytes(stateResultWithHistory(2*points, levels, 64))
	if twice <= long {
		t.Fatalf("a window of %d points costs %d and one of %d points costs %d: the charge no longer follows "+
			"the arrays the round allocated", points, long, 2*points, twice)
	}

	// The derived expectation the deployment is verified against. A point is
	// 48 bytes and a fact 40, so one mutation of this shape costs
	// 1469*48 + 1469*2*40 = 188,032 bytes, and newEffectBytes doubles it to
	// 376,064. The Slot that measured 224,670,798 across 227 mutations is
	// therefore expected at 227 * 376,064 = 85.4 MB, a 2.63x fall. What is
	// left is still above the memory truly allocated - about 32 MB, the point
	// arrays alone - because the fact arrays of the older points are counted
	// though they are headers into the loaded history.
	const wantPerMutation = uint64(points*48 + points*levels*40)
	if got := retainedStateResultBytes(stateResultWithHistory(points, levels, 64)); got < wantPerMutation ||
		got > wantPerMutation+4096 {
		t.Fatalf("one mutation of the verified shape costs %d, want the %d bytes of its arrays plus a small "+
			"fixed part: the deployment expectation in the comment above is derived from this number",
			got, wantPerMutation)
	}
}

// And the Slot's own accounting reads it, on both the paths that charge a
// series.
//
// The case above proves the sizer computes the right number; it cannot prove
// anybody calls it. newEffectBytes has two branches - the first series of a
// Plan and every series after it - and each charges state results separately,
// so leaving either on the generic sizer leaves a Slot charged for borrowed
// strings on half its series. Both are driven from here with the same pair of
// windows: identical but for string length, so a branch still following string
// content answers differently for the two.
func TestTheSlotChargesBothItsFirstSeriesAndTheRestForArraysOnly(t *testing.T) {
	const points, levels = 512, 2
	identity := execution.PlanIdentity{TenantID: "default", BusinessID: "2", StrategyID: "1001"}
	result := func(stringLength int) execution.EvaluationResult {
		return execution.EvaluationResult{Plans: []execution.PlanEvaluationResult{{
			Plan: identity, StateResults: stateResultWithHistory(points, levels, stringLength),
		}}}
	}
	empty := execution.EvaluationResult{}
	alreadyHolding := execution.EvaluationResult{Plans: []execution.PlanEvaluationResult{{Plan: identity}}}

	for _, path := range []struct {
		name    string
		current execution.EvaluationResult
	}{
		{"first series of the Plan", empty},
		{"a series after the first", alreadyHolding},
	} {
		short := newEffectBytes(path.current, result(1))
		long := newEffectBytes(path.current, result(64))
		if short != long {
			t.Fatalf("%s: a window of 1-byte strings is charged %d and one of 64-byte strings %d; this branch "+
				"still charges the Slot for string content shared with the loaded history", path.name, short, long)
		}
		if short == 0 {
			t.Fatalf("%s: charged nothing at all for a %d point window", path.name, points)
		}
	}
}
