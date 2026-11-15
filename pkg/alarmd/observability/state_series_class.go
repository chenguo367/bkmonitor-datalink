// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

// StateSeriesClass is what a series admitted for a State write decided this
// round (execution.SeriesClass), as a label.
type StateSeriesClass string

const (
	StateSeriesClassTransition       StateSeriesClass = "transition"
	StateSeriesClassCountingRecovery StateSeriesClass = "counting_recovery"
	StateSeriesClassCountingTrigger  StateSeriesClass = "counting_trigger"
	StateSeriesClassGapped           StateSeriesClass = "gapped"
	StateSeriesClassWarming          StateSeriesClass = "warming"
	StateSeriesClassOther            StateSeriesClass = "other"
	StateSeriesClassStableNormal     StateSeriesClass = "stable_normal"
)

// StateSeriesKind says whether the series is one the query produced or a
// synthetic no-data one, which is judged by its no-data Level alone.
type StateSeriesKind string

const (
	StateSeriesReal   StateSeriesKind = "real"
	StateSeriesNoData StateSeriesKind = "no_data"
)

// StateNotWrittenReason is why a mutation admitted for writing - counted in a
// series class - was not written by the attempt, one per exit of the write
// path after the admission.
type StateNotWrittenReason string

const (
	// StateNotWrittenOutputWithheld: the Plan's output did not start or did
	// not land - the lease too short for a batch, a refusal decided in this
	// process, an acknowledgement unknown - so none of its State is written.
	StateNotWrittenOutputWithheld StateNotWrittenReason = "output_withheld"
	// StateNotWrittenOutputPartial: the sink wrote the batch but would not
	// represent some of its events; their series keep their State.
	StateNotWrittenOutputPartial StateNotWrittenReason = "output_partial"
	// StateNotWrittenAdmissionRejected: the State admission refused the
	// mutation by name (a terminal reason), so its write is not made.
	StateNotWrittenAdmissionRejected StateNotWrittenReason = "state_admission_rejected"
	// StateNotWrittenApplyFailed: the write was made and the store did not
	// take it - the apply failed, or refused the mutation.
	StateNotWrittenApplyFailed StateNotWrittenReason = "state_apply_failed"
	// StateNotWrittenSlotFailed: the attempt ended with an error before the
	// mutation was written, by any other exit: a later step of its own Plan,
	// or another Plan, or the completion.
	StateNotWrittenSlotFailed StateNotWrittenReason = "slot_failed"
	// StateNotWrittenUnaccounted: what the census cannot place, zero always.
	// On an attempt that completed, admitted mutations no named exit counted:
	// recorded here, the classes sum again to written plus not written. On
	// any attempt, more counted than admitted, by the difference: the sum
	// does not hold then - the census itself counted wrong. Either way the
	// attempt says so on a line of its own.
	StateNotWrittenUnaccounted StateNotWrittenReason = "unaccounted"
)

// StateSeriesClassKey is one cell of the class reading.
type StateSeriesClassKey struct {
	Class  StateSeriesClass
	Series StateSeriesKind
}

// AllStateSeriesClasses, AllStateSeriesKinds and AllStateNotWrittenReasons are
// the closed label sets, for a reader to pre-create and bound the families by.
func AllStateSeriesClasses() []StateSeriesClass {
	return []StateSeriesClass{
		StateSeriesClassTransition, StateSeriesClassCountingRecovery, StateSeriesClassCountingTrigger,
		StateSeriesClassGapped, StateSeriesClassWarming, StateSeriesClassOther, StateSeriesClassStableNormal,
	}
}

func AllStateSeriesKinds() []StateSeriesKind {
	return []StateSeriesKind{StateSeriesReal, StateSeriesNoData}
}

func AllStateNotWrittenReasons() []StateNotWrittenReason {
	return []StateNotWrittenReason{
		StateNotWrittenOutputWithheld, StateNotWrittenOutputPartial, StateNotWrittenAdmissionRejected,
		StateNotWrittenApplyFailed, StateNotWrittenSlotFailed, StateNotWrittenUnaccounted,
	}
}

// NormalizeStateSeriesClass bounds the class label: an unknown one is other.
func NormalizeStateSeriesClass(class StateSeriesClass) StateSeriesClass {
	for _, known := range AllStateSeriesClasses() {
		if class == known {
			return class
		}
	}
	return StateSeriesClassOther
}

// NormalizeStateNotWrittenReason bounds the reason label: an unknown one is
// unaccounted, the reason that names no exit.
func NormalizeStateNotWrittenReason(reason StateNotWrittenReason) StateNotWrittenReason {
	for _, known := range AllStateNotWrittenReasons() {
		if reason == known {
			return reason
		}
	}
	return StateNotWrittenUnaccounted
}
