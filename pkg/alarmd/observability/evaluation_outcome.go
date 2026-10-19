// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import "log/slog"

// CarriedReasonFacts is where a carried reason came from: the Slot that first
// stamped the stored guard (its evaluation time) and the reason it stamped it
// with. Both zero when the guard was stamped without an origin - by a build
// that did not record one - and then the line says carried and nothing more.
type CarriedReasonFacts struct {
	OriginSlot  int64
	OriginCause ReasonCode
}

// Known says the origin was recorded.
func (facts CarriedReasonFacts) Known() bool {
	return facts.OriginSlot > 0 && facts.OriginCause != ""
}

// ReasonOrigin is the label value a counter of reasons takes for whether a
// reason is the round's own or a stored guard's: physical for what the round
// itself met, carried for a guard's reason repeated on a later round. Closed
// and two-valued.
const (
	ReasonOriginPhysical = "physical"
	ReasonOriginCarried  = "carried"
)

// ReasonOriginLabel is the origin label for a reason that was or was not
// carried.
func ReasonOriginLabel(carried bool) string {
	if carried {
		return ReasonOriginCarried
	}
	return ReasonOriginPhysical
}

// evaluationHeldQuery is a query_completed line whose physical queries
// answered whole while the round's evaluation did not succeed. The line's own
// result is success, and it is not routine: it is the line that says a
// Level was held while its query was fine, written as the evaluation's
// degraded line was before, at the query's level (INFO).
func evaluationHeldQuery(observation Observation) bool {
	return observation.Component == ComponentAccess && observation.Stage == StageQueryCompleted &&
		observation.EvaluationResult != "" && !routineResult(observation.EvaluationResult)
}

func carriedReasonStage(stage Stage) bool {
	switch stage {
	case StageQueryCompleted, StageEvaluationCompleted, StageSlotCompleted, StageProgressCommitted:
		return true
	}
	return false
}

func normalizeEvaluationOutcome(observation Observation) Observation {
	if observation.Component != ComponentAccess || observation.Stage != StageQueryCompleted {
		observation.EvaluationResult, observation.EvaluationReason = "", ""
	} else if observation.EvaluationResult != "" {
		observation.EvaluationResult = NormalizeResult(observation.EvaluationResult)
		observation.EvaluationReason = NormalizeReason(observation.EvaluationReason, observation.EvaluationResult)
	}
	if observation.Stage != StageSlotCompleted {
		observation.SlotCompletionCause, observation.SlotCompletionReason = "", ""
	}
	if facts := observation.CarriedReason; facts != nil {
		if !carriedReasonStage(observation.Stage) {
			observation.CarriedReason = nil
		} else {
			normalized := *facts
			if !normalized.Known() {
				normalized = CarriedReasonFacts{}
			} else {
				normalized.OriginCause = NormalizeReason(normalized.OriginCause, ResultDegraded)
			}
			observation.CarriedReason = &normalized
		}
	}
	return observation
}

// appendEvaluationOutcome writes the evaluation's outcome on query_completed,
// the completion's cause on slot_completed, and on any of the lines that
// carry one, that the reason was carried and where it came from.
func appendEvaluationOutcome(attributes []slog.Attr, observation Observation) []slog.Attr {
	if observation.Component == ComponentAccess && observation.Stage == StageQueryCompleted && observation.EvaluationResult != "" {
		attributes = append(attributes,
			slog.String("evaluation_result", string(observation.EvaluationResult)),
			slog.String("evaluation_reason", string(observation.EvaluationReason)),
		)
	}
	if observation.Stage == StageSlotCompleted && observation.SlotCompletionCause != "" {
		attributes = append(attributes,
			slog.String("completion_cause", NormalizeProgressCompletionCause(observation.SlotCompletionCause)),
			slog.String("completion_reason", string(NormalizeReason(ReasonCode(observation.SlotCompletionReason), ResultDegraded))),
		)
	}
	if facts := observation.CarriedReason; facts != nil {
		attributes = append(attributes, slog.Bool("evaluation_reason_carried", true))
		if facts.Known() {
			attributes = append(attributes,
				slog.Int64("evaluation_reason_origin_slot", facts.OriginSlot),
				slog.String("evaluation_reason_origin_cause", string(facts.OriginCause)),
			)
		}
	}
	return attributes
}
