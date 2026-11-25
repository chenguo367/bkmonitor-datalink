// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package detect

import (
	"context"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// multiDetectThresholdDetector judges an input of an item detected over
// several: the input's own point of the series at the round's source time,
// by the input's own threshold - the first input's from the PRIMARY, every
// other's from its own named input. The round's record may be another
// input's, where the PRIMARY has none of the series at that time: an input
// that has no point there, or whose read is not one to trust, leaves the
// round undecided for the series.
type multiDetectThresholdDetector struct{}

func (multiDetectThresholdDetector) Key() DetectorKey {
	return DetectorKey{Kind: strategy.DetectorKindMultiDetectThreshold, Version: 1}
}

func (multiDetectThresholdDetector) Evaluate(
	ctx context.Context,
	algorithm strategy.CompiledAlgorithmPlan,
	input execution.SeriesEvaluationInputRequest,
	primary execution.RecordView,
) namedAlgorithmResult {
	dataset, spec, normalizer, ok := algorithm.MultiDetectThreshold()
	if !ok {
		return namedTerminal(contract.ReasonRecordInvalid)
	}
	binding, ok := namedBinding(input, execution.DatasetName(dataset))
	if !ok || !namedInputTrusted(binding) {
		return unavailableFromBinding(binding)
	}
	point, found, err := exactPoint(binding, primary.SourceTime())
	if err != nil {
		return namedTerminal(contract.ReasonRecordInvalid)
	}
	if !found {
		return namedUnavailable(contract.ReasonMultiDetectInputMissing)
	}
	raw, present := point.Value(spec.ValueRef())
	if !present {
		raw = nil
	}
	normalized := normalizer.Normalize(raw)
	if !normalized.Available() {
		return namedUnavailable(normalized.ReasonCode())
	}
	if err := ctx.Err(); err != nil {
		return namedTerminal(contract.ReasonRecordInvalid)
	}
	evaluation, err := spec.Predicate().Evaluate(normalized.Value())
	if err != nil {
		return namedTerminal(contract.ReasonRecordInvalid)
	}
	result := namedAlgorithmResult{result: FactResultNormal, canonicalDecimal: normalized.Value().CanonicalDecimal(),
		predicateDigest: evaluation.PredicateDigest()}
	if evaluation.Matched() {
		result.result = FactResultAnomalous
	}
	return result
}
