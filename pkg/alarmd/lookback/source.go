// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package lookback measures data that arrives after the formal read. A small
// share of first reads is kept as it was seen; the same window is read again
// later, and the two are compared series by series and bucket by bucket, and
// under each Level's own static threshold. It never writes State, Progress or
// an event: what it produces is counts and a few bounded examples.
package lookback

import (
	"bytes"
	"encoding/json"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// The sources the first version measures, closed: the log platform's COUNT
// and the collector's log event count. Both are single-source,
// single-metric queries whose value is a count.
const (
	SourceLogSearch    = "bk_log_search/log"
	SourceCollectorLog = "bk_monitor/log"
)

// Sources is every measured source.
var Sources = []string{SourceLogSearch, SourceCollectorLog}

// SourceOf names the measured source a query reads, and false for a query
// the first version does not measure.
func SourceOf(facts execution.QueryPlanFacts) (string, bool) {
	if facts.PromQL != nil || len(facts.QueryList) != 1 || len(facts.SourceSemantics) != 1 {
		return "", false
	}
	clause := facts.QueryList[0]
	switch facts.SourceSemantics[0] {
	case SourceLogSearch:
		if clause.TimeAggregation.Method == "count_over_time" {
			return SourceLogSearch, true
		}
	case SourceCollectorLog:
		if clause.FieldName == "event.count" && clause.TimeAggregation.Method == "sum_over_time" {
			return SourceCollectorLog, true
		}
	}
	return "", false
}

// planCheck is one Plan's static thresholds, as its Levels evaluate one
// value. comparable is false for a Plan any of whose Levels evaluates
// something else -- another algorithm, another input -- which the lookback
// cannot re-decide from a value alone; such a Plan's series are compared as
// data only.
type planCheck struct {
	identity   execution.PlanIdentity
	comparable bool
	levels     []levelCheck
}

type levelCheck struct {
	levelID   uint32
	and       bool
	detectors []detectorCheck
}

type detectorCheck struct {
	normalizer strategy.NumericNormalizerSpec
	predicate  strategy.Predicate
}

func planCheckOf(identity execution.PlanIdentity, plan *strategy.CompiledPlan, valueField string) planCheck {
	check := planCheck{identity: identity, comparable: plan != nil}
	if plan == nil {
		return check
	}
	for _, level := range plan.Levels() {
		compiled := levelCheck{levelID: level.Definition().LevelID, and: level.Connector() == contract.LevelConnectorAND}
		for _, detector := range level.Detectors() {
			normalizer, found := plan.Normalizer(detector.NormalizerRef())
			if detector.Kind() != strategy.DetectorKindThreshold || detector.ValueRef() != valueField || !found {
				return planCheck{identity: identity}
			}
			compiled.detectors = append(compiled.detectors, detectorCheck{normalizer: normalizer, predicate: detector.Predicate()})
		}
		if len(compiled.detectors) == 0 {
			return planCheck{identity: identity}
		}
		check.levels = append(check.levels, compiled)
	}
	check.comparable = len(check.levels) > 0
	return check
}

// abnormal says whether the Level's threshold holds on one value, and false
// for ok when the value cannot be evaluated at all.
func (level levelCheck) abnormal(raw []byte) (abnormal, ok bool) {
	matched := level.and
	for _, detector := range level.detectors {
		normalized := detector.normalizer.Normalize(json.RawMessage(bytes.TrimSpace(raw)))
		if !normalized.Available() {
			return false, false
		}
		evaluation, err := detector.predicate.Evaluate(normalized.Value())
		if err != nil {
			return false, false
		}
		if level.and {
			matched = matched && evaluation.Matched()
		} else {
			matched = matched || evaluation.Matched()
		}
	}
	return matched, true
}
