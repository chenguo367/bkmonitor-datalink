// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package admission

import (
	"encoding/json"
	"regexp"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// RealtimeConditionRejected is why RealtimeConditionFilter turns a series
// away: it passed no group of its item's conditions.
const RealtimeConditionRejected = "condition_rejected"

// RealtimeConditions is a realtime item's condition groups compiled once for
// its Plan: what each point's series is judged by in process, as
// VictoriaMetrics judges the same conditions for the item's aggregated
// counterpart (016 section 5.1). A series passes when it passes every
// condition of any one group; no group is every series.
type RealtimeConditions struct {
	groups [][]realtimeCondition
}

// realtimeCondition is one condition compiled: a label's value equal to one
// of a set (eq, neq), or matching a whole-string expression (include,
// exclude, reg, nreg), negated for neq, exclude and nreg.
type realtimeCondition struct {
	key     string
	negated bool
	values  map[string]struct{}
	pattern *regexp.Regexp
}

// NewRealtimeConditions compiles a realtime item's condition groups
// (contract.RealtimeItemV2.Conditions), refusing groups that do not hold
// (contract.ValidateRealtimeConditions): a Plan frozen with them is refused
// before it is run, so this is the second time they are asked, not the
// first.
func NewRealtimeConditions(groups [][]contract.RealtimeConditionV2) (*RealtimeConditions, error) {
	if err := contract.ValidateRealtimeConditions(groups); err != nil {
		return nil, err
	}
	conditions := &RealtimeConditions{groups: make([][]realtimeCondition, 0, len(groups))}
	for _, group := range groups {
		compiled := make([]realtimeCondition, 0, len(group))
		for _, condition := range group {
			pattern, err := condition.Pattern()
			if err != nil {
				return nil, err
			}
			one := realtimeCondition{key: condition.Key, pattern: pattern}
			switch condition.Method {
			case contract.RealtimeConditionNeq, contract.RealtimeConditionExclude, contract.RealtimeConditionNreg:
				one.negated = true
			}
			if pattern == nil {
				one.values = make(map[string]struct{}, len(condition.Values))
				for _, value := range condition.Values {
					one.values[value] = struct{}{}
				}
			}
			compiled = append(compiled, one)
		}
		conditions.groups = append(conditions.groups, compiled)
	}
	return conditions, nil
}

// Pass reports whether a series whose label values label gives passes: any
// group whose every condition holds. label is a label's value, the empty
// string for a label the series does not have - as the label matchers of
// PromQL read a missing label, and as VictoriaMetrics stores none with an
// empty value.
func (conditions *RealtimeConditions) Pass(label func(key string) string) bool {
	if conditions == nil || len(conditions.groups) == 0 {
		return true
	}
	for _, group := range conditions.groups {
		passed := true
		for _, condition := range group {
			if !condition.holds(label(condition.key)) {
				passed = false
				break
			}
		}
		if passed {
			return true
		}
	}
	return false
}

func (condition realtimeCondition) holds(value string) bool {
	var matched bool
	if condition.pattern != nil {
		matched = condition.pattern.MatchString(value)
	} else {
		_, matched = condition.values[value]
	}
	return matched != condition.negated
}

// RealtimeConditionFilter admits a point's series for a realtime Plan when
// it passes the item's conditions (PlanContext.RealtimeConditions), and every
// series for a Plan without them. It reads each label's value exactly as the
// series carries it - a string's content, any other value's own text - and
// not admission's normalized dimension text: a condition is an exact label
// match, as the provider's is.
type RealtimeConditionFilter struct{}

// Name names the filter in an admission's reason.
func (RealtimeConditionFilter) Name() string { return "realtime_condition" }

// Admit judges facts' series by plan's realtime conditions.
func (RealtimeConditionFilter) Admit(plan PlanContext, facts *Facts) Decision {
	if plan.RealtimeConditions == nil {
		return Decision{Admit: true}
	}
	if plan.RealtimeConditions.Pass(func(key string) string { return labelValue(facts.Dimensions[key]) }) {
		return Decision{Admit: true}
	}
	return Decision{Admit: false, Reason: RealtimeConditionRejected}
}

// labelValue is a dimension's value as a label carries it: a JSON string's
// content, null or absent as the empty string, any other value its own text.
func labelValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	if string(raw) == "null" {
		return ""
	}
	return string(raw)
}
