// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package realtimeoracle

import (
	"fmt"

	"github.com/VictoriaMetrics/metricsql"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
)

// uqSelector is the label filters VictoriaMetrics selects series by for a
// realtime item's condition groups on the queried path, one slice of
// matchers per group a series passes by matching every one: each
// condition's method compiled to the operator alarmd's metric dialect sends
// unify-query (controlplane.MetricConditionOperator), the groups rendered by
// unify-query's own AllConditions.VMString, and the selector it renders
// parsed back by MetricsQL into its groups of label filters. Nil for no
// groups: every series.
//
// The one place a change of a signature on that path is adapted to: after
// unify-query #1628, VMString also returns an error.
func uqSelector(groups [][]contract.RealtimeConditionV2) ([][]*labels.Matcher, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	conditions := make(structured.AllConditions, 0, len(groups))
	for _, group := range groups {
		fields := make([]structured.ConditionField, 0, len(group))
		for _, condition := range group {
			operator, known := controlplane.MetricConditionOperator(condition.Method)
			if !known {
				return nil, fmt.Errorf("realtimeoracle: the metric dialect compiles no operator for %q", condition.Method)
			}
			fields = append(fields, structured.ConditionField{DimensionName: condition.Key, Operator: operator,
				Value: append([]string(nil), condition.Values...)})
		}
		conditions = append(conditions, fields)
	}
	selector, _ := conditions.VMString("", "", false)
	expr, err := metricsql.Parse("{" + string(selector) + "}")
	if err != nil {
		return nil, fmt.Errorf("realtimeoracle: MetricsQL does not parse the selector %q: %w", selector, err)
	}
	metric, ok := expr.(*metricsql.MetricExpr)
	if !ok {
		return nil, fmt.Errorf("realtimeoracle: the selector %q parses as %T, not a series selector", selector, expr)
	}
	matchers := make([][]*labels.Matcher, 0, len(metric.LabelFilterss))
	for _, filters := range metric.LabelFilterss {
		group := make([]*labels.Matcher, 0, len(filters))
		for _, filter := range filters {
			matcher, err := labels.NewMatcher(matchType(filter), filter.Label, filter.Value)
			if err != nil {
				return nil, fmt.Errorf("realtimeoracle: label filter %+v: %w", filter, err)
			}
			group = append(group, matcher)
		}
		matchers = append(matchers, group)
	}
	return matchers, nil
}

// matchType is the label matcher a MetricsQL label filter is: = != =~ !~.
func matchType(filter metricsql.LabelFilter) labels.MatchType {
	switch {
	case filter.IsRegexp && filter.IsNegative:
		return labels.MatchNotRegexp
	case filter.IsRegexp:
		return labels.MatchRegexp
	case filter.IsNegative:
		return labels.MatchNotEqual
	}
	return labels.MatchEqual
}

// selects is whether a series of these labels passes selector: any group
// whose every matcher matches, a missing label matched as the empty string.
// A nil selector is every series.
func selects(selector [][]*labels.Matcher, series map[string]string) bool {
	if selector == nil {
		return true
	}
	for _, group := range selector {
		matched := true
		for _, matcher := range group {
			if !matcher.Matches(series[matcher.Name]) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
