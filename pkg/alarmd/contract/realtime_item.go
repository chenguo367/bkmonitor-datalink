// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package contract

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// RealtimeItemV2 is a realtime item as its Plan freezes it
// (EvaluationPlanV2.Realtime): the metric its points are and the conditions
// a point's series is judged by before it is filed under the Plan (016
// section 5). Whether the item's channel has taken effect is not here: it
// moves with every change of the table's clean rules, and is read from the
// channel record as each period closes, not frozen with the Plan.
type RealtimeItemV2 struct {
	// Item is which strategy item this is, as the channel record names the
	// items its clean rules cover (006 C4): whether it has taken effect is
	// looked up by it as a period closes (realtimeperiod.ChannelStates).
	Item   RealtimeItemKey `json:"item"`
	Metric string          `json:"metric"`
	// Conditions are groups the point's series passes when it passes any
	// one, and a group when it passes every condition of it: or between
	// groups, and within one (016 section 5.1). A condition with no values is dropped with its connector
	// as the item is compiled; a group left with none is refused there, as
	// the queried path refuses it (QUERY_CONDITION_EMPTY_GROUP). No groups is
	// every series.
	Conditions [][]RealtimeConditionV2 `json:"conditions,omitempty"`
}

// RealtimeItemKey is a strategy item as the channel record's applied and
// refused items name it: its strategy, the item, and the item's query
// digest. An item whose query changed is another key until the table's
// rules are applied again with it, and so has not taken effect.
type RealtimeItemKey struct {
	StrategyID int64  `json:"strategy_id"`
	ItemID     int64  `json:"item_id"`
	QueryMD5   string `json:"query_md5"`
}

// RealtimeConditionV2 is one condition on a series' label: Key's value, the
// empty string when the series has no such label, judged by Method against
// Values.
type RealtimeConditionV2 struct {
	Key    string   `json:"key"`
	Method string   `json:"method"`
	Values []string `json:"values"`
}

// The methods a realtime condition is judged by, closed: the six the
// strategy page offers, in its own words (016 section 5.1). Any other is
// refused as the item is compiled (REALTIME_CONDITION_UNSUPPORTED).
const (
	// RealtimeConditionEq: the value equals one of Values exactly.
	RealtimeConditionEq = "eq"
	// RealtimeConditionNeq: the value equals none of Values.
	RealtimeConditionNeq = "neq"
	// RealtimeConditionInclude: the whole value matches ^(?:v1|v2)$, the
	// values joined as written - a whole-string regular expression, not a
	// substring, as VictoriaMetrics matches it.
	RealtimeConditionInclude = "include"
	// RealtimeConditionExclude: the whole value does not match it.
	RealtimeConditionExclude = "exclude"
	// RealtimeConditionReg is RealtimeConditionInclude under its other name.
	RealtimeConditionReg = "reg"
	// RealtimeConditionNreg is RealtimeConditionExclude under its other name.
	RealtimeConditionNreg = "nreg"
)

// RealtimeConditionMethods is the closed set of the methods a realtime
// condition is judged by.
func RealtimeConditionMethods() []string {
	return []string{RealtimeConditionEq, RealtimeConditionNeq, RealtimeConditionInclude, RealtimeConditionExclude,
		RealtimeConditionReg, RealtimeConditionNreg}
}

// Validate reports why item is not a realtime part a Plan can be run with,
// nil when it is: its strategy item, a metric, and conditions that hold
// (ValidateRealtimeConditions), so a part frozen with one that does not is
// refused where the Plan is read rather than failing each point it is
// judged on.
func (item RealtimeItemV2) Validate() error {
	if item.Item.StrategyID <= 0 || item.Item.ItemID <= 0 || item.Item.QueryMD5 == "" {
		return errors.New("contract: a realtime item names its strategy, its item and its query digest")
	}
	if item.Metric == "" {
		return errors.New("contract: a realtime item names its metric")
	}
	return ValidateRealtimeConditions(item.Conditions)
}

// ValidateRealtimeConditions reports why groups are not condition groups a
// point's series can be judged by, nil when they are: every group holding a
// condition at least, and every condition naming its label, one of the six
// methods and its values - a regular expression's compiled as the
// whole-string match it is judged by. No group is every series.
func ValidateRealtimeConditions(groups [][]RealtimeConditionV2) error {
	for _, group := range groups {
		if len(group) == 0 {
			return errors.New("contract: a realtime condition group holds a condition at least")
		}
		for _, condition := range group {
			if condition.Key == "" || len(condition.Values) == 0 {
				return errors.New("contract: a realtime condition names its label and its values")
			}
			if !slices.Contains(RealtimeConditionMethods(), condition.Method) {
				return fmt.Errorf("contract: realtime condition method %q is not one of %v", condition.Method, RealtimeConditionMethods())
			}
			if _, err := condition.Pattern(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Pattern is the whole-string expression a regular-expression condition
// (include, exclude, reg, nreg) is judged by: its values joined as written,
// ^(?:v1|v2)$, the match VictoriaMetrics makes of the same condition. Nil
// for a condition judged by equality.
func (condition RealtimeConditionV2) Pattern() (*regexp.Regexp, error) {
	switch condition.Method {
	case RealtimeConditionInclude, RealtimeConditionExclude, RealtimeConditionReg, RealtimeConditionNreg:
	default:
		return nil, nil
	}
	pattern, err := regexp.Compile("^(?:" + strings.Join(condition.Values, "|") + ")$")
	if err != nil {
		return nil, fmt.Errorf("contract: realtime condition on %q is not a regular expression: %w", condition.Key, err)
	}
	return pattern, nil
}
