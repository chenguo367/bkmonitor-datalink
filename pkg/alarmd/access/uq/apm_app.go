// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"regexp"
	"slices"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The APM application a series belongs to is asked of the provider
// alongside the strategy's own dimensions, so that the event can name its
// APM object from the data rather than from a guess.
//
// An APM metric or trace table holds one application: the platform creates
// one per application, under the application's normalised name, and keeps
// those names unique. Grouping such a query by app_name therefore returns
// the same series it returned without, each carrying its application -- the
// name as the application wrote it, hyphens included, which the table's own
// name cannot give back. It is added here, to the request body, and nowhere
// else: the compiled plan, the Query Group's identity, the series identity,
// the state keys and the alert fingerprint are all what they were, because
// none of them is built from the body. The record carries app_name beside
// its identity dimensions; only the APM object reads it.
//
// It is added only when every clause reads the same APM table. Clauses on
// different tables are joined by the provider on their group keys, and a
// key one side has and the other does not, or that differs between two
// applications, would change the join.
const apmAppDimension = "app_name"

var apmTable = regexp.MustCompile(`^(?:space_)?\d+_bkapm_(?:metric|trace)_[A-Za-z0-9_-]+(?:\.__default__)?$`)

// carriesAPMApp says whether spec's request asks for the APM application.
func carriesAPMApp(spec execution.PhysicalQuerySpec) bool {
	facts := spec.PlanFacts
	if facts.PromQL != nil || len(facts.QueryList) == 0 {
		return false
	}
	table := facts.QueryList[0].TableID
	if !apmTable.MatchString(table) {
		return false
	}
	for _, clause := range facts.QueryList {
		if clause.TableID != table {
			return false
		}
	}
	return true
}

// withAPMApp is clause grouped by app_name as well: its dimensions, the
// columns it keeps, and each aggregation grouped by the clause's dimensions.
// A clause that already groups by app_name is returned as it is.
func withAPMApp(clause execution.QueryClause) execution.QueryClause {
	if slices.Contains(clause.Dimensions, apmAppDimension) {
		return clause
	}
	groupBy := clause.Dimensions
	clause.Dimensions = append(append([]string(nil), groupBy...), apmAppDimension)
	if len(clause.KeepColumns) > 0 && !slices.Contains(clause.KeepColumns, apmAppDimension) {
		clause.KeepColumns = append(append([]string(nil), clause.KeepColumns...), apmAppDimension)
	}
	functions := append([]execution.QueryFunction(nil), clause.Functions...)
	for index := range functions {
		if slices.Equal(functions[index].Dimensions, groupBy) {
			functions[index].Dimensions = append(append([]string(nil), groupBy...), apmAppDimension)
		}
	}
	clause.Functions = functions
	return clause
}
