// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package realtimeoracle checks alarmd's in-process judgement of a realtime
// item's conditions (admission.RealtimeConditions) against the series
// VictoriaMetrics selects for the same conditions on the queried path,
// judged by Prometheus' label matchers (016 section 5.2): the operators
// alarmd's metric dialect compiles them to, the selector unify-query's
// VMString renders, as MetricsQL parses it. The expectation comes from the
// compiler and the renderer called as the queried path calls them, not from
// a table or a loop written beside the judgement: a change of either fails
// the comparison at once. It compares VictoriaMetrics alone: InfluxQL
// matches an expression anywhere in a value, not the whole of it.
//
// It is a module of its own so that alarmd's module does not take
// unify-query's dependencies: requiring it from alarmd's would raise eight
// of the production binary's dependencies and its Go version for a test
// alone (016 section 5.2). Its replaces point at this tree's alarmd and
// unify-query by relative path, so it compares with the renderer of the same
// commit. The gate runs it as a step of its own.
package realtimeoracle
