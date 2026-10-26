// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package controlplane

import (
	"strconv"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// ReasonGlobalStrategyUnsupported withholds a global strategy this build
// cannot run as one. The detail names which condition it failed
// (reason=<word>); the words are below.
const ReasonGlobalStrategyUnsupported = "GLOBAL_STRATEGY_UNSUPPORTED"

// Why a global business strategy is withheld.
const (
	// GlobalBusinessLegacyTarget: the strategy names its target in the old
	// form. That path's no-data roster keeps only the hosts of the Plan's
	// own business, so every other business's hosts would lose no-data
	// detection without a word.
	GlobalBusinessLegacyTarget = "legacy_target"
	// GlobalBusinessQueryKind: the query does not read the metric router's
	// time series: a structured query over logs, events or the computing
	// platform's tables, or a PromQL selector whose name starts with such a
	// source (bkdata:, bklog:, bkapm:). How those are routed with the space
	// skipped has not been established.
	GlobalBusinessQueryKind = "query_kind"
	// GlobalBusinessQueryTable: a query names no table or data label. With
	// the space skipped the provider has no space to find tables in, answers
	// no series with SPACE_TABLE_ID_FIELD_IS_NOT_EXISTS, and the query is
	// unavailable on every round - the strategy would never be evaluated,
	// and would say so only as a query failure. A PromQL selector with a
	// bare metric name is worse: the provider answers 200 with no series and
	// no status, so it is refused by name (metric=<name> in the detail).
	GlobalBusinessQueryTable = "query_table"
	// GlobalBusinessOutputProtocol: the Plan would publish the compatible
	// event, which has no place for the business an alert is about.
	GlobalBusinessOutputProtocol = "output_protocol"
)

// GlobalBusinessRefusalWords is every word a GLOBAL_STRATEGY_UNSUPPORTED
// refusal names, the reason label's closed set in the global composition.
var GlobalBusinessRefusalWords = []string{
	GlobalBusinessLegacyTarget, GlobalBusinessQueryKind, GlobalBusinessQueryTable, GlobalBusinessOutputProtocol,
}

// globalBusinessQuerySemantics are the query sources a global business Plan
// may read: the structured time series the metric router serves. An empty
// SourceSemantics is the first of them alone.
var globalBusinessQuerySemantics = map[string]struct{}{
	"bk_monitor/time_series": {},
	"custom/time_series":     {},
}

// GlobalQuerySourcePromQL labels a global strategy whose query is PromQL.
// PromQL names no source semantics of its own - its selectors name their
// tables or data labels in the metric name - so it is counted under this
// label, accepted or refused, rather than under the time series it reads.
const GlobalQuerySourcePromQL = "promql"

// GlobalQuerySource is the label a global strategy's query is counted under in
// the global composition and named by in its refusal: promql, or the source
// semantics partition label (SourceSemanticsLabel), so the set stays closed.
//
// It exists because the first reading after global strategies reach a
// deployment is which query sources they were refused for, and that decides
// which source alarmd learns to run globally next. Until this the refusal
// said only the word; the source was in the compiled facts and went nowhere.
func GlobalQuerySource(facts execution.QueryPlanFacts) string {
	if facts.PromQL != nil {
		return GlobalQuerySourcePromQL
	}
	return SourceSemanticsLabel(facts.SourceSemantics)
}

// globalBusinessRefusal is the admission a global business strategy passes
// before its Plan is compiled, and nil for one that passes or is not global.
// The word it was refused for is returned beside the record, for the round's
// global composition to count without reading it back out of the detail.
// The output protocol is decided later and checked where it is.
func globalBusinessRefusal(
	sourceID string, identity SourceIdentity, targetScope *contract.TargetScopeV2, facts execution.QueryPlanFacts,
) (*ObjectDisposition, string) {
	if !identity.GlobalBusiness {
		return nil, ""
	}
	source := GlobalQuerySource(facts)
	refuse := func(reason, fieldPath string) (*ObjectDisposition, string) {
		refusal := globalBusinessUnsupported(sourceID, reason, fieldPath, source)
		return &refusal, reason
	}
	if targetScope != nil {
		return refuse(GlobalBusinessLegacyTarget, "items[0].target")
	}
	if facts.PromQL != nil {
		reason, metric := globalPromQLRefusal(facts.PromQL.Expression)
		if reason == "" {
			return nil, ""
		}
		refusal := globalBusinessUnsupported(sourceID, reason, "items[0].query_configs", source)
		refusal.Detail += " metric=" + metric
		return &refusal, reason
	}
	for _, semantics := range facts.SourceSemantics {
		if _, supported := globalBusinessQuerySemantics[semantics]; !supported {
			return refuse(GlobalBusinessQueryKind, "items[0].query_configs")
		}
	}
	for _, clause := range facts.QueryList {
		if !namesTableOrDataLabel(clause.TableID) {
			return refuse(GlobalBusinessQueryTable, "items[0].query_configs")
		}
	}
	return nil, ""
}

// namesTableOrDataLabel reports whether a query's table id routes on its
// own: the provider splits it at the first dot and finds tables by the part
// before it, a data label or the database of a table.
func namesTableOrDataLabel(tableID string) bool {
	database, _, _ := strings.Cut(tableID, ".")
	return strings.TrimSpace(database) != ""
}

// globalPromQLRefusal is the word a global business Plan's PromQL is refused
// for and the metric it was refused over, or empty words for one every
// selector of which routes without a space. The provider routes a selector
// by its metric name alone, the first __name__ matcher written = or =~
// (query/structured route.go MetricsToRouter), so that is the name read
// here; a selector with no such matcher has no route and is refused with an
// empty name. A name routes on its own when it names a data label
// (bkmonitor:<label>:<metric>) or a table (bkmonitor:<db>:<table>:<metric>);
// the source prefix may be left out, as the provider supplies bkmonitor. An
// expression this cannot read is refused as query_kind and named unreadable.
func globalPromQLRefusal(expression string) (string, string) {
	selectors, ok := promQLSelectorMetrics(expression)
	if !ok {
		return GlobalBusinessQueryKind, "unreadable"
	}
	if len(selectors) == 0 {
		return GlobalBusinessQueryTable, ""
	}
	for _, name := range selectors {
		switch promQLMetricRoute(name) {
		case promQLRouteOtherSource:
			return GlobalBusinessQueryKind, name
		case promQLRouteBare:
			return GlobalBusinessQueryTable, name
		}
	}
	return "", ""
}

// How a PromQL metric name routes with the space skipped.
const (
	promQLRouteTable = iota
	promQLRouteBare
	promQLRouteOtherSource
)

// promQLRouterSources are the metric name prefixes the provider takes as a
// source (route.go dataSourceMap); the first two read the metric router.
var promQLRouterSources = map[string]bool{"bkmonitor": true, "bk_monitor": true, "custom": true, "bkdata": false, "bklog": false, "bkapm": false}

// promQLMetricRoute is the provider's MakeRouteFromMetricName read for the
// database it yields: a name of fewer than four parts whose first is not a
// source is taken as bkmonitor's, and then four parts are source, database,
// table and metric, three are source, database (a data label or a table's
// database) and metric, and anything else names no database.
func promQLMetricRoute(name string) int {
	parts := strings.Split(name, ":")
	if parts[len(parts)-1] == "" {
		return promQLRouteBare
	}
	if _, isSource := promQLRouterSources[parts[0]]; len(parts) < 4 && !isSource {
		parts = append([]string{"bkmonitor"}, parts...)
	}
	if len(parts) != 3 && len(parts) != 4 || strings.TrimSpace(parts[1]) == "" {
		return promQLRouteBare
	}
	if router, isSource := promQLRouterSources[parts[0]]; isSource && !router {
		return promQLRouteOtherSource
	}
	return promQLRouteTable
}

// promQLKeywords are PromQL's words that are not metric names: binary and
// set operators, vector matching and grouping modifiers, offset, and the
// two float literals.
var promQLKeywords = map[string]bool{
	"and": true, "or": true, "unless": true, "atan2": true, "bool": true, "offset": true,
	"by": true, "without": true, "on": true, "ignoring": true, "group_left": true, "group_right": true,
	"inf": true, "nan": true,
}

// promQLLabelListModifiers are the words a parenthesised label list follows.
var promQLLabelListModifiers = map[string]bool{
	"by": true, "without": true, "on": true, "ignoring": true, "group_left": true, "group_right": true,
}

// promQLSelectorMetrics lists the metric name of every vector selector in a
// PromQL expression, in order: the identifier a selector is written with, or
// else the value of its first __name__ matcher written = or =~, or empty
// when it has neither. Function and aggregation names, keywords, label
// lists, strings, numbers, durations and comments are skipped. False is an
// expression it cannot read: an unterminated string, brace or bracket.
func promQLSelectorMetrics(expression string) ([]string, bool) {
	var names []string
	s := expression
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '"' || c == '\'' || c == '`':
			end, ok := promQLStringEnd(s, i)
			if !ok {
				return nil, false
			}
			i = end
		case c == '[':
			end := strings.IndexByte(s[i:], ']')
			if end < 0 {
				return nil, false
			}
			i += end + 1
		case c == '{':
			name, end, ok := promQLMatcherName(s, i)
			if !ok {
				return nil, false
			}
			names, i = append(names, name), end
		case c >= '0' && c <= '9' || c == '.' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
			for i < len(s) && (promQLWordByte(s[i]) || s[i] == '.' ||
				(s[i] == '+' || s[i] == '-') && (s[i-1] == 'e' || s[i-1] == 'E')) {
				i++
			}
		case promQLWordByte(c) && !(c >= '0' && c <= '9'):
			start := i
			for i < len(s) && promQLWordByte(s[i]) {
				i++
			}
			word := s[start:i]
			next := promQLSkipSpace(s, i)
			lower := strings.ToLower(word)
			switch {
			case promQLLabelListModifiers[lower] && next < len(s) && s[next] == '(':
				end := strings.IndexByte(s[next:], ')')
				if end < 0 {
					return nil, false
				}
				i = next + end + 1
			case promQLKeywords[lower]:
			case next < len(s) && s[next] == '(':
				// A function or an aggregation over its arguments.
			case promQLFollowedByGrouping(s, next):
				// An aggregation with its grouping written first.
			default:
				if next < len(s) && s[next] == '{' {
					_, end, ok := promQLMatcherName(s, next)
					if !ok {
						return nil, false
					}
					i = end
				}
				names = append(names, word)
			}
		default:
			i++
		}
	}
	return names, true
}

// promQLWordByte is a byte of a metric name: letters, digits, _ and :.
func promQLWordByte(c byte) bool {
	return c == '_' || c == ':' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

func promQLSkipSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

// promQLFollowedByGrouping is a by or without written next, as a word.
func promQLFollowedByGrouping(s string, i int) bool {
	for _, word := range []string{"by", "without"} {
		if len(s) >= i+len(word) && strings.EqualFold(s[i:i+len(word)], word) &&
			(len(s) == i+len(word) || !promQLWordByte(s[i+len(word)])) {
			return true
		}
	}
	return false
}

// promQLStringEnd is the index after the string starting at i; a backquoted
// string has no escapes.
func promQLStringEnd(s string, i int) (int, bool) {
	quote := s[i]
	for j := i + 1; j < len(s); j++ {
		switch {
		case s[j] == '\\' && quote != '`':
			j++
		case s[j] == quote:
			return j + 1, true
		}
	}
	return 0, false
}

// promQLMatcherName reads the matcher list starting at the brace at i and
// returns the value of its first __name__ matcher written = or =~, and the
// index after the closing brace.
func promQLMatcherName(s string, i int) (string, int, bool) {
	name, found := "", false
	for j := i + 1; j < len(s); {
		j = promQLSkipSpace(s, j)
		if j >= len(s) {
			break
		}
		switch s[j] {
		case '}':
			return name, j + 1, true
		case ',':
			j++
			continue
		}
		label := j
		for j < len(s) && promQLWordByte(s[j]) {
			j++
		}
		if label == j {
			return "", 0, false
		}
		key := s[label:j]
		j = promQLSkipSpace(s, j)
		op := j
		for j < len(s) && strings.IndexByte("=!~", s[j]) >= 0 {
			j++
		}
		operator := s[op:j]
		j = promQLSkipSpace(s, j)
		if j >= len(s) || strings.IndexByte("\"'`", s[j]) < 0 {
			return "", 0, false
		}
		end, ok := promQLStringEnd(s, j)
		if !ok {
			return "", 0, false
		}
		if key == "__name__" && !found && (operator == "=" || operator == "=~") {
			value, err := strconv.Unquote(promQLDoubleQuoted(s[j:end]))
			if err != nil {
				return "", 0, false
			}
			name, found = value, true
		}
		j = end
	}
	return "", 0, false
}

// promQLDoubleQuoted rewrites a single-quoted PromQL string as a Go
// double-quoted one for strconv.Unquote; double and back quotes are Go's.
func promQLDoubleQuoted(quoted string) string {
	if quoted[0] != '\'' {
		return quoted
	}
	inner := strings.ReplaceAll(quoted[1:len(quoted)-1], `\'`, `'`)
	return `"` + strings.ReplaceAll(inner, `"`, `\"`) + `"`
}

// globalBusinessUnsupported names the word and the query source in the
// detail, so the object page says which source a strategy was refused for
// without a second read.
func globalBusinessUnsupported(sourceID, reason, fieldPath, source string) ObjectDisposition {
	return ObjectDisposition{
		SourceID: sourceID, Scope: "PLAN", Disposition: DispositionUnsupported,
		Reason: ReasonGlobalStrategyUnsupported, FieldPath: fieldPath, Detail: "reason=" + reason + " source=" + source,
	}
}
