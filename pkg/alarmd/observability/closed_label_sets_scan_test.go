// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// A closed label set folds a word it does not name onto its "other" value,
// so that a label stays bounded whatever its input. That fold is for words
// from outside - a store, a broker, a newer peer - and not for the words this
// build writes itself: a word the code emits and its own set leaves out is
// rewritten on every line and every cell, and reads as an unclassified
// emitter where the site had said exactly what happened. A supplement whose
// kept read was absent wrote result=skipped and every reader saw _other.
//
// The test below reads every non-test file of alarmd for the values that
// reach each set - constants and literals put in its field by a composite
// literal or an assignment, through a local name bound to one, through a
// parameter from each call that passes it, through the return of a function
// of the module, and through a field of a struct from every write of that
// field - and fails on any value the set's own normaliser would fold.
//
// Where the source does not say what a value is - a map lookup, a value
// decoded from a store, a word handed through a func value - the scan cannot
// check it, and it does not skip it quietly either: every such expression is
// listed in closedLabelSetUnresolved, and a new one, or one more at a listed
// place, fails.

// labelSetFoldAllowed lists values the scan finds reaching a set that are
// not in it, by set and value, each with why it is not a fold.
var labelSetFoldAllowed = map[string]string{
	// The State preflight that a store refused outright passes an empty
	// result with its error; emitObservation writes failed over it, as it
	// writes success over an empty result without one. No empty result
	// reaches NormalizeResult from there.
	"result|": "emitObservation fills an empty result from Err",

	// PENDING triage, each a value the scan sees reach the set along a
	// path it does not tell apart from the one the value really takes, or a
	// fold whose design is still to be decided.
	//
	// The query_completed reason of a failed query takes the failure's code
	// only for provider transport, admission and delivery failures; the
	// budget codes and OTHER reach the code field on other branches.
	"reason|BUDGET_EVENTS":          "pending: path-guarded in worker/query_failure.go observeQueryFailure",
	"reason|BUDGET_GAP_MUTATIONS":   "pending: path-guarded in worker/query_failure.go observeQueryFailure",
	"reason|BUDGET_OTHER":           "pending: path-guarded in worker/query_failure.go observeQueryFailure",
	"reason|BUDGET_RETAINED_BYTES":  "pending: path-guarded in worker/query_failure.go observeQueryFailure",
	"reason|BUDGET_SERIES":          "pending: path-guarded in worker/query_failure.go observeQueryFailure",
	"reason|BUDGET_STATE_MUTATIONS": "pending: path-guarded in worker/query_failure.go observeQueryFailure",
	"reason|OTHER":                  "pending: path-guarded in worker/query_failure.go observeQueryFailure",
	"reason|UNEXPLAINED_OUTCOME":    "pending: path-guarded in worker/query_failure.go observeQueryFailure",
	// slot_skipped's reason is the query-free result's reason code; whether
	// a Level's history state can reach it is not yet traced.
	"reason|GAPPED":  "pending: slot_skipped reason path not yet traced",
	"reason|WARMING": "pending: slot_skipped reason path not yet traced",
	// LevelOutcomeReasonLabel keeps only coverage reasons for the metric by
	// design (the log line keeps the exact reason); whether these belong in
	// it is a decision still to be made.
	"level_outcome_reason|BACKEND_CAPABILITY_MISSING": "pending: metric keeps coverage reasons only, by design",
	"level_outcome_reason|GAPPED":                     "pending: metric keeps coverage reasons only, by design",
	"level_outcome_reason|REDIS_UNAVAILABLE":          "pending: metric keeps coverage reasons only, by design",
	"level_outcome_reason|STATE_BUDGET_EXCEEDED":      "pending: metric keeps coverage reasons only, by design",
	"level_outcome_reason|STATE_CORRUPT":              "pending: metric keeps coverage reasons only, by design",
	"level_outcome_reason|STATE_READ_DEADLINE":        "pending: metric keeps coverage reasons only, by design",
	"level_outcome_reason|STATE_READ_TIMEOUT":         "pending: metric keeps coverage reasons only, by design",
	"level_outcome_reason|STATE_SCHEMA_UNSUPPORTED":   "pending: metric keeps coverage reasons only, by design",
	"level_outcome_reason|WARMING":                    "pending: metric keeps coverage reasons only, by design",
	"level_outcome_reason|none":                       "pending: metric keeps coverage reasons only, by design",
	// Each of these is guarded at its site (an empty format becomes
	// WireFormatOther before it is counted, an unnamed rule becomes
	// OutputRejectOther, a NORMAL record builds no event), which the scan
	// does not yet read; the gap scope's empty reason is still to be traced.
	"wire_format|":             "pending: guarded by format == \"\" in worker/coordinator.go writeEvents",
	"output_reject_rule|":      "pending: guarded by !named in kafka/trigger_event_sink.go",
	"output_event_kind|NORMAL": "pending: NORMAL records build no event in trigger/evaluator_v2.go",
	"gap_scope_reason|":        "pending: empty fold of no reasons not yet traced",
}

// labelSet is one closed set: where its values come in and what it keeps.
type labelSet struct {
	name   string
	fields []fieldSink
	params []paramSink
	known  func(string) bool
}

func closedLabelSets() []labelSet {
	const obs = "observability"
	isResult := func(v string) bool { return NormalizeResult(Result(v)) == Result(v) }
	isReason := func(v string) bool {
		// none and empty are the absence of a reason; which word that
		// reads as by result is the reasonless-lines test's concern.
		return v == "" || v == string(ReasonNone) || NormalizeReason(ReasonCode(v), ResultDegraded) == ReasonCode(v)
	}
	oneOf := func(values ...string) func(string) bool {
		return func(v string) bool {
			for _, value := range values {
				if v == value {
					return true
				}
			}
			return false
		}
	}
	failureCategories := oneOf(QueryFailureCategoryBudget, QueryFailureCategorySourceBackend,
		QueryFailureCategorySeriesIdentity, QueryFailureCategoryCompletionContract, QueryFailureCategoryNamedInput,
		QueryFailureCategoryProviderTransport, QueryFailureCategoryAdmission, QueryFailureCategoryEvaluation,
		QueryFailureCategoryDelivery, QueryFailureCategoryOther)
	stateAdmissionCalls := func(arg int) []paramSink {
		return []paramSink{{dir: "worker", recv: "SlotExecutionCoordinator", name: "observeChunk", arg: arg,
			when: map[int]string{1: StageStateAdmission}}}
	}
	statePreflightCalls := func(arg int) []paramSink {
		return []paramSink{{dir: "worker", recv: "SlotExecutionCoordinator", name: "observeWithCounts", arg: arg,
			when: map[int]string{2: StageStatePreflight}}}
	}
	return []labelSet{
		{name: "result", fields: []fieldSink{{"observability.Observation", "Result"}}, known: isResult},
		{
			// Empty is how a line that is not query_completed says it has
			// no evaluation outcome; normalizeEvaluationOutcome keeps it.
			name: "evaluation_result", fields: []fieldSink{{"observability.Observation", "EvaluationResult"}},
			known: func(v string) bool { return v == "" || isResult(v) },
		},
		{
			name: "operation", fields: []fieldSink{{"observability.Observation", "Operation"}},
			known: func(v string) bool { return v == "" || NormalizeOperation(Operation(v)) == Operation(v) },
		},
		{
			name: "direction", fields: []fieldSink{{"observability.Observation", "Direction"}},
			known: func(v string) bool { return NormalizeDirection(Direction(v)) == Direction(v) },
		},
		{
			name: "reason",
			fields: []fieldSink{
				{"observability.Observation", "ReasonCode"}, {"observability.Observation", "EvaluationReason"},
				{"observability.Observation", "SlotCompletionReason"}, {"observability.Observation", "ProgressCompletionReason"},
				{"observability.CarriedReasonFacts", "OriginCause"},
			},
			params: []paramSink{{dir: obs, name: "ViewStreamReasonCode", arg: 0}},
			known:  isReason,
		},
		{
			name: "capacity_budget", fields: []fieldSink{{"observability.Observation", "CapacityBudget"}},
			known: func(v string) bool { return NormalizeCapacityBudget(CapacityBudget(v)) == CapacityBudget(v) },
		},
		{
			// NormalizeSourceKind folds an unknown kind to empty, not to
			// an other: the word is lost all the same.
			name: "source_kind", fields: []fieldSink{{"observability.Observation", "SourceKind"}},
			known: func(v string) bool { return v == "" || NormalizeSourceKind(SourceKind(v)) == SourceKind(v) },
		},
		{
			name: "completion_cause",
			fields: []fieldSink{
				{"observability.Observation", "ProgressCompletionCause"}, {"observability.Observation", "SlotCompletionCause"},
			},
			known: func(v string) bool { return v == "" || NormalizeProgressCompletionCause(v) == v },
		},
		{
			name: "cursor_refusal", fields: []fieldSink{{"observability.CursorAdvanceFacts", "Refusal"}},
			known: oneOf("", CursorRefusalProgressMissing, CursorRefusalRangeInFlight, CursorRefusalCursorMoved,
				CursorRefusalCASConflict, CursorRefusalOther),
		},
		{
			name: "level_outcome_reason", fields: []fieldSink{{"observability.LevelOutcomeFact", "Reason"}},
			known: func(v string) bool { return v == "" || LevelOutcomeReasonLabel(v) == v },
		},
		{
			name: "query_status_code", fields: []fieldSink{{"observability.QueryStatusFacts", "Code"}},
			known: func(v string) bool { return v == "" || NormalizeQueryStatusCode(v) == v },
		},
		{
			name: "query_status_outcome", fields: []fieldSink{{"observability.QueryStatusFacts", "Outcome"}},
			known: oneOf(QueryStatusOutcomeAllowed, QueryStatusOutcomeUnavailable, QueryStatusOutcomeOther),
		},
		{
			name: "readiness_boundary", fields: []fieldSink{{"observability.SlotReadinessFacts", "Boundary"}},
			known: oneOf(ReadinessBoundaryUnified, ReadinessBoundaryMixed, ReadinessBoundaryNone, ReadinessBoundaryOther),
		},
		{
			name: "readiness_basis", fields: []fieldSink{{"observability.ReadinessDeferralFacts", "Basis"}},
			known: oneOf(ReadinessBasisSettleWait, ReadinessBasisReadHold, ReadinessBasisOther),
		},
		{
			name: "query_unavailable_attribution", fields: []fieldSink{{"observability.QueryUnavailableFacts", "Attribution"}},
			known: validQueryUnavailableAttribution,
		},
		{
			name: "query_http_class", fields: []fieldSink{{"observability.QueryUnavailableFacts", "HTTPClass"}},
			known: oneOf(QueryHTTPClass4xx, QueryHTTPClass5xx, QueryHTTPClassOther, QueryHTTPClassNone),
		},
		{
			name: "slot_skipped_path", fields: []fieldSink{{"observability.SlotSkippedFacts", "Path"}},
			known: func(v string) bool { return oneOf(SlotSkippedPaths...)(v) || v == SlotSkippedOther },
		},
		{
			name: "query_failure_stage", fields: []fieldSink{{"observability.QueryFailureFacts", "Stage"}},
			known: oneOf(QueryFailureStageExecute, QueryFailureStageStreamComplete, QueryFailureStageProvider,
				QueryFailureStageOther),
		},
		{
			name: "query_failure_category", fields: []fieldSink{{"observability.QueryFailureFacts", "Category"}},
			known: failureCategories,
		},
		{
			name: "query_failure_code", fields: []fieldSink{{"observability.QueryFailureFacts", "Code"}, {"observability.QueryRejection", "Class"}},
			known: func(v string) bool { return NormalizeQueryFailureCode(v) == v },
		},
		{
			name:   "state_write_reuse_class",
			params: []paramSink{{dir: obs, recv: "StateWriteReuseFacts", name: "Record", arg: 0}},
			known: func(v string) bool {
				return NormalizeStateWriteReuseClass(StateWriteReuseClass(v)) == StateWriteReuseClass(v)
			},
		},
		{
			name:   "state_write_reuse_stored",
			params: []paramSink{{dir: obs, recv: "StateWriteReuseFacts", name: "Record", arg: 1}},
			known: func(v string) bool {
				return NormalizeStateWriteReuseStored(StateWriteReuseStored(v)) == StateWriteReuseStored(v)
			},
		},
		{
			name:   "state_already_applied_kind",
			params: []paramSink{{dir: obs, recv: "StateAlreadyAppliedFacts", name: "Record", arg: 1}},
			known: func(v string) bool {
				return NormalizeStateAlreadyAppliedKind(StateAlreadyAppliedKind(v)) == StateAlreadyAppliedKind(v)
			},
		},
		{
			name:   "state_version_conflict_kind",
			params: []paramSink{{dir: obs, recv: "StateVersionConflictFacts", name: "Record", arg: 1}},
			known: func(v string) bool {
				return NormalizeStateVersionConflictKind(StateVersionConflictKind(v)) == StateVersionConflictKind(v)
			},
		},
		{
			name:   "state_admission_result",
			params: stateAdmissionCalls(5),
			known:  func(v string) bool { r, _ := NormalizeStateAdmission(Result(v), ""); return r == Result(v) },
		},
		{
			name:   "state_admission_reason",
			params: stateAdmissionCalls(6),
			known: func(v string) bool {
				_, r := NormalizeStateAdmission("", ReasonCode(v))
				return v == "" || v == string(ReasonNone) || r == ReasonCode(v)
			},
		},
		{
			name:   "state_preflight_result",
			params: statePreflightCalls(5),
			known: func(v string) bool {
				r, _ := NormalizeStatePreflight(Result(v), "")
				return v == "" || r == Result(v)
			},
		},
		{
			name:   "state_preflight_reason",
			params: statePreflightCalls(6),
			known: func(v string) bool {
				_, r := NormalizeStatePreflight("", ReasonCode(v))
				return v == "" || v == string(ReasonNone) || r == ReasonCode(v)
			},
		},
		{
			name: "wire_format",
			fields: []fieldSink{
				{"observability.OutputRejectedEvent", "Format"}, {"observability.OutputWithoutMessage", "Format"}, {"observability.OutputEventKindKey", "Format"},
			},
			params: []paramSink{{dir: obs, name: "NormalizeWireFormat", arg: 0}},
			known:  func(v string) bool { return NormalizeWireFormat(v) == v },
		},
		{
			name:   "output_event_kind",
			fields: []fieldSink{{"observability.OutputWithoutMessage", "EventKind"}, {"observability.OutputEventKindKey", "EventKind"}},
			params: []paramSink{{dir: obs, name: "NormalizeOutputEventKind", arg: 0}},
			known:  func(v string) bool { return NormalizeOutputEventKind(v) == v },
		},
		{
			name:   "output_reject_rule",
			fields: []fieldSink{{"observability.OutputRejectedEvent", "Rule"}, {"linkdoutput.RejectedError", "Rule"}},
			params: []paramSink{{dir: obs, name: "NormalizeOutputRejectRule", arg: 0}},
			known:  func(v string) bool { return NormalizeOutputRejectRule(v) == v },
		},
		{
			name:   "gap_scope_reason",
			fields: []fieldSink{{"observability.GapProgressFacts", "Reason"}},
			params: []paramSink{{dir: "contract", name: "NormalizeGapScopeReason", arg: 0}},
			known:  closedSetMember("contract", "GapScopeReasons"),
		},
		{
			name:   "fleet_anomaly_kind",
			params: []paramSink{{dir: "fleet", name: "MetricKind", arg: 0}},
			known:  closedSetMember("fleet", "AnomalyKinds"),
		},
		{
			name:   "fleet_failure_category",
			params: []paramSink{{dir: "fleet", name: "MetricFailureCategory", arg: 0}},
			known:  failureCategories,
		},
	}
}

// closedSetMember reads a set this package cannot import from its source:
// the string constants a package-level slice var, or a function building
// one, lists.
func closedSetMember(dir, name string) func(string) bool {
	return func(v string) bool {
		for _, member := range closedSetMembers(dir, name) {
			if member == v {
				return true
			}
		}
		return false
	}
}

var closedSetMemberCache sync.Map

func closedSetMembers(dir, name string) []string {
	key := dir + "|" + name
	if cached, ok := closedSetMemberCache.Load(key); ok {
		return cached.([]string)
	}
	index := scanIndexValue
	resolver := newScanResolver(index, nil)
	var members []string
	literals := func(file *scanFile, node ast.Node) {
		ast.Inspect(node, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			for _, element := range literal.Elts {
				members = append(members, resolver.values(&scanScope{file: file}, element, 0)...)
			}
			return false
		})
	}
	if def, ok := index.vars[dir][name]; ok {
		literals(def.file, def.expr)
	}
	for _, function := range index.functions[dir+"|"+name] {
		literals(function.file, function.decl.Body)
	}
	closedSetMemberCache.Store(key, members)
	return members
}

func TestEveryValueTheCodeWritesIsInItsClosedLabelSet(t *testing.T) {
	index := alarmdScanIndex(t)
	found := map[string]int{}
	where := map[string]string{}
	for _, set := range closedLabelSets() {
		values, unresolved, at := index.collect(set)
		if len(values) == 0 && len(unresolved) == 0 {
			t.Errorf("%s: the scan found no value and nothing it could not read; it is reading nothing", set.name)
		}
		checkLabelSet(t, set.name, values, set.known)
		for key, count := range unresolved {
			found[set.name+"|"+key] += count
			where[set.name+"|"+key] = at[key]
		}
	}
	keys := make([]string, 0, len(found)+len(closedLabelSetUnresolved))
	for key := range found {
		keys = append(keys, key)
	}
	for key := range closedLabelSetUnresolved {
		if _, seen := found[key]; !seen {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		switch got, listed := found[key], closedLabelSetUnresolved[key]; {
		case got > listed:
			t.Errorf("%s: %d values the scan cannot read, %d listed (first at %s): make the value one the source "+
				"says, or list it with what bounds it", key, got, listed, where[key])
		case got < listed:
			t.Errorf("%s: %d values the scan cannot read, %d listed: lower the count", key, got, listed)
		}
	}
}

// The lines that wrote result=skipped and were read as _other - the
// supplement whose kept read was absent, the Slot held for a supplement or
// for maintenance - keep their word; so does the control plane's recovery,
// with no special case of its own.
func TestResultsTheCodeWritesNormalizeToThemselves(t *testing.T) {
	for _, result := range []Result{ResultSkipped, ResultRecovered} {
		if got := NormalizeResult(result); got != result {
			t.Errorf("NormalizeResult(%q) = %q", result, got)
		}
		for _, component := range []Component{ComponentControlPlane, ComponentScheduler} {
			observation := NormalizeObservation(Observation{
				Component: component, Stage: StageSlotCompleted, Result: result, ReasonCode: ReasonInternalUnknown,
			})
			if observation.Result != result {
				t.Errorf("%s %q normalized to %q", component, result, observation.Result)
			}
		}
	}
	index := alarmdScanIndex(t)
	values, _, _ := index.collect(labelSet{name: "result", fields: []fieldSink{{"observability.Observation", "Result"}}})
	seen := map[string]bool{}
	for _, value := range values {
		seen[value.value] = true
	}
	for _, want := range []string{ResultSkipped, ResultRecovered} {
		if !seen[want] {
			t.Errorf("the scan no longer sees %q reach Observation.Result; it has stopped reading the sites that write it", want)
		}
	}
}

// labelValue is one value found to reach a set, and the first place it was
// found.
type labelValue struct {
	value string
	at    string
}

func checkLabelSet(t *testing.T, set string, values []labelValue, known func(string) bool) {
	t.Helper()
	for _, found := range values {
		if known(found.value) {
			continue
		}
		if _, allowed := labelSetFoldAllowed[set+"|"+found.value]; allowed {
			continue
		}
		t.Errorf("%s %q, reaching the set at %s, is not in it: every line and cell would read it as the other value",
			set, found.value, found.at)
	}
	if testing.Verbose() {
		names := make([]string, 0, len(values))
		for _, found := range values {
			names = append(names, strconv.Quote(found.value))
		}
		t.Logf("%s: %d values checked: %s", set, len(values), strings.Join(names, " "))
	}
}

// fieldSink is a struct field a value reaches a set through: a composite
// literal of the type keyed by the field, or an assignment to the field of a
// name or field chain of that type.
type fieldSink struct {
	typ   string
	field string
}

// paramSink is a parameter a value reaches a set through: what every call of
// the function passes there. recv is the receiver's type name, empty for a
// function; when keeps only the calls whose argument at a position is that
// constant.
type paramSink struct {
	dir  string
	recv string
	name string
	arg  int
	when map[int]string
}

type scanFile struct {
	dir     string
	path    string
	file    *ast.File
	imports map[string]string
}

type scanDef struct {
	file *scanFile
	expr ast.Expr
}

type scanFunc struct {
	file *scanFile
	decl *ast.FuncDecl
}

type scanCall struct {
	file   *scanFile
	caller *scanFunc
	call   *ast.CallExpr
}

type scanWrite struct {
	scope *scanScope
	expr  ast.Expr
}

type scanIndex struct {
	fset      *token.FileSet
	files     []*scanFile
	consts    map[string]map[string]scanDef
	vars      map[string]map[string]scanDef
	types     map[string]map[string]bool
	functions map[string][]*scanFunc
	methods   map[string][]*scanFunc
	enclosing map[*ast.FuncDecl]*scanFunc
	// fields is each struct type's field types, by type name.
	fields map[string]map[string]string
	// calls is every call in the module, by the name it calls.
	calls map[string][]scanCall
	// writes is every write of a struct field, by type and field.
	writes map[string][]scanWrite
	locals map[*ast.FuncDecl]map[string]string
}

var (
	scanIndexOnce  sync.Once
	scanIndexValue *scanIndex
	scanIndexErr   error
)

func alarmdScanIndex(t *testing.T) *scanIndex {
	t.Helper()
	scanIndexOnce.Do(func() { scanIndexValue, scanIndexErr = buildScanIndex("..") })
	if scanIndexErr != nil {
		t.Fatal(scanIndexErr)
	}
	return scanIndexValue
}

func buildScanIndex(root string) (*scanIndex, error) {
	module, err := modulePath(filepath.Join(root, "go.mod"))
	if err != nil {
		return nil, err
	}
	index := &scanIndex{
		fset:      token.NewFileSet(),
		consts:    map[string]map[string]scanDef{},
		vars:      map[string]map[string]scanDef{},
		types:     map[string]map[string]bool{},
		functions: map[string][]*scanFunc{},
		methods:   map[string][]*scanFunc{},
		enclosing: map[*ast.FuncDecl]*scanFunc{},
		fields:    map[string]map[string]string{},
		calls:     map[string][]scanCall{},
		writes:    map[string][]scanWrite{},
		locals:    map[*ast.FuncDecl]map[string]string{},
	}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, parseErr := parser.ParseFile(index.fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		relative, _ := filepath.Rel(root, path)
		scanned := &scanFile{
			dir: filepath.ToSlash(filepath.Dir(relative)), path: filepath.ToSlash(relative),
			file: file, imports: map[string]string{},
		}
		for _, spec := range file.Imports {
			importPath, _ := strconv.Unquote(spec.Path.Value)
			if importPath != module && !strings.HasPrefix(importPath, module+"/") {
				continue
			}
			dir := strings.TrimPrefix(strings.TrimPrefix(importPath, module), "/")
			if dir == "" {
				dir = "."
			}
			alias := importPath[strings.LastIndex(importPath, "/")+1:]
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			scanned.imports[alias] = dir
		}
		index.files = append(index.files, scanned)
		index.add(scanned)
		return nil
	})
	if err != nil {
		return nil, err
	}
	index.indexWrites()
	return index, nil
}

func modulePath(goMod string) (string, error) {
	content, err := os.ReadFile(goMod)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(content), "\n") {
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	return "", os.ErrNotExist
}

func (index *scanIndex) add(file *scanFile) {
	put := func(table map[string]map[string]scanDef, name string, expr ast.Expr) {
		if table[file.dir] == nil {
			table[file.dir] = map[string]scanDef{}
		}
		table[file.dir][name] = scanDef{file: file, expr: expr}
	}
	for _, declaration := range file.file.Decls {
		switch typed := declaration.(type) {
		case *ast.GenDecl:
			for _, spec := range typed.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					if index.types[file.dir] == nil {
						index.types[file.dir] = map[string]bool{}
					}
					index.types[file.dir][spec.Name.Name] = true
					if spec.Assign.IsValid() {
						scanTypeAliases[file.dir+"."+spec.Name.Name] = scanAlias{file: file, expr: spec.Type}
					}
					structType, ok := spec.Type.(*ast.StructType)
					if !ok {
						continue
					}
					owner := file.dir + "." + spec.Name.Name
					if index.fields[owner] == nil {
						index.fields[owner] = map[string]string{}
					}
					for _, field := range structType.Fields.List {
						for _, name := range field.Names {
							index.fields[owner][name.Name] = qualifiedType(file, field.Type)
						}
						if len(field.Names) == 0 {
							embedded := qualifiedType(file, field.Type)
							index.fields[owner][embedded[strings.LastIndex(embedded, ".")+1:]] = embedded
						}
					}
				case *ast.ValueSpec:
					if len(spec.Values) != len(spec.Names) {
						continue
					}
					for i, name := range spec.Names {
						if typed.Tok == token.CONST {
							put(index.consts, name.Name, spec.Values[i])
						} else {
							put(index.vars, name.Name, spec.Values[i])
						}
					}
				}
			}
		case *ast.FuncDecl:
			function := &scanFunc{file: file, decl: typed}
			index.enclosing[typed] = function
			ast.Inspect(typed, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if name := calleeName(call.Fun); name != "" {
						index.calls[name] = append(index.calls[name], scanCall{file: file, caller: function, call: call})
					}
				}
				return true
			})
			if typed.Recv == nil {
				key := file.dir + "|" + typed.Name.Name
				index.functions[key] = append(index.functions[key], function)
			} else {
				index.methods[typed.Name.Name] = append(index.methods[typed.Name.Name], function)
			}
		}
	}
}

// isNormaliser says a function is one of the folds themselves: what it
// writes, and what it is passed, is a value already read where it was first
// written, and reading it again here would only list the fold as a source.
func isNormaliser(function *scanFunc) bool {
	if function == nil {
		return false
	}
	return strings.HasPrefix(strings.ToLower(function.decl.Name.Name), "normalize")
}

// indexWrites records every write of a struct field: each keyed element of a
// composite literal of a named type, and each assignment to a field of a name
// or field chain whose type the source says.
func (index *scanIndex) indexWrites() {
	resolver := newScanResolver(index, nil)
	for _, file := range index.files {
		for _, declaration := range file.file.Decls {
			var function *scanFunc
			if decl, ok := declaration.(*ast.FuncDecl); ok {
				function = index.enclosing[decl]
			}
			if isNormaliser(function) {
				continue
			}
			scope := &scanScope{file: file, function: function}
			ast.Inspect(declaration, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.CompositeLit:
					typ := qualifiedType(file, node.Type)
					if index.fields[typ] == nil {
						return true
					}
					for _, element := range node.Elts {
						pair, ok := element.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						if key, ok := pair.Key.(*ast.Ident); ok {
							write := scanWrite{scope: scope, expr: pair.Value}
							index.writes[typ+"|"+key.Name] = append(index.writes[typ+"|"+key.Name], write)
						}
					}
				case *ast.AssignStmt:
					if len(node.Lhs) != len(node.Rhs) || (node.Tok != token.ASSIGN && node.Tok != token.DEFINE) {
						return true
					}
					for i, left := range node.Lhs {
						selector, ok := left.(*ast.SelectorExpr)
						if !ok {
							continue
						}
						typ := resolver.typeOf(scope, selector.X)
						if typ == "" {
							continue
						}
						key := typ + "|" + selector.Sel.Name
						index.writes[key] = append(index.writes[key], scanWrite{scope: scope, expr: node.Rhs[i]})
					}
				}
				return true
			})
		}
	}
}

// collect is every value that reaches a set, one entry per distinct value,
// sorted, and the expressions the scan could not read a value from.
func (index *scanIndex) collect(set labelSet) ([]labelValue, map[string]int, map[string]string) {
	sinks := map[string]bool{}
	for _, sink := range set.fields {
		sinks[sink.typ+"|"+sink.field] = true
	}
	resolver := newScanResolver(index, sinks)
	first := map[string]string{}
	record := func(values []string, at token.Pos) {
		for _, value := range values {
			if _, ok := first[value]; !ok {
				first[value] = index.fset.Position(at).String()
			}
		}
	}
	for _, sink := range set.fields {
		for _, write := range index.writes[sink.typ+"|"+sink.field] {
			record(resolver.values(write.scope, write.expr, 0), write.expr.Pos())
		}
	}
	for _, sink := range set.params {
		function := index.function(sink)
		if function == nil {
			resolver.unresolved["missing function "+sink.dir+"."+sink.recv+"."+sink.name]++
			continue
		}
		scope := &scanScope{file: function.file, function: function}
		parameter := parameterIdent(function.decl, sink.arg)
		resolver.when = sink.when
		record(resolver.callers(scope, parameter, sink.arg, 0), parameter.Pos())
		resolver.when = nil
	}
	values := make([]labelValue, 0, len(first))
	for value, at := range first {
		values = append(values, labelValue{value: value, at: at})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].value < values[j].value })
	return values, resolver.unresolved, resolver.where
}

func (index *scanIndex) function(sink paramSink) *scanFunc {
	candidates := index.functions[sink.dir+"|"+sink.name]
	if sink.recv != "" {
		candidates = index.methods[sink.name]
	}
	for _, candidate := range candidates {
		if candidate.file.dir != sink.dir {
			continue
		}
		if sink.recv == "" || qualifiedType(candidate.file, candidate.decl.Recv.List[0].Type) == sink.dir+"."+sink.recv {
			return candidate
		}
	}
	return nil
}

func parameterIdent(decl *ast.FuncDecl, position int) *ast.Ident {
	index := 0
	for _, field := range decl.Type.Params.List {
		for _, name := range field.Names {
			if index == position {
				return name
			}
			index++
		}
	}
	return decl.Name
}

// typedLocals are the names of one function whose type can be read from the
// source: the receiver, parameters and results, var declarations, names bound
// to a composite literal, to a call of a function of the module or to a field
// chain of a known type, and range values over one.
func (index *scanIndex) typedLocals(decl *ast.FuncDecl) map[string]string {
	if typed, ok := index.locals[decl]; ok {
		return typed
	}
	typed := map[string]string{}
	index.locals[decl] = typed
	function := index.enclosing[decl]
	resolver := newScanResolver(index, nil)
	scope := &scanScope{file: function.file, function: function}
	fieldList := func(list *ast.FieldList) {
		if list == nil {
			return
		}
		for _, field := range list.List {
			for _, name := range field.Names {
				typed[name.Name] = qualifiedType(function.file, field.Type)
			}
		}
	}
	fieldList(decl.Recv)
	fieldList(decl.Type.Params)
	fieldList(decl.Type.Results)
	if decl.Body == nil {
		return typed
	}
	ast.Inspect(decl.Body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.FuncLit:
			fieldList(node.Type.Params)
		case *ast.ValueSpec:
			for i, name := range node.Names {
				if node.Type != nil {
					typed[name.Name] = qualifiedType(function.file, node.Type)
				} else if i < len(node.Values) {
					if literal, ok := unwrapLiteral(node.Values[i]); ok {
						typed[name.Name] = qualifiedType(function.file, literal.Type)
					}
				}
			}
		case *ast.AssignStmt:
			if node.Tok != token.DEFINE {
				return true
			}
			if len(node.Lhs) == len(node.Rhs) {
				for i, left := range node.Lhs {
					name, ok := left.(*ast.Ident)
					if !ok {
						continue
					}
					if call, ok := node.Rhs[i].(*ast.CallExpr); ok {
						if result := resolver.resultType(scope, call, 0); result != "" {
							typed[name.Name] = result
						}
					} else if known := resolver.typeOf(scope, node.Rhs[i]); known != "" {
						typed[name.Name] = known
					}
				}
			} else if call, ok := node.Rhs[0].(*ast.CallExpr); ok && len(node.Rhs) == 1 {
				for i, left := range node.Lhs {
					if name, ok := left.(*ast.Ident); ok {
						if result := resolver.resultType(scope, call, i); result != "" {
							typed[name.Name] = result
						}
					}
				}
			}
		case *ast.RangeStmt:
			if name, ok := node.Value.(*ast.Ident); ok {
				if element := resolver.typeOf(scope, node.X); element != "" {
					typed[name.Name] = element
				}
			}
		}
		return true
	})
	return typed
}

// qualifiedType is the type a type expression names, as its package
// directory and name - observability.Observation for Observation in this
// package, *observability.Observation or observability.Observation in another
// - so that two packages' types of one name stay two types. A slice or a map
// reads as its element, which is what ranging over it gives; a predeclared
// type is its bare name.
func qualifiedType(file *scanFile, expression ast.Expr) string {
	name := namedType(file, expression)
	for range 8 {
		alias, ok := scanTypeAliases[name]
		if !ok {
			break
		}
		name = namedType(alias.file, alias.expr)
	}
	return name
}

// scanTypeAliases are the module's type aliases, by qualified name: an alias
// names the type it stands for.
var scanTypeAliases = map[string]scanAlias{}

type scanAlias struct {
	file *scanFile
	expr ast.Expr
}

func namedType(file *scanFile, expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.StarExpr:
		return namedType(file, typed.X)
	case *ast.SelectorExpr:
		if owner, ok := typed.X.(*ast.Ident); ok {
			if dir, imported := file.imports[owner.Name]; imported {
				return dir + "." + typed.Sel.Name
			}
		}
		return typed.Sel.Name
	case *ast.Ident:
		if types.Universe.Lookup(typed.Name) != nil {
			return typed.Name
		}
		return file.dir + "." + typed.Name
	case *ast.ArrayType:
		return namedType(file, typed.Elt)
	case *ast.MapType:
		return namedType(file, typed.Value)
	case *ast.Ellipsis:
		return namedType(file, typed.Elt)
	}
	return ""
}

func calleeName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.SelectorExpr:
		return typed.Sel.Name
	case *ast.Ident:
		return typed.Name
	}
	return ""
}

type scanScope struct {
	file     *scanFile
	function *scanFunc
	// args are the arguments of the call the scan entered the function
	// through, by parameter position: a parameter read there is what that
	// call passed, not what every call passes.
	args map[int]scanArg
}

type scanArg struct {
	scope *scanScope
	expr  ast.Expr
}

type scanResolver struct {
	index *scanIndex
	// sinks are the fields of the set being read: a value read back from one
	// of them was counted where it was written.
	sinks map[string]bool
	when  map[int]string
	seen  map[string]bool
	// unresolved counts the expressions a value came from that the source
	// does not say the value of, by file, function and expression.
	unresolved map[string]int
	where      map[string]string
}

func newScanResolver(index *scanIndex, sinks map[string]bool) *scanResolver {
	return &scanResolver{
		index: index, sinks: sinks, seen: map[string]bool{},
		unresolved: map[string]int{}, where: map[string]string{},
	}
}

// leaf records an expression the scan could not read a value from.
func (r *scanResolver) leaf(scope *scanScope, expression ast.Expr) []string {
	name := "-"
	if scope.function != nil {
		name = scope.function.decl.Name.Name
	}
	key := scope.file.path + "|" + name + "|" + types.ExprString(expression)
	r.unresolved[key]++
	if _, ok := r.where[key]; !ok {
		r.where[key] = r.index.fset.Position(expression.Pos()).String()
	}
	return nil
}

// maxScanDepth bounds how many hops - a bound name, a parameter, a return, a
// field - a value is followed through.
const maxScanDepth = 24

// values is every string constant an expression can be, as far as the source
// says without running it.
func (r *scanResolver) values(scope *scanScope, expression ast.Expr, depth int) []string {
	if expression == nil {
		return nil
	}
	if depth > maxScanDepth {
		return r.leaf(scope, expression)
	}
	key := r.index.fset.Position(expression.Pos()).String() + "#" + r.index.fset.Position(expression.End()).String()
	if r.seen[key] {
		return nil
	}
	r.seen[key] = true
	defer delete(r.seen, key)

	switch typed := expression.(type) {
	case *ast.ParenExpr:
		return r.values(scope, typed.X, depth)
	case *ast.BasicLit:
		if typed.Kind != token.STRING {
			return r.leaf(scope, expression)
		}
		value, err := strconv.Unquote(typed.Value)
		if err != nil {
			return r.leaf(scope, expression)
		}
		return []string{value}
	case *ast.BinaryExpr:
		if typed.Op != token.ADD {
			return r.leaf(scope, expression)
		}
		var joined []string
		for _, left := range r.values(scope, typed.X, depth+1) {
			for _, right := range r.values(scope, typed.Y, depth+1) {
				joined = append(joined, left+right)
			}
		}
		return joined
	case *ast.CallExpr:
		if r.isConversion(scope, typed.Fun) && len(typed.Args) == 1 {
			return r.values(scope, typed.Args[0], depth)
		}
		return r.returns(scope, typed, 0, depth)
	case *ast.SelectorExpr:
		if owner, ok := typed.X.(*ast.Ident); ok {
			if dir, imported := scope.file.imports[owner.Name]; imported {
				if !r.isPackageValue(dir, typed.Sel.Name) {
					return r.leaf(scope, expression)
				}
				return r.packageValue(dir, typed.Sel.Name, depth)
			}
		}
		return r.field(scope, typed, depth)
	case *ast.Ident:
		return r.ident(scope, typed, depth)
	}
	return r.leaf(scope, expression)
}

// field is every value written to a field, where the source says whose field
// it is and something writes it.
func (r *scanResolver) field(scope *scanScope, selector *ast.SelectorExpr, depth int) []string {
	owner := r.typeOf(scope, selector.X)
	if owner == "" {
		return r.leaf(scope, selector)
	}
	key := owner + "|" + selector.Sel.Name
	if r.sinks[key] {
		return nil
	}
	writes := r.index.writes[key]
	if len(writes) == 0 {
		return r.leaf(scope, selector)
	}
	var values []string
	for _, write := range writes {
		values = append(values, r.values(write.scope, write.expr, depth+1)...)
	}
	return values
}

func (r *scanResolver) isConversion(scope *scanScope, fun ast.Expr) bool {
	switch typed := fun.(type) {
	case *ast.ParenExpr:
		return r.isConversion(scope, typed.X)
	case *ast.Ident:
		return typed.Name == "string" || r.index.types[scope.file.dir][typed.Name]
	case *ast.SelectorExpr:
		owner, ok := typed.X.(*ast.Ident)
		if !ok {
			return false
		}
		dir, imported := scope.file.imports[owner.Name]
		return imported && r.index.types[dir][typed.Sel.Name]
	}
	return false
}

func (r *scanResolver) isPackageValue(dir, name string) bool {
	_, constant := r.index.consts[dir][name]
	_, variable := r.index.vars[dir][name]
	return constant || variable
}

func (r *scanResolver) packageValue(dir, name string, depth int) []string {
	if def, ok := r.index.consts[dir][name]; ok {
		return r.values(&scanScope{file: def.file}, def.expr, depth+1)
	}
	if def, ok := r.index.vars[dir][name]; ok {
		return r.values(&scanScope{file: def.file}, def.expr, depth+1)
	}
	return nil
}

func (r *scanResolver) ident(scope *scanScope, ident *ast.Ident, depth int) []string {
	if scope.function != nil {
		if bound, found := boundValues(scope.function.decl.Body, ident.Name); found {
			var values []string
			for _, binding := range bound {
				switch {
				case binding.expr == nil:
					r.leaf(scope, ident)
				case binding.result < 0:
					values = append(values, r.values(scope, binding.expr, depth+1)...)
				default:
					call, _ := binding.expr.(*ast.CallExpr)
					if call == nil {
						r.leaf(scope, ident)
						continue
					}
					values = append(values, r.returns(scope, call, binding.result, depth+1)...)
				}
			}
			return values
		}
		if position, ok := parameterIndex(scope.function.decl, ident.Name); ok {
			if arg, entered := scope.args[position]; entered {
				return r.values(arg.scope, arg.expr, depth+1)
			}
			if isNormaliser(scope.function) {
				return nil
			}
			return r.callers(scope, ident, position, depth)
		}
	}
	if !r.isPackageValue(scope.file.dir, ident.Name) {
		return r.leaf(scope, ident)
	}
	return r.packageValue(scope.file.dir, ident.Name, depth)
}

// binding is one value a function gives a name: an expression of its own
// (result < 0), one result of a call, or nil where the source does not say
// (a range variable, a tuple from something other than a call).
type binding struct {
	expr   ast.Expr
	result int
}

// boundValues is every value a function gives a name, and whether it
// declares or assigns the name at all.
func boundValues(body *ast.BlockStmt, name string) ([]binding, bool) {
	var bound []binding
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.AssignStmt:
			for i, left := range node.Lhs {
				ident, ok := left.(*ast.Ident)
				if !ok || ident.Name != name {
					continue
				}
				found = true
				switch {
				case len(node.Lhs) == len(node.Rhs):
					bound = append(bound, binding{expr: node.Rhs[i], result: -1})
				case len(node.Rhs) == 1:
					bound = append(bound, binding{expr: node.Rhs[0], result: i})
				default:
					bound = append(bound, binding{})
				}
			}
		case *ast.ValueSpec:
			for i, ident := range node.Names {
				if ident.Name != name {
					continue
				}
				found = true
				switch {
				case len(node.Names) == len(node.Values):
					bound = append(bound, binding{expr: node.Values[i], result: -1})
				case len(node.Values) == 1:
					bound = append(bound, binding{expr: node.Values[0], result: i})
				}
			}
		case *ast.RangeStmt:
			for _, variable := range []ast.Expr{node.Key, node.Value} {
				if ident, ok := variable.(*ast.Ident); ok && ident.Name == name {
					found = true
					bound = append(bound, binding{})
				}
			}
		}
		return true
	})
	return bound, found
}

func parameterIndex(decl *ast.FuncDecl, name string) (int, bool) {
	position := 0
	for _, field := range decl.Type.Params.List {
		if len(field.Names) == 0 {
			position++
			continue
		}
		for _, ident := range field.Names {
			if ident.Name == name {
				if _, variadic := field.Type.(*ast.Ellipsis); variadic {
					return 0, false
				}
				return position, true
			}
			position++
		}
	}
	return 0, false
}

// callers is what every call of a function passes as one of its parameters.
func (r *scanResolver) callers(scope *scanScope, parameter *ast.Ident, position, depth int) []string {
	function := scope.function
	when := r.when
	r.when = nil
	var values []string
	matched := 0
	for _, site := range r.index.calls[function.decl.Name.Name] {
		if position >= len(site.call.Args) || site.call.Ellipsis.IsValid() {
			continue
		}
		callerScope := &scanScope{file: site.file, function: site.caller}
		if !r.calls(callerScope, site.call.Fun, function) || !r.matches(callerScope, site.call, when) {
			continue
		}
		matched++
		values = append(values, r.values(callerScope, site.call.Args[position], depth+1)...)
	}
	if matched == 0 {
		// Called through a func value or an interface, or not at all.
		r.leaf(scope, parameter)
	}
	return values
}

func (r *scanResolver) matches(scope *scanScope, call *ast.CallExpr, when map[int]string) bool {
	for position, want := range when {
		if position >= len(call.Args) {
			return false
		}
		values := newScanResolver(r.index, nil).values(scope, call.Args[position], 0)
		if len(values) != 1 || values[0] != want {
			return false
		}
	}
	return true
}

// calls says whether a call's function expression can be a call of this
// function. A method is matched by its receiver's type where the source says
// what that is, and by its name only where one type alone has the method.
func (r *scanResolver) calls(scope *scanScope, fun ast.Expr, function *scanFunc) bool {
	switch fun := fun.(type) {
	case *ast.Ident:
		return function.decl.Recv == nil && scope.file.dir == function.file.dir
	case *ast.SelectorExpr:
		if owner, ok := fun.X.(*ast.Ident); ok {
			if dir, imported := scope.file.imports[owner.Name]; imported {
				return function.decl.Recv == nil && dir == function.file.dir
			}
		}
		if function.decl.Recv == nil || len(function.decl.Recv.List) == 0 {
			return false
		}
		receiver := qualifiedType(function.file, function.decl.Recv.List[0].Type)
		if known := r.typeOf(scope, fun.X); known != "" {
			return known == receiver
		}
		for _, other := range r.index.methods[function.decl.Name.Name] {
			if qualifiedType(other.file, other.decl.Recv.List[0].Type) != receiver {
				return false
			}
		}
		return true
	}
	return false
}

// typeOf is the type name of a name or a field chain, where the source says
// it: a local of a known type, then each field's declared type.
func (r *scanResolver) typeOf(scope *scanScope, expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.ParenExpr:
		return r.typeOf(scope, typed.X)
	case *ast.StarExpr:
		return r.typeOf(scope, typed.X)
	case *ast.UnaryExpr:
		return r.typeOf(scope, typed.X)
	case *ast.IndexExpr:
		return r.typeOf(scope, typed.X)
	case *ast.CompositeLit:
		return qualifiedType(scope.file, typed.Type)
	case *ast.Ident:
		if scope.function == nil {
			return ""
		}
		return r.index.typedLocals(scope.function.decl)[typed.Name]
	case *ast.SelectorExpr:
		owner := r.typeOf(scope, typed.X)
		if owner == "" {
			return ""
		}
		return r.index.fields[owner][typed.Sel.Name]
	}
	return ""
}

func (r *scanResolver) callees(scope *scanScope, call *ast.CallExpr) []*scanFunc {
	var candidates []*scanFunc
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		candidates = r.index.functions[scope.file.dir+"|"+fun.Name]
	case *ast.SelectorExpr:
		if owner, ok := fun.X.(*ast.Ident); ok {
			if dir, imported := scope.file.imports[owner.Name]; imported {
				candidates = r.index.functions[dir+"|"+fun.Sel.Name]
				break
			}
		}
		candidates = r.index.methods[fun.Sel.Name]
	}
	var callees []*scanFunc
	for _, candidate := range candidates {
		if r.calls(scope, call.Fun, candidate) {
			callees = append(callees, candidate)
		}
	}
	return callees
}

// resultType is the type name of one result of a call of a function of the
// module.
func (r *scanResolver) resultType(scope *scanScope, call *ast.CallExpr, result int) string {
	for _, callee := range r.callees(scope, call) {
		if callee.decl.Type.Results == nil {
			continue
		}
		position := 0
		for _, field := range callee.decl.Type.Results.List {
			count := max(len(field.Names), 1)
			if result < position+count {
				return qualifiedType(callee.file, field.Type)
			}
			position += count
		}
	}
	return ""
}

// returns is what a call to a function of the module can return as one of
// its results.
func (r *scanResolver) returns(scope *scanScope, call *ast.CallExpr, result, depth int) []string {
	var values []string
	matched := 0
	for _, callee := range r.callees(scope, call) {
		if callee.decl.Body == nil || callee.decl.Type.Results == nil {
			continue
		}
		results := callee.decl.Type.Results.NumFields()
		if result >= results {
			continue
		}
		matched++
		calleeScope := &scanScope{file: callee.file, function: callee}
		if !call.Ellipsis.IsValid() {
			calleeScope.args = map[int]scanArg{}
			for position, arg := range call.Args {
				calleeScope.args[position] = scanArg{scope: scope, expr: arg}
			}
		}
		ast.Inspect(callee.decl.Body, func(node ast.Node) bool {
			if _, closure := node.(*ast.FuncLit); closure {
				return false
			}
			ret, ok := node.(*ast.ReturnStmt)
			if !ok {
				return true
			}
			switch {
			case len(ret.Results) == results:
				values = append(values, r.values(calleeScope, ret.Results[result], depth+1)...)
			case len(ret.Results) == 0:
				r.leaf(calleeScope, callee.decl.Name)
			default:
				r.leaf(calleeScope, ret.Results[0])
			}
			return true
		})
	}
	if matched == 0 {
		return r.leaf(scope, call)
	}
	return values
}

// closedLabelSetUnresolved is every expression a value comes into a closed
// set from that the scan cannot read, by set, file, function and expression,
// with how many times it is reached. A new one, or one reached more often,
// fails; one that goes away fails until its count is lowered.
//
// PENDING: these are listed as found and not yet classified. Each is still
// to be made readable by the scan, bounded by a test that enumerates what
// its producer can return, or shown to be outside any closed set.
var closedLabelSetUnresolved = map[string]int{
	"capacity_budget|worker/gap_reservation.go|retainTargetBytes|err.budget":                                                                                                            1,
	"capacity_budget|worker/sideeffect_reservation.go|checkEffectCounts|check.kind":                                                                                                     25,
	"completion_cause|cmd/alarmd/runtime_phase_two_production.go|Execute|result.CompletionCause.Cause":                                                                                  1,
	"completion_cause|execution/model.go|deriveCompletionAttribution|candidate":                                                                                                         4,
	"fleet_anomaly_kind|cmd/alarmd/runtime_phase_two_fleet.go|fleetVerdictOf|kind":                                                                                                      2,
	"fleet_failure_category|cmd/alarmd/runtime_phase_two_fleet.go|fleetVerdictOf|category":                                                                                              1,
	"gap_scope_reason|contract/gap_reason_fold.go|FoldGapReason|reason":                                                                                                                 16,
	"gap_scope_reason|metric/phase_two.go|observe|facts.Reason":                                                                                                                         1,
	"gap_scope_reason|worker/stream_session.go|gapMutationForReasons|reasons[scope]":                                                                                                    16,
	"level_outcome_reason|contract/gap_reason_fold.go|FoldGapReason|reason":                                                                                                             32,
	"level_outcome_reason|evaluation/evaluator.go|buildMutation|guarded":                                                                                                                18,
	"level_outcome_reason|evaluation/evaluator.go|buildMutation|reason":                                                                                                                 18,
	"level_outcome_reason|evaluation/evaluator.go|evaluateRecordWith|current.HistoryCompleteness == execution.HistoryGapped || current.HistoryCompleteness == execution.HistoryWarming": 1,
	"level_outcome_reason|evaluation/evaluator.go|evaluateRecordWith|f.ReasonCode":                                                                                                      1,
	"level_outcome_reason|evaluation/evaluator.go|evaluateRecordWith|guarded":                                                                                                           2,
	"level_outcome_reason|evaluation/evaluator.go|evaluateRecordWith|reason":                                                                                                            1,
	"level_outcome_reason|execution/gap_scope_reason.go|RoundGuardReasonForLevel|reason":                                                                                                8,
	"level_outcome_reason|state/execution_v2_store.go|decodeRuntime|reason":                                                                                                             3,
	"level_outcome_reason|worker/resume_applied_series.go|resumedSeriesResult|guard.ReasonCode":                                                                                         1,
	"level_outcome_reason|worker/stream_session.go|gapMutationForReasons|reasons[scope]":                                                                                                32,
	"operation|cmd/alarmd/runtime_phase_two_production.go|AcquireQueryPermit|operation":                                                                                                 41,
	"operation|scheduler/slot_source.go|classifyRecovery|source.classifyRecoveryOf(ctx, evaluationTime, deadline, nil, hold, at, fence)":                                                276,
	"output_event_kind|kafka/trigger_event_sink.go|WriteBatch|key.eventKind":                                                                                                            1,
	"output_event_kind|metric/phase_two.go|observe|key.EventKind":                                                                                                                       1,
	"output_event_kind|worker/coordinator.go|withoutMessageReported|k.kind":                                                                                                             1,
	"output_event_kind|worker/coordinator.go|writeEvents|dropped.EventKind":                                                                                                             1,
	"query_failure_code|access/uq/rejection.go|rejectionClass|strconv.Itoa(status)":                                                                                                     1,
	"query_failure_code|worker/stream_session.go|physicalFailureReason|execution.AttributeUnavailable(facts, execution.ReasonCode(contract.ReasonQueryUnavailable))":                    1,
	"query_status_code|access/uq/series_decoder.go|complete|status.Code":                                                                                                                2,
	"reason|cmd/alarmd/runtime_phase_two_production.go|Execute|result.CompletionCause.Reason":                                                                                           1,
	"reason|cmd/alarmd/runtime_phase_two_production.go|Execute|result.ReasonCode":                                                                                                       1,
	"reason|cmd/alarmd/slot_failure_reason.go|slotFailureReason|admission.QueryFailure()":                                                                                               1,
	"reason|contract/gap_reason_fold.go|FoldGapReason|reason":                                                                                                                           7840,
	"reason|evaluation/evaluator.go|buildMutation|guarded":                                                                                                                              9488,
	"reason|evaluation/evaluator.go|buildMutation|reason":                                                                                                                               9488,
	"reason|evaluation/evaluator.go|evaluateRecordWith|current.HistoryCompleteness == execution.HistoryGapped || current.HistoryCompleteness == execution.HistoryWarming":               244,
	"reason|evaluation/evaluator.go|evaluateRecordWith|f.ReasonCode":                                                                                                                    244,
	"reason|evaluation/evaluator.go|evaluateRecordWith|guarded":                                                                                                                         488,
	"reason|evaluation/evaluator.go|evaluateRecordWith|reason":                                                                                                                          244,
	"reason|execution/gap_scope_reason.go|RoundGuardReasonForLevel|reason":                                                                                                              1952,
	"reason|execution/guard_origin.go|StampGapScopeOrigins|before.ReasonOriginCause":                                                                                                    1,
	"reason|execution/model.go|deriveCompletionAttribution|candidateReason":                                                                                                             2,
	"reason|execution/unavailable_attribution.go|AttributeUnavailable|facts.Attempts[index].ReasonCode":                                                                                 161,
	"reason|scheduler/slot_source.go|repairedLanding|repair.ReasonCode()":                                                                                                               2,
	"reason|state/execution_v2_no_data.go|applyOneNoData|reason":                                                                                                                        2,
	"reason|state/execution_v2_store.go|decodeRuntime|reason":                                                                                                                           1214,
	"reason|viewstream/client.go|reprobe|fmt.Sprintf(\"missing=%d probed=%t\", missing, probed)":                                                                                        2,
	"reason|viewstream/client.go|serveOnce|client.discovery.Leader(ctx)":                                                                                                                2,
	"reason|viewstream/client.go|serveOnce|leader.Endpoint":                                                                                                                             8,
	"reason|viewstream/client.go|session|body.Refusal.Reason":                                                                                                                           2,
	"reason|viewstream/server.go|admit|server.admission.Admit(ctx, hello.WorkerId, hello.StreamToken)":                                                                                  4,
	"reason|worker/coordinator.go|Execute|begin.ReasonCode":                                                                                                                             1,
	"reason|worker/coordinator.go|Execute|finalization.ReasonCode":                                                                                                                      1,
	"reason|worker/coordinator.go|admitPlan|result.ReasonCode":                                                                                                                          1,
	"reason|worker/coordinator.go|applyState|&StateConflictError{…}":                                                                                                                    2,
	"reason|worker/coordinator.go|commitProgress|progress.ReasonCode":                                                                                                                   1,
	"reason|worker/coordinator.go|finalizePreparedWithGaps|planResult.ReasonCode":                                                                                                       1,
	"reason|worker/coordinator.go|finalizePreparedWithGaps|reason":                                                                                                                      22,
	"reason|worker/coordinator.go|firstStateAdmissionFailureReason|item.ReasonCode":                                                                                                     1,
	"reason|worker/coordinator.go|firstStateApplyFailureReason|item.ReasonCode":                                                                                                         1,
	"reason|worker/coordinator.go|outputRejectionReason|rejection.OutputRejectionReason()":                                                                                              23,
	"reason|worker/no_data_skip_streak.go|noteNoDataStreaks|stream.noDataOutcomes[index]":                                                                                               1,
	"reason|worker/resume_applied_series.go|resumedSeriesResult|guard.ReasonCode":                                                                                                       524,
	"reason|worker/stream_session.go|gapMutationForReasons|reasons[scope]":                                                                                                              7840,
	"reason|worker/stream_session.go|observeCompletionOnlyProbe|binding.ReasonCode":                                                                                                     40,
	"reason|worker/stream_session.go|physicalFailureReason|execution.AttributeUnavailable(facts, execution.ReasonCode(contract.ReasonQueryUnavailable))":                                4,
	"reason|worker/supplement.go|supplementAdmitted|result.ReasonCode":                                                                                                                  1,
	"result|cmd/alarmd/runtime_phase_two_production.go|Execute|result.Result":                                                                                                           1,
	"state_admission_reason|worker/coordinator.go|firstStateAdmissionFailureReason|item.ReasonCode":                                                                                     1,
	"state_preflight_reason|state/execution_v2_store.go|decodeRuntime|reason":                                                                                                           2,
	"wire_format|controlplane/object_catalog_read.go|AssembleQueryGroup|context.WireFormat":                                                                                             2,
	"wire_format|kafka/output_partial.go|partialRejection|formats[index]":                                                                                                               1,
	"wire_format|kafka/trigger_event_sink.go|WriteBatch|key.format":                                                                                                                     1,
	"wire_format|metric/phase_two.go|observe|format":                                                                                                                                    1,
	"wire_format|metric/phase_two.go|observe|key.Format":                                                                                                                                1,
	"wire_format|worker/coordinator.go|withoutMessageReported|k.format":                                                                                                                 1,
	"wire_format|worker/coordinator.go|writeEvents|dropped.Format":                                                                                                                      1,
}
