// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"sort"
	"time"
)

// A Check is one line on the page's first screen.
//
// The page used to open on the objects: a few hundred rows, each with its own
// situation and three sentences, sorted and tabbed several ways. However it
// was arranged, a reader saw a few hundred rows. Ceph's health, Alertmanager's
// grouping and Nagios's service checks all put the same thing first instead: a
// short closed list of named conditions, each with a count and one sentence,
// and the objects behind a condition one click further in. This is that list.
//
// A check is a rule over the dimensions an object carries -- what it is doing
// now, how its last round ended, how long that has held, what its windows hold
// -- not a word per combination of them. The rules are in finding.go.
type Check string

const (
	// This deployment's own. The first two are deployment-wide standings
	// rather than rules over objects: the fleet executing content that is no
	// longer the current publication, and a replica past a bound the design
	// accepts. Neither has an object row under it, and both decided the
	// verdict without a line on the first screen until a running deployment
	// spent half a day executing a stale publication behind a DEGRADED badge
	// whose one sentence named something else.
	CheckCutoverFailing      Check = "CUTOVER_FAILING"
	CheckReplicaDegraded     Check = "REPLICA_DEGRADED"
	CheckSlotsOverdue        Check = "SLOTS_OVERDUE"
	CheckNeverEvaluated      Check = "NEVER_EVALUATED"
	CheckRoundsStalled       Check = "ROUNDS_STALLED"
	CheckDetectionAbandoned  Check = "DETECTION_ABANDONED"
	CheckTimelinePruned      Check = "TIMELINE_PRUNED"
	CheckDependencyDown      Check = "DEPENDENCY_DOWN"
	CheckDefect              Check = "DEFECT"
	CheckObservationGap      Check = "OBSERVATION_GAP"
	CheckBackendNotAnswering Check = "BACKEND_NOT_ANSWERING"
	CheckQueryRefused        Check = "QUERY_REFUSED"
	// QueryTargetMissing is the refusal that names what is missing: the
	// backend read the query and answered that the table or field the
	// strategy references does not exist. That is the strategy's, and the
	// page's pool card already called these "策略本身不可用" while the line
	// under it said 待确认 -- one page, two verdicts on the same objects.
	CheckQueryTargetMissing Check = "QUERY_TARGET_MISSING"
	CheckNoDataPersistent   Check = "NO_DATA_PERSISTENT"
	CheckSeriesChurning     Check = "SERIES_CHURNING"
	CheckSeriesDataMissing  Check = "SERIES_DATA_MISSING"
	CheckWindowUndecided    Check = "WINDOW_UNDECIDED"
	CheckPlanUnevaluable    Check = "PLAN_UNEVALUABLE"
	CheckConfigUnresolved   Check = "CONFIG_UNRESOLVED"
)

// GroupBy is the key a check's objects are folded on. One backend not
// answering is one line with sixty objects under it, not sixty lines; which
// key makes the fold is a property of the check.
type GroupBy string

const (
	GroupByReplica    GroupBy = "replica"
	GroupByReasonCode GroupBy = "reason_code"
	GroupByDetail     GroupBy = "detail"
	GroupByStrategy   GroupBy = "strategy"
	GroupByGapKind    GroupBy = "gap_kind"
	// GroupByCause folds on what the window counts say happened: the reason
	// the detection could not use the record, or that the series are a mix of
	// new and old. It is the fold for the one check whose objects share a
	// symptom and not yet an owner.
	GroupByCause GroupBy = "cause"
	// GroupByDegradation folds on the kind of replica-level standing, the
	// closed DegradationKinds; the replicas in it are named on the group.
	GroupByDegradation GroupBy = "degradation"
)

// checkAnswers is the closed table: who acts on each check and what its
// objects fold on. Nineteen rows, and a test holds the count there. A check
// whose owner is UNDETERMINED is one whose grouping key does not reach an
// external entity yet -- it stays on this deployment's side of the page until
// it does, rather than being handed to whichever owner is likeliest.
var checkAnswers = map[Check]struct {
	Owner   Owner
	GroupBy GroupBy
}{
	CheckCutoverFailing:     {OwnerAlarmd, GroupByReasonCode},
	CheckReplicaDegraded:    {OwnerAlarmd, GroupByDegradation},
	CheckSlotsOverdue:       {OwnerAlarmd, GroupByReplica},
	CheckNeverEvaluated:     {OwnerAlarmd, GroupByReplica},
	CheckRoundsStalled:      {OwnerAlarmd, GroupByReplica},
	CheckDetectionAbandoned: {OwnerAlarmd, GroupByReplica},
	CheckTimelinePruned:     {OwnerAlarmd, GroupByReplica},
	CheckDependencyDown:     {OwnerAlarmd, GroupByReasonCode},
	CheckDefect:             {OwnerAlarmd, GroupByReasonCode},
	CheckObservationGap:     {OwnerAlarmd, GroupByGapKind},

	CheckBackendNotAnswering: {OwnerData, GroupByDetail},
	CheckNoDataPersistent:    {OwnerData, GroupByStrategy},
	CheckSeriesDataMissing:   {OwnerData, GroupByStrategy},

	CheckSeriesChurning:     {OwnerStrategy, GroupByStrategy},
	CheckPlanUnevaluable:    {OwnerStrategy, GroupByStrategy},
	CheckQueryTargetMissing: {OwnerStrategy, GroupByDetail},

	CheckQueryRefused:     {OwnerUndetermined, GroupByDetail},
	CheckWindowUndecided:  {OwnerUndetermined, GroupByCause},
	CheckConfigUnresolved: {OwnerUndetermined, GroupByStrategy},
}

// checkOrder is the order the first screen lists the checks in, and the order
// a reader acts in: this deployment's own first, worst first -- work not being
// done at all, then work lost, then infrastructure, then defects, then what
// cannot be spoken for -- then what nobody can hand to anyone yet, then what
// is confirmed as somebody else's. It is the table's severity, stated as an
// order rather than as a fifth field beside each row. A test holds it to the
// same keys as checkAnswers.
var checkOrder = []Check{
	CheckCutoverFailing,
	CheckReplicaDegraded,
	CheckSlotsOverdue,
	CheckNeverEvaluated,
	CheckRoundsStalled,
	CheckDetectionAbandoned,
	CheckTimelinePruned,
	CheckDependencyDown,
	CheckDefect,
	CheckObservationGap,
	CheckQueryRefused,
	CheckWindowUndecided,
	CheckConfigUnresolved,
	CheckBackendNotAnswering,
	CheckNoDataPersistent,
	CheckSeriesDataMissing,
	CheckSeriesChurning,
	CheckPlanUnevaluable,
	CheckQueryTargetMissing,
}

// Checks lists every check the table answers, in the order the page lists
// them, for the tests that walk it and for the page's completeness check.
func Checks() []Check {
	list := make([]Check, len(checkOrder))
	copy(list, checkOrder)
	return list
}

func checkRank(check Check) int {
	for index, candidate := range checkOrder {
		if candidate == check {
			return index
		}
	}
	return len(checkOrder)
}

// ChecksWithoutAProducer names the checks nothing decides yet. They are in the
// table so the page has words for them the day they arrive, and named here so
// that a check with no writer cannot read as a mechanism that is wired: the
// due index will produce NEVER_EVALUATED once it knows when each object was
// taken over, and a test holds this list to exactly that one.
var ChecksWithoutAProducer = []Check{CheckNeverEvaluated}

// decidingCode is the code the check was decided on, in the order checkOf
// reads them. It is the grouping key for the checks that fold on a code.
func decidingCode(anomaly Anomaly) string {
	failureCode := ""
	if anomaly.Failure != nil {
		failureCode = anomaly.Failure.Code
	}
	for _, code := range []string{anomaly.CauseReason, string(anomaly.Cause), failureCode, anomaly.ReasonCode} {
		if code != "" {
			return code
		}
	}
	return ""
}

// The key an object falls under within its check. Objects with no key of the
// kind the check folds on share one group named for the absence, so they are
// counted rather than dropped.
const (
	groupNoStrategy = "(未观察到策略)"
	groupNoDetail   = "(没有症状记录)"
)

func groupKeyOf(anomaly Anomaly, check Check) string {
	switch checkAnswers[check].GroupBy {
	case GroupByReplica:
		return anomaly.Replica
	case GroupByReasonCode:
		return decidingCode(anomaly)
	case GroupByDetail:
		if anomaly.Failure != nil && anomaly.Failure.Detail != "" {
			return anomaly.Failure.Detail
		}
		if code := decidingCode(anomaly); code != "" {
			return code
		}
		return groupNoDetail
	case GroupByStrategy:
		key := ""
		for _, strategy := range anomaly.Strategies {
			if key == "" || strategy.StrategyID < key {
				key = strategy.StrategyID
			}
		}
		if key == "" {
			return groupNoStrategy
		}
		return key
	case GroupByGapKind:
		// The only object-borne member of the observation gap is the object
		// restored without its cause; the view's own gaps are added by kind
		// in ReportChecks.
		return gapRestoredWithoutCause
	case GroupByCause:
		return windowCause(anomaly.Coverage, anomaly.CauseReason)
	}
	return ""
}

// The folds of a window that will not fill and whose owner the counts do not
// decide. A starved window is a record that arrived and could not be used --
// the reason the detection gave is the fold, because REQUIRED_VALUE_MISSING
// and an algorithm's refusal are different conversations -- and a window
// short over a mix of new and old series is its own.
const (
	causeSeriesMixed    = "新老序列混合"
	causeUnusableNoWord = "检测用不了记录（原因没带上）"
	causeNoCounts       = "没有窗口计数（副本没报）"
	// A window held by a durable guard: the reason on the row is the one
	// the guard was established with, carried onto every round until the
	// guard releases, not what happened this round. Folded on that trigger,
	// with whether the live window is still short or already full -- the
	// second is a guard that should have released and has not.
	causeGuardHeld           = "保护未解除"
	causeGuardHeldWindowFull = "保护未解除且窗口已满"
)

func windowCause(coverage *HistoryCoverage, reason string) string {
	switch {
	case coverage == nil || coverage.Levels == 0:
		return causeNoCounts
	case coverage.Guarded > 0 && reason != "" && reason != "HISTORY_WARMING" && reason != "HISTORY_GAPPED":
		if coverage.Short == 0 {
			return causeGuardHeldWindowFull + "（最初触发 " + reason + "）"
		}
		return causeGuardHeld + "（最初触发 " + reason + "）"
	case coverage.Starved():
		if coverage.UnusableReason != "" {
			return coverage.UnusableReason
		}
		return causeUnusableNoWord
	default:
		return causeSeriesMixed
	}
}

// Result is how the last round ended. COMPLETED is a round that ran to its
// end and produced its outcome -- the reason code beside it says what that
// outcome was, and the window numbers say whether it could decide recovery.
// ERROR is a round that failed. REFUSED is a backend that read the query and
// would not run it, which is neither the backend being down nor the data
// being absent. NO_DATA -- a query that answered with no points -- arrives
// when the bit that separates it from a detection error crosses from the
// evaluator; until then those rounds read as COMPLETED with their reason.
type Result string

const (
	ResultCompleted Result = "COMPLETED"
	ResultError     Result = "ERROR"
	ResultRefused   Result = "REFUSED"
	ResultNoData    Result = "NO_DATA"
)

// Results lists every result value, for the page's completeness check.
var Results = []Result{ResultCompleted, ResultError, ResultRefused, ResultNoData}

// resultOf reads the last round off the anomaly. Empty when there was no
// round to speak of: an object whose wake time passed has no last result.
func resultOf(anomaly Anomaly) Result {
	switch {
	case anomaly.Kind == KindOverdueWake, anomaly.Kind == KindSkippedSpan:
		return ""
	case anomaly.Kind == KindNoData:
		return ResultNoData
	case queryRejected(anomaly.Failure):
		return ResultRefused
	case anomaly.Failure != nil, anomaly.Kind == KindBlockedRun, failedExecution(anomaly.ReasonCode):
		return ResultError
	}
	return ResultCompleted
}

// CheckReport is one line of the first screen: the check, who acts on it, how
// much it covers, and the groups its objects fold into.
type CheckReport struct {
	Code  Check `json:"code"`
	Owner Owner `json:"owner"`
	// GroupBy names what the groups below are folded on, so the page can say
	// "by strategy" without knowing the table.
	GroupBy GroupBy `json:"group_by"`
	// Objects, Strategies and Businesses are over every object under this
	// check, across every column. Strategies and Businesses are distinct
	// counts, not sums over groups: a strategy in two groups is one strategy.
	Objects    int `json:"objects"`
	Strategies int `json:"strategies"`
	Businesses int `json:"businesses"`
	// Demoted is how many of Objects sit in the demoted pool: this deployment
	// has already stopped re-querying them. The pool card says so of the
	// pool; the line has to say so of its own objects, or the two read as
	// different verdicts on the same strategies.
	Demoted int `json:"demoted,omitempty"`
	// Current and Retained split Objects into what is wrong now and what was
	// lost in the past and is kept on record. A line that added the two read
	// as 393 objects to act on when 10 were anomalous and 383 were records of
	// Slots skipped hours ago; the reader could not tell which without
	// opening every group. RetainedLastHour and RetainedNewest are the part
	// of the record a reader can still do something about: what was just
	// lost, and when.
	Current          int        `json:"current"`
	Retained         int        `json:"retained,omitempty"`
	RetainedLastHour int        `json:"retained_last_hour,omitempty"`
	RetainedNewest   *time.Time `json:"retained_newest,omitempty"`
	// Partial says at least one column this check draws from was truncated by
	// its replica, so the counts here are a sample of that column.
	Partial bool         `json:"partial,omitempty"`
	Groups  []CheckGroup `json:"groups"`
	// Activation and Replica are on CUTOVER_FAILING only: the leader's
	// standing, whole, and which replica it is. The line's sentence is built
	// from them -- which publication is running, which one is not, since
	// when -- and an object count cannot say any of that.
	Activation *ActivationFacts `json:"activation,omitempty"`
	Replica    string           `json:"replica,omitempty"`
}

// CheckGroup is one fold of a check's objects: the objects sharing one key.
// Replicas is on the groups of the two standing checks, whose folds have no
// objects and name the replicas instead.
type CheckGroup struct {
	Key        string   `json:"key"`
	Objects    int      `json:"objects"`
	Strategies int      `json:"strategies"`
	Businesses int      `json:"businesses"`
	Replicas   []string `json:"replicas,omitempty"`
}

// ReportChecks folds every object in every column into the checks it is
// under, and adds the observation gaps -- which are not objects but are the
// same line: things this deployment cannot currently speak for.
//
// Ordered as checkOrder is, so the page renders the list in the order the
// reader acts and does not sort by a rule of its own.
func ReportChecks(columns [][]Anomaly, truncated map[string]bool, view *View, now time.Time) []CheckReport {
	type tally struct {
		objects    int
		strategies map[string]struct{}
		businesses map[string]struct{}
		groups     map[string]*CheckGroup
		groupSets  map[string][2]map[string]struct{}
		partial    bool
		demoted    int
		current    int
		retained   int
		lastHour   int
		newest     time.Time
		activation *ActivationFacts
		replica    string
	}
	tallies := map[Check]*tally{}
	ensure := func(check Check) *tally {
		entry := tallies[check]
		if entry == nil {
			entry = &tally{strategies: map[string]struct{}{}, businesses: map[string]struct{}{},
				groups: map[string]*CheckGroup{}, groupSets: map[string][2]map[string]struct{}{}}
			tallies[check] = entry
		}
		return entry
	}
	add := func(entry *tally, key string, anomaly *Anomaly) {
		entry.objects++
		group := entry.groups[key]
		if group == nil {
			group = &CheckGroup{Key: key}
			entry.groups[key] = group
			entry.groupSets[key] = [2]map[string]struct{}{{}, {}}
		}
		group.Objects++
		if anomaly == nil {
			return
		}
		sets := entry.groupSets[key]
		for _, strategy := range anomaly.Strategies {
			entry.strategies[strategy.StrategyID] = struct{}{}
			sets[0][strategy.StrategyID] = struct{}{}
			if strategy.BusinessID != "" {
				entry.businesses[strategy.BusinessID] = struct{}{}
				sets[1][strategy.BusinessID] = struct{}{}
			}
		}
	}
	listed := map[string]struct{}{}
	for columnIndex, column := range columns {
		columnPartial := false
		if truncated != nil && columnIndex < len(columnNames) {
			columnPartial = truncated[columnNames[columnIndex]]
		}
		for index := range column {
			anomaly := &column[index]
			check := anomaly.Finding.Check
			if check == "" {
				continue
			}
			entry := ensure(check)
			entry.partial = entry.partial || columnPartial
			add(entry, anomaly.Finding.Group, anomaly)
			entry.current++
			if columnIndex < len(columnNames) && columnNames[columnIndex] == ColumnDemoted {
				entry.demoted++
			}
			listed[underKey(check, anomaly.QueryGroup)] = struct{}{}
		}
	}
	// What this deployment gave up on and never evaluated, retained past the
	// rounds that followed. An object that skipped Slots an hour ago and has
	// run normally since is under no column, and it stays on this line until a
	// restart forgets it: the loss is permanent and the row is the only record.
	// And the objects whose data stopped: under no column either, their rounds
	// complete, and on the data side's line.
	if view != nil {
		for _, row := range skippedRows(view, listed) {
			entry := ensure(row.Finding.Check)
			add(entry, row.Finding.Group, &row)
			entry.retained++
			if row.Skip != nil {
				if now.Sub(row.Skip.At) <= time.Hour {
					entry.lastHour++
				}
				if row.Skip.At.After(entry.newest) {
					entry.newest = row.Skip.At
				}
			}
		}
		for index := range view.NoData {
			row := &view.NoData[index]
			if row.Finding.Check == "" {
				continue
			}
			add(ensure(row.Finding.Check), row.Finding.Group, row)
			ensure(row.Finding.Check).current++
		}
	}
	// What the view cannot speak for. Unknown is the objects a replica holds
	// and has said nothing conclusive about; the gaps are the reasons the rest
	// of the answer may be incomplete. Neither has objects the list can show,
	// so they are groups with a count and no rows behind them.
	if view != nil {
		if view.Unknown > 0 {
			entry := ensure(CheckObservationGap)
			add(entry, string(GapUndetermined), nil)
			entry.objects += view.Unknown - 1
			entry.current += view.Unknown
			entry.groups[string(GapUndetermined)].Objects += view.Unknown - 1
		}
		for _, gap := range view.Gaps {
			if gap.Kind == GapUndetermined {
				// Counted above, by object, rather than once per replica here.
				continue
			}
			entry := ensure(CheckObservationGap)
			if entry.groups[string(gap.Kind)] == nil {
				entry.groups[string(gap.Kind)] = &CheckGroup{Key: string(gap.Kind)}
			}
		}
		// The two standings. Not objects either: the fleet executing a stale
		// publication is one fact about the whole deployment, folded on why
		// the activation fails; a replica past a bound is one fact per
		// replica, folded on which bound.
		if view.Activation != nil && view.Activation.BehindBeyondBound {
			entry := ensure(CheckCutoverFailing)
			entry.activation, entry.replica = view.Activation, view.ActivationReplica
			key := view.Activation.Reason()
			entry.groups[key] = &CheckGroup{Key: key, Replicas: []string{view.ActivationReplica}}
		}
		for _, degradation := range view.Degradations {
			if degradation.Kind == DegradationActivationBehind {
				// Its own line, above.
				continue
			}
			entry := ensure(CheckReplicaDegraded)
			group := entry.groups[string(degradation.Kind)]
			if group == nil {
				group = &CheckGroup{Key: string(degradation.Kind)}
				entry.groups[string(degradation.Kind)] = group
			}
			group.Replicas = append(group.Replicas, degradation.Replica)
		}
	}
	reports := make([]CheckReport, 0, len(tallies))
	for check, entry := range tallies {
		report := CheckReport{Code: check, Owner: checkAnswers[check].Owner, GroupBy: checkAnswers[check].GroupBy,
			Objects: entry.objects, Strategies: len(entry.strategies), Businesses: len(entry.businesses),
			Partial: entry.partial, Demoted: entry.demoted, Activation: entry.activation, Replica: entry.replica,
			Current: entry.current, Retained: entry.retained, RetainedLastHour: entry.lastHour}
		if !entry.newest.IsZero() {
			newest := entry.newest
			report.RetainedNewest = &newest
		}
		for key, group := range entry.groups {
			if sets, known := entry.groupSets[key]; known {
				group.Strategies, group.Businesses = len(sets[0]), len(sets[1])
			}
			report.Groups = append(report.Groups, *group)
		}
		sort.Slice(report.Groups, func(i, j int) bool {
			if report.Groups[i].Objects != report.Groups[j].Objects {
				return report.Groups[i].Objects > report.Groups[j].Objects
			}
			return report.Groups[i].Key < report.Groups[j].Key
		})
		reports = append(reports, report)
	}
	sort.Slice(reports, func(i, j int) bool {
		return checkRank(reports[i].Code) < checkRank(reports[j].Code)
	})
	return reports
}

// Todo is the first screen's arithmetic, done once here rather than by the
// page adding lines up. The page summed every check's object count and said
// "需要处理 768 个对象": records of past losses counted in, and an object under
// two lines counted twice. What a reader needs is how many lines are theirs,
// how many distinct objects those lines cover now, and -- apart from that --
// how much was lost in the past and how much of it just now.
type Todo struct {
	// Checks is the lines this reader acts on that have something on them
	// now: an object, or a standing of the deployment itself.
	Checks int `json:"checks"`
	// Objects is the distinct objects under those lines, now. An object
	// under two lines is one object.
	Objects int `json:"objects"`
	// Retained is the distinct objects with a record of past loss, kept
	// until somebody looks; RetainedLastHour is how many of those records
	// were made in the last hour and RetainedNewest when the newest was.
	Retained         int        `json:"retained"`
	RetainedLastHour int        `json:"retained_last_hour"`
	RetainedNewest   *time.Time `json:"retained_newest,omitempty"`
	// Governance is the lines already confirmed as somebody else's, and the
	// distinct objects under them.
	Governance        int `json:"governance"`
	GovernanceObjects int `json:"governance_objects"`
}

// SummarizeTodo counts the first screen. The reports say which lines exist;
// the columns say which objects are under them, so the distinct count is
// taken from the objects and not from the lines.
func SummarizeTodo(reports []CheckReport, columns [][]Anomaly, view *View, now time.Time) Todo {
	todo := Todo{}
	ours := map[string]struct{}{}
	theirs := map[string]struct{}{}
	count := func(list []Anomaly) {
		for _, anomaly := range list {
			if anomaly.Finding.Check == "" {
				continue
			}
			if checkAnswers[anomaly.Finding.Check].Owner.actionRequired() {
				ours[anomaly.QueryGroup] = struct{}{}
			} else {
				theirs[anomaly.QueryGroup] = struct{}{}
			}
		}
	}
	for _, column := range columns {
		count(column)
	}
	if view != nil {
		count(view.NoData)
	}
	todo.Objects, todo.GovernanceObjects = len(ours), len(theirs)
	if view != nil {
		// The objects a replica holds and has said nothing conclusive about
		// are under OBSERVATION_GAP and have no row to be distinct by; they
		// are in no column, so adding the count cannot double-count.
		todo.Objects += view.Unknown
	}
	for _, report := range reports {
		switch {
		case !report.ActionRequired():
			todo.Governance++
		case report.Current > 0 || report.Activation != nil || report.Code == CheckReplicaDegraded:
			todo.Checks++
		}
	}
	if view != nil {
		var newest time.Time
		note := func(at time.Time) {
			todo.Retained++
			if now.Sub(at) <= time.Hour {
				todo.RetainedLastHour++
			}
			if at.After(newest) {
				newest = at
			}
		}
		for _, skip := range view.GapSkips {
			note(skip.At)
		}
		for _, skip := range view.PrunedSkips {
			note(skip.At)
		}
		if !newest.IsZero() {
			todo.RetainedNewest = &newest
		}
	}
	return todo
}

func (owner Owner) actionRequired() bool {
	return owner == OwnerAlarmd || owner == OwnerUndetermined
}

// columnNames is the order the handler passes the columns in, so a truncated
// flag can be looked up by position.
var columnNames = []string{ColumnAnomalies, ColumnDemoted, ColumnUndecidable, ColumnByDesign}

// ActionRequired reports whether the check is this reader's to act on: this
// deployment's own, or one nobody can yet hand to anyone.
func (report CheckReport) ActionRequired() bool {
	return report.Owner.actionRequired()
}

func knownCheck(name string) bool {
	_, known := checkAnswers[Check(name)]
	return known
}

func checkNames() []string {
	checks := Checks()
	names := make([]string, len(checks))
	for index, check := range checks {
		names[index] = string(check)
	}
	return names
}

// UnderCheck is every object in every column that is under one check, and
// within one of its groups when group is not empty, plus the retained skip
// records under it. This is the list a line on the first screen opens; its
// total is how many it holds.
func UnderCheck(check Check, group string, view *View) []Anomaly {
	list := []Anomaly{}
	listed := map[string]struct{}{}
	for _, column := range [][]Anomaly{view.Anomalies, view.Demoted, view.Undecidable, view.ByDesign, view.NoData} {
		for _, anomaly := range column {
			if anomaly.Finding.Check != check {
				continue
			}
			// Marked as listed before the group narrows, so an object in
			// another group is not re-listed from its retained skip.
			listed[underKey(check, anomaly.QueryGroup)] = struct{}{}
			if group != "" && anomaly.Finding.Group != group {
				continue
			}
			list = append(list, anomaly)
		}
	}
	for _, row := range skippedRows(view, listed) {
		if row.Finding.Check != check || (group != "" && row.Finding.Group != group) {
			continue
		}
		list = append(list, row)
	}
	// Oldest first: on one line every owner is the same, so age is the only
	// order left, and the oldest is the one to look at. The two lines that
	// keep records of past loss are the exception: the record grows, and
	// what a reader can act on is the newest entry -- who was just lost and
	// which span -- not the oldest.
	if check == CheckDetectionAbandoned || check == CheckTimelinePruned {
		SortAnomaliesNewestFirst(list)
	} else {
		sortOldestFirst(list)
	}
	return list
}

// underKey names one object under one check, so a retained skip does not add
// a second row for an object a column already lists under that check while an
// object listed under some other check still gets its skip row: those are two
// facts, and Ceph lists an OSD under every check it fails.
func underKey(check Check, queryGroup string) string {
	return string(check) + "|" + queryGroup
}

// skippedRows turns the view's retained skip records into rows, one per
// object not already listed under the same check. A pruned span is
// TIMELINE_PRUNED and a replay-window skip is DETECTION_ABANDONED; both are
// this deployment's and fold on the replica that applied them.
func skippedRows(view *View, listed map[string]struct{}) []Anomaly {
	rows := []Anomaly{}
	row := func(queryGroup string, check Check, reason string, skip SkippedSpan) {
		if _, already := listed[underKey(check, queryGroup)]; already {
			return
		}
		listed[underKey(check, queryGroup)] = struct{}{}
		item := Anomaly{QueryGroup: queryGroup, Kind: KindSkippedSpan, ReasonCode: reason,
			Since: skip.At, SinceFrom: SinceSnapshotContinuity, Replica: skip.Replica, Skip: &skip,
			Strategies: skip.Strategies}
		item.Finding = Finding{Check: check, Group: skip.Replica, Owner: checkAnswers[check].Owner}
		item.Attribution = attributionOf(item)
		rows = append(rows, item)
	}
	for queryGroup, skip := range view.GapSkips {
		row(queryGroup, CheckDetectionAbandoned, "GAP_SKIPPED", skip)
	}
	for queryGroup, pruned := range view.PrunedSkips {
		row(queryGroup, CheckTimelinePruned, "SCHEDULE_PRUNED",
			SkippedSpan{FirstSlot: pruned.From, LastSlot: pruned.To, At: pruned.At, Replica: pruned.Replica,
				Strategies: pruned.Strategies})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].QueryGroup < rows[j].QueryGroup })
	return rows
}
