// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

// The product vocabulary: the words a reader of the page sees, decided here
// and sent with every response, so the page carries no table of its own.
//
// Two closed lists, fourteen words between them. A row's standing is one
// word from each: what state the strategy is in, and who does what about it.
// Every check the fleet can file a row under maps to exactly one pair (the
// table below, held closed by a test that walks checkOrder), and a handful
// of rules refine the pair from facts on the row -- which minutes a short
// window lacks and whose they are, whether a guard is still moving, whether
// the object has been heard from at all. Everything finer than these words
// -- the check, the reason code, the verdict -- rides beside them as the
// coordinate a developer reads, never in the sentence.
//
// A code word with no pair folds to DEFECT / SERVICE_FIX and nowhere else.
// The one direction a mis-fold must never take is towards "nothing to do":
// an error that reads as "no need to look" is never found.

// StateWord is what state the strategy is in.
type StateWord string

const (
	StateDetecting            StateWord = "DETECTING"
	StateResultUntrusted      StateWord = "RESULT_UNTRUSTED"
	StateNotDetecting         StateWord = "NOT_DETECTING"
	StateDataAbsent           StateWord = "DATA_ABSENT"
	StateStrategyInvalid      StateWord = "STRATEGY_INVALID"
	StateDependencyUnanswered StateWord = "DEPENDENCY_UNANSWERED"
	StateDefect               StateWord = "DEFECT"
	StateRecovered            StateWord = "RECOVERED"
)

// StateWords is the closed list, in the order the page lists them.
var StateWords = []StateWord{StateDetecting, StateResultUntrusted, StateNotDetecting, StateDataAbsent,
	StateStrategyInvalid, StateDependencyUnanswered, StateDefect, StateRecovered}

// ActionWord is who does what about it.
type ActionWord string

const (
	ActionServiceFix      ActionWord = "SERVICE_FIX"
	ActionStrategyEdit    ActionWord = "STRATEGY_EDIT"
	ActionDataCheck       ActionWord = "DATA_CHECK"
	ActionCacheWriterFill ActionWord = "CACHE_WRITER_FILL"
	ActionWatch           ActionWord = "WATCH"
	ActionNone            ActionWord = "NONE"
)

// ActionWords is the closed list, in the order the page's columns run:
// this service first, then the two owners a reader hands work to, then the
// cache writer, then what needs no hand.
var ActionWords = []ActionWord{ActionServiceFix, ActionStrategyEdit, ActionDataCheck, ActionCacheWriterFill, ActionWatch, ActionNone}

// WatchReason is why a WATCH standing is a wait and not a hand-off: each is
// decided by a rule on the row, never by the check alone. WINDOW_FILLING:
// the worst short window gained points this round or its holes are sliding
// out. GUARD_MOVING: a guard's count moved within the last StalledRounds
// rounds. NEXT_ROUND: the round that will say is the next one -- the
// configuration just changed, or the cause did not survive a restart -- for
// at most StalledRounds rounds, after which the row is this side's.
//
// A wait is an assertion about the future: one more round and this will
// clear. So every reason here is evidence that something is moving, and
// nothing here is the absence of evidence. "Nothing heard from the object
// within the window" is not a reason to wait -- an object nobody is hearing
// from is one nobody is evaluating, and the check table already files that
// as overdue or stalled, this side's to look at. Read as a wait it would be
// the state that is never looked at.
type WatchReason string

const (
	WatchWindowFilling WatchReason = "WINDOW_FILLING"
	WatchGuardMoving   WatchReason = "GUARD_MOVING"
	WatchNextRound     WatchReason = "NEXT_ROUND"
)

// WatchReasons is the closed list.
var WatchReasons = []WatchReason{WatchWindowFilling, WatchGuardMoving, WatchNextRound}

// Words is the vocabulary as sent: each word with its rendering. The page
// looks a word up here and nowhere else.
type Words struct {
	State  map[StateWord]string   `json:"state"`
	Action map[ActionWord]string  `json:"action"`
	Watch  map[WatchReason]string `json:"watch"`
}

// ProductWords is the one rendering of the vocabulary.
func ProductWords() Words {
	return Words{
		State: map[StateWord]string{
			StateDetecting: "在检测", StateResultUntrusted: "检测结果不能采信", StateNotDetecting: "没在检测",
			StateDataAbsent: "数据没到", StateStrategyInvalid: "策略定义有问题", StateDependencyUnanswered: "依赖没应答",
			StateDefect: "程序缺陷", StateRecovered: "已恢复",
		},
		Action: map[ActionWord]string{
			ActionServiceFix: "本服务处理", ActionStrategyEdit: "策略负责人改", ActionDataCheck: "数据负责人查",
			ActionCacheWriterFill: "缓存写入方补", ActionWatch: "等着看", ActionNone: "不用处理",
		},
		Watch: map[WatchReason]string{
			WatchWindowFilling: "窗口在补", WatchGuardMoving: "保护在解除", WatchNextRound: "等下一轮",
		},
	}
}

// wordPair is one check's pair.
type wordPair struct {
	State  StateWord
	Action ActionWord
}

// checkWords is the closed table: every check the fleet files a row under,
// to one pair. Held to checkOrder by a test, so a check added without a pair
// is red before it ships rather than folded at runtime.
var checkWords = map[Check]wordPair{
	CheckSourceIncomplete:      {StateNotDetecting, ActionCacheWriterFill},
	CheckSourceSetFlapping:     {StateNotDetecting, ActionCacheWriterFill},
	CheckCapabilityUnsupported: {StateNotDetecting, ActionServiceFix},
	CheckConfigRejected:        {StateNotDetecting, ActionStrategyEdit},
	CheckCutoverFailing:        {StateResultUntrusted, ActionServiceFix},
	CheckReplicaDegraded:       {StateResultUntrusted, ActionServiceFix},
	CheckOwnershipSkewed:       {StateDetecting, ActionNone},
	CheckSlotsOverdue:          {StateNotDetecting, ActionServiceFix},
	CheckNeverEvaluated:        {StateNotDetecting, ActionServiceFix},
	CheckRoundsStalled:         {StateNotDetecting, ActionServiceFix},
	CheckDetectionAbandoned:    {StateNotDetecting, ActionServiceFix},
	CheckTimelinePruned:        {StateNotDetecting, ActionServiceFix},
	CheckBookkeepingAbandoned:  {StateDetecting, ActionServiceFix},
	CheckNoDataMemoryRefused:   {StateResultUntrusted, ActionServiceFix},
	CheckDependencyDown:        {StateDependencyUnanswered, ActionServiceFix},
	CheckDefect:                {StateDefect, ActionServiceFix},
	CheckObservationGap:        {StateResultUntrusted, ActionWatch},
	CheckQueryRefused:          {StateDependencyUnanswered, ActionServiceFix},
	CheckWindowUndecided:       {StateResultUntrusted, ActionServiceFix},
	CheckConfigUnresolved:      {StateResultUntrusted, ActionWatch},
	CheckBackendNotAnswering:   {StateDependencyUnanswered, ActionServiceFix},
	CheckSeriesDataMissing:     {StateResultUntrusted, ActionServiceFix},
	CheckNoDataPersistent:      {StateDataAbsent, ActionDataCheck},
	CheckEmptyEveryRound:       {StateDataAbsent, ActionStrategyEdit},
	CheckSeriesChurning:        {StateResultUntrusted, ActionStrategyEdit},
	CheckPlanUnevaluable:       {StateStrategyInvalid, ActionStrategyEdit},
	CheckQueryTargetMissing:    {StateStrategyInvalid, ActionStrategyEdit},
}

// unpairedWords is where a code word the table does not know folds: this
// side's, to look at. Never towards nothing to do.
var unpairedWords = wordPair{StateDefect, ActionServiceFix}

// Standing is a row's two words, the coordinate they were decided from, and
// which rule decided them when it was not the check's own pair.
type Standing struct {
	State  StateWord   `json:"state"`
	Action ActionWord  `json:"action"`
	Watch  WatchReason `json:"watch,omitempty"`
	// Check is the coordinate: the check the row is under, empty for a row
	// under none.
	Check Check `json:"check,omitempty"`
	// RefinedBy names the rule that changed the check's own pair, from the
	// closed list StandingRules; empty when the pair is the check's.
	RefinedBy StandingRule `json:"refined_by,omitempty"`
}

// StandingRule names each rule that can refine a check's pair.
type StandingRule string

const (
	// RuleUnpaired: the check has no pair; folded to this side's.
	RuleUnpaired StandingRule = "UNPAIRED"
	// RuleHistoricalLoss: a retained record older than the window -- the
	// object runs, only the record is left.
	RuleHistoricalLoss StandingRule = "HISTORICAL_LOSS"
	// RuleWindowVerdict: the short windows' holes decided whose the window
	// is (coverage.windows[].verdict, every short window named).
	RuleWindowVerdict StandingRule = "WINDOW_VERDICT"
	// RuleStalled: nothing has moved for StalledRounds rounds; whose to act
	// is read from whether the Plans bound any series.
	RuleStalled StandingRule = "STALLED"
	// RuleWatch: the row is moving or unheard, and the wait has a reason.
	RuleWatch StandingRule = "WATCH"
)

// StandingRules is the closed list.
var StandingRules = []StandingRule{RuleUnpaired, RuleHistoricalLoss, RuleWindowVerdict, RuleStalled, RuleWatch}

// standingOf decides a row's words: the check's pair, then the rules in
// order, each one a closed predicate on the row. A row under no check is
// detecting with nothing to do.
func standingOf(row Anomaly) Standing {
	if row.Finding.Check == "" {
		return Standing{State: StateDetecting, Action: ActionNone}
	}
	standing := Standing{Check: row.Finding.Check}
	pair, paired := checkWords[row.Finding.Check]
	if !paired {
		pair, standing.RefinedBy = unpairedWords, RuleUnpaired
	}
	standing.State, standing.Action = pair.State, pair.Action
	// A retained record older than the window: what is left of a loss the
	// object has run past. Its object runs; the record is history.
	if row.Loss == LossHistorical {
		standing.State, standing.Action, standing.RefinedBy = StateRecovered, ActionNone, RuleHistoricalLoss
		return standing
	}
	// The undecided windows: decided by their holes when every short window
	// is named, and by whether anything is moving otherwise. Stalled is read
	// before the wait on purpose: a wait asserts that the next round will
	// move things, and a guard or window flat for StalledRounds rounds is
	// the direct evidence that it will not. Read the other way round, a
	// stuck object would read as "give it one more round" for ever -- the
	// exact state STALLED was added to name.
	if row.Finding.Check == CheckWindowUndecided || row.Finding.Check == CheckSeriesDataMissing {
		if stalled(&row) {
			standing.RefinedBy = RuleStalled
			if verdict, decided := windowVerdictWords(row); decided {
				standing.State, standing.Action, standing.RefinedBy = verdict.State, verdict.Action, RuleWindowVerdict
			} else if planBoundNoSeries(row) {
				standing.State, standing.Action = StateDataAbsent, ActionDataCheck
			} else {
				standing.State, standing.Action = StateResultUntrusted, ActionServiceFix
			}
			return standing
		}
		if reason, waiting := watchReasonOf(row); waiting {
			standing.Action, standing.Watch, standing.RefinedBy = ActionWatch, reason, RuleWatch
			return standing
		}
		if verdict, decided := windowVerdictWords(row); decided {
			standing.State, standing.Action, standing.RefinedBy = verdict.State, verdict.Action, RuleWindowVerdict
		}
		return standing
	}
	// The checks whose own pair is a wait carry the reason for it -- and a
	// wait that has outlived its bound is not a wait: a row still saying
	// the same thing after StalledRounds rounds is this side's to look at,
	// or the column would be where things go to not be seen.
	if standing.Action == ActionWatch {
		if reason, waiting := watchReasonOf(row); waiting {
			standing.Watch = reason
		} else {
			standing.Action, standing.RefinedBy = ActionServiceFix, RuleStalled
		}
	}
	return standing
}

// windowVerdictWords reads the named windows into a pair, when they decide:
// every short window named, and their verdicts agree on an owner. One
// window this side did not see whole keeps the row this side's; unusable
// records without that are the strategy's; every hole the data's when
// asked for is the data's. Unknown holes decide nothing.
func windowVerdictWords(row Anomaly) (wordPair, bool) {
	coverage := row.Coverage
	if coverage == nil || len(coverage.Windows) == 0 || uint32(len(coverage.Windows)) != coverage.Short {
		return wordPair{}, false
	}
	incomplete, unusable, unknown := 0, 0, 0
	for _, window := range coverage.Windows {
		switch window.Verdict {
		case VerdictInputIncomplete:
			incomplete++
		case VerdictPointsUnusable:
			unusable++
		case VerdictDataAbsentWhenQueried:
		default:
			unknown++
		}
	}
	switch {
	case incomplete > 0:
		return wordPair{StateResultUntrusted, ActionServiceFix}, true
	case unusable > 0:
		return wordPair{StateStrategyInvalid, ActionStrategyEdit}, true
	case unknown > 0:
		return wordPair{}, false
	default:
		return wordPair{StateDataAbsent, ActionDataCheck}, true
	}
}

// planBoundNoSeries reports whether any of the row's Plans was bound to no
// series on its latest round: a guard on such a Plan sits at zero for as
// long as the Plan has no input, and that is the data's.
func planBoundNoSeries(row Anomaly) bool {
	for _, plan := range row.PlanSeries {
		if plan.Matched == 0 {
			return true
		}
	}
	return false
}

// watchReasonOf reads whether the row is a wait, and why, from the facts
// the wait is about: a window gaining points or sliding its holes out, a
// guard whose count moved within StalledRounds, or a cause that the next
// round decides. Every reason is something moving; silence is not one.
func watchReasonOf(row Anomaly) (WatchReason, bool) {
	if coverage := row.Coverage; coverage != nil && coverage.Short > 0 {
		if (coverage.PreviousKnown && coverage.WorstValid > coverage.PreviousWorstValid) ||
			(row.WindowFill != nil && row.WindowFill.Sliding) {
			return WatchWindowFilling, true
		}
	}
	for _, guard := range row.Guards {
		if guard.Required > 0 && guard.Observed > 0 && guard.Observed < guard.Required && guard.UnchangedRounds < StalledRounds {
			return WatchGuardMoving, true
		}
	}
	// The next round decides -- for at most StalledRounds rounds. A row that
	// has said the same thing for longer than that is not waiting on a
	// round, whatever its check says; Consecutive is the row's own count of
	// rounds under its current result and reason.
	if (row.ConfigChanged || row.Finding.Check == CheckObservationGap || row.Finding.Check == CheckConfigUnresolved) &&
		row.Consecutive <= StalledRounds {
		return WatchNextRound, true
	}
	return "", false
}
