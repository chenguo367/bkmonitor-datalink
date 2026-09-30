// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package lookback

import (
	"sort"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// What a completed sample's rungs found against its first read, closed, by
// the facts of the series and never by a share of them. A sample is classed
// by what its data settled to: the read of its last change, which a later
// rung read again the same. A change that came back, or a series that came
// and went, is not one.
//
//   - window_read_early: the first read was empty and data came later, or a
//     series the first read had came back with other points or values, or
//     not at all. The whole window was read before its data was complete;
//     the strategy's time_delay is what moves the read, and a value revised
//     after it was judged is evidence for that and is not judged again.
//   - partial_revised: some series the first read had came back with other
//     points or values and a value there, and others with a value came back
//     as they were -- a series quiet in both is neither: the
//     first read had those whole. Not the window read early -- the Query
//     Group's read is not what was late -- and not series_late -- the series
//     were there, so a supplement of new series does not reach them. Counted
//     apart from the first version on; the time_delay run treats it as it
//     treats window_read_early, as it did before the two were told apart.
//   - series_late: every series the first read had came back as it was, and
//     series it did not have came later: some series are late, which is the
//     supplementary detection's to fill.
//   - complete: what the data settled to is what the first read had.
//   - unclassified: which of these it is is not known, counted by why
//     (UnclassifiedReasons) and not a fault. memory_refused: a rung differed
//     and its series could not be compared, one side's table having been
//     refused by the memory line, and no other rung's series said which it
//     was. unsettled: the deepest rung still changed, so no later read says
//     the data had settled; a value still moving then is late data or a
//     source that revises, and one read cannot tell them apart.
const (
	ClassComplete        = "complete"
	ClassWindowReadEarly = "window_read_early"
	ClassPartialRevised  = "partial_revised"
	ClassSeriesLate      = "series_late"
	ClassUnclassified    = "unclassified"
)

// SampleClasses is every class.
var SampleClasses = []string{ClassComplete, ClassWindowReadEarly, ClassPartialRevised, ClassSeriesLate, ClassUnclassified}

// Why a sample is unclassified: the process's memory line refused a series
// table it needed, or the deepest rung still changed.
const (
	UnclassifiedMemoryRefused = "memory_refused"
	UnclassifiedUnsettled     = "unsettled"
)

// UnclassifiedReasons is every reason.
var UnclassifiedReasons = []string{UnclassifiedMemoryRefused, UnclassifiedUnsettled}

const (
	// readEarlyRepeat is how many completed samples of a Query Group in a
	// row must find its window read early before it is reported: once may
	// be an upstream hiccup, twice in a row is the configuration.
	readEarlyRepeat = 2
	// readEarlyKept is how many of those samples a report carries, and
	// maxEvidenceBuckets how many changed buckets each names.
	readEarlyKept      = 3
	maxEvidenceBuckets = 8
	// seriesLateCleanToEnd is how many complete samples in a row end a
	// series_late Query Group's directed reads: once may be a quiet hour,
	// twice in a row is its series arriving with the rest again.
	seriesLateCleanToEnd = 2
)

// ReadEarlySample is one sample that found its window read early: when its
// first read was, how long after the window's end it read and when the data
// was complete, and the rung at which a series of the first read was first
// seen changed, with the buckets that changed there.
type ReadEarlySample struct {
	EvaluationTime       execution.EvaluationTime `json:"evaluation_time"`
	FirstReadAgeSeconds  int64                    `json:"first_read_age_seconds"`
	FirstReadyAgeSeconds int64                    `json:"first_ready_age_seconds"`
	CompletionAgeSeconds int64                    `json:"completion_age_seconds"`
	Rung                 string                   `json:"rung"`
	ChangedAgeSeconds    int64                    `json:"changed_age_seconds"`
	Buckets              []int64                  `json:"buckets,omitempty"`
	// PartialRevised is a sample some of whose series the first read had
	// whole (ClassPartialRevised), in a run otherwise read early.
	PartialRevised  bool  `json:"partial_revised,omitempty"`
	ReadHoldSeconds int64 `json:"read_hold_seconds,omitempty"`
}

// ReadEarlyReading is a Query Group whose window was read early in
// readEarlyRepeat completed samples in a row: the time_delay its query runs
// under, the one that would have read its samples complete - the most any
// of them completed past its first read's age, added and aligned up to the
// step, as a strategy's time_delay is aligned when it is compiled - and the
// samples it is read from, newest last. A completion is the age of the
// recheck that first read the data whole, so the suggestion is an upper
// bound: the data came between that recheck and the one before it.
type ReadEarlyReading struct {
	QueryGroup            execution.QueryGroupIdentity `json:"query_group"`
	Source                string                       `json:"source"`
	StepSeconds           int64                        `json:"step_seconds"`
	CurrentDelaySeconds   int64                        `json:"current_time_delay_seconds"`
	SuggestedDelaySeconds int64                        `json:"suggested_time_delay_seconds"`
	Since                 time.Time                    `json:"since"`
	Samples               []ReadEarlySample            `json:"samples"`
}

// readEarlyState is a Query Group's run of window_read_early samples.
type readEarlyState struct {
	consecutive  int
	since        time.Time
	delaySeconds int64
	samples      []ReadEarlySample
}

// seriesLateState is a Query Group some of whose series were seen late: the
// rung they were seen at, which a directed recheck of every Slot reads at.
type seriesLateState struct {
	rung  int
	since time.Time
	seen  uint64
	clean int
}

// classOf is a completed sample's class, and for an unclassified one why.
func classOf(candidate *sample) (string, string) {
	changed := candidate.lastChange >= 0
	switch {
	case changed && candidate.lastChange >= candidate.planned-1:
		// Its last rung read changed and none after it read it again: the
		// deepest rung, as a sample still changing at its last planned rung
		// reads one further while there is one.
		return ClassUnclassified, UnclassifiedUnsettled
	case candidate.emptyFirstRead && changed && len(candidate.last) > 0:
		// candidate.last is the read the data settled to: data that came
		// to an empty first read and went again was not read early.
		return ClassWindowReadEarly, ""
	case candidate.existingChanged && candidate.existingSteady > 0 && candidate.existingArrived > 0:
		// Some series came with other points or values and others with a
		// value the first read had whole: the series were late, not the
		// window. A series that went is not data that came late, and beside
		// series that stood it is read as before.
		return ClassPartialRevised, ""
	case candidate.existingChanged:
		// Every series with a value there changed: the whole window was
		// read before its data was complete, however many series were
		// quiet in it.
		return ClassWindowReadEarly, ""
	case candidate.seriesAdded:
		return ClassSeriesLate, ""
	case candidate.seriesUnknown:
		return ClassUnclassified, UnclassifiedMemoryRefused
	default:
		// No rung changed, or every one that did was compared: a series
		// that changes changes its buckets, so a sample whose buckets stood
		// is complete whatever its series tables were.
		return ClassComplete, ""
	}
}

// noteClassLocked records a completed sample's class on its Query Group.
func (engine *Engine) noteClassLocked(state *group, candidate *sample, now time.Time) {
	class, reason := classOf(candidate)
	engine.counts.classes[key2(candidate.source, class)]++
	state.reading.classes[wordIndex(SampleClasses, class)]++
	engine.recordReadHoldLocked(candidate)
	if class == ClassPartialRevised {
		engine.recordIgnoredLocked(candidate, ClassPartialRevised)
		state.reading.ignored[wordIndex(ReadHoldIgnoredReasons, ClassPartialRevised)]++
	} else if class == ClassWindowReadEarly && !wholeWindowArrival(candidate) {
		engine.recordIgnoredLocked(candidate, "noise")
		state.reading.ignored[wordIndex(ReadHoldIgnoredReasons, "noise")]++
	}
	switch class {
	case ClassWindowReadEarly, ClassPartialRevised:
		early := ReadEarlySample{EvaluationTime: candidate.evaluation,
			ReadHoldSeconds:      candidate.contract.ReadHoldMillis / 1000,
			FirstReadAgeSeconds:  int64(candidate.readAt.Sub(candidate.windowEnd) / time.Second),
			FirstReadyAgeSeconds: int64(firstReadAge(candidate) / time.Second),
			CompletionAgeSeconds: int64(candidate.completion / time.Second), PartialRevised: class == ClassPartialRevised}
		if candidate.early != nil {
			early.Rung, early.ChangedAgeSeconds, early.Buckets = candidate.early.Rung, candidate.early.ChangedAgeSeconds, candidate.early.Buckets
		}
		if state.readEarly == nil {
			state.readEarly = &readEarlyState{since: now}
		}
		run := state.readEarly
		run.consecutive++
		run.delaySeconds = candidate.delaySeconds
		run.samples = append(run.samples, early)
		if len(run.samples) > readEarlyKept {
			run.samples = append([]ReadEarlySample(nil), run.samples[len(run.samples)-readEarlyKept:]...)
		}
		// While its window is read early the group rests no longer than its
		// deepest rung, however long it rested before: the row it is reported
		// on goes when a sample reads the data whole again, and that sample
		// is captured one such rest after this one, not up to restCap later.
		if floor := RungSteps[state.depth-1]; state.rest > floor {
			state.rest = floor
			next := now.Add(time.Duration(floor * float64(state.step) * restSpread(candidate.queryGroup)))
			if next.Before(state.nextAt) {
				state.nextAt = next
			}
		}
	case ClassUnclassified:
		// Not known either way: it neither adds to a run nor ends one.
		engine.counts.unclassified[key2(candidate.source, reason)]++
	default:
		state.readEarly = nil
	}
	switch class {
	case ClassSeriesLate:
		if state.seriesLate == nil {
			state.seriesLate = &seriesLateState{since: now}
		}
		state.seriesLate.rung = candidate.seriesAddedRung
		state.seriesLate.seen++
		state.seriesLate.clean = 0
	case ClassComplete:
		// seriesLateCleanToEnd complete samples in a row end the directed
		// reads: its series are late no more, as far as its samples read.
		if state.seriesLate != nil {
			if state.seriesLate.clean++; state.seriesLate.clean >= seriesLateCleanToEnd {
				endLateSeries(state)
			}
		}
	}
}

// readingOf is the group's report when its run is long enough.
func readingOf(queryGroup execution.QueryGroupIdentity, state *group) (ReadEarlyReading, bool) {
	run := state.readEarly
	if run == nil || run.consecutive < readEarlyRepeat {
		return ReadEarlyReading{}, false
	}
	step := int64(state.step / time.Second)
	later := int64(0)
	for _, sample := range run.samples {
		later = max(later, sample.CompletionAgeSeconds-sample.FirstReadyAgeSeconds+sample.ReadHoldSeconds)
	}
	suggested := run.delaySeconds + later
	if step > 0 {
		suggested = (suggested + step - 1) / step * step
	}
	return ReadEarlyReading{QueryGroup: queryGroup, Source: state.source, StepSeconds: step,
		CurrentDelaySeconds: run.delaySeconds, SuggestedDelaySeconds: suggested, Since: run.since,
		Samples: append([]ReadEarlySample(nil), run.samples...)}, true
}

// ReadEarly is every Query Group this process owns whose window was read
// early in readEarlyRepeat completed samples in a row, by Query Group.
// Ownership is asked outside the engine's lock, as Stats asks it.
func (engine *Engine) ReadEarly() []ReadEarlyReading {
	if engine == nil {
		return nil
	}
	engine.mu.Lock()
	readings := make([]ReadEarlyReading, 0)
	for queryGroup, state := range engine.groups {
		if reading, reported := readingOf(queryGroup, state); reported {
			readings = append(readings, reading)
		}
	}
	engine.mu.Unlock()
	owned := readings[:0]
	for _, reading := range readings {
		if engine.options.Owns(reading.QueryGroup) {
			owned = append(owned, reading)
		}
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i].QueryGroup < owned[j].QueryGroup })
	return owned
}
