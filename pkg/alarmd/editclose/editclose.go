// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package editclose decides which open alerts an edit of their strategy
// orphaned: alerts the strategy's current revision can no longer produce or
// recover, so nothing would ever end them. It follows the platform's own
// edit rules (bk-monitor alert manager, CloseStatusChecker
// .check_strategy_changed) as far as an alert's own record can decide them.
//
// The decision is level-triggered. An alert is a candidate when the
// revision it was raised under (its labels.strategy_version, which the link
// copies from the opening event and never changes) is older than the
// revision the strategy's current Plans raise alerts under. Nothing about
// the edit itself is observed or remembered: a process that starts after
// the edit finds the same candidates as one that watched it happen, and an
// alert at the current revision is never one.
//
// Each candidate is judged from its own record against the current Plans'
// facts, and closed only on a fact both sides state:
//
//   - dimensions_changed: the alert carries a dimension the current Plans no
//     longer tell series apart by - a grouping dimension removed or renamed.
//     The output identity the alert's fingerprint is built from is what is
//     compared, minus the dimensions this process adds beside the strategy's
//     own (the no-data tag and the host identity CMDB fills in). A
//     dimension added to the grouping is not decided: the record omits a
//     grouping dimension the series did not carry, so an alert lacking one
//     of the current dimensions may be either. A Plan grouping by every
//     dimension its series carries (dynamic dimensions) is not decided
//     either. A no-data alert is compared against the no-data grouping, the
//     dimensions its group was reduced to.
//   - level_removed: a threshold alert whose level is no longer among the
//     Plans' Levels. Decided only when every Level the strategy declares is
//     detected (a refused Level is not a removed one) and only for the
//     built-in levels, whose names the link stores unchanged; a level whose
//     name the link may have mapped is not decided.
//   - no_data_disabled: a no-data alert of a strategy that no longer
//     configures no-data detection. A strategy that configures it and whose
//     Plans do not attach it is not switched off and is not decided.
//
// Every candidate that is not closed is counted under the word that kept
// it, so a still-firing alert whose group survived the edit is a reading
// (kept) and not a silence.
package editclose

import (
	"strconv"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
)

// The per-alert outcomes, closed: a metric label is made of them.
const (
	// OutcomeCurrentRevision is an alert raised under the current revision:
	// not a candidate.
	OutcomeCurrentRevision = "current_revision"
	// OutcomeRevisionAhead is an alert raised under a revision newer than
	// the current Plans': the publication has not caught up with the source
	// yet. Not a candidate until it has.
	OutcomeRevisionAhead = "revision_ahead"
	// OutcomeRevisionUnknown is a record that names no revision.
	OutcomeRevisionUnknown = "revision_unknown"
	// OutcomeFamilyUnknown is a record whose evaluation family is missing,
	// not one this process writes, or contradicted by its no-data tag, or
	// that carries no dimensions object: which rules apply is not known.
	OutcomeFamilyUnknown = "family_unknown"
	// OutcomeKept is a candidate every decidable rule keeps: its group,
	// level and family all survive the edit.
	OutcomeKept = "kept"
	// OutcomeLevelsPartial is a threshold candidate whose level is not
	// among the Plans' Levels while some Level of the strategy is refused:
	// it may be that one. Not closed.
	OutcomeLevelsPartial = "levels_partial"
	// OutcomeSeverityUnmapped is a threshold candidate whose level is not
	// among the Plans' Levels and is, or the Plans have, a level the link
	// may have stored under a mapped name. Not closed.
	OutcomeSeverityUnmapped = "severity_unmapped"
	// OutcomeNoDataSuspended is a no-data candidate of a strategy that
	// configures no-data and whose Plans do not attach it. Not closed.
	OutcomeNoDataSuspended = "no_data_suspended"

	// The three closes, by the change that decided them: the same words the
	// close names (linkdoutput.StrategyChanged*).
	OutcomeDimensionsChanged = linkdoutput.StrategyChangedDimensions
	OutcomeLevelRemoved      = linkdoutput.StrategyChangedLevel
	OutcomeNoDataDisabled    = linkdoutput.StrategyChangedNoData
)

// Decisions is every word Decide returns.
var Decisions = []string{OutcomeCurrentRevision, OutcomeRevisionAhead, OutcomeRevisionUnknown, OutcomeFamilyUnknown,
	OutcomeKept, OutcomeLevelsPartial, OutcomeSeverityUnmapped, OutcomeNoDataSuspended,
	OutcomeDimensionsChanged, OutcomeLevelRemoved, OutcomeNoDataDisabled}

// Closes reports whether a decision is a close.
func Closes(decision string) bool {
	return decision == OutcomeDimensionsChanged || decision == OutcomeLevelRemoved || decision == OutcomeNoDataDisabled
}

// Plan is what the strategy's current Plans state (controlplane
// .StrategyPlanFacts).
type Plan struct {
	Revision          int64
	DimensionFields   []string
	DynamicDimensions bool
	Levels            []contract.LevelDefinitionV2
	LevelsWhole       bool
	NoData            bool
	NoDataDimensions  []string
	NoDataSuspended   bool
}

// Key is the plan's facts as one comparable word: two Plans with the same
// key decide every alert the same way, which is what lets a decision made
// against one be remembered while the key holds.
func (plan Plan) Key() string {
	var b strings.Builder
	b.WriteString(strconv.FormatInt(plan.Revision, 10))
	b.WriteString("|")
	b.WriteString(strings.Join(plan.DimensionFields, ","))
	b.WriteString("|" + strconv.FormatBool(plan.DynamicDimensions) + "|")
	for _, level := range plan.Levels {
		b.WriteString(strconv.FormatUint(uint64(level.LevelID), 10) + ":" + level.LevelCode + ",")
	}
	b.WriteString("|" + strconv.FormatBool(plan.LevelsWhole) + "|" + strconv.FormatBool(plan.NoData) + "|")
	b.WriteString(strings.Join(plan.NoDataDimensions, ","))
	b.WriteString("|" + strconv.FormatBool(plan.NoDataSuspended))
	return b.String()
}

// Alert is what an alert's record states (openalerts.AlertRecord).
type Alert struct {
	Revision         int64
	DimensionKeys    []string
	DimensionsRead   bool
	EvaluationFamily string
	Severity         string
}

// The evaluation families this process writes (linkdoutput's
// extra_data.evaluation_family).
const (
	familyMetricAlgorithm = "metric_algorithm"
	familyNoData          = "no_data"
)

// addedDimensions are the dimensions this process writes into an alert's
// dimensions beside the strategy's grouping: the no-data tag, and the host
// identity Python's fuller fills in from CMDB (admission.Facts.HostOverlay).
// An alert carrying one of them says nothing about the grouping. A grouping
// that named one and dropped it is not decided for that dimension, which
// can only cost a close.
var addedDimensions = map[string]struct{}{
	contract.NoDataDimensionTag: {}, "__additional_dimensions": {},
	"bk_target_ip": {}, "bk_target_cloud_id": {}, "bk_host_id": {},
}

// builtInLevels are the levels whose names the link stores as this process
// writes them (linkdoutput.SeverityFor): its default severities, by name.
var builtInLevels = map[string]uint32{"critical": 1, "warning": 2, "info": 3}

// Decide is one alert's verdict against the current Plans.
func Decide(plan Plan, alert Alert) string {
	switch {
	case alert.Revision <= 0:
		return OutcomeRevisionUnknown
	case alert.Revision == plan.Revision:
		return OutcomeCurrentRevision
	case alert.Revision > plan.Revision:
		return OutcomeRevisionAhead
	}
	noData, known := alertFamily(alert)
	if !known {
		return OutcomeFamilyUnknown
	}
	if noData {
		if !plan.NoData {
			if plan.NoDataSuspended {
				return OutcomeNoDataSuspended
			}
			return OutcomeNoDataDisabled
		}
		if carriesDroppedDimension(alert.DimensionKeys, plan.NoDataDimensions) {
			return OutcomeDimensionsChanged
		}
		return OutcomeKept
	}
	if !plan.DynamicDimensions && carriesDroppedDimension(alert.DimensionKeys, plan.DimensionFields) {
		return OutcomeDimensionsChanged
	}
	return levelVerdict(plan, alert.Severity)
}

// alertFamily is whether the alert is a no-data alert, and whether that is
// known: the family written, agreeing with the tag the no-data series
// carries in its dimensions.
func alertFamily(alert Alert) (noData, known bool) {
	if !alert.DimensionsRead {
		return false, false
	}
	tagged := false
	for _, key := range alert.DimensionKeys {
		if key == contract.NoDataDimensionTag {
			tagged = true
		}
	}
	switch alert.EvaluationFamily {
	case familyNoData:
		return true, tagged
	case familyMetricAlgorithm:
		return false, !tagged
	}
	return false, false
}

// carriesDroppedDimension reports whether the alert has a dimension the
// grouping no longer names. A "tags." prefix is read the way the
// fingerprint reads it (contract.ProjectMonitorTarget), on both sides.
func carriesDroppedDimension(keys, grouping []string) bool {
	grouped := make(map[string]struct{}, len(grouping))
	for _, field := range grouping {
		grouped[strings.TrimPrefix(field, "tags.")] = struct{}{}
	}
	for _, key := range keys {
		if _, added := addedDimensions[key]; added {
			continue
		}
		if _, ok := grouped[strings.TrimPrefix(key, "tags.")]; !ok {
			return true
		}
	}
	return false
}

// levelVerdict decides a threshold alert's level against the Plans' Levels.
func levelVerdict(plan Plan, severity string) string {
	builtInOnly := true
	for _, level := range plan.Levels {
		if linkdoutput.SeverityFor(contract.LevelResultV1{LevelID: level.LevelID, LevelCode: level.LevelCode}) == severity {
			return OutcomeKept
		}
		if !linkdoutput.SeverityIsBuiltIn(contract.LevelResultV1{LevelID: level.LevelID}) {
			builtInOnly = false
		}
	}
	if !plan.LevelsWhole {
		return OutcomeLevelsPartial
	}
	if _, builtIn := builtInLevels[severity]; !builtIn || !builtInOnly || len(plan.Levels) == 0 {
		return OutcomeSeverityUnmapped
	}
	return OutcomeLevelRemoved
}
