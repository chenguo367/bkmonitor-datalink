// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import "sort"

// WithheldLineBudget bounds how many withheld objects one round names.
//
// It is a line budget rather than a rule about what matters: what does not fit
// is counted and reported, so a reader can tell a report that fitted from one
// that was cut. The number covers a full first round on the deployments this
// runs on today with headroom, and it bounds a round on a deployment many
// times larger - where the first round after a leader election would otherwise
// name every rejected strategy at once.
const WithheldLineBudget = 2000

// WithheldReport is what one refresh round has to say about the objects it did
// not accept, and how much of it fitted.
//
// Lines and Dropped are reported together on purpose. A capped report that does
// not say how much it cut reads as a complete one, and the reader has no way to
// tell a deployment with eleven problems from a deployment with eleven thousand.
type WithheldReport struct {
	Lines   []ObjectDisposition
	Dropped int
}

// withheldIdentity is what makes two records the same object across rounds.
// The scope is part of it: one strategy can be withheld at the plan and at a
// level, and those are two facts, not one changing its mind.
type withheldIdentity struct {
	SourceID string
	Scope    string
	LevelID  uint32
}

// ChangedWithheld is the objects whose disposition this round differs from what
// the last published audit recorded, capped.
//
// Only the differences, because the steady state is the answer nobody needs
// repeated: a deployment with two hundred rejected strategies would otherwise
// write two hundred identical lines every refresh, and the one strategy that
// started failing this morning would be somewhere in the middle of the two
// hundredth copy. A round with no previous audit reports everything, which is
// what the first round after a restart should do - nothing has been said yet,
// so nothing is a repeat.
//
// Records that stopped being withheld are not reported. This answers "what is
// being held back and why", and a strategy that is now accepted is not being
// held back; the counts are where a reader sees the total move.
//
// The cap is a line budget, not a filter: what does not fit is counted, so a
// reader can tell a report that fitted from one that was cut.
func ChangedWithheld(current, previous []ObjectDisposition, limit int) WithheldReport {
	was := make(map[withheldIdentity]ObjectDisposition, len(previous))
	for _, record := range previous {
		was[identityOf(record)] = record
	}
	changed := make([]ObjectDisposition, 0, len(current))
	for _, record := range current {
		before, known := was[identityOf(record)]
		if known && before.Disposition == record.Disposition && before.Reason == record.Reason {
			continue
		}
		changed = append(changed, record)
	}
	// Sorted so the same round always reports in the same order, and so a cap
	// cuts the same tail rather than whichever entries the map happened to
	// yield: a report that drops a different arbitrary subset each round would
	// let a strategy hide by never being in the first N.
	sort.Slice(changed, func(left, right int) bool {
		if changed[left].SourceID != changed[right].SourceID {
			return changed[left].SourceID < changed[right].SourceID
		}
		if changed[left].Scope != changed[right].Scope {
			return changed[left].Scope < changed[right].Scope
		}
		return changed[left].LevelID < changed[right].LevelID
	})
	if limit > 0 && len(changed) > limit {
		return WithheldReport{Lines: changed[:limit], Dropped: len(changed) - limit}
	}
	return WithheldReport{Lines: changed}
}

func identityOf(record ObjectDisposition) withheldIdentity {
	return withheldIdentity{SourceID: record.SourceID, Scope: record.Scope, LevelID: record.LevelID}
}
