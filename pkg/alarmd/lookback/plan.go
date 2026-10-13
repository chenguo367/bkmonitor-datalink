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
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Plan is the control plane's word that a Query Group read late series at a
// recheck rung, good until ExpiresAt.
//
// What a process learns about a Query Group's late series lives in its
// memory, and a restart or a change of owner loses it: on one production
// deployment the Query Groups read directed fell from about fifty to none at
// each rollout and took an hour to an hour and a half to come back, every
// late series of those Query Groups unsupplemented meanwhile. A plan lets the
// next process start where the last one was. It is advice, never evidence: a
// Query Group a sample of which has completed here is decided by its own
// samples, and a plan the control plane withdrew or let expire takes the
// directed reads it started with it, unless a sample here has since seen the
// late series too.
type Plan struct {
	QueryGroup execution.QueryGroupIdentity
	Rung       int
	ExpiresAt  time.Time
}

// What became of a plan entry, closed: seeded, the Query Group was read
// directed at its rung from its first read here; kept_local, the Query
// Group's own samples here already decide it; expired, a seeded Query Group
// none of whose samples here saw late series left directed reading when its
// entry expired or was withdrawn.
const (
	PlanSeeded    = "seeded"
	PlanKeptLocal = "kept_local"
	PlanExpired   = "expired"
)

// PlanOutcomes is every plan outcome.
var PlanOutcomes = []string{PlanSeeded, PlanKeptLocal, PlanExpired}

// ApplyPlans replaces the plan set with plans: the whole set the control
// plane holds for this Worker, never a change to the last one. Entries
// already past their expiry or naming a rung this build does not have are
// not plans.
func (engine *Engine) ApplyPlans(plans []Plan) {
	if engine == nil {
		return
	}
	now := engine.options.Now()
	engine.mu.Lock()
	defer engine.mu.Unlock()
	next := make(map[execution.QueryGroupIdentity]Plan, len(plans))
	for _, plan := range plans {
		if plan.QueryGroup == "" || plan.Rung < 0 || plan.Rung >= len(RungSteps) || !now.Before(plan.ExpiresAt) {
			continue
		}
		next[plan.QueryGroup] = plan
	}
	engine.plans = next
	engine.expirePlansLocked(now)
	for queryGroup, plan := range engine.plans {
		if state := engine.groups[queryGroup]; state != nil {
			engine.seedLocked(state, plan, now)
		}
	}
}

// seedLocked starts a Query Group's directed reads at its plan's rung, when
// nothing here says otherwise: no sample of it has completed in this
// process, and it is not already read directed.
func (engine *Engine) seedLocked(state *group, plan Plan, now time.Time) {
	if state.seriesLate != nil && state.seriesLate.seeded {
		return
	}
	if state.sampled || state.seriesLate != nil {
		if !state.planKept {
			state.planKept = true
			engine.counts.plans[PlanKeptLocal]++
		}
		return
	}
	state.seriesLate = &seriesLateState{rung: plan.Rung, since: now, seeded: true}
	engine.counts.plans[PlanSeeded]++
}

// expirePlansLocked drops the entries past their expiry, and ends the
// directed reads of every seeded Query Group whose entry is gone while none
// of its own samples here has seen late series.
func (engine *Engine) expirePlansLocked(now time.Time) {
	for queryGroup, plan := range engine.plans {
		if !now.Before(plan.ExpiresAt) {
			delete(engine.plans, queryGroup)
		}
	}
	for queryGroup, state := range engine.groups {
		if late := state.seriesLate; late != nil && late.seeded && late.seen == 0 {
			if _, planned := engine.plans[queryGroup]; !planned {
				endLateSeries(state)
				engine.counts.plans[PlanExpired]++
			}
		}
	}
}
