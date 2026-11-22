// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// planGenerations is the State generation each due Plan of a Query Group's
// last normal Slot on this process ran under (014 B4 H7): a supplement of
// an earlier Slot writing another generation's key writes one that Plan
// reads no more for a series its generation now has a record of. A Query
// Group with no normal Slot here for planGenerationsIdle is let go.
type planGenerations struct {
	mu     sync.Mutex
	groups map[execution.QueryGroupIdentity]planGenerationsAt
	swept  time.Time
}

type planGenerationsAt struct {
	at    time.Time
	plans map[execution.PlanKey]execution.StateGeneration
}

// planGenerationsIdle is how long a Query Group's generations are kept
// without a normal Slot: past it, it is not held here, or is held at the
// rate of an hourly Plan, whose next Slot says them again.
const planGenerationsIdle = 2 * time.Hour

// note keeps the generations a normal Slot of the Query Group ran its due
// Plans under, at now.
func (generations *planGenerations) note(queryGroup execution.QueryGroupIdentity, duePlans []execution.DuePlan, now time.Time) {
	plans := make(map[execution.PlanKey]execution.StateGeneration, len(duePlans))
	for _, due := range duePlans {
		plans[due.Key()] = due.StateGeneration
	}
	generations.mu.Lock()
	defer generations.mu.Unlock()
	if generations.groups == nil {
		generations.groups = map[execution.QueryGroupIdentity]planGenerationsAt{}
	}
	before := generations.groups[queryGroup]
	for key, generation := range before.plans {
		if _, due := plans[key]; !due {
			plans[key] = generation
		}
	}
	generations.groups[queryGroup] = planGenerationsAt{at: now, plans: plans}
	if now.Sub(generations.swept) < time.Minute {
		return
	}
	generations.swept = now
	for other, held := range generations.groups {
		if now.Sub(held.at) > planGenerationsIdle {
			delete(generations.groups, other)
		}
	}
}

// moved is whether due's Plan last ran a normal Slot of the Query Group
// here under another generation than due's.
func (generations *planGenerations) moved(queryGroup execution.QueryGroupIdentity, due execution.DuePlan) bool {
	generations.mu.Lock()
	defer generations.mu.Unlock()
	generation, held := generations.groups[queryGroup].plans[due.Key()]
	return held && generation != due.StateGeneration
}
