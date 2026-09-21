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
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// One strategy's standing, asked by its id: is it detecting, and if not,
// why not -- answered from the Leader's memory of the catalog it last
// published, joined with what the fleet sees of the objects that run it.
//
// It exists because the question "why is my strategy not alerting" had no
// bounded answer on a small deployment: the strategy directory that could
// answer it is a memory projection reserved out of the resource quota, and
// a deployment too small to reserve it answered 503 -- for the five
// strategies whose blocking was the question. This reads no Redis, copies
// no cache and runs nothing in the background: the Leader indexes the
// catalog it built anyway, once per round, and a follower forwards the one
// request to the Leader.

// StrategyLookupFacts is what the process's catalog memory says about one
// strategy, in the fleet's own terms; the runtime maps the control plane's
// answer onto it.
type StrategyLookupFacts struct {
	// Available says this process holds a publication to answer from. A
	// follower, or a Leader before its first round, does not; the request
	// is then forwarded to the Leader, or refused with why there is none.
	Available bool
	// Publication identifies the catalog answered from.
	Publication StrategyPublication
	// Found says the publication records the strategy at all -- a Plan or a
	// disposition -- which tells "the source never listed it" apart from
	// "listed and withheld".
	Found bool
	// Retained says the Plans are the last good ones kept under a refusal:
	// the strategy detects on its previous configuration while the new one
	// is withheld, and both facts are on the answer.
	Retained bool
	// Plans is where the strategy runs, one per Query Group, sorted.
	Plans []StrategyPlanRef
	// Dispositions is every disposition the round recorded for the
	// strategy, accepted and withheld, one per item.
	Dispositions []StrategyDisposition
}

// StrategyPublication is the catalog a standing was answered from.
type StrategyPublication struct {
	SnapshotRevision string `json:"snapshot_revision"`
	Epoch            uint64 `json:"epoch"`
}

// StrategyPlanRef is one Plan of the strategy: which object runs it, under
// which frozen revisions, and the digest the object's content is read by.
type StrategyPlanRef struct {
	Tenant           string `json:"tenant"`
	Business         string `json:"business"`
	QueryGroup       string `json:"query_group"`
	ObjectDigest     string `json:"object_digest,omitempty"`
	SnapshotRevision string `json:"snapshot_revision"`
	QueryRevision    string `json:"query_revision"`
	ScheduleRevision string `json:"schedule_revision"`
}

// StrategyDisposition is what the round decided about one item of the
// strategy: ACCEPTED, or a disposition with the reason and the field it
// refused on. The same words the first screen's source facts count by.
type StrategyDisposition struct {
	Scope       string `json:"scope,omitempty"`
	LevelID     uint32 `json:"level_id,omitempty"`
	Disposition string `json:"disposition"`
	Reason      string `json:"reason,omitempty"`
	FieldPath   string `json:"field_path,omitempty"`
}

// StrategyLookupFunc answers the standing of one strategy from the
// process's catalog memory.
type StrategyLookupFunc func(strategyID string) StrategyLookupFacts

// LeaderForward hands a request this process cannot answer to the Leader.
// It reports whether it did -- the response is then already written -- or,
// when there is no Leader to hand it to, the closed word for why (the view
// stream's discovery misses), which the refusal carries.
type LeaderForward func(response http.ResponseWriter, request *http.Request) (forwarded bool, refusal string)

// StrategyStandingKind is the one word for the strategy's standing. Closed.
type StrategyStandingKind string

const (
	// StandingDetecting: every item became a Plan and the Plans run.
	StandingDetecting StrategyStandingKind = "DETECTING"
	// StandingWithheld: listed by the source, and no item became a Plan.
	StandingWithheld StrategyStandingKind = "WITHHELD"
	// StandingPartlyWithheld: some items became Plans, some were withheld.
	StandingPartlyWithheld StrategyStandingKind = "PARTLY_WITHHELD"
	// StandingRetainedLastGood: the new configuration was withheld and the
	// Plans are the last good ones, still detecting on the old.
	StandingRetainedLastGood StrategyStandingKind = "RETAINED_LAST_GOOD"
	// StandingNotListed: the source never listed the strategy in the
	// publication answered from.
	StandingNotListed StrategyStandingKind = "NOT_LISTED"
)

// StrategyStandingKinds is the closed list, for the page's wording table.
var StrategyStandingKinds = []StrategyStandingKind{StandingDetecting, StandingWithheld, StandingPartlyWithheld, StandingRetainedLastGood, StandingNotListed}

// StrategyStanding is the answer.
type StrategyStanding struct {
	StrategyID string `json:"strategy_id"`
	// Tenant and Business are the filters the question carried, when it
	// did; a strategy id can repeat across tenants.
	Tenant   string `json:"tenant,omitempty"`
	Business string `json:"business,omitempty"`
	// AnsweredBy is the replica whose catalog memory answered: the Leader's,
	// whether the request reached it directly or was forwarded.
	AnsweredBy  string               `json:"answered_by"`
	Publication StrategyPublication  `json:"publication"`
	Standing    StrategyStandingKind `json:"standing"`
	Found       bool                 `json:"found"`
	Retained    bool                 `json:"retained"`
	// Plans is where the strategy runs, each with what the fleet sees of
	// that object now.
	Plans []StrategyPlanStanding `json:"plans"`
	// Dispositions is every item's decision, accepted and withheld.
	Dispositions []StrategyDisposition `json:"dispositions"`
	// Line is the answer in one sentence, composed here so the page and
	// any other reader say the same thing.
	Line string `json:"line"`
}

// StrategyPlanStanding is one Plan with the fleet's reading of its object:
// which replica holds it, and the row it is on, if any.
type StrategyPlanStanding struct {
	StrategyPlanRef
	// Replica is the replica that owns the object now, from the fleet's
	// snapshots; empty when no snapshot lists it, which Existence explains.
	Replica string `json:"replica,omitempty"`
	// Existence is the object against the catalog's active set: active,
	// absent, or unknown -- the same word the object page uses.
	Existence string `json:"existence"`
	// Rows is what the fleet lists the object under, every row: an object
	// under no row is healthy for the equation and appears as none.
	Rows []Anomaly `json:"rows"`
}

// StrategyStandingOf composes the answer from the lookup and the fleet's
// view. tenant and business narrow the Plans and dispositions when given.
func StrategyStandingOf(strategyID, tenant, business, replica string, facts StrategyLookupFacts, view *View, now time.Time) StrategyStanding {
	standing := StrategyStanding{StrategyID: strategyID, Tenant: tenant, Business: business, AnsweredBy: replica,
		Publication: facts.Publication, Found: facts.Found, Retained: facts.Retained,
		Plans: []StrategyPlanStanding{}, Dispositions: []StrategyDisposition{}}
	for _, plan := range facts.Plans {
		if tenant != "" && plan.Tenant != tenant || business != "" && plan.Business != business {
			continue
		}
		entry := StrategyPlanStanding{StrategyPlanRef: plan, Existence: "unknown", Rows: []Anomaly{}}
		if view != nil {
			entry.Existence = objectExistence(plan.QueryGroup, view.expectation)
			entry.Replica = view.ownerOf[plan.QueryGroup]
			walkObjectRows("", "", plan.QueryGroup, view, now, func(row Anomaly) {
				if len(entry.Rows) < MaxPageSize {
					entry.Rows = append(entry.Rows, row)
				}
			})
		}
		standing.Plans = append(standing.Plans, entry)
	}
	standing.Dispositions = append(standing.Dispositions, facts.Dispositions...)
	sort.SliceStable(standing.Dispositions, func(left, right int) bool {
		if standing.Dispositions[left].Scope != standing.Dispositions[right].Scope {
			return standing.Dispositions[left].Scope < standing.Dispositions[right].Scope
		}
		return standing.Dispositions[left].LevelID < standing.Dispositions[right].LevelID
	})
	standing.Standing = strategyStandingKindOf(standing)
	standing.Line = strategyStandingLine(standing)
	return standing
}

func strategyStandingKindOf(standing StrategyStanding) StrategyStandingKind {
	if !standing.Found {
		return StandingNotListed
	}
	withheld := 0
	for _, disposition := range standing.Dispositions {
		if disposition.Disposition != dispositionAccepted {
			withheld++
		}
	}
	switch {
	case standing.Retained:
		return StandingRetainedLastGood
	case len(standing.Plans) == 0:
		return StandingWithheld
	case withheld > 0:
		return StandingPartlyWithheld
	default:
		return StandingDetecting
	}
}

// strategyStandingLine is the sentence: the standing, the objects and who
// holds them, and every withheld item with its reason and field.
func strategyStandingLine(standing StrategyStanding) string {
	withheld := make([]string, 0, len(standing.Dispositions))
	for _, disposition := range standing.Dispositions {
		if disposition.Disposition == dispositionAccepted {
			continue
		}
		item := disposition.Disposition + "/" + disposition.Reason
		if disposition.Scope != "" {
			where := disposition.Scope
			if disposition.LevelID != 0 {
				where += fmt.Sprintf(" 级别 %d", disposition.LevelID)
			}
			item = where + "：" + item
		}
		if disposition.FieldPath != "" {
			item += "（" + disposition.FieldPath + "）"
		}
		withheld = append(withheld, item)
	}
	objects := make([]string, 0, len(standing.Plans))
	for _, plan := range standing.Plans {
		object := shortObjectName(plan.QueryGroup)
		switch {
		case plan.Replica != "":
			object += "（" + shortReplicaName(plan.Replica) + " 持有"
			if len(plan.Rows) > 0 {
				object += "，在 " + string(plan.Rows[0].Finding.Check) + " 行"
			}
			object += "）"
		case plan.Existence == "active":
			object += "（在活动集，暂无副本快照列出）"
		default:
			object += "（" + plan.Existence + "）"
		}
		objects = append(objects, object)
	}
	switch standing.Standing {
	case StandingNotListed:
		return "策略 " + standing.StrategyID + "：这一轮的策略源没有列出它——不是被扣，是源里没有"
	case StandingWithheld:
		return fmt.Sprintf("策略 %s：未生效，%d 项全部被扣住：%s", standing.StrategyID, len(withheld), strings.Join(withheld, "；"))
	case StandingPartlyWithheld:
		return fmt.Sprintf("策略 %s：部分生效——%d 个对象在检测：%s；%d 项被扣住：%s",
			standing.StrategyID, len(objects), strings.Join(objects, "、"), len(withheld), strings.Join(withheld, "；"))
	case StandingRetainedLastGood:
		return fmt.Sprintf("策略 %s：新配置被扣住，仍按上一次生效的配置检测——%d 个对象：%s；扣住的原因：%s",
			standing.StrategyID, len(objects), strings.Join(objects, "、"), strings.Join(withheld, "；"))
	default:
		return fmt.Sprintf("策略 %s：已生效，%d 个对象在检测：%s", standing.StrategyID, len(objects), strings.Join(objects, "、"))
	}
}

func shortObjectName(queryGroup string) string {
	if len(queryGroup) > 12 {
		return queryGroup[:12]
	}
	return queryGroup
}

// WithStrategyStanding serves GET /api/strategies/{id}[?tenant=&business=]
// in front of the fleet API. A process without a publication forwards the
// request to the Leader once; a forwarded request that lands on a process
// without one is refused rather than forwarded again.
func WithStrategyStanding(next http.Handler, service *Service, lookup StrategyLookupFunc, forward LeaderForward,
	replica string, now func() time.Time, stallAfter time.Duration) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if !strings.HasPrefix(request.URL.Path, "/api/strategies/") {
			next.ServeHTTP(response, request)
			return
		}
		if request.Method != http.MethodGet {
			response.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		strategyID := strings.TrimPrefix(request.URL.Path, "/api/strategies/")
		if strategyID == "" || strings.Contains(strategyID, "/") {
			writeJSON(response, http.StatusBadRequest, map[string]string{"error": "STRATEGY_ID_REQUIRED"})
			return
		}
		if lookup == nil {
			writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "LOOKUP_NOT_WIRED"})
			return
		}
		facts := lookup(strategyID)
		if !facts.Available {
			if forward != nil && request.Header.Get(forwardedHeader) == "" {
				if forwarded, refusal := forward(response, request); forwarded {
					return
				} else if refusal != "" {
					writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "LEADER_UNAVAILABLE", "reason": refusal})
					return
				}
			}
			writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "NOT_PUBLISHED",
				"reason": "this replica has published no catalog: not the Leader, or the Leader before its first round"})
			return
		}
		query := request.URL.Query()
		var view *View
		if service != nil {
			current := service.View(request.Context())
			Decide(&current, now(), stallAfter)
			view = &current
		}
		writeJSON(response, http.StatusOK, StrategyStandingOf(strategyID, query.Get("tenant"), query.Get("business"), replica, facts, view, now()))
	})
}

// forwardedHeader marks a request a follower handed to the Leader, so it
// is answered or refused there and never handed on again.
const forwardedHeader = "X-Alarmd-Forwarded"

// ForwardedHeader is the header's name, for the runtime's forwarder.
func ForwardedHeader() string { return forwardedHeader }
