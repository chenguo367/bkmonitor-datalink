// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// lookbackTick is how often due rechecks are looked for: inside the window
// of a rung at a ten-second step, so a rung is tried more than once before
// it yields.
const lookbackTick = 5 * time.Second

// lookbackStanding is what lookback.get answers besides the counts: whether
// this process runs the lookback.
type lookbackStanding struct {
	Running bool `json:"running"`
}

// lookbackOwnership answers the lookback's ownership question from the
// bundle, which is built after the query path the lookback sits on; until
// it is bound nothing is owned, and a first read completed before then is
// dropped as owner_lost.
type lookbackOwnership struct {
	bundle atomic.Pointer[phaseTwoWorkerBundle]
}

func (ownership *lookbackOwnership) bind(bundle *phaseTwoWorkerBundle) {
	ownership.bundle.Store(bundle)
}

func (ownership *lookbackOwnership) owns(queryGroup execution.QueryGroupIdentity) bool {
	bundle := ownership.bundle.Load()
	return bundle != nil && bundle.ownsQueryGroup(queryGroup)
}

// ownsQueryGroup says whether this replica holds a Runner for the Query
// Group now.
func (bundle *phaseTwoWorkerBundle) ownsQueryGroup(queryGroup execution.QueryGroupIdentity) bool {
	bundle.mu.RLock()
	defer bundle.mu.RUnlock()
	_, owned := bundle.runners[queryGroup]
	return owned
}

// count is how many Query Groups the bound bundle owns: the lookback's
// coverage denominator.
func (ownership *lookbackOwnership) count() int {
	bundle := ownership.bundle.Load()
	if bundle == nil {
		return 0
	}
	bundle.mu.RLock()
	defer bundle.mu.RUnlock()
	return len(bundle.runners)
}

// buildLookback builds this process's lookback. Nothing configures it and
// it takes no share of memory: it keeps one summary per owned Query Group,
// and its rechecks read through the same query client as the formal reads,
// one lookback permit at a time, never queued behind them and yielded the
// moment a formal query waits. Its sources are the ones a query can be
// compiled from; a fault, which normal running never meets, is logged.
func buildLookback(
	recheck lookback.Recheck,
	flights *scheduler.FlightCoordinator,
	ownership *lookbackOwnership,
	logger *observability.Logger,
	now func() time.Time,
) (*lookback.Engine, lookbackStanding, error) {
	engine, err := lookback.New(lookbackOptions(recheck, flights, ownership, logger, now))
	if err != nil {
		return nil, lookbackStanding{}, err
	}
	return engine, lookbackStanding{Running: true}, nil
}

// lookbackOptions wire the lookback to this process: its query client, its
// permits - a refusal at the lookback's own share of them a fault - its
// Runner set, the sources a query can be compiled from, and its log.
func lookbackOptions(
	recheck lookback.Recheck,
	flights *scheduler.FlightCoordinator,
	ownership *lookbackOwnership,
	logger *observability.Logger,
	now func() time.Time,
) lookback.Options {
	return lookback.Options{Now: now, Recheck: recheck, Owns: ownership.owns, Owned: ownership.count,
		Sources: controlplane.SupportedSourceSemantics, Refusals: scheduler.LookbackRefusals, LimitRefusal: scheduler.LookbackRefusedLimit,
		Permit: lookbackPermit(flights),
		OnFault: func(reason string, queryGroup execution.QueryGroupIdentity) {
			if logger != nil {
				logger.Warn("lookback", "fault", 0, 0, slog.String("reason", reason), slog.String("query_group", string(queryGroup)))
			}
		}}
}

// lookbackPermit is the lookback's permit from the process's query budget:
// granted only with room to spare, and yielded the moment a formal query has
// to wait for one.
func lookbackPermit(flights *scheduler.FlightCoordinator) lookback.Permit {
	return func() (func(), <-chan struct{}, string) {
		permit, refused := flights.TryAcquireLookbackPermit()
		if permit == nil {
			return nil, nil, refused
		}
		return permit.Release, permit.Yield(), ""
	}
}

// runLookback rechecks due samples until the bundle stops.
func runLookback(ctx context.Context, engine *lookback.Engine) {
	engine.Run(ctx, lookbackTick)
}

// cliLookbackReading is lookback.get's answer: this process's lookback as it
// stands, its counts since the process started, and the recent differing
// rechecks with bounded examples.
type cliLookbackReading struct {
	Scope  string    `json:"scope"`
	ReadAt time.Time `json:"read_at"`
	lookbackStanding
	Stats *lookback.Stats `json:"stats,omitempty"`
}

// cliLookbackOperation reads the answering process's lookback. Every replica
// keeps its own; the operation is targetable so each can be read in turn.
func cliLookbackOperation(engine *lookback.Engine, standing lookbackStanding) obchannel.Operation {
	return obchannel.Operation{ID: "lookback.get",
		Summary:       "读取实际回答进程的晚到数据回看：拥有的查询组有新鲜测量的覆盖率（目标 100%）；按来源给出每档复查与上一次读有变化的窗口数、按变化类别的桶数、到齐时刻的分布与最大值、未观测比例（unobserved 与 truncated_tail 占已结束样本）、各深度的查询组数与平均休息期、首读与复查的次数和字节（额外查询量）、取不出回看的样本数、让出与许可拒绝；到齐最晚的查询组与最近有变化的复查；可指定实例。",
		EvidenceScope: "process", Targetable: true, Fields: map[string]obchannel.Field{},
		OutputSchema: obchannel.SchemaOf(cliLookbackReading{}),
		Limits:       map[string]any{"redis_commands": 0, "scope": "answering_replica", "recent": 32, "latest": 32},
		Run: func(context.Context, obchannel.Params) obchannel.Outcome {
			reading := cliLookbackReading{Scope: "answering_replica", ReadAt: time.Now().UTC(), lookbackStanding: standing}
			if engine != nil {
				stats := engine.Stats()
				reading.Stats = &stats
			}
			return obchannel.Outcome{Value: reading, Complete: true, Limitations: []string{
				"Counts are this process's since it started; use meta.answered_by, and target each replica for the deployment.",
				"Each rung is compared with the read before it; a window is complete at the last rung that changed, or at the first read when none did. Only rechecks with outcome compared are windows observed; every other outcome is a window not observed, not a window that did not change.",
				"Rungs are at 1.5, 3.5, 7.5, 15.5, 31.5 and 63.5 of the Query Group's data steps. Each Query Group learns from its own samples how many to read and how long to rest between samples, at most an hour; a source only sums its groups. A recheck reads and compares only the window's tail - the deepest planned rung and a step - from the query's own lookback before it.",
				"A recheck reads through the same query service as the first read. The query service keeps no result cache by its source (its caches hold routing metadata and reload coordination); the deployed version is read from its workload image, not from here. A storage-layer cache that answers until its next refresh, such as a search engine's request cache, is a known boundary: it can return the first read again.",
			}}
		}}
}
