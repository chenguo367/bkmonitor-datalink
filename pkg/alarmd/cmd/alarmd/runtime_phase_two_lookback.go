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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
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
	memory func(bytes uint64) bool,
) (*lookback.Engine, lookbackStanding, error) {
	engine, err := lookback.New(lookbackOptions(recheck, flights, ownership, logger, now, memory))
	if err != nil {
		return nil, lookbackStanding{}, err
	}
	return engine, lookbackStanding{Running: true}, nil
}

// lookbackOptions wire the lookback to this process: its query client, its
// permits, its Runner set, its log and the observation memory line its
// series tables grow under.
func lookbackOptions(
	recheck lookback.Recheck,
	flights *scheduler.FlightCoordinator,
	ownership *lookbackOwnership,
	logger *observability.Logger,
	now func() time.Time,
	memory func(bytes uint64) bool,
) lookback.Options {
	return lookback.Options{Now: now, Recheck: recheck, Owns: ownership.owns, Owned: ownership.count,
		Refusals: scheduler.LookbackRefusals, Permit: lookbackPermit(flights), Memory: memory,
		OnFault: func(reason string, queryGroup execution.QueryGroupIdentity) {
			if logger != nil {
				logger.Warn("lookback", "fault", 0, 0, slog.String("reason", reason), slog.String("query_group", string(queryGroup)))
			}
		}}
}

// lookbackPermit is the lookback's permit from the process's query budget:
// granted only from a permit nobody is waiting for, and yielded the moment a
// formal query has to wait for one.
func lookbackPermit(flights *scheduler.FlightCoordinator) lookback.Permit {
	return func() (func(), <-chan struct{}, string) {
		permit, refused := flights.TryAcquireLookbackPermit()
		if permit == nil {
			return nil, nil, refused
		}
		return permit.Release, permit.Yield(), ""
	}
}

// lookbackReadEarly is the lookback's report of the objects read before
// their data was complete, as the fleet snapshot carries it; nil without a
// lookback.
func lookbackReadEarly(engine *lookback.Engine) func() map[string]fleet.ReadEarlyFacts {
	if engine == nil {
		return nil
	}
	return func() map[string]fleet.ReadEarlyFacts {
		readings := engine.ReadEarly()
		facts := make(map[string]fleet.ReadEarlyFacts, len(readings))
		for _, reading := range readings {
			// The samples ride with the values: the suggestion and what it
			// rests on are one read, bounded by the fleet's row.
			row := fleet.ReadEarlyFacts{StepSeconds: reading.StepSeconds,
				CurrentDelaySeconds: reading.CurrentDelaySeconds, SuggestedDelaySeconds: reading.SuggestedDelaySeconds,
				Since: reading.Since}
			for _, sample := range reading.Samples {
				row.Samples = append(row.Samples, fleet.ReadEarlySample{EvaluationTime: int64(sample.EvaluationTime),
					FirstReadAgeSeconds: sample.FirstReadAgeSeconds, CompletionAgeSeconds: sample.CompletionAgeSeconds,
					Rung: sample.Rung, ChangedAgeSeconds: sample.ChangedAgeSeconds, Buckets: sample.Buckets})
			}
			facts[string(reading.QueryGroup)] = row
		}
		return facts
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
		Summary:       "读取实际回答进程的晚到数据回看：拥有的查询组有新鲜测量的覆盖率（覆盖数 ÷（拥有数 − 从未有过完整首读的组数），目标 100%；从未有过完整首读的组单列计数，并按查询组列出不完整首读的次数，最多 32 个）；按来源给出每档复查与上一次读有变化的窗口数、按变化类别的桶数、到齐时刻的分布与最大值、未观测比例（unobserved 占已结束样本）、深探结果（干净、有变化、未读到）与深探才发现迟到的样本比例（probe_changed 占已结束样本，不进到齐分布）、首读完整但为空的样本后来是否到数及其到齐时刻、按事实分的四类样本数（整窗读早、部分序列迟到、完整、未分类：序列表被内存安全线拒绝而分不出，按原因计）与连续两次整窗读早的查询组（read_early：当前有效 time_delay、建议值（上界）、依据的样本与变化的桶）、各深度的查询组数与平均休息期、首读与复查的次数和字节（额外查询量）、取不出回看的样本数、让出与许可拒绝；到齐最晚的查询组与最近有变化的复查；可指定实例。",
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
				"Rungs are at 1.5, 3.5, 7.5, 15.5, 31.5 and 63.5 of the Query Group's data steps. Each Query Group learns from its own samples how many to read and how long to rest between samples, at most an hour; a source only sums its groups. A recheck reads and compares only the window's last 65 steps - the whole of a shorter window - from the query's own lookback before them.",
				"A Query Group's first sample and one in four after it are read once more at the deepest rung after the rungs the group reads; data found there makes that sample probe_changed, with no completion, and the group reads every rung and settles from its next sample. A window still changing at the deepest rung has its completion counted there, and lateness past it is not measured; its class is unclassified (unsettled), not complete, because no later read agrees with that last one.",
				"Each completed sample is classed by the facts of its series against the first read, as its data settled: the read of its last change, which a later rung read again the same, so a change that came back is not one; values are compared to one part in 2^28 (about 3.7e-9 of the value), so reads that differ only in the order the store summed them are one read: window_read_early when the first read was empty and data came later, or a series it had came back with other points or values or not at all (the strategy's time_delay moves the read; a value revised after it was judged is not judged again); series_late when every series it had came back as it was and others came later (supplementary detection's); unclassified when a rung changed and its series could not be compared because the memory line refused a series table (memory_refused), or the deepest rung still changed so no later read says the data settled (unsettled) (counted by reason under unclassified, not a fault); complete otherwise. read_early lists the Query Groups read early twice in a row, with the time_delay they run under and the one that would have read their samples complete: the largest completion past the first read added, aligned up to the step as a strategy's time_delay is compiled. A completion is the age of the recheck that first read the data whole, so the suggestion is an upper bound.",
				"A sample waiting for its deep recheck does not hold its group's next sample back: a punctual Query Group settles at 1 to 1.25 rechecks an hour, about a fifth of them deep (simulated: 1.23 at a ten-second step, 1.21 at a minute, 1.02 at five minutes - a group rests from its first rung, 1.5 steps after its read, and waits for its next first read).",
				"A recheck reads through the same query service as the first read. The query service keeps no result cache by its source (its caches hold routing metadata and reload coordination); the deployed version is read from its workload image, not from here. A storage-layer cache that answers until its next refresh, such as a search engine's request cache, is a known boundary: it can return the first read again.",
			}}
		}}
}
