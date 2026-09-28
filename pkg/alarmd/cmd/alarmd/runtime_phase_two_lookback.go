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
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/obchannel"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/scheduler"
)

// lookbackTick is how often due rechecks are looked for: well inside a
// tier's window, so a tier is tried several times before it yields.
const lookbackTick = 5 * time.Second

// Why a process configured for the lookback is not running it, closed.
const (
	lookbackNotConfigured      = "not_configured"
	lookbackNoObservationShare = "no_observation_capacity"
)

// lookbackStanding is what lookback.get answers besides the counts: whether
// this process was asked to run the lookback, whether it does, and if not,
// why.
type lookbackStanding struct {
	Configured bool   `json:"configured"`
	Running    bool   `json:"running"`
	Reason     string `json:"reason,omitempty"`
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

// buildLookback builds the lookback when the configuration asks for it and
// the observation share has room for it; otherwise nil and why. Its rechecks
// read through the same query client as the formal reads, one lookback
// permit at a time and never queued behind them.
func buildLookback(
	allocation config.PhaseTwoObservationConfig,
	capacity config.ObservationCapacity,
	recheck lookback.Recheck,
	flights *scheduler.FlightCoordinator,
	ownership *lookbackOwnership,
	now func() time.Time,
) (*lookback.Engine, lookbackStanding, error) {
	if !allocation.LookbackEnabled {
		return nil, lookbackStanding{Reason: lookbackNotConfigured}, nil
	}
	if capacity.LookbackBytes <= 0 {
		// The share is allocated but this container's memory is not known,
		// so no diagnostics run; see config.DeriveObservationCapacity.
		return nil, lookbackStanding{Configured: true, Reason: lookbackNoObservationShare}, nil
	}
	engine, err := lookback.New(lookback.Options{Now: now, Recheck: recheck, Owns: ownership.owns, MemoryBytes: capacity.LookbackBytes,
		Permit: func() (func(), string) {
			permit, refused := flights.TryAcquireLookbackPermit()
			if permit == nil {
				return nil, refused
			}
			return permit.Release, ""
		}})
	if err != nil {
		return nil, lookbackStanding{}, err
	}
	return engine, lookbackStanding{Configured: true, Running: true}, nil
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
		Summary:       "读取实际回答进程的晚到数据回看：是否开启、没开的原因、抽样与再读的具名计数（只有 compared 进分母）、按（序列，桶）与按原阈值判定的差异分类、按实际读取时延的分布，以及最近有差异的再读及其有界样例；可指定实例。",
		EvidenceScope: "process", Targetable: true, Fields: map[string]obchannel.Field{},
		OutputSchema: obchannel.SchemaOf(cliLookbackReading{}),
		Limits:       map[string]any{"redis_commands": 0, "scope": "answering_replica", "recent": 32, "examples_per_recheck": 8},
		Run: func(context.Context, obchannel.Params) obchannel.Outcome {
			reading := cliLookbackReading{Scope: "answering_replica", ReadAt: time.Now().UTC(), lookbackStanding: standing}
			if engine != nil {
				stats := engine.Stats()
				reading.Stats = &stats
			}
			return obchannel.Outcome{Value: reading, Complete: true, Limitations: []string{
				"Counts are this process's since it started; use meta.answered_by, and target each replica for the deployment.",
				"Only rechecks with outcome compared enter compared_buckets, compared_windows, differences, judgments and by_age; every other outcome is a window not observed, not a window that did not change.",
				"A recheck reads through the same query service as the first read. A cache in front of the storage that answered the first read again would hide late data; whether the deployed query service has one is not established here.",
				"Judgments cover static-threshold Levels of Plans that admitted the series at the first read; other algorithms are compared as data only.",
			}}
		}}
}
