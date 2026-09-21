// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package scheduler

import (
	"sort"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// QueryGroupCostReport is one Query Group's cost as the Worker running it
// reported on its heartbeat (decision-020 section 5.2): the largest
// retained bytes one Slot held in the last round, and the smoothed
// evaluation-and-query wall time per second of schedule, in thousandths.
type QueryGroupCostReport struct {
	QueryGroup         execution.QueryGroupIdentity
	RetainedBytesPeak  uint64
	CostPerSecondMilli uint64
}

// CostEntry is what the ledger holds for one Query Group on one Worker.
type CostEntry struct {
	QueryGroupCostReport
	WorkerID   string
	ReportedAt time.Time
}

// CostLedger is the Control Leader's memory of what each Worker reported
// its Query Groups cost, the placement side of decision-020 section 5.2.
// Entries are keyed by Worker and Query Group and replaced by the latest
// report. A Worker reports only readings that moved and never a Query
// Group it no longer runs, so an entry is cleared by the assignment roster
// (Retain) and never by its absence from a later heartbeat; between a move
// and the new holder's first heartbeat the Query Group has no reading, and
// the readings say so rather than carry the old holder's number over.
type CostLedger struct {
	mu      sync.Mutex
	now     func() time.Time
	entries map[string]map[execution.QueryGroupIdentity]CostEntry
}

func NewCostLedger(now func() time.Time) *CostLedger {
	if now == nil {
		now = time.Now
	}
	return &CostLedger{now: now, entries: map[string]map[execution.QueryGroupIdentity]CostEntry{}}
}

// Record replaces the Worker's entries for the Query Groups reported.
func (ledger *CostLedger) Record(workerID string, reports []QueryGroupCostReport) {
	if ledger == nil || workerID == "" || len(reports) == 0 {
		return
	}
	at := ledger.now()
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	byGroup := ledger.entries[workerID]
	if byGroup == nil {
		byGroup = map[execution.QueryGroupIdentity]CostEntry{}
		ledger.entries[workerID] = byGroup
	}
	for _, report := range reports {
		if report.QueryGroup == "" {
			continue
		}
		byGroup[report.QueryGroup] = CostEntry{QueryGroupCostReport: report, WorkerID: workerID, ReportedAt: at}
	}
}

// Retain keeps only the entries the roster agrees with - the Query Group is
// desired on the Worker that reported it - and drops the rest: a Query
// Group that moved, retired, or was reported by a Worker that no longer
// owns it. Called once per Leader round with the round's final owners.
func (ledger *CostLedger) Retain(owners map[execution.QueryGroupIdentity]string) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	for workerID, byGroup := range ledger.entries {
		for queryGroup := range byGroup {
			if owners[queryGroup] != workerID {
				delete(byGroup, queryGroup)
			}
		}
		if len(byGroup) == 0 {
			delete(ledger.entries, workerID)
		}
	}
}

// Readings is what the byte constraint is judged from this round: each
// Worker's pool as its registration carries it, and each Query Group's
// peak as reported by the Worker the owners name for it. A report from
// any other Worker is not the running holder's and is not read.
func (ledger *CostLedger) Readings(
	owners map[execution.QueryGroupIdentity]string,
	workers []ownership.WorkerRegistration,
) ByteReadings {
	readings := ByteReadings{Pool: map[string]uint64{}, Peak: map[execution.QueryGroupIdentity]uint64{}}
	for _, worker := range workers {
		if worker.Load != nil && worker.Load.RetainedPoolBytes > 0 {
			readings.Pool[worker.WorkerID] = worker.Load.RetainedPoolBytes
		}
	}
	if ledger == nil {
		return readings
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	for queryGroup, owner := range owners {
		if entry, reported := ledger.entries[owner][queryGroup]; reported {
			readings.Peak[queryGroup] = entry.RetainedBytesPeak
		}
	}
	return readings
}

// Entries is every entry the ledger holds, ordered by Worker then Query
// Group, for a reader that lists costs across the fleet.
func (ledger *CostLedger) Entries() []CostEntry {
	if ledger == nil {
		return nil
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	var entries []CostEntry
	for _, byGroup := range ledger.entries {
		for _, entry := range byGroup {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].WorkerID != entries[right].WorkerID {
			return entries[left].WorkerID < entries[right].WorkerID
		}
		return entries[left].QueryGroup < entries[right].QueryGroup
	})
	return entries
}
