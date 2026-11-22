// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/activecopy"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/lookback"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// A Query Group's handover ledger is what the process that lets it go leaves
// for the next owner, under the Query Group's fence, before it releases the
// lease; the next owner reads it once it holds the lease, before the Query
// Group's first Slot. It holds what is lost with the process otherwise: the
// recovery gate's record of the alerts this process opened (activecopy.Own),
// on a deployment without the link - without it the next owner sees none of
// those alerts and holds every one's RECOVERY for good; and the Query
// Group's lookback, its Slots on their reread ladder and its series_late
// state (lookback.Handover) - without it each of those Slots' late series is
// never supplemented, and the Query Group read directed again only once a
// plan or its own samples say so.
//
// The ledger is advice, not evidence: one that is not there, or not read,
// leaves the next owner as it was before the ledger existed. One is read
// only when it was written by the owner just before this one - its owner
// epoch the one before the epoch this process holds the Query Group under.
// An older one was left by an owner before the previous one, which left
// none (it crashed, or its write failed), and what it says may have been
// ended since.
//
// A Query Group this process rebuilds - released and acquired again here, one
// epoch later, nobody in between - keeps its record in the process, and its
// ledger is not read; one this process lost the lease of and acquires again
// at the next epoch is the same. The process tells them from a takeover by
// the epoch it last held each Query Group under, kept until it lets the
// Query Group go.
const (
	handoverLedgerNamespace = "handover"
	handoverLedgerField     = "ledger"
	handoverLedgerVersion   = 1
	// handoverLedgerLife is how long a ledger nobody reads stays: the owner
	// epoch decides whether one is current, and the life only lets a
	// retired Query Group's go.
	handoverLedgerLife = 24 * time.Hour
)

// handoverLedgerStore is the ownership store's fenced hash write and its
// batched field read.
type handoverLedgerStore interface {
	FencedHashUpdate(context.Context, ownership.FencedHashRequest) error
	ReadControlHashFieldBatch(context.Context, []execution.QueryGroupIdentity, string, string) ([]ownership.ControlRead, error)
}

type handoverLedgerValue struct {
	Version     int                    `json:"version"`
	OwnerEpoch  uint64                 `json:"owner_epoch"`
	WrittenAtMs int64                  `json:"written_at_ms"`
	Own         []activecopy.OwnRecord `json:"own,omitempty"`
	Lookback    *lookback.Handover     `json:"lookback,omitempty"`
}

type productionHandoverLedger struct {
	store handoverLedgerStore
	// own is the recovery gate's record, nil where the gate reads the link's
	// copy; lookback the late-data lookback, nil where none runs.
	own      *activecopy.Own
	lookback *lookback.Engine
	now      func() time.Time
	record   func(result string, exported, imported int)
	mu       sync.Mutex
	groups   map[execution.QueryGroupIdentity]*handoverLedgerGroup
	// pending is each Query Group's lookback as it was forgotten on its way
	// out (holdLookback), until its ledger is written.
	pending map[execution.QueryGroupIdentity]*lookback.Handover
}

// handoverLedgerGroup is what the ledger keeps of a Query Group this process
// holds or held: the session it holds it under now (nil between a release
// and the next acquisition), the owner epoch it last held it under, and the
// strategies its Slots evaluated here or its ledger named. Kept across a
// rebuild; let go of when the process lets the Query Group go.
type handoverLedgerGroup struct {
	session    *ownership.Session
	held       uint64
	strategies map[activecopy.StrategyKey]struct{}
}

func newProductionHandoverLedger(store handoverLedgerStore, own *activecopy.Own, engine *lookback.Engine, now func() time.Time,
	record func(string, int, int)) *productionHandoverLedger {
	if store == nil || own == nil && engine == nil {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	if record == nil {
		record = func(string, int, int) {}
	}
	return &productionHandoverLedger{store: store, own: own, lookback: engine, now: now, record: record,
		groups:  map[execution.QueryGroupIdentity]*handoverLedgerGroup{},
		pending: map[execution.QueryGroupIdentity]*lookback.Handover{}}
}

// holdLookback keeps a Query Group's lookback as the lookback forgot it on
// its way out - moved away, declined, stopping (lookback.ForgetHandingOver)
// - for its ledger, written once its Slots here are done.
func (ledger *productionHandoverLedger) holdLookback(qg execution.QueryGroupIdentity, handover *lookback.Handover) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if handover == nil {
		delete(ledger.pending, qg)
		return
	}
	ledger.pending[qg] = handover
}

func (ledger *productionHandoverLedger) groupLocked(qg execution.QueryGroupIdentity) *handoverLedgerGroup {
	group := ledger.groups[qg]
	if group == nil {
		group = &handoverLedgerGroup{strategies: map[activecopy.StrategyKey]struct{}{}}
		ledger.groups[qg] = group
	}
	return group
}

// bind notes the session a Query Group was just acquired under.
func (ledger *productionHandoverLedger) bind(qg execution.QueryGroupIdentity, session *ownership.Session) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	ledger.groupLocked(qg).session = session
	// Held again: a lookback held on its way out of an earlier session went
	// nowhere, and is no longer this one's to hand over.
	delete(ledger.pending, qg)
	ledger.mu.Unlock()
}

// unbind lets go of the session as it is released, and of nothing else.
func (ledger *productionHandoverLedger) unbind(qg execution.QueryGroupIdentity, session *ownership.Session) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	if group := ledger.groups[qg]; group != nil && group.session == session {
		group.session = nil
	}
	ledger.mu.Unlock()
}

// notePlans notes the strategies a Slot of the Query Group evaluates here.
func (ledger *productionHandoverLedger) notePlans(qg execution.QueryGroupIdentity, keys []activecopy.StrategyKey) {
	if ledger == nil || len(keys) == 0 {
		return
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	group := ledger.groups[qg]
	if group == nil || group.session == nil {
		// A Slot of a session already let go of: nothing to hand over.
		return
	}
	for _, key := range keys {
		group.strategies[key] = struct{}{}
	}
}

// load reads the ledgers of the Query Groups just acquired here and takes
// what each current one holds; see productionHandoverLedger. Before their
// Runners start, so no Slot of theirs is evaluated without it.
func (ledger *productionHandoverLedger) load(ctx context.Context, queryGroups []execution.QueryGroupIdentity) {
	if ledger == nil || len(queryGroups) == 0 {
		return
	}
	read := make([]execution.QueryGroupIdentity, 0, len(queryGroups))
	epochs := make([]uint64, 0, len(queryGroups))
	ledger.mu.Lock()
	for _, qg := range queryGroups {
		group := ledger.groups[qg]
		if group == nil || group.session == nil {
			continue
		}
		lease, _ := group.session.Current()
		epoch := lease.Fence.OwnerEpoch
		if epoch == 0 || epoch == group.held {
			continue
		}
		rebuilt := group.held != 0 && epoch == group.held+1
		group.held = epoch
		if !rebuilt {
			read, epochs = append(read, qg), append(epochs, epoch)
		}
	}
	ledger.mu.Unlock()
	for start := 0; start < len(read); start += ownership.ControlReadBatch {
		end := min(start+ownership.ControlReadBatch, len(read))
		values, err := ledger.store.ReadControlHashFieldBatch(ctx, read[start:end], handoverLedgerNamespace, handoverLedgerField)
		for offset, qg := range read[start:end] {
			if err != nil {
				ledger.record(observability.HandoverLedgerReadFailed, 0, 0)
				continue
			}
			ledger.take(qg, epochs[start+offset], values[offset])
		}
	}
}

func (ledger *productionHandoverLedger) take(qg execution.QueryGroupIdentity, epoch uint64, value ownership.ControlRead) {
	switch {
	case value.Err != nil:
		ledger.record(observability.HandoverLedgerReadFailed, 0, 0)
		return
	case value.Missing:
		ledger.record(observability.HandoverLedgerAbsent, 0, 0)
		return
	}
	var decoded handoverLedgerValue
	if err := json.Unmarshal(value.Raw, &decoded); err != nil || decoded.Version != handoverLedgerVersion || decoded.OwnerEpoch == 0 {
		ledger.record(observability.HandoverLedgerUnreadable, 0, 0)
		return
	}
	if decoded.OwnerEpoch+1 != epoch {
		ledger.record(observability.HandoverLedgerStale, 0, 0)
		return
	}
	imported := ledger.own.Import(decoded.Own)
	ledger.lookback.Restore(qg, decoded.Lookback)
	ledger.mu.Lock()
	if group := ledger.groups[qg]; group != nil {
		for _, record := range decoded.Own {
			group.strategies[activecopy.StrategyKey{TenantID: record.TenantID, StrategyID: record.StrategyID}] = struct{}{}
		}
	}
	ledger.mu.Unlock()
	ledger.record(observability.HandoverLedgerLoaded, 0, imported)
}

// handOver writes the Query Group's ledger as this process lets it go, under
// the lease it still holds - after the Query Group's last Slot here, before
// the release - and lets go of what the ledger kept of it. A write that
// fails is counted and changes nothing else: the release goes on, and the
// next owner does as before the ledger.
func (ledger *productionHandoverLedger) handOver(ctx context.Context, qg execution.QueryGroupIdentity) {
	if ledger == nil {
		return
	}
	ledger.mu.Lock()
	group := ledger.groups[qg]
	delete(ledger.groups, qg)
	handover := ledger.pending[qg]
	delete(ledger.pending, qg)
	ledger.mu.Unlock()
	if group == nil || group.session == nil {
		return
	}
	lease, _ := group.session.Current()
	if lease.Fence.OwnerEpoch == 0 {
		return
	}
	keys := make([]activecopy.StrategyKey, 0, len(group.strategies))
	for key := range group.strategies {
		keys = append(keys, key)
	}
	records := ledger.own.Export(keys)
	if len(records) == 0 && handover == nil {
		ledger.record(observability.HandoverLedgerEmpty, 0, 0)
		return
	}
	raw, err := json.Marshal(handoverLedgerValue{Version: handoverLedgerVersion, OwnerEpoch: lease.Fence.OwnerEpoch,
		WrittenAtMs: ledger.now().UnixMilli(), Own: records, Lookback: handover})
	if err == nil {
		err = ledger.store.FencedHashUpdate(ctx, ownership.FencedHashRequest{Fence: lease.Fence, Namespace: handoverLedgerNamespace,
			Set: map[string][]byte{handoverLedgerField: raw}, TTL: handoverLedgerLife})
	}
	if err != nil {
		ledger.record(observability.HandoverLedgerWriteFailed, 0, 0)
		return
	}
	ledger.record(observability.HandoverLedgerWritten, len(records), 0)
}
