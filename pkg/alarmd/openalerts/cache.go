// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// UnavailableReason is why the latest read did not yield an authoritative
// publication. Closed: a metric label.
type UnavailableReason string

const (
	UnavailableReadError UnavailableReason = "read_error"
	// UnavailableMembersDisjoint: the consumer's sets hold members, or were
	// read, but none of the alerts this process sent ABNORMAL for is in
	// them once the consumer has had time to open it. The sets are then not
	// keyed the way this process asks, and every lookup would miss; see
	// DisjointMinimum.
	UnavailableMembersDisjoint UnavailableReason = "members_disjoint"
)

// UnavailableReasons lists every reason, for the metric that pre-creates
// them all: a reason at zero has to be readable as "never happened".
var UnavailableReasons = []UnavailableReason{UnavailableReadError, UnavailableMembersDisjoint}

// SentConfirmAfter is how long after this process first sent an alert's
// ABNORMAL a read of the consumer's set is expected to carry it. The
// consumer opens the alert on the message and rebuilds the set on a hint
// it batches for about a second; five minutes covers that and a slow
// rebuild several times over. An alert younger than this at the read is
// not counted either way.
const SentConfirmAfter = 5 * time.Minute

// DisjointMinimum is how many alerts this process sent, each past
// SentConfirmAfter at the latest read and none of them found, before the
// sets are taken to be keyed differently from this process's lookups.
//
// One is enough. An alert the consumer closed on its own can put a quiet
// deployment into the state wrongly, and the price of that is the gate as
// it was before it existed: a RECOVERY for an alert the consumer no longer
// holds, which it records as orphaned and changes nothing for. A higher
// bar would leave a deployment with one or two alerts outside the fallback
// for good, holding exactly the recoveries it exists to release.
//
// Leaving the state takes positive evidence only: an alert of ours found
// in a set, or nothing of ours left open. The count dropping does not end
// it, because the recoveries the fallback lets through are what make it
// drop; ending on that would hold the last few again against sets that
// still carry none of ours.
const DisjointMinimum = 1

// Answer is how a lookup was answered. Closed: a metric label. The index
// members and absences are the consumer's word; the rest say the copy
// answered on its own and why, so that a gate working from the copy's own
// knowledge shows up as such and not as the consumer's word.
type Answer string

const (
	// AnswerRecentlySent: the publication does not carry the fingerprint but
	// this process sent its ABNORMAL within the publisher's lag. Counted apart
	// from member because it is the copy's word, not the consumer's.
	AnswerRecentlySent Answer = "recently_sent"
	// AnswerSelfMaintained: the publication is unavailable and the copy
	// answered from what it last read plus what this process sent.
	AnswerSelfMaintained Answer = "self_maintained"
	// AnswerPassedThrough: the publication is unavailable and the policy is
	// to let every recovery go, as before the gate existed.
	AnswerPassedThrough Answer = "passed_through"
	AnswerIndexMember   Answer = "index_member"
	AnswerIndexAbsent   Answer = "index_absent"
)

// Answers lists every Answer, for the metric that pre-creates them all.
var Answers = []Answer{AnswerRecentlySent, AnswerSelfMaintained, AnswerPassedThrough, AnswerIndexMember, AnswerIndexAbsent}

// UnavailablePolicy is what the copy answers while the publication is
// unavailable. It is one decision point on purpose, because the two answers
// fail in opposite directions and which one a deployment wants is a ruling,
// not an implementation default.
//
// Self-maintain (the ruling in force, 2026-09-14): answer from the last
// publication plus what this process sent. It keeps the gate working through
// an outage, at the price of holding recoveries for alerts this process did
// not open itself (opened before it started, or by another worker before a
// rebalance) until the publication returns. Pass-through: let every recovery
// go, which is the behaviour before the gate; its price is the orphan
// resolutions the gate exists to stop, for the length of the outage. Neither
// holds everything: that would turn one dependency's outage into a platform
// where no alert resolves.
type UnavailablePolicy string

const (
	PolicySelfMaintain UnavailablePolicy = "self_maintain"
	PolicyPassThrough  UnavailablePolicy = "pass_through"
)

// Stats is the copy's state and cumulative counts, read for metrics.
type Stats struct {
	Available         bool
	UnavailableReason UnavailableReason
	// LoadedAt is the oldest calibration among the tracked sets; zero if none
	// has been calibrated. A metric derived from it must not be emitted while
	// zero.
	LoadedAt                        time.Time
	Tracked                         int
	Loaded                          int
	Members                         int
	Added                           int
	Removed                         int
	Evictions                       uint64
	Refreshes                       map[string]uint64
	Unavailable                     map[UnavailableReason]uint64
	Lookups                         map[Answer]uint64
	IndexReadAt                     time.Time
	PendingReads, PendingReconciles int
	OldestPendingAt                 time.Time
	SubscriptionReady               bool
	MemberBytes                     int
	// CalibrationConfigured says a reconciler is bound: without one the
	// index knows members but never their severity, so no close is ever
	// sent, and a deployment has to be able to read that as "off" rather
	// than wonder why nothing closes.
	CalibrationConfigured bool
	Calibrated            int
	// SentInSet and SentNotInSet split the alerts this process sent
	// ABNORMAL for, and has not sent RECOVERY for, by whether the latest
	// read of their strategy's set carries them. Only alerts first sent at
	// least SentConfirmAfter before that read count. Disjoint is the state
	// DisjointMinimum describes.
	SentInSet, SentNotInSet int
	Disjoint                bool
	// OwnLookups is Lookups for the lookups of this process's own open
	// alerts; OwnHeld how many of those the gate answered "not open", which
	// holds a RECOVERY whose alert stays open. RecentLookups and
	// RecentOwnHeld are the last RecentGateLookups of each, whole.
	OwnLookups    map[Answer]uint64
	OwnHeld       uint64
	RecentLookups []GateLookup
	RecentOwnHeld []GateLookup
	// GateSince is when the own split started: what this process sent is
	// held in memory and starts empty at every start, so an alert opened
	// before it is not "own" here, and "no own lookup" says only that none
	// of the alerts sent since reached the gate.
	GateSince time.Time
	// SentDepartures counts why alerts left Added since the process
	// started, every path in SentDepartures present. OwnOpen is how many
	// alerts this process opened and has not sent the RECOVERY for --
	// the alerts the gate treats as its own -- and OwnOpenDepartures why
	// alerts left that record; OwnOpenRefusals how many times a first
	// ABNORMAL found the record full -- a count of refusals, not of alerts:
	// an alert still firing is refused again every round it is sent.
	SentDepartures    map[string]uint64
	OwnOpen           int
	OwnOpenDepartures map[string]uint64
	OwnOpenRefusals   uint64
	// RecoveriesResent counts RECOVERY events the broker took for an alert
	// whose earlier RECOVERY the ledger still held: sent again because the
	// consumer's set still carried the alert once the ledger let it through.
	RecoveriesResent uint64
}

type member struct {
	key         StrategyKey
	fingerprint string
}

type stamped struct {
	at time.Time
	// resent is a RECOVERY whose alert the ledger already held closed when
	// the broker took it: the recovery went out again. Such an entry waits
	// longer before it lets the alert through again (removalHides), so a
	// consumer that is behind gets each recovery at most once more.
	resent bool
	// severity is, in removed, the severity the RECOVERY closed the alert
	// at: while the set still carries the alert, it stands there (standing).
	severity string
}

// Cache is this process's copy of the consumer's open alert set. It
// implements contract.OpenAlertSet.
type Cache struct {
	index  *indexState
	mu     sync.Mutex
	now    func() time.Time
	policy UnavailablePolicy
	// maxLocal bounds the fingerprints kept from this process's own sends.
	// Past it the oldest is evicted and counted.
	maxLocal int

	added   map[member]stamped
	removed map[member]stamped

	evictions uint64
	// recoveriesResent: see Stats.RecoveriesResent.
	recoveriesResent uint64
	// sentDepartures and openDepartures count why alerts left added and
	// index.opened; openRefusals the times index.opened was full when an
	// alert not in it was sent -- refusals, not alerts.
	// See departures.go.
	sentDepartures, openDepartures map[string]uint64
	openRefusals                   uint64
	refreshes                      map[string]uint64
	unavailable                    map[UnavailableReason]uint64
	lookups                        map[Answer]uint64
	// lastAnswer, ownLookups, ownHeld and the two samples are the gate's
	// lookups as recordGate keeps them.
	lastAnswer    Answer
	ownLookups    map[Answer]uint64
	ownHeld       uint64
	recentLookups gateRing
	recentOwnHeld gateRing
	// gateSince is when this copy was made: the own split rests on what
	// this process sent (index.opened, added), which lives in memory and
	// starts empty with the process.
	gateSince time.Time
}

// Track registers strategies to read on the next refresh; see TrackOwned,
// whose refusal it does not report.
func (cache *Cache) Track(keys ...StrategyKey) {
	if cache == nil {
		return
	}
	_ = cache.TrackOwned(keys...)
}

// Contains implements contract.OpenAlertSet. See Answer for how it answers.
// Every answer is also recorded for reading (see recordGate).
func (cache *Cache) Contains(tenantID, strategyID, fingerprint string) bool {
	if cache == nil {
		return false
	}
	key := StrategyKey{TenantID: tenantID, StrategyID: strategyID}
	m := member{key: key, fingerprint: fingerprint}
	now := cache.now()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	open := cache.indexGate(m, now)
	cache.recordGate(m, now, open)
	return open
}

// Acknowledged records what this process sent once the sink has taken it,
// as the consumer will apply it (nextStanding): a trigger opens the
// fingerprint in the copy, or keeps it open at the severity the alert now
// stands at; a RECOVERY closes it only when it resolves that severity. A
// RECOVERY for any other Level leaves the alert open, as it does in the
// consumer. Only envelopes the consumer will see count: a
// compatibility-protocol envelope goes to another consumer, and one without
// a fingerprint opens nothing. Called after the broker ACK and never
// before, or a batch the sink refused would move the copy for alerts that
// were never opened.
func (cache *Cache) Acknowledged(events []contract.TriggerEventV1) {
	if cache == nil || len(events) == 0 {
		return
	}
	now := cache.now()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for _, event := range events {
		if event.DedupeMD5 == "" || event.StrategyRef == nil || event.LegacyOutput != nil {
			continue
		}
		m := member{key: StrategyKey{TenantID: event.TenantID, StrategyID: event.PlanRef.StrategyID}, fingerprint: event.DedupeMD5}
		if cache.index.entries[m.key] == nil {
			continue
		}
		wasOpen, severity := cache.standing(m, now)
		open, next, triggered := nextStanding(wasOpen, severity, event.LevelResults)
		switch {
		case triggered:
			cache.added[m] = stamped{at: now}
			delete(cache.removed, m)
			cache.noteOpened(m, now, next)
		case wasOpen && !open:
			// A RECOVERY for an alert the ledger still holds closed is the
			// same recovery sent again; the entry remembers it. Departures
			// do not count it: the alert left added and opened with the
			// first one, and both leave* are no-ops for it now.
			_, resending := cache.removed[m]
			if resending {
				cache.recoveriesResent++
			}
			cache.removed[m] = stamped{at: now, resent: resending, severity: severity}
			cache.leaveSent(m, DepartureRecoveryAcked)
			cache.leaveOpen(m, DepartureRecoveryAcked)
		}
	}
	cache.boundLocal()
}

// boundLocal keeps added plus removed inside maxLocal by evicting the
// oldest. Called with the lock held.
func (cache *Cache) boundLocal() {
	over := len(cache.added) + len(cache.removed) - cache.maxLocal
	if over <= 0 {
		return
	}
	type aged struct {
		m       member
		at      time.Time
		removed bool
	}
	all := make([]aged, 0, len(cache.added)+len(cache.removed))
	for m, s := range cache.added {
		all = append(all, aged{m: m, at: s.at})
	}
	for m, s := range cache.removed {
		all = append(all, aged{m: m, at: s.at, removed: true})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	for _, entry := range all[:over] {
		if entry.removed {
			delete(cache.removed, entry.m)
		} else {
			cache.leaveSent(entry.m, DepartureEvicted)
		}
		cache.evictions++
	}
}

// Refresh runs one round of index reads, and of calibrations when a
// reconciler is bound, for the tracked strategies; see refreshIndex.
func (cache *Cache) Refresh(ctx context.Context) {
	if cache == nil {
		return
	}
	cache.refreshIndex(ctx)
}

// Stats reads the copy's state and cumulative counts.
func (cache *Cache) Stats() Stats {
	if cache == nil {
		return Stats{}
	}
	return cache.indexStats()
}

// StaleBeyondBound reports whether a set calibrated once has gone without a
// calibration for longer than CalibrationMaxAge. That is the shape fleet
// health degrades on: the gate is working from the copy's own knowledge past
// the exposure it was designed for. A set never calibrated is not stale: the
// reconciler may not be bound, and CalibrationConfigured says so on its own.
func (cache *Cache) StaleBeyondBound() bool {
	if cache == nil {
		return false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for _, entry := range cache.index.entries {
		if !entry.calibratedAt.IsZero() && !cache.calibrated(entry, cache.now()) {
			return true
		}
	}
	return false
}

var _ contract.OpenAlertSet = (*Cache)(nil)
