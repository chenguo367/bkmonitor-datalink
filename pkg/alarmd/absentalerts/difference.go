// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

// Package absentalerts answers one question the rest of the program cannot:
// which strategies still hold unrecovered alerts after the strategy itself
// stopped existing.
//
// Every other close path starts from a Plan. A replica owns a Query Group,
// the Query Group holds the strategy's Plan, and the Plan says whether the
// strategy is inactive right now. A strategy that was disabled or deleted
// has no Plan and no owner, so no replica is looking at it, and its alerts
// stay open with nothing left to recover them. The difference is therefore
// taken where the whole strategy snapshot is in hand, which is the control
// leader, and the close is pushed from there.
//
// The difference is the alert link's roster minus the strategy snapshot:
// every strategy the link says holds an unrecovered alert, less every
// strategy the source says exists. The roster is the link's own list, read
// through its Console, walked by the link on its own connection. This exists
// only where the link does: a deployment without it has no roster, and
// nothing here runs.
//
// What makes a strategy in that difference safe to close is not the roster.
// The roster can only err by missing a strategy or listing one twice - both
// cost a close, neither invents one - and a close the link receives for an
// alert that is no longer active is a no-op on its side. The side that can
// close a live strategy's alerts is the snapshot: a snapshot read that loses
// entries makes live strategies look deleted, and this deployment has seen
// that for minutes at a time. So the gates are on the snapshot:
//
//   - the snapshot was observed, is not empty, and is not old;
//   - it did not shrink against the round before or against what the
//     catalog is running;
//   - the catalog is not running a Plan of the strategy;
//   - this loop has seen the strategy missing for the grace, under two
//     different observations of the source.
//
// The link's own health is a gate as well, because a roster from a link
// whose maintenance has stopped says nothing current, and the round that
// reads it decides nothing rather than something stale.
package absentalerts

import (
	"sort"
	"time"
)

// Key identifies a strategy in both sets being compared: the alert link's
// roster and the source snapshot.
type Key struct {
	TenantID   string
	StrategyID string
}

// Identity is what a close needs about a strategy beyond its key: the
// business it belonged to and the snapshot revision. The catalog remembers
// it for the strategies it let go while this process was watching; for the
// rest it is read from the alert's own record at close time, which is where
// the labels the alert was created with are still written down.
type Identity struct {
	BusinessID int64
	Revision   int64
}

// Absence is this loop's own memory of a candidate: when it first found the
// strategy missing from the snapshot, and which observation found it. The
// observation is kept because the source reconciler reuses one observation
// across rounds, and a candidate confirmed twice against one observation was
// confirmed once.
type Absence struct {
	Since       time.Time
	Observation string
}

// Round is one difference and everything it may decide on.
type Round struct {
	// LinkRead says the link's roster was read at all this round. Without it
	// there is no difference to take.
	LinkRead bool
	// Roster is every strategy the link listed with at least one member in
	// its open alert set.
	Roster map[Key]struct{}
	// RosterUnreadable is how many listed strategies the link could not read
	// the set of. Not in Roster, because not read is not empty.
	RosterUnreadable int
	// RosterComplete says the walk reached its end. An incomplete walk is a
	// smaller roster, which costs closes and never makes one.
	RosterComplete bool
	// LinkLastSuccess and LinkError are the link's account of its own set
	// maintenance: when a full discovery last succeeded, and why the latest
	// one failed if it did.
	LinkLastSuccess time.Time
	LinkError       string
	// SnapshotStrategies is every strategy the source says exists right now.
	// SnapshotUsable says it was observed rather than assumed, and
	// SnapshotObservation identifies which observation it is.
	SnapshotStrategies  map[Key]struct{}
	SnapshotUsable      bool
	SnapshotObservation string
	SnapshotAgeSeconds  int64
	// PreviousSnapshotStrategies is how large the snapshot was when this
	// loop last decided on one. Zero means there is no round to compare
	// against, which is the first round of a leader term.
	PreviousSnapshotStrategies int
	// Published is every strategy with a Plan in the publication the control
	// plane currently runs. A strategy here is executing, whatever the
	// snapshot says, and is never closed.
	Published map[Key]struct{}
	// Identities is what the catalog remembers about the strategies it let
	// go. A candidate not here is still closed; its identity is read from its
	// alerts.
	Identities map[Key]Identity
	// FirstAbsent is this loop's memory of when each candidate was first
	// found missing by this loop itself.
	FirstAbsent map[Key]Absence
	Now         time.Time
}

// Absent is one strategy the round decided to close.
type Absent struct {
	Key Key
	// Identity is zero when the catalog does not remember the strategy; the
	// close then reads it from the alert's own record.
	Identity Identity
	// AbsentSince is when this loop first found the strategy missing.
	AbsentSince time.Time
}

// The refusals of a whole round. Each names the fact that was not good
// enough to decide on, because "closed nothing" has to be answerable with
// why.
const (
	// RefusalNone is a round that decided. It is a word rather than an empty
	// string so that the metric has a cell for it: how many rounds decided
	// is the denominator every refusal count is read against.
	RefusalNone = "none"
	// RefusalLinkUnavailable: the link's roster could not be read. Nothing
	// read is not an empty roster.
	RefusalLinkUnavailable = "link_unavailable"
	// RefusalLinkUnhealthy: the roster was read, and the link itself says the
	// process maintaining it is failing or has not succeeded recently. A
	// roster the link is not keeping current is not one to act on.
	RefusalLinkUnhealthy = "link_unhealthy"
	// RefusalSnapshotUnusable: the snapshot was not observed this round. An
	// unread snapshot is not an empty one, and reading it as empty would
	// close every unrecovered alert in the deployment.
	RefusalSnapshotUnusable = "snapshot_unusable"
	// RefusalSnapshotEmpty: the snapshot was observed and holds no strategy
	// at all. On a live deployment that is a source that lost its content,
	// not a deployment without strategies.
	RefusalSnapshotEmpty = "snapshot_empty"
	// RefusalSnapshotStale: the observation is older than this round may
	// decide on. Strategies created since it was read would read as absent.
	RefusalSnapshotStale = "snapshot_stale"
	// RefusalSnapshotShrunk: the snapshot itself lost a large share of its
	// strategies since the round before, or against the objects the catalog
	// is running. The gate is on the input and not on the difference: the
	// difference is an output filtered by "still holds an unrecovered
	// alert", so a badly truncated snapshot whose lost strategies happened
	// to have no open alerts produces a small difference and reads as a
	// healthy round, while an honest backlog of deleted strategies - the
	// state this capability exists for - produces a large one.
	RefusalSnapshotShrunk = "snapshot_shrunk"
)

// Refusals is the closed list of whole-round refusals.
var Refusals = []string{RefusalNone, RefusalLinkUnavailable, RefusalLinkUnhealthy, RefusalSnapshotUnusable,
	RefusalSnapshotEmpty, RefusalSnapshotStale, RefusalSnapshotShrunk}

// The per-strategy outcomes of the decision. Every candidate the round did
// not close lands on one of these, and every one of them is counted.
const (
	// OutcomeClosed: absent from the snapshot for the grace under two
	// observations, not running a Plan, and within this round's bound.
	OutcomeClosed = "closed"
	// OutcomeStillPublished: absent from the snapshot, but the catalog is
	// still running a Plan of it. The strategy is detecting; the snapshot
	// read is the side that is wrong.
	OutcomeStillPublished = "still_published"
	// OutcomeWithinGrace: absent, but not for long enough by this loop's own
	// clock. A leader that has just been elected cannot close on its first
	// round.
	OutcomeWithinGrace = "within_grace"
	// OutcomeUnconfirmed: absent long enough, but every round that found it
	// absent read the same observation of the source.
	OutcomeUnconfirmed = "unconfirmed"
	// OutcomeDeferred: decided, but this round's close bound was already
	// spent. Counted per strategy; the next round takes it.
	OutcomeDeferred = "deferred"
	// OutcomeIndexUnreadable: the link listed the strategy and could not read
	// its set. Counted per strategy and per round, and never folded into "it
	// has no alerts".
	OutcomeIndexUnreadable = "index_unreadable"
)

// The outcomes of carrying a decision out. They are counted in different
// units, which the metric's help has to say.
const (
	// OutcomeIdentityUnknown: no business could be found for the strategy -
	// the catalog does not remember it and none of its alerts' records names
	// one. Named rather than guessed: a close sent to the wrong business is
	// worse than an alert that stays open. Per strategy.
	OutcomeIdentityUnknown = "identity_unknown"
	// OutcomeRevisionUnknown: the business is known and the snapshot
	// revision is not. The close contract requires one. Per strategy.
	OutcomeRevisionUnknown = "revision_unknown"
	// OutcomeEvidenceUnavailable: the strategy's alerts could not be read
	// from the link, so there is nothing to address a close to. Per
	// strategy.
	OutcomeEvidenceUnavailable = "evidence_unavailable"
	// OutcomeAlertClosed: an alert the broker acknowledged a close for. Per
	// alert, where OutcomeClosed counts strategies.
	OutcomeAlertClosed = "alert_closed"
	// OutcomeSendFailed: the producer did not acknowledge the batch. Per
	// alert.
	OutcomeSendFailed = "send_failed"
	// OutcomeWouldSend: an alert a round decided on and did not send,
	// because the deployment has not armed the close. Per alert, so that
	// what arming would do is a number and not an estimate from the strategy
	// counts.
	OutcomeWouldSend = "would_send"
	// OutcomeNotLeader: this replica is not the control leader and takes no
	// difference. Counted so that a deployment where no replica ever ran a
	// round is not read as a deployment with nothing to close.
	OutcomeNotLeader = "not_leader"
	// OutcomeMemoryFull: a candidate the bounded memory would not hold. Such
	// a strategy is never closed, so the bound being reached is a reading.
	OutcomeMemoryFull = "memory_full"
	// OutcomeProducerForeign: an alert another producer wrote. The link's
	// set is keyed by strategy, not by who created the alert, and a set
	// shared by several sources holds all of theirs. Per alert, never
	// closed.
	OutcomeProducerForeign = "producer_foreign"
	// OutcomeProducerUnknown: an alert that names no producer. Counted and
	// never closed: "it does not say it is someone else's" is not "it is
	// ours".
	OutcomeProducerUnknown = "producer_unknown"
)

// Outcomes is the closed list, for the metric that reports every cell.
var Outcomes = []string{OutcomeClosed, OutcomeStillPublished, OutcomeWithinGrace, OutcomeUnconfirmed,
	OutcomeDeferred, OutcomeIndexUnreadable, OutcomeIdentityUnknown, OutcomeRevisionUnknown,
	OutcomeEvidenceUnavailable, OutcomeAlertClosed, OutcomeSendFailed, OutcomeWouldSend,
	OutcomeNotLeader, OutcomeMemoryFull, OutcomeProducerForeign, OutcomeProducerUnknown}

// Counts is what the round measured. The denominators are reported with
// every reading, because a zero means different things - nothing was
// absent, or nothing was decided - and without them a reader cannot tell a
// healthy deployment from a difference that never ran.
type Counts struct {
	Roster                     int
	RosterUnreadable           int
	SnapshotStrategies         int
	PublishedStrategies        int
	PreviousSnapshotStrategies int
	// Candidates is the difference this round acts on: listed by the link,
	// not listed by the snapshot.
	Candidates     int
	Closed         int
	StillPublished int
	WithinGrace    int
	Unconfirmed    int
	Deferred       int
}

// Bounds are the gates a round is decided under.
type Bounds struct {
	// Grace is how long this loop must have seen the strategy missing
	// before closing its alerts.
	Grace time.Duration
	// MaxSnapshotAge is how old the observation may be. A snapshot older
	// than this has not seen the strategies created since, and every one of
	// them holding an alert would read as absent.
	MaxSnapshotAge time.Duration
	// MaxLinkHealthAge is how long ago the link's last successful full
	// discovery may be. Older, the link's maintenance has stopped and its
	// roster is not current.
	MaxLinkHealthAge time.Duration
	// MaxSnapshotShrinkRatio is how much smaller this round's snapshot may
	// be than the round before's, or than the number of strategies the
	// catalog is running, before the round is refused.
	MaxSnapshotShrinkRatio float64
	// MinSnapshotForShrink is the size below which the shrink ratio says
	// nothing: on a deployment with four strategies one deletion is
	// twenty-five percent.
	MinSnapshotForShrink int
	// MaxCloseStrategies bounds how many strategies one round closes, so a
	// backlog is worked off over rounds instead of in one batch.
	MaxCloseStrategies int
}

// Result is the round's decision. Close is what the round decided to
// close, which is not the same as what it sent: arming the send is the
// caller's, and the two are counted apart so that a deployment can read
// what the difference would do before it does it.
type Result struct {
	Close   []Absent
	Counts  Counts
	Refusal string
}

// Candidates is the first half of the difference: the strategies with
// unrecovered alerts that the snapshot does not list, or the refusal that
// stopped the round before it named any. It is separate from the decision
// so that the caller can start a candidate's grace clock before the
// decision reads it, without the decision itself mutating memory.
func Candidates(round Round, bounds Bounds) ([]Key, Counts, string) {
	counts := Counts{Roster: len(round.Roster), RosterUnreadable: round.RosterUnreadable,
		SnapshotStrategies: len(round.SnapshotStrategies), PublishedStrategies: len(round.Published),
		PreviousSnapshotStrategies: round.PreviousSnapshotStrategies}
	if !round.LinkRead {
		return nil, counts, RefusalLinkUnavailable
	}
	if linkUnhealthy(round, bounds) {
		return nil, counts, RefusalLinkUnhealthy
	}
	if !round.SnapshotUsable || round.SnapshotObservation == "" {
		return nil, counts, RefusalSnapshotUnusable
	}
	if len(round.SnapshotStrategies) == 0 {
		return nil, counts, RefusalSnapshotEmpty
	}
	if bounds.MaxSnapshotAge > 0 && time.Duration(round.SnapshotAgeSeconds)*time.Second > bounds.MaxSnapshotAge {
		return nil, counts, RefusalSnapshotStale
	}
	if shrunk(counts, bounds) {
		return nil, counts, RefusalSnapshotShrunk
	}
	candidates := make([]Key, 0)
	for key := range round.Roster {
		if _, present := round.SnapshotStrategies[key]; present {
			continue
		}
		candidates = append(candidates, key)
	}
	counts.Candidates = len(candidates)
	sortKeys(candidates)
	return candidates, counts, RefusalNone
}

// Compute takes the whole difference. It reads nothing and writes nothing:
// the caller supplies both sides and the memory, and applies what comes
// back.
func Compute(round Round, bounds Bounds) Result {
	candidates, counts, refusal := Candidates(round, bounds)
	if refusal != RefusalNone {
		return Result{Counts: counts, Refusal: refusal}
	}
	result := Result{Counts: counts, Refusal: RefusalNone}
	for _, key := range candidates {
		if _, published := round.Published[key]; published {
			result.Counts.StillPublished++
			continue
		}
		absence, tracked := round.FirstAbsent[key]
		if !tracked || round.Now.Sub(absence.Since) < bounds.Grace {
			result.Counts.WithinGrace++
			continue
		}
		if absence.Observation == round.SnapshotObservation {
			result.Counts.Unconfirmed++
			continue
		}
		if bounds.MaxCloseStrategies > 0 && len(result.Close) >= bounds.MaxCloseStrategies {
			result.Counts.Deferred++
			continue
		}
		result.Close = append(result.Close, Absent{Key: key, Identity: round.Identities[key], AbsentSince: absence.Since})
		result.Counts.Closed++
	}
	return result
}

// linkUnhealthy reads the link's own account of its maintenance. A latest
// discovery that failed, one that never succeeded, and one that last
// succeeded longer ago than the bound all say the roster is not current.
func linkUnhealthy(round Round, bounds Bounds) bool {
	if round.LinkError != "" || round.LinkLastSuccess.IsZero() {
		return true
	}
	return bounds.MaxLinkHealthAge > 0 && round.Now.Sub(round.LinkLastSuccess) > bounds.MaxLinkHealthAge
}

// shrunk gates on the input: the strategy list itself, against what it was
// and against what the catalog is running. Either comparison is enough, and
// both are denominators a reader is given, so "the snapshot did not shrink"
// and "nothing compared it" are different readings.
func shrunk(counts Counts, bounds Bounds) bool {
	if bounds.MaxSnapshotShrinkRatio <= 0 {
		return false
	}
	against := func(before int) bool {
		if before < max(bounds.MinSnapshotForShrink, 1) || counts.SnapshotStrategies >= before {
			return false
		}
		return float64(before-counts.SnapshotStrategies)/float64(before) > bounds.MaxSnapshotShrinkRatio
	}
	return against(counts.PreviousSnapshotStrategies) || against(counts.PublishedStrategies)
}

func sortKeys(keys []Key) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].TenantID != keys[j].TenantID {
			return keys[i].TenantID < keys[j].TenantID
		}
		return keys[i].StrategyID < keys[j].StrategyID
	})
}
