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
// Two facts have to agree before a strategy's alerts are closed, and they
// come from different places on purpose:
//
//   - the control plane's own catalog let the strategy go, which it does
//     only after the strategy was missing from the source for the removal
//     grace and across two observations; and
//   - the source snapshot this round read still does not list it.
//
// Either one alone would be enough to close alerts on strategies that are
// still detecting. A snapshot read that loses entries - which this
// deployment has seen, for minutes at a time - makes live strategies look
// deleted. And the catalog is not a list of existing strategies either: a
// strategy kept alive on its last good definition, one refused this round,
// and one inside its removal grace all have no newly compiled Plan, and
// look exactly like a deleted one from there.
//
// The difference is taken from the catalog's side, not the alert link's,
// and that is a cost decision with a correctness consequence. Walking the
// alert link for every strategy that holds an unrecovered alert means a
// cursor over the whole key space it shares - the cost of which follows the
// size of that database and not the number of alerts, and which on this
// deployment lands on the same connection and the same single-threaded
// server as the state reads, with no instrument able to tell the two apart.
// Starting from the strategies the catalog let go turns that walk into one
// point read per departed strategy.
//
// What it gives up is the strategies that were already gone before this
// control plane ever saw them: nothing remembers their business or their
// revision, so a close for them cannot be addressed either way, but with
// the walk they could at least be counted. That count is not worth a
// periodic pass over another service's key space. It is a named boundary,
// and closing it needs the alert link to expose either a roster of the
// strategies it holds alerts for, or the strategy labels each alert already
// carries.
package absentalerts

import (
	"sort"
	"time"
)

// Key identifies a strategy in both sets being compared: the alert link's
// per-strategy open alert index and the source snapshot.
type Key struct {
	TenantID   string
	StrategyID string
}

// Identity is what a close needs about a strategy beyond its key, and what
// the strategy can no longer be asked for once it is gone: the business it
// belonged to and the snapshot revision its Plan last carried. It is kept
// while the strategy still exists, because that is the only time it can be
// read.
type Identity struct {
	BusinessID int64
	Revision   int64
}

// Departure is a strategy the catalog let go, with the identity it had when
// it did.
type Departure struct {
	Identity Identity
	At       time.Time
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
	// Departed is what the catalog remembers about the strategies it let
	// go. These are the round's candidates: a strategy is here only after
	// the catalog stopped publishing it, which already required the removal
	// grace and two observations of the source.
	Departed map[Key]Departure
	// WithOpenAlerts is the departed strategies the alert link still holds
	// at least one unrecovered alert for, read one strategy at a time.
	WithOpenAlerts map[Key]struct{}
	// Unreadable is the departed strategies whose alert index could not be
	// read this round. Absent from WithOpenAlerts because nothing was read,
	// which is not the same as read and empty.
	Unreadable map[Key]struct{}
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
	// plane currently runs. A strategy here is executing, whatever any
	// memory says, and is never closed.
	Published map[Key]struct{}
	// FirstAbsent is this loop's memory of when each candidate was first
	// found missing by this loop itself.
	FirstAbsent map[Key]Absence
	Now         time.Time
}

// Absent is one strategy the round decided to close.
type Absent struct {
	Key      Key
	Identity Identity
	// AbsentSince is when the catalog let the strategy go. It travels with
	// the close so a reading can say how long the alerts outlived the
	// strategy, not only that they did.
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
	// is running. This is the gate on the input, and it is the one that
	// works: the difference is an output filtered by "still holds an
	// unrecovered alert", so a badly truncated snapshot whose lost
	// strategies happened to have no open alerts produces a small difference
	// and reads as a healthy round. The strategy list shrinking is the fact;
	// the difference is that fact seen through two filters.
	//
	// The removal grace covers a single round's flutter. This covers an
	// upstream that has been wrong for longer than the grace, which the
	// grace cannot see because by then every round agrees.
	RefusalSnapshotShrunk = "snapshot_shrunk"
	// RefusalDifferenceTooLarge: the difference is a large share of the
	// snapshot. A backstop under the gate above, not a substitute for it:
	// deletions come a few at a time, so a difference that is a large share
	// of the whole deployment is worth refusing even when the input gate saw
	// nothing. It can be fooled the way that gate cannot, which is why it is
	// second.
	RefusalDifferenceTooLarge = "difference_too_large"
)

// Refusals is the closed list of whole-round refusals.
var Refusals = []string{RefusalNone, RefusalSnapshotUnusable, RefusalSnapshotEmpty, RefusalSnapshotStale,
	RefusalSnapshotShrunk, RefusalDifferenceTooLarge}

// The per-strategy outcomes. Every candidate the round did not close lands
// on one of these, and every one of them is counted.
const (
	// OutcomeClosed: absent from the snapshot, let go by the catalog, and
	// identified.
	OutcomeClosed = "closed"
	// OutcomeStillPublished: absent from the snapshot, but the catalog is
	// still running a Plan of it. The strategy is detecting; the snapshot
	// read is the side that is wrong.
	OutcomeStillPublished = "still_published"
	// OutcomeWithinGrace: absent, but not for long enough by this loop's own
	// clock. The catalog's removal grace is not the only one: this loop
	// starts its own when it first sees the strategy missing, so a leader
	// that has just been elected cannot close on its first round.
	OutcomeWithinGrace = "within_grace"
	// OutcomeUnconfirmed: absent long enough, but every round that found it
	// absent read the same observation of the source.
	OutcomeUnconfirmed = "unconfirmed"
	// OutcomeIdentityUnknown: nothing remembers which business the strategy
	// belonged to, so the close cannot be addressed. Named rather than
	// guessed - a close sent to the wrong business is worse than an alert
	// that stays open - and expected for strategies that were already gone
	// before this deployment first ran the difference.
	OutcomeIdentityUnknown = "identity_unknown"
	// OutcomeRevisionUnknown: the business is known and the snapshot
	// revision is not, which is what a legacy source that publishes no
	// revision leaves behind. The close contract requires one.
	OutcomeRevisionUnknown = "revision_unknown"
)

// The outcomes of carrying a decision out. The ones above say what the
// difference decided about a strategy; these say what happened when the
// round tried to act on it, and they are counted in different units, which
// the metric's help has to say.
const (
	// OutcomeDeferred: decided, but this round's close bound was already
	// spent. Counted per strategy; it is not a refusal, the next round takes
	// it.
	OutcomeDeferred = "deferred"
	// OutcomeEvidenceUnavailable: the strategy's alerts could not be read
	// from the alert link, so there is nothing to address a close to. This
	// is also what a deployment without the reconciliation endpoint reports
	// on every round, which is the honest answer: without it there is no
	// authoritative alert metadata and nothing is closed.
	OutcomeEvidenceUnavailable = "evidence_unavailable"
	// OutcomeMetadataMissing: an alert the link exposes no severity for.
	// Counted per alert. A close carries the alert's own severity; inventing
	// one would close an alert at a level it never had.
	OutcomeMetadataMissing = "metadata_missing"
	// OutcomeAlertClosed: an alert the broker acknowledged a close for.
	// Counted per alert, where OutcomeClosed counts strategies.
	OutcomeAlertClosed = "alert_closed"
	// OutcomeSendFailed: the producer did not acknowledge the batch.
	OutcomeSendFailed = "send_failed"
	// OutcomeWouldSend: an alert a round decided on and did not send,
	// because the deployment has not armed the close. Counted per alert, so
	// that what arming would do is a number and not an estimate from the
	// strategy counts - eight strategies can be eight alerts or eight
	// thousand.
	OutcomeWouldSend = "would_send"
	// OutcomeIndexUnreadable: this strategy's open alert set could not be
	// read. Counted per strategy and per round, and never folded into "it
	// has no alerts": read and empty is a fact, not read is an absence of
	// one, and only the first may end a strategy's candidacy.
	OutcomeIndexUnreadable = "index_unreadable"
	// OutcomeNotLeader: this replica is not the control leader and takes no
	// difference. Counted so that a deployment where no replica ever ran a
	// round is not read as a deployment with nothing to close.
	OutcomeNotLeader = "not_leader"
	// OutcomeMemoryFull: a departure or a candidate the bounded memories
	// would not hold. Such a strategy is never closed, so the bound being
	// reached has to be a reading.
	OutcomeMemoryFull = "memory_full"
	// OutcomeProducerForeign: an alert another producer wrote. The index is
	// keyed by strategy, not by who created the alert, and a deployment
	// sharing a prefix or a database with another one - which the contract
	// warns about, since Pub/Sub does not isolate databases - would hand
	// this loop another deployment's alerts to close. Whose alert it is, is
	// a fact each alert carries; it is answered here, per alert, and not
	// guessed from how much the two sets overlap.
	OutcomeProducerForeign = "producer_foreign"
	// OutcomeProducerUnknown: an alert that names no producer. Written
	// before the field existed, or by something that does not set it.
	// Counted and never closed: "it does not say it is someone else's" is
	// not "it is ours", and reading a missing producer as our own is how a
	// close reaches an alert this deployment never made.
	OutcomeProducerUnknown = "producer_unknown"
)

// Outcomes is the closed list, for the metric that reports every cell.
var Outcomes = []string{OutcomeClosed, OutcomeStillPublished, OutcomeWithinGrace, OutcomeUnconfirmed,
	OutcomeIdentityUnknown, OutcomeRevisionUnknown, OutcomeDeferred, OutcomeEvidenceUnavailable,
	OutcomeMetadataMissing, OutcomeAlertClosed, OutcomeSendFailed, OutcomeIndexUnreadable,
	OutcomeNotLeader, OutcomeMemoryFull, OutcomeProducerForeign, OutcomeProducerUnknown, OutcomeWouldSend}

// Counts is what the round measured. The two denominators are reported with
// every reading, because a zero means two different things - nothing was
// absent, or nothing was decided - and without them a reader cannot tell a
// healthy deployment from a difference that never ran.
type Counts struct {
	// The denominators. Departed is the whole candidate pool, WithOpenAlerts
	// how many of those still hold an alert, and the two snapshot numbers
	// what the pool was compared against.
	Departed            int
	WithOpenAlerts      int
	UnreadableIndex     int
	SnapshotStrategies  int
	PublishedStrategies int
	// PreviousSnapshotStrategies is the size of the snapshot the round
	// before saw, which is what this round's size is judged against.
	PreviousSnapshotStrategies int
	// Returned is departed strategies the snapshot lists again. A reading
	// rather than a silent skip: a steady number here says the catalog and
	// the source disagree about what exists, which is a different fault from
	// anything else this loop reports.
	Returned int
	// Candidates is the difference this round acts on: departed, still
	// holding an alert, not listed by the snapshot, not running a Plan.
	Candidates int
	// By outcome, over the candidates.
	Closed          int
	StillPublished  int
	WithinGrace     int
	Unconfirmed     int
	IdentityUnknown int
	RevisionUnknown int
	// Deferred is the candidates this round's close bound left for a later
	// round. Not an outcome: they are still candidates.
	Deferred int
}

// Bounds are the gates a round is decided under.
type Bounds struct {
	// Grace is how long this loop must have seen the strategy missing
	// before closing its alerts, on top of the catalog's own removal grace.
	Grace time.Duration
	// MaxSnapshotAge is how old the observation may be. A snapshot older
	// than this has not seen the strategies created since, and every one of
	// them holding an alert would read as absent.
	MaxSnapshotAge time.Duration
	// MaxDifferenceRatio is the share of the snapshot the difference may
	// reach before the whole round is refused. The snapshot is the
	// denominator that means something here: a difference that is a large
	// share of it says the snapshot lost strategies, and the safe reading of
	// "a tenth of the strategies are gone" is that the list is wrong, not
	// that the strategies are.
	//
	// The other side is deliberately not a statistic at all. A deployment
	// can honestly have every one of its few unrecovered alerts on deleted
	// strategies - that is the backlog this capability exists to clear - so
	// neither a ratio against the alert index nor a check that the two sides
	// overlap can be a gate: both refuse precisely that state. "This alert
	// is not ours" is a fact each alert carries, and it is answered per
	// alert where it is a fact; see OutcomeProducerForeign.
	MaxDifferenceRatio float64
	// MinDifferenceForRatio is the difference below which the ratio says
	// nothing. On a deployment with four strategies one deletion is
	// twenty-five percent.
	MinDifferenceForRatio int
	// MaxSnapshotShrinkRatio is how much smaller this round's snapshot may
	// be than the round before's, or than the number of strategies the
	// catalog is running, before the round is refused. This is the gate on
	// the input; see RefusalSnapshotShrunk.
	MaxSnapshotShrinkRatio float64
	// MinSnapshotForShrink is the size below which the shrink ratio says
	// nothing, for the same reason the difference ratio has one.
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
	counts := Counts{Departed: len(round.Departed), WithOpenAlerts: len(round.WithOpenAlerts),
		UnreadableIndex: len(round.Unreadable), SnapshotStrategies: len(round.SnapshotStrategies),
		PublishedStrategies: len(round.Published), PreviousSnapshotStrategies: round.PreviousSnapshotStrategies}
	if !round.SnapshotUsable || round.SnapshotObservation == "" {
		return nil, counts, RefusalSnapshotUnusable
	}
	if len(round.SnapshotStrategies) == 0 {
		return nil, counts, RefusalSnapshotEmpty
	}
	if bounds.MaxSnapshotAge > 0 && time.Duration(round.SnapshotAgeSeconds)*time.Second > bounds.MaxSnapshotAge {
		return nil, counts, RefusalSnapshotStale
	}
	candidates := make([]Key, 0)
	for key := range round.Departed {
		if _, present := round.SnapshotStrategies[key]; present {
			counts.Returned++
			continue
		}
		if _, holds := round.WithOpenAlerts[key]; !holds {
			continue
		}
		candidates = append(candidates, key)
	}
	counts.Candidates = len(candidates)
	if shrunk(counts, bounds) {
		return nil, counts, RefusalSnapshotShrunk
	}
	if tooLarge(counts, bounds) {
		return nil, counts, RefusalDifferenceTooLarge
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].TenantID != candidates[j].TenantID {
			return candidates[i].TenantID < candidates[j].TenantID
		}
		return candidates[i].StrategyID < candidates[j].StrategyID
	})
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
		departure, departed := round.Departed[key]
		if !departed || departure.Identity.BusinessID == 0 {
			result.Counts.IdentityUnknown++
			continue
		}
		if departure.Identity.Revision <= 0 {
			result.Counts.RevisionUnknown++
			continue
		}
		if bounds.MaxCloseStrategies > 0 && len(result.Close) >= bounds.MaxCloseStrategies {
			result.Counts.Deferred++
			continue
		}
		result.Close = append(result.Close, Absent{Key: key, Identity: departure.Identity, AbsentSince: departure.At})
		result.Counts.Closed++
	}
	return result
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

// tooLarge applies the size gate against the snapshot, which is the
// denominator that says whether the snapshot is missing strategies it should
// have. See MaxDifferenceRatio for why there is no gate on the other side.
func tooLarge(counts Counts, bounds Bounds) bool {
	if bounds.MaxDifferenceRatio <= 0 || counts.Candidates < max(bounds.MinDifferenceForRatio, 1) {
		return false
	}
	return counts.SnapshotStrategies > 0 && float64(counts.Candidates)/float64(counts.SnapshotStrategies) > bounds.MaxDifferenceRatio
}
