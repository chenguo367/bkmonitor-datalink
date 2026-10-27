// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

// Package scopeclose closes the alerts of targets that left their strategy's
// monitoring scope, and the alerts of series whose alert identity moved: a
// host whose alerts were filed under the fingerprint of the record's own
// address before its identity read CMDB's (Drop.CloseReason). Both are
// fact-closes of an alert nothing evaluates any more, decided under the same
// rules below; only the decision the close carries differs.
//
// A record the target filter turns away is never evaluated, so nothing this
// process sends will ever end the alert its series opened while it was still
// in scope; the alert link only moves an alert on an event, so the alert
// stays open. The one place that knows the target let the record go is the
// admission step, and the one fact that makes the drop actionable is that
// the record's fingerprint is an open alert of this deployment's own, as
// alarmd's own copy of the link's active alerts knows it (activecopy). This
// package joins the two, and closes through the link's filter
// (filterclose): by target - every key of the target's identity, an IP and
// its cloud area together - where the strategy's target names one the
// record carries, and by the fingerprints otherwise.
//
// What has to hold before anything is closed, each named when it does not:
//
//   - the rejection is the target's own verdict on facts that were all read
//     and current (admission.DefinitelyOutside); a cache that could not
//     answer is cache_unavailable, never a reason to close;
//   - the copy knows an open alert of the strategy at all (not_known_open
//     otherwise, counted in bulk), and of this fingerprint: held by its own
//     send or a current search hit (not_member on a judged miss,
//     set_unavailable when the copy has no word - a scope drop is not a
//     decision the copy refreshes for);
//   - two different Slots turned the same target away. One Slot is a first
//     observation, unconfirmed; the second makes it a close;
//   - the last of them is fresh when the close is run (Freshness): a target
//     that came back into scope stops being rejected, and nothing else
//     would tell this memory so.
//
// The close itself is the filter's: a dry run against the expectation (the
// targets times the series observed per target, or the fingerprints), then
// the strategy's active alerts of this source the filter names, each closed
// by its alert id.
//
// Closes are run at most Batch entries per step, starting after the last
// one the previous step decided, so a large backlog is walked rather than
// retried from its head.
package scopeclose

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/filterclose"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdoutput"
)

// The outcomes, closed: a metric label is made of them.
const (
	// OutcomeCloseSent counts entries - a target, or a series without one -
	// the link's filter closed or found nothing active of.
	OutcomeCloseSent = "close_sent"
	// OutcomeUnconfirmed counts first observations: a target of an open
	// alert turned away by one Slot, waiting for a second.
	OutcomeUnconfirmed = "unconfirmed"
	// OutcomeCacheUnavailable counts rejections the target could not stand
	// behind: facts that were not read, a target plan that did not resolve
	// in full. Never closed.
	OutcomeCacheUnavailable = "cache_unavailable"
	// OutcomeNotMember counts definitive rejections whose fingerprint the
	// copy judged not open.
	OutcomeNotMember = "not_member"
	// OutcomeNotKnownOpen counts, in bulk, the rejections of a strategy the
	// copy knows no open alert of: by far the common case, a wide table
	// filtered by a target.
	OutcomeNotKnownOpen = "not_known_open"
	// OutcomeSetUnavailable counts rejections whose fingerprint the copy
	// has no word on.
	OutcomeSetUnavailable = "set_unavailable"
	// OutcomeSendFailed counts entries the link did not settle - busy,
	// failed, refused, past the expectation - kept and tried next step.
	OutcomeSendFailed = "send_failed"
	// OutcomeUnsupported counts entries not closed because filtered close
	// is unsupported: the link answered 501, 404 or 405. Nothing else
	// closes them.
	OutcomeUnsupported = "filter_close_unsupported"
	// OutcomeUnrouted counts entries confirmed on a replica with no link to
	// close through.
	OutcomeUnrouted = "unrouted"
	// OutcomeIdentityInvalid is a close whose strategy id is not a positive
	// number or whose revision is not positive. Nothing was sent, and the
	// observation is forgotten.
	OutcomeIdentityInvalid = "close_identity_invalid"
	// OutcomeMemoryFull counts first observations not recorded because the
	// observation table was at its bound.
	OutcomeMemoryFull = "memory_full"
	// OutcomeFingerprintUnsupported counts definitive rejections of a Plan
	// fed by more than one input, whose alert fingerprint no single series
	// carries; never closed.
	OutcomeFingerprintUnsupported = "fingerprint_unsupported"
	// OutcomeStaleDeferred counts closes held back at the step because the
	// last observation is older than the freshness bound.
	OutcomeStaleDeferred = "stale_deferred"
	// OutcomeIndefinite counts rejections that are themselves not a verdict
	// on the record's place - a key or object identity that could not be
	// built. Never closed.
	OutcomeIndefinite = "indefinite"
	// OutcomeObservationExpired counts first observations whose second did
	// not arrive inside the observation's lifetime.
	OutcomeObservationExpired = "observation_expired"
	// OutcomeSupersededCloseSent counts, among close_sent, the closes of
	// alerts a series opened under the fingerprint of its own address
	// before its alert identity read CMDB's.
	OutcomeSupersededCloseSent = "superseded_identity_close_sent"
	// OutcomeTargetCloseSent counts, among close_sent, the closes by target
	// rather than by fingerprint.
	OutcomeTargetCloseSent = "target_close_sent"
)

// Outcomes lists every outcome, for the metric that pre-creates them.
var Outcomes = []string{OutcomeCloseSent, OutcomeUnconfirmed, OutcomeCacheUnavailable, OutcomeNotMember, OutcomeNotKnownOpen,
	OutcomeSetUnavailable, OutcomeSendFailed, OutcomeUnsupported, OutcomeUnrouted, OutcomeMemoryFull, OutcomeFingerprintUnsupported,
	OutcomeStaleDeferred, OutcomeIndefinite, OutcomeIdentityInvalid, OutcomeObservationExpired, OutcomeSupersededCloseSent,
	OutcomeTargetCloseSent}

// The bounds of what the facts carry, and of the series an entry counts.
const (
	FactsStrategies         = 8
	FactsSamplesPerStrategy = 3
	samplePrefix            = 8
	// maxTalliedStrategies bounds the per-strategy counts kept for the facts.
	maxTalliedStrategies = 256
	// maxSeriesPerEntry bounds the fingerprints one target entry counts for
	// its expectation; past it the expectation is the bound.
	maxSeriesPerEntry = 64
)

// The decisions the closes carry (filterclose decision words).
const (
	DecisionTarget     = filterclose.DecisionScope
	DecisionSuperseded = "superseded_identity"
)

// Key names one strategy.
type Key struct {
	TenantID   string
	StrategyID string
}

// Drop is one definitive target rejection of one series of a strategy that
// Screen cleared.
type Drop struct {
	TenantID, StrategyID string
	// Fingerprint is the evaluator's dedupe identity of the record; empty
	// when there is none to compute.
	Fingerprint string
	// Target is the record's values of the keys of its strategy's target
	// identity, as its alert carries them; nil where the strategy names no
	// target identity or the record does not carry every key, and then the
	// close is by Fingerprint.
	Target           map[string]string
	StrategyRevision int64
	// Round is the Slot's evaluation time.
	Round int64
	// IntervalSeconds is the strategy's evaluation period, and ReadOffset
	// how long after Round its Slot was frozen to read. The second
	// observation arrives when the next Slot is read, which these two place.
	IntervalSeconds int64
	ReadOffset      time.Duration
	// CloseReason is the reason the close carries (linkdoutput.CloseReason*):
	// empty is CloseReasonTargetOutOfScope; CloseReasonSupersededIdentity is
	// a series whose alert identity now reads CMDB's address, observed with
	// the fingerprint it was filed under before, and always closed by it.
	CloseReason string
}

// OpenSet is what the close asks alarmd's copy of the link's active alerts
// (activecopy.Copy).
type OpenSet interface {
	// KnowsOpen is asked once per strategy per query before any record.
	KnowsOpen(tenantID, strategyID string) bool
	// Known is the copy's word on one fingerprint, without refreshing it.
	Known(tenantID, strategyID, fingerprint string) (held, judged bool)
}

// Link is the link's close by filter (filterclose.Closer).
type Link interface {
	RemovedTargets(ctx context.Context, scope filterclose.Scope, targets []filterclose.Target, seriesPerTarget int, revision int64,
		decision string) []string
	Fingerprints(ctx context.Context, scope filterclose.Scope, fingerprints []string, decision string, revision int64) string
}

// Options bound the close.
type Options struct {
	Now func() time.Time
	// MaxEntries bounds the entries observed and not yet decided.
	MaxEntries int
	// Batch bounds the entries one step closes.
	Batch int
	// ObservationTTL is how long an observation waits for its second
	// before it is forgotten, and how long a decided entry is not decided
	// again.
	ObservationTTL time.Duration
	// Freshness is how recent the last observation must be when a close is
	// run.
	Freshness time.Duration
}

type entryKey struct {
	key Key
	// id is the target's canonical text, or "fingerprint:" and the
	// fingerprint for an entry without a target.
	id string
}

type entry struct {
	closeReason string
	target      filterclose.Target
	// series are the fingerprints observed, at most maxSeriesPerEntry; an
	// entry without a target holds exactly one.
	series     map[string]struct{}
	revision   int64
	firstRound int64
	confirmed  bool
	lastSeen   time.Time
	// ttl is how long the observation waits for its second (lifetime).
	ttl time.Duration
}

// lifetime is how long an observation of drop waits for its second: two of
// the strategy's periods plus its Slot's read offset, at least
// ObservationTTL.
func (closer *Closer) lifetime(drop Drop) time.Duration {
	return max(closer.options.ObservationTTL, 2*time.Duration(drop.IntervalSeconds)*time.Second+drop.ReadOffset)
}

// bulkKey and bulkRound remember, per reporter and outcome, the last round
// counted in bulk and how much of it, so a retried round is counted once.
type bulkKey struct {
	key      Key
	reporter string
	outcome  string
}

type bulkRound struct {
	round int64
	n     int
}

type tally struct {
	counts  map[string]uint64
	decided []string
}

// Closer holds the observations and decides the closes.
type Closer struct {
	options Options
	mu      sync.Mutex
	set     OpenSet
	link    Link
	source  string
	entries map[entryKey]*entry
	// decided remembers what was closed, so the Slots that keep turning the
	// same target away do not decide it again within the TTL.
	decided map[entryKey]time.Time
	after   entryKey
	counts  map[string]uint64
	tallies map[Key]*tally
	bulk    map[bulkKey]bulkRound
}

// New builds a closer with nothing bound; Bind attaches the copy and the
// link once they exist. Until then every observation is set_unavailable.
func New(options Options) *Closer {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MaxEntries <= 0 {
		options.MaxEntries = 1024
	}
	if options.Batch <= 0 {
		options.Batch = 64
	}
	if options.ObservationTTL <= 0 {
		options.ObservationTTL = 30 * time.Minute
	}
	if options.Freshness <= 0 {
		options.Freshness = time.Minute
	}
	return &Closer{options: options, entries: map[entryKey]*entry{}, decided: map[entryKey]time.Time{},
		counts: map[string]uint64{}, tallies: map[Key]*tally{}, bulk: map[bulkKey]bulkRound{}}
}

// Bind attaches the copy, the link and the event source every filter names.
// A nil link is a replica with no link control plane: confirmed entries are
// counted unrouted.
func (closer *Closer) Bind(set OpenSet, link Link, source string) {
	closer.mu.Lock()
	closer.set, closer.link, closer.source = set, link, source
	closer.mu.Unlock()
}

// Screen answers once per strategy per query whether its rejections are
// worth a fingerprint each: "" when the copy knows an open alert of the
// strategy, and otherwise the outcome all of them are counted under in bulk.
func (closer *Closer) Screen(key Key) string {
	closer.mu.Lock()
	set := closer.set
	closer.mu.Unlock()
	switch {
	case set == nil:
		return OutcomeSetUnavailable
	case !set.KnowsOpen(key.TenantID, key.StrategyID):
		return OutcomeNotKnownOpen
	}
	return ""
}

// Count adds one reporter's rejections of a strategy under one outcome, in
// bulk. A retried Slot reports as the same reporter and round again; that is
// counted once, at the largest total any of its attempts reported. Only
// readings depend on this; no close is decided from it.
func (closer *Closer) Count(key Key, reporter string, round int64, outcome string, n int) {
	if n <= 0 {
		return
	}
	closer.mu.Lock()
	defer closer.mu.Unlock()
	bk := bulkKey{key: key, reporter: reporter, outcome: outcome}
	last, seen := closer.bulk[bk]
	if seen && last.round == round {
		if n <= last.n {
			return
		}
		closer.countLocked(key, outcome, n-last.n)
		closer.bulk[bk] = bulkRound{round: round, n: n}
		return
	}
	if seen || len(closer.bulk) < closer.options.MaxEntries {
		closer.bulk[bk] = bulkRound{round: round, n: n}
	}
	closer.countLocked(key, outcome, n)
}

// targetID is a target's canonical text: its keys sorted, each key=value.
func targetID(target map[string]string) string {
	keys := make([]string, 0, len(target))
	for name := range target {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, name := range keys {
		b.WriteString(strconv.Quote(name))
		b.WriteByte('=')
		b.WriteString(strconv.Quote(target[name]))
		b.WriteByte(';')
	}
	return b.String()
}

// Observe records one rejection of a strategy Screen cleared. It is called
// on the query's goroutine and does no I/O: one lookup in the copy, and a
// map update.
func (closer *Closer) Observe(drop Drop) {
	key := Key{TenantID: drop.TenantID, StrategyID: drop.StrategyID}
	// A superseded-identity offer is made for every admitted series whose
	// host overlay moves its fingerprint, every Slot, whether or not an
	// alert was ever opened under the old one. An offer that finds nothing
	// to close counts nothing, so not_member and set_unavailable keep
	// meaning target rejections.
	superseded := drop.CloseReason == linkdoutput.CloseReasonSupersededIdentity
	if drop.Fingerprint == "" {
		if !superseded {
			closer.count(key, OutcomeNotMember, 1)
		}
		return
	}
	closer.mu.Lock()
	set := closer.set
	closer.mu.Unlock()
	if set == nil {
		if !superseded {
			closer.count(key, OutcomeSetUnavailable, 1)
		}
		return
	}
	held, judged := set.Known(drop.TenantID, drop.StrategyID, drop.Fingerprint)
	switch {
	case !judged:
		if !superseded {
			closer.count(key, OutcomeSetUnavailable, 1)
		}
		return
	case !held:
		if !superseded {
			closer.count(key, OutcomeNotMember, 1)
		}
		return
	}
	ek := entryKey{key: key, id: "fingerprint:" + drop.Fingerprint}
	var target filterclose.Target
	if len(drop.Target) > 0 && !superseded {
		target = filterclose.Target(drop.Target)
		ek.id = targetID(drop.Target)
	}
	now := closer.options.Now()
	closer.mu.Lock()
	defer closer.mu.Unlock()
	if until, done := closer.decided[ek]; done && now.Before(until) {
		return
	}
	existing := closer.entries[ek]
	if existing != nil && now.Sub(existing.lastSeen) >= existing.ttl {
		// Waited too long for its second: the two are not one absence.
		delete(closer.entries, ek)
		closer.countLocked(key, OutcomeObservationExpired, 1)
		existing = nil
	}
	if existing == nil {
		if len(closer.entries) >= closer.options.MaxEntries {
			closer.countLocked(key, OutcomeMemoryFull, 1)
			return
		}
		closer.entries[ek] = &entry{closeReason: closeReasonOf(drop), target: target, series: map[string]struct{}{drop.Fingerprint: {}},
			revision: drop.StrategyRevision, firstRound: drop.Round, lastSeen: now, ttl: closer.lifetime(drop)}
		closer.countLocked(key, OutcomeUnconfirmed, 1)
		return
	}
	existing.lastSeen = now
	existing.revision = drop.StrategyRevision
	existing.ttl = closer.lifetime(drop)
	if len(existing.series) < maxSeriesPerEntry {
		existing.series[drop.Fingerprint] = struct{}{}
	}
	if drop.Round != existing.firstRound {
		existing.confirmed = true
	}
}

// group is the confirmed entries of one strategy, close reason and
// revision that one filter call closes: the targets, or the fingerprints.
type group struct {
	key      Key
	reason   string
	revision int64
	byTarget bool
}

// Step decides and closes at most Batch confirmed entries.
func (closer *Closer) Step(ctx context.Context) {
	now := closer.options.Now()
	closer.mu.Lock()
	for ek, e := range closer.entries {
		if now.Sub(e.lastSeen) >= e.ttl {
			delete(closer.entries, ek)
			closer.countLocked(ek.key, OutcomeObservationExpired, 1)
		}
	}
	for ek, until := range closer.decided {
		if !now.Before(until) {
			delete(closer.decided, ek)
		}
	}
	confirmed := make([]entryKey, 0)
	for ek, e := range closer.entries {
		if e.confirmed {
			confirmed = append(confirmed, ek)
		}
	}
	link, source, after := closer.link, closer.source, closer.after
	closer.mu.Unlock()
	if len(confirmed) == 0 {
		return
	}
	sort.Slice(confirmed, func(i, j int) bool { return lessEntry(confirmed[i], confirmed[j]) })
	start := sort.Search(len(confirmed), func(i int) bool { return lessEntry(after, confirmed[i]) })
	var order []group
	members := map[group][]entryKey{}
	picked := 0
	for n := 0; n < len(confirmed) && picked < closer.options.Batch; n++ {
		ek := confirmed[(start+n)%len(confirmed)]
		after = ek
		closer.mu.Lock()
		e := closer.entries[ek]
		closer.mu.Unlock()
		if e == nil {
			continue
		}
		if now.Sub(e.lastSeen) > closer.options.Freshness {
			closer.count(ek.key, OutcomeStaleDeferred, 1)
			continue
		}
		if id, err := strconv.ParseInt(ek.key.StrategyID, 10, 64); err != nil || id <= 0 || e.revision <= 0 {
			closer.forget(ek, OutcomeIdentityInvalid)
			continue
		}
		if link == nil || source == "" {
			closer.count(ek.key, OutcomeUnrouted, 1)
			continue
		}
		g := group{key: ek.key, reason: e.closeReason, revision: e.revision, byTarget: e.target != nil}
		if _, seen := members[g]; !seen {
			order = append(order, g)
		}
		members[g] = append(members[g], ek)
		picked++
	}
	closer.mu.Lock()
	closer.after = after
	closer.mu.Unlock()
	for _, g := range order {
		if ctx.Err() != nil {
			return
		}
		closer.closeGroup(ctx, link, source, g, members[g], now)
	}
}

// closeGroup runs one group's filter close and decides its entries by the
// words: every filter batch done or matching nothing decides them; filtered
// close unsupported counts them; anything else keeps them for the next step.
func (closer *Closer) closeGroup(ctx context.Context, link Link, source string, g group, keys []entryKey, now time.Time) {
	scope := filterclose.Scope{TenantID: g.key.TenantID, EventSourceID: source, StrategyID: g.key.StrategyID}
	decision := DecisionTarget
	if g.reason == linkdoutput.CloseReasonSupersededIdentity {
		decision = DecisionSuperseded
	}
	var words []string
	closer.mu.Lock()
	if g.byTarget {
		targets := make([]filterclose.Target, 0, len(keys))
		series := 1
		for _, ek := range keys {
			e := closer.entries[ek]
			targets = append(targets, e.target)
			series = max(series, len(e.series))
		}
		closer.mu.Unlock()
		words = link.RemovedTargets(ctx, scope, targets, series, g.revision, decision)
	} else {
		fingerprints := make([]string, 0, len(keys))
		for _, ek := range keys {
			for fingerprint := range closer.entries[ek].series {
				fingerprints = append(fingerprints, fingerprint)
			}
		}
		closer.mu.Unlock()
		sort.Strings(fingerprints)
		words = []string{link.Fingerprints(ctx, scope, fingerprints, decision, g.revision)}
	}
	settled, unsupported := len(words) > 0, false
	for _, word := range words {
		switch word {
		case filterclose.OutcomeJobDone, filterclose.OutcomeNothingMatched:
		case filterclose.OutcomeUnsupported:
			settled, unsupported = false, true
		default:
			settled = false
		}
	}
	switch {
	case settled:
		closer.decide(keys, g, now)
	case unsupported:
		closer.countEach(keys, OutcomeUnsupported)
	default:
		// Kept: the next step runs the filter again, from a new dry run.
		closer.countEach(keys, OutcomeSendFailed)
	}
}

func (closer *Closer) forget(ek entryKey, outcome string) {
	closer.mu.Lock()
	delete(closer.entries, ek)
	closer.countLocked(ek.key, outcome, 1)
	closer.mu.Unlock()
}

func (closer *Closer) decide(keys []entryKey, g group, now time.Time) {
	closer.mu.Lock()
	defer closer.mu.Unlock()
	for _, ek := range keys {
		ttl := closer.options.ObservationTTL
		if e := closer.entries[ek]; e != nil {
			ttl = e.ttl
		}
		delete(closer.entries, ek)
		if len(closer.decided) < closer.options.MaxEntries {
			closer.decided[ek] = now.Add(ttl)
		}
		closer.countLocked(ek.key, OutcomeCloseSent, 1)
		switch {
		case g.reason == linkdoutput.CloseReasonSupersededIdentity:
			closer.countLocked(ek.key, OutcomeSupersededCloseSent, 1)
		case g.byTarget:
			closer.countLocked(ek.key, OutcomeTargetCloseSent, 1)
		}
		t := closer.tallyLocked(ek.key)
		if t == nil {
			continue
		}
		t.decided = append(t.decided, prefix(sampleOf(ek)))
		if len(t.decided) > FactsSamplesPerStrategy {
			t.decided = t.decided[len(t.decided)-FactsSamplesPerStrategy:]
		}
	}
}

func (closer *Closer) countEach(keys []entryKey, outcome string) {
	closer.mu.Lock()
	defer closer.mu.Unlock()
	for _, ek := range keys {
		closer.countLocked(ek.key, outcome, 1)
	}
}

func (closer *Closer) count(key Key, outcome string, n int) {
	closer.mu.Lock()
	closer.countLocked(key, outcome, n)
	closer.mu.Unlock()
}

func (closer *Closer) countLocked(key Key, outcome string, n int) {
	closer.counts[outcome] += uint64(n)
	if t := closer.tallyLocked(key); t != nil {
		t.counts[outcome] += uint64(n)
	}
}

// tallyLocked is the strategy's counts, created while there is room; a
// strategy past the bound is counted in the totals only.
func (closer *Closer) tallyLocked(key Key) *tally {
	t := closer.tallies[key]
	if t == nil && len(closer.tallies) < maxTalliedStrategies {
		t = &tally{counts: map[string]uint64{}}
		closer.tallies[key] = t
	}
	return t
}

// closeReasonOf is the reason a drop's close carries.
func closeReasonOf(drop Drop) string {
	if drop.CloseReason == "" {
		return linkdoutput.CloseReasonTargetOutOfScope
	}
	return drop.CloseReason
}

// Stats is the outcome totals, every outcome present.
func (closer *Closer) Stats() map[string]uint64 {
	closer.mu.Lock()
	defer closer.mu.Unlock()
	counts := make(map[string]uint64, len(Outcomes))
	for _, outcome := range Outcomes {
		counts[outcome] = closer.counts[outcome]
	}
	return counts
}

// Facts is what a replica publishes about the close.
type Facts struct {
	Pending, Confirmed   int
	MaxEntries           int
	Outcomes             map[string]uint64
	Strategies           []StrategyFacts
	TalliedStrategies    int
	MaxTalliedStrategies int
}

// StrategyFacts is one strategy's reading: its counts, the entries it has
// waiting, and the last ones decided, each a bounded prefix.
type StrategyFacts struct {
	TenantID, StrategyID string
	Pending, Confirmed   int
	Outcomes             map[string]uint64
	PendingSample        []string
	DecidedSample        []string
}

// Facts reads the close's state: totals, and up to FactsStrategies
// strategies, those with entries waiting first.
func (closer *Closer) Facts() Facts {
	closer.mu.Lock()
	defer closer.mu.Unlock()
	facts := Facts{MaxEntries: closer.options.MaxEntries, Outcomes: map[string]uint64{},
		TalliedStrategies: len(closer.tallies), MaxTalliedStrategies: maxTalliedStrategies}
	for _, outcome := range Outcomes {
		facts.Outcomes[outcome] = closer.counts[outcome]
	}
	rows := map[Key]*StrategyFacts{}
	row := func(key Key) *StrategyFacts {
		r := rows[key]
		if r == nil {
			r = &StrategyFacts{TenantID: key.TenantID, StrategyID: key.StrategyID}
			rows[key] = r
		}
		return r
	}
	pending := map[Key][]entryKey{}
	for ek, e := range closer.entries {
		r := row(ek.key)
		if e.confirmed {
			r.Confirmed++
			facts.Confirmed++
		} else {
			r.Pending++
			facts.Pending++
		}
		pending[ek.key] = append(pending[ek.key], ek)
	}
	for key, t := range closer.tallies {
		r := row(key)
		r.Outcomes = make(map[string]uint64, len(t.counts))
		for outcome, count := range t.counts {
			r.Outcomes[outcome] = count
		}
		r.DecidedSample = append([]string(nil), t.decided...)
	}
	for key, keys := range pending {
		sort.Slice(keys, func(i, j int) bool {
			ci, cj := closer.entries[keys[i]].confirmed, closer.entries[keys[j]].confirmed
			if ci != cj {
				return ci
			}
			return keys[i].id < keys[j].id
		})
		for i := 0; i < len(keys) && i < FactsSamplesPerStrategy; i++ {
			rows[key].PendingSample = append(rows[key].PendingSample, prefix(sampleOf(keys[i])))
		}
	}
	ordered := make([]*StrategyFacts, 0, len(rows))
	for _, r := range rows {
		ordered = append(ordered, r)
	}
	sort.Slice(ordered, func(i, j int) bool {
		wi, wj := ordered[i].Pending+ordered[i].Confirmed, ordered[j].Pending+ordered[j].Confirmed
		if wi != wj {
			return wi > wj
		}
		if ordered[i].TenantID != ordered[j].TenantID {
			return ordered[i].TenantID < ordered[j].TenantID
		}
		return ordered[i].StrategyID < ordered[j].StrategyID
	})
	for i := 0; i < len(ordered) && i < FactsStrategies; i++ {
		facts.Strategies = append(facts.Strategies, *ordered[i])
	}
	return facts
}

func lessEntry(a, b entryKey) bool {
	if a.key.TenantID != b.key.TenantID {
		return a.key.TenantID < b.key.TenantID
	}
	if a.key.StrategyID != b.key.StrategyID {
		return a.key.StrategyID < b.key.StrategyID
	}
	return a.id < b.id
}

// sampleOf is what an entry is shown as: its fingerprint, or its target's
// text.
func sampleOf(ek entryKey) string {
	return strings.TrimPrefix(ek.id, "fingerprint:")
}

func prefix(value string) string {
	if len(value) <= samplePrefix {
		return value
	}
	return value[:samplePrefix]
}
