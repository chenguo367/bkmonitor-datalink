// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

// stateCacheCollector reports the write-through State cache (fourth-batch
// design §4.7) from the execution store bound to it. Every cell of every
// closed vocabulary is published from the start, so a zero is a reading; a
// process with no store bound publishes none of them.
type stateCacheCollector struct {
	mu                                                             sync.Mutex
	source                                                         func() state.StateCacheStats
	lookups, departures, restored, declined, bytes, budget, groups *prometheus.Desc
	entries, readback, memberRenewals, fallbacks, compactDeclined  *prometheus.Desc
	verified, sweeps, sweptEntries                                 *prometheus.Desc
	cells, cellBytes                                               *prometheus.Desc
}

func newStateCacheCollector() *stateCacheCollector {
	descriptor := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName("", metricSubsystem, name), help, labels, nil)
	}
	return &stateCacheCollector{
		lookups: descriptor("state_cache_lookups_total",
			"Runtime State keys a load asked the write-through State cache for, by result: hit (served from "+
				"what the cache holds: what this process last wrote, a stable member's value read back, or its "+
				"compact entry - on a Plan's sampled round a hit is read back from the store all the same and "+
				"checked against it), or why it was read back - miss_absent (nothing kept), "+
				"miss_fence (another owner fence), miss_unknown (the last write's outcome unknown), "+
				"miss_expired (past its key's life less the replay age), miss_replay (a replay, retry or "+
				"older Slot), miss_activation (the Plan's activation changed: moved, put back, reactivated, "+
				"edited), miss_sharded (a split strategy's Plan, never served), miss_over_budget (its Query "+
				"Group kept out: a round of it does not fit the budget), miss_yielded (its Query Group kept out: a "+
				"round of it fits the budget but not what the Slots leave of the retained pool, or it was given back "+
				"for a Slot). Every cell exists from the start, so a zero is a reading.", "result"),
		departures: descriptor("state_cache_departures_total",
			"Entries that left the State cache, by reason and kind - state, a Runtime State value; segment, a Plan's "+
				"stable segment mark or member page (014 B3); the two share the budget - : released (the Query Group stopped, was lost, "+
				"declined or handed over, or a Plan's compact table let go whole when its owner fence or activation "+
				"changed), unknown (a write of its request ended in doubt), conflict (its "+
				"write did not apply), expired, renewal_missing, untouched (neither read nor written for two "+
				"of its own intervals), over_budget (its Query Group did not fit the budget: it left whole, or a write "+
				"of it was not taken in), yielded (the same for what the Slots leave of the retained pool, or its "+
				"Query Group given back whole for a Slot that needed the room), mismatch (a value a Plan's sampled "+
				"round found the store no longer held as the cache did). A group kept out is judged again "+
				"once a round, by a whole round of it, so one that never fits leaves once rather than every round. "+
				"Every cell exists from the start.", "reason", "kind"),
		restored: descriptor("state_cache_restored_total",
			"Values written back over an external change to a key the State cache served, by cause: missing "+
				"(the key was deleted or evicted) and rolled_back (older bytes than this process last wrote, "+
				"with the Query Group's Progress still at this Slot). A legitimate writer is never written "+
				"over: see state_cache_restore_declined_total.", "cause"),
		declined: descriptor("state_cache_restore_declined_total",
			"Conflicts on a key the State cache served that were not written back over, by reason: "+
				"progress_rolled_back (the Progress went back with the State, so the Slot is replayed from "+
				"the store) and newer_version (the stored bytes are no older than the cached ones: a "+
				"legitimate writer's, handled as an ordinary conflict).", "reason"),
		bytes: descriptor("state_cache_bytes",
			"The State cache's charge: each entry's value and key rounded to their size classes plus a fixed "+
				"overhead, summed, by kind - state, the Runtime State values; segment, the Plans' stable segment marks "+
				"and member pages (014 B3). The two share one budget (state_cache_budget_bytes): read their sum "+
				"against it, and each kind for what it takes of it.", "kind"),
		budget: descriptor("state_cache_budget_bytes",
			"The most the State cache may hold, set once a minute: the series this replica holds "+
				"(state_series_held) times a State entry's charge as the cache measures it, a quarter more, beside what "+
				"the segments hold; never less than the container's thirty-second. Borrowed from the retained-bytes pool "+
				"while no Slot has reserved it and given back when one needs it, or when the soft memory limit is passed. "+
				"A Query Group that would go past it or past the pool's room is kept out and read back; the resident "+
				"ones stay."),
		groups: descriptor("state_cache_query_groups", "Query Groups holding entries in the State cache."),
		entries: descriptor("state_cache_entries",
			"Runtime State entries the State cache holds, by kind: written (what this process wrote), readback (what a "+
				"preflight read back of a stable member its round leaves unwritten, kept as stored), compact (the same "+
				"kept in its compact form). Every cell exists from the start.", "kind"),
		readback: descriptor("state_cache_readback_inserted_total",
			"Records a Slot's own preflight found under its fence, by what the State cache did with them: kept (taken "+
				"in, or judged against the budget as a write is), declined_not_member (not an open member of its Plan's "+
				"stable segment, or a Plan of a split strategy), declined_operation (a member its round writes whole: "+
				"not a normal round, the segment skip not followed, the Plan's sampled round, a stopped Plan, a page "+
				"past half its life unrenewed), declined_no_renewal (a member left out whose page's last renewal leaves "+
				"its key less than the replay age), declined_refused (a member of a Query Group the cache keeps out for "+
				"the Slot, state_cache_lookups_total{result=miss_over_budget|miss_yielded}: read back from Redis "+
				"and not made compact, as before the cache). Read kept against state_cache_lookups_total{result=\"hit\"}: a "+
				"member kept is served on the rounds after. Every cell exists from the start.", "result"),
		memberRenewals: descriptor("state_cache_member_renewed_total",
			"State entries of stable members at their page's renewal, by result: renewed (the renewal found every key "+
				"of its chunk, and the entries live as long as the keys now do) and dropped_missing (it found some "+
				"gone, and the chunk's entries were let go: which ones is not known). Every cell exists from the start.",
			"result"),
		compactDeclined: descriptor("state_cache_compact_declined_total",
			"Values a preflight read back of a stable member that the State cache kept as their bytes rather than "+
				"compact, by why: legacy_frame (a record carrying stored record ids), series_guard (a record carrying a "+
				"series guard), digest_shape (a digest not 64 lowercase hexadecimal characters), rebuilt_bytes (the member "+
				"rebuilt to other bytes than were read back: a header written another way, by another build), "+
				"rebuilt_view (it decoded to another view than the bytes did), unreadable (the bytes did not parse). Each "+
				"kept as its bytes costs some four times a compact one in state_cache_bytes. Every cell exists from the "+
				"start.", "reason"),
		verified: descriptor("state_cache_readback_verify_total",
			"Values the State cache held that a Plan's sampled round of the day read from the store all the same and "+
				"compared, by result: match, or mismatch (the store held another value, or none: the cache's value was "+
				"let go and the round went on with the store's). Every kind of value is checked - written, read back, "+
				"compact. mismatch is to be zero, every one: a state_cache_verified line names the strategy, the kind and "+
				"the first field of the first; match above zero says the check ran. Both cells exist from the start.",
			"result"),
		sweeps: descriptor("state_cache_sweeps_total",
			"Runtime State and segment reads that asked the State cache to sweep their Query Group - take out the "+
				"entries past their life or untouched for two of their intervals - by result: walked (a Slot's first "+
				"read, which walks every entry and compact member of the group under the cache's lock) or "+
				"already_swept (a later read of the same Slot: its State is read in calls of at most a batch's keys, "+
				"and the group is walked once a Slot). walked's rate is about the Slots' of the Query Groups the "+
				"cache holds; state_cache_sweep_entries_total over it is the entries a walk goes through.",
			"result"),
		cells: descriptor("state_cache_entry_cells",
			"State cache entries of the Query Groups it holds by cell, as each group's sweep at its latest Slot's "+
				"first read found them (014 n8): kind (written, readback, compact); generation, current when the "+
				"group's lookups since the sweep before read the entry's own of its Plan, other for another or none; "+
				"sharded, a piece of a split strategy's key, never served, as the entry's latest lookup marked it and "+
				"kept by the write that replaces it (false while no lookup has); member, an open member of its Plan's "+
				"stable segment as its latest lookup's coverage or its read back marked it, kept by the write that "+
				"replaces it (false while no lookup with coverage has; a lookup without coverage leaves it), so a member "+
				"its round closed and wrote counts as one until the round after. A piece has no coverage and its member "+
				"is false: read split strategies' entries by sharded. touched, looked up and served, written or read "+
				"back since that sweep before. A group's "+
				"cells are counted at a Slot later than its last counted one only: a supplement's earlier Slot sweeps "+
				"and counts nothing. Read against state_cache_entries and state_cache_entry_cell_bytes; every cell "+
				"exists from the start.", "kind", "generation", "sharded", "member", "touched"),
		cellBytes: descriptor("state_cache_entry_cell_bytes",
			"The charge, in bytes against the State cache's budget, of state_cache_entry_cells' entries, cell by "+
				"cell as the same counts found them. Every cell exists from the start.",
			"kind", "generation", "sharded", "member", "touched"),
		sweptEntries: descriptor("state_cache_sweep_entries_total",
			"Entries and compact members the State cache's sweeps walked (state_cache_sweeps_total{result=walked}), "+
				"all of a Query Group's at each walk."),
		fallbacks: descriptor("state_cache_fallback_mget_total",
			"Stable members their round leaves unwritten that a preflight read back from Redis rather than serving "+
				"from the State cache, by why: absent (nothing kept: the first round after a start or a takeover, or "+
				"one it left), yielded and over_budget (its Query Group kept out of the cache), expired, activation "+
				"(its Plan's activation changed). The members' reads the cache exists to spare; hit low against "+
				"expectation reads here first, then state_cache_readback_inserted_total's declined cells. Every cell "+
				"exists from the start.", "reason"),
	}
}

// SetStateCacheSource binds the collector to the live execution store.
func (r *Recorder) SetStateCacheSource(source func() state.StateCacheStats) {
	if r == nil || r.phaseTwo.stateCache == nil {
		return
	}
	r.phaseTwo.stateCache.mu.Lock()
	r.phaseTwo.stateCache.source = source
	r.phaseTwo.stateCache.mu.Unlock()
}

func (c *stateCacheCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.lookups, c.departures, c.restored, c.declined, c.bytes, c.budget, c.groups,
		c.entries, c.readback, c.memberRenewals, c.fallbacks, c.compactDeclined, c.verified, c.sweeps, c.sweptEntries,
		c.cells, c.cellBytes} {
		ch <- desc
	}
}

func (c *stateCacheCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	source := c.source
	c.mu.Unlock()
	if source == nil {
		return
	}
	stats := source()
	for _, family := range []struct {
		desc   *prometheus.Desc
		words  []string
		counts map[string]uint64
	}{
		{c.lookups, state.StateCacheLookupResults, stats.Lookups},
		{c.restored, state.StateCacheRestoreCauses, stats.Restored},
		{c.declined, state.StateCacheDeclineReasons, stats.Declined},
		{c.readback, state.StateCacheReadbackResults, stats.Readback},
		{c.memberRenewals, state.StateCacheMemberRenewalResults, stats.MemberRenewals},
		{c.fallbacks, state.StateCacheFallbackReasons, stats.Fallbacks},
		{c.compactDeclined, state.StateCacheCompactDeclineReasons, stats.CompactDeclined},
		{c.verified, state.StateCacheVerifyResults, stats.Verified},
		{c.sweeps, state.StateCacheSweepResults, stats.Sweeps},
	} {
		for _, word := range family.words {
			ch <- prometheus.MustNewConstMetric(family.desc, prometheus.CounterValue, float64(family.counts[word]), word)
		}
	}
	// Each kind's departures as the cache counts them apart, never one
	// taken from the other: two counters read a moment apart could be.
	for _, reason := range state.StateCacheDepartureReasons {
		ch <- prometheus.MustNewConstMetric(c.departures, prometheus.CounterValue, float64(stats.StateDepartures[reason]), reason,
			state.StateCacheKindState)
		ch <- prometheus.MustNewConstMetric(c.departures, prometheus.CounterValue, float64(stats.SegmentDepartures[reason]), reason,
			state.StateCacheKindSegment)
	}
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(stats.Bytes-stats.SegmentBytes), state.StateCacheKindState)
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(stats.SegmentBytes), state.StateCacheKindSegment)
	ch <- prometheus.MustNewConstMetric(c.sweptEntries, prometheus.CounterValue, float64(stats.SweptEntries))
	for _, cell := range state.StateCacheCells() {
		labels := []string{cell.Kind, cell.Generation, strconv.FormatBool(cell.Sharded), strconv.FormatBool(cell.Member),
			strconv.FormatBool(cell.Touched)}
		counted := stats.Cells[cell]
		ch <- prometheus.MustNewConstMetric(c.cells, prometheus.GaugeValue, float64(counted.Entries), labels...)
		ch <- prometheus.MustNewConstMetric(c.cellBytes, prometheus.GaugeValue, float64(counted.Bytes), labels...)
	}
	ch <- prometheus.MustNewConstMetric(c.budget, prometheus.GaugeValue, float64(stats.BudgetBytes))
	ch <- prometheus.MustNewConstMetric(c.groups, prometheus.GaugeValue, float64(stats.QueryGroups))
	for _, kind := range state.StateCacheEntryKinds {
		ch <- prometheus.MustNewConstMetric(c.entries, prometheus.GaugeValue, float64(stats.Entries[kind]), kind)
	}
}
