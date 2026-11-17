// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package segment_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

func digest(seed string) execution.SeriesIdentityDigest {
	sum := sha256.Sum256([]byte(seed))
	return execution.SeriesIdentityDigest(hex.EncodeToString(sum[:]))
}

// A page keeps every member's whole digest: two series that share all but
// the last character are two entries, and a page read back is the page
// written, in digest order whatever the order given.
func TestAPageKeepsWholeDigestsAndReadsBackAsWritten(t *testing.T) {
	member := digest("member")
	twin := execution.SeriesIdentityDigest(string(member[:63]) + string("0123456789abcdef"[(strings.IndexByte("0123456789abcdef", member[63])+1)%16]))
	entries := []segment.Entry{{Digest: twin, Start: 600}, {Digest: member, Start: 540, Closed: 900}}
	raw, err := segment.EncodePage(entries)
	if err != nil {
		t.Fatal(err)
	}
	read, err := segment.DecodePage(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := []segment.Entry{entries[0], entries[1]}
	if want[0].Digest > want[1].Digest {
		want[0], want[1] = want[1], want[0]
	}
	if !reflect.DeepEqual(read, want) {
		t.Fatalf("read back %+v, want %+v", read, want)
	}
	if counted := segment.PageBytes(entries); counted != len(raw) {
		t.Fatalf("PageBytes counted %d, the page is %d bytes", counted, len(raw))
	}
	if per := float64(len(raw)) / 2; per > 40 {
		t.Fatalf("%.0f bytes an entry, want about 40: a full page must stay under a script's 200 KiB", per)
	}
}

// What a page refuses: more entries than a page holds, a series twice, a
// digest that is not one, a segment closed before it starts; and bytes that
// are not a page, cut short or with bytes after it.
func TestAPageRefusesWhatItCannotHold(t *testing.T) {
	full := make([]segment.Entry, segment.PageEntries+1)
	for index := range full {
		full[index] = segment.Entry{Digest: digest(fmt.Sprint(index)), Start: 60}
	}
	if _, err := segment.EncodePage(full); !errors.Is(err, segment.ErrPageFull) {
		t.Fatalf("%d entries: %v, want ErrPageFull", len(full), err)
	}
	if _, err := segment.EncodePage(full[:segment.PageEntries]); err != nil {
		t.Fatalf("%d entries, a full page: %v", segment.PageEntries, err)
	}
	for _, refused := range [][]segment.Entry{
		{{Digest: digest("a"), Start: 60}, {Digest: digest("a"), Start: 120}},
		{{Digest: digest("a")[:63], Start: 60}},
		{{Digest: digest("a")[:62], Start: 60}}, // a prefix, whole bytes of it: still no digest
		{{Digest: execution.SeriesIdentityDigest(strings.ToUpper(string(digest("a")))), Start: 60}},
		{{Digest: digest("a"), Start: 600, Closed: 540}},
		{{Digest: digest("a"), Start: -60}},
	} {
		if _, err := segment.EncodePage(refused); err == nil {
			t.Errorf("%+v: encoded, want refused", refused)
		}
	}
	raw, err := segment.EncodePage([]segment.Entry{{Digest: digest("a"), Start: 60}})
	if err != nil {
		t.Fatal(err)
	}
	for name, bytes := range map[string][]byte{
		"empty": nil, "magic": append([]byte("XYZ"), raw[3:]...), "short": raw[:len(raw)-1],
		"trailing": append(append([]byte(nil), raw...), 0),
	} {
		if _, err := segment.DecodePage(bytes); err == nil {
			t.Errorf("%s: decoded, want refused", name)
		}
	}
}

// Pages are a power of two, sized for PageFill a page; a count that doubles
// splits each page in two - every member of a page of the doubled count was
// on the one page Sources names - and one that halves joins two.
func TestPagesSplitAndJoinAlongTheirDigests(t *testing.T) {
	for members, want := range map[int]int{0: 1, 1: 1, segment.PageFill: 1, segment.PageFill + 1: 2, 4 * segment.PageFill: 4, 4*segment.PageFill + 1: 8} {
		if got := segment.PageCount(members); got != want {
			t.Errorf("PageCount(%d) = %d, want %d", members, got, want)
		}
	}
	for index := 0; index < 2000; index++ {
		series := digest(fmt.Sprint("split", index))
		for _, pages := range [][2]int{{1, 2}, {2, 4}, {4, 16}} {
			before, _ := segment.BucketOf(series, pages[0])
			after, _ := segment.BucketOf(series, pages[1])
			if sources := segment.Sources(after, pages[0], pages[1]); len(sources) != 1 || sources[0] != before {
				t.Fatalf("%s on page %d of %d, page %d of %d whose sources are %v", series, before, pages[0], after, pages[1], sources)
			}
			found := false
			for _, source := range segment.Sources(before, pages[1], pages[0]) {
				found = found || source == after
			}
			if !found {
				t.Fatalf("%s: page %d of %d is not among the pages page %d of %d joins", series, after, pages[1], before, pages[0])
			}
		}
	}
	if _, err := segment.BucketOf(digest("a"), 3); err == nil {
		t.Fatal("three pages bucketed, want refused")
	}
}

// A book's changes name the pages they rewrite: a join, a close and a remove
// each the one page its series is on, an expiry the pages it took entries
// from, and a count that changes every page. Settle forgets them.
func TestABookNamesThePagesItsChangesRewrite(t *testing.T) {
	var entries []segment.Entry
	for index := 0; index < segment.PageFill+1; index++ {
		entries = append(entries, segment.Entry{Digest: digest(fmt.Sprint("book", index)), Start: 60})
	}
	book, err := segment.NewBook(entries)
	if err != nil {
		t.Fatal(err)
	}
	if pages := book.Pages(); len(pages) != 2 {
		t.Fatalf("%d members on %d pages, want 2", book.Len(), len(pages))
	}
	book.Settle()
	if dirty := book.Dirty(); len(dirty) != 0 {
		t.Fatalf("settled book dirty %v", dirty)
	}
	closing := entries[0].Digest
	page, _ := segment.BucketOf(closing, 2)
	if err := book.Close(closing, 600); err != nil {
		t.Fatal(err)
	}
	if dirty := book.Dirty(); !reflect.DeepEqual(dirty, []int{page}) {
		t.Fatalf("a close rewrote %v, want its page %d", dirty, page)
	}
	if err := book.Close(closing, 660); err == nil {
		t.Fatal("a closed segment closed again")
	}
	book.Settle()
	if expired, err := book.Expire(600); err != nil || expired != 0 {
		t.Fatalf("Expire(600) = %d, %v: a segment closed at 600 is still in reach", expired, err)
	}
	if expired, err := book.Expire(601); err != nil || expired != 1 {
		t.Fatalf("Expire(601) = %d, %v, want the one closed before", expired, err)
	}
	if _, member := book.Entry(closing); member {
		t.Fatal("an expired entry is still a member")
	}
	// Down to PageFill members the count halves: every page is rewritten.
	if dirty := book.Dirty(); !reflect.DeepEqual(dirty, []int{0}) {
		t.Fatalf("a count that halved rewrote %v, want its one page", dirty)
	}
	if err := book.Join(entries[1].Digest, 60); err == nil {
		t.Fatal("a member joined twice")
	}
	if err := book.Remove(digest("not a member")); err != nil {
		t.Fatalf("removing a series that is no member: %v", err)
	}
}

// The renewal rules keep R - every open member's key expires no sooner than
// its page's At + TTL - through lifetimes that grow and shrink and members
// joining, renewed whenever due: modelled here on the keys' own expiries.
func TestThePageRenewalRulesKeepEveryKeyAliveAsLongAsThePageSays(t *testing.T) {
	const minute = int64(60_000)
	ttls := []int64{20 * minute, 20 * minute, 30 * minute, 30 * minute, 12 * minute, 12 * minute, 40 * minute, 16 * minute}
	now := int64(1_800_000_000_000)
	page := segment.NewPage(now, "plan", 0, ttls[0])
	expiries := map[int]int64{0: now + ttls[0]} // every member was just written with the round's lifetime
	renewals := 0
	for round := 0; round < 400; round++ {
		now += minute
		ttlNow := ttls[(round/40)%len(ttls)]
		page = page.Lowered(ttlNow)
		member := len(expiries)
		expiries[member] = now + ttlNow // a member joins, written whole this round
		if page.Due(now, ttlNow) {
			at := now // read before the first segment
			for key := range expiries {
				expiries[key] = at + 500 + ttlNow // PEXPIRE lands after at
			}
			page = segment.Renewed(at, ttlNow)
			renewals++
		}
		for key, expiry := range expiries {
			if expiry < page.At+page.TTL {
				t.Fatalf("round %d: key %d expires %d, before the page's %d", round, key, expiry, page.At+page.TTL)
			}
			if expiry <= now {
				t.Fatalf("round %d: key %d expired", round, key)
			}
		}
		if page.Fallback(now) {
			t.Fatalf("round %d: renewed whenever due, the page reached its fallback", round)
		}
	}
	if renewals < 20 {
		t.Fatalf("%d renewals in 400 rounds, want one about every quarter lifetime", renewals)
	}
}

// Pages made together fall due on different rounds, each no later than it
// was made; a page made of others carries the earliest time and shortest
// lifetime of them; and a lifetime is lowered, never raised.
func TestNewPagesAreSpreadAndCarriedPagesRenewNoLater(t *testing.T) {
	const now, ttl = int64(1_800_000_000_000), int64(20 * 60_000)
	seen := map[int64]bool{}
	for page := 0; page < 16; page++ {
		renewal := segment.NewPage(now, "plan", page, ttl)
		if renewal.At > now || renewal.At <= now-ttl/4 || renewal.TTL != ttl {
			t.Fatalf("page %d made at %d: %+v, want within the quarter lifetime before", page, now, renewal)
		}
		seen[renewal.At] = true
	}
	if len(seen) < 12 {
		t.Fatalf("16 pages made together at %d distinct times, want them spread", len(seen))
	}
	if a, b := segment.NewPage(now, "plan-a", 0, ttl), segment.NewPage(now, "plan-b", 0, ttl); a.At == b.At {
		t.Fatalf("two Plans' first pages at the same time %d", a.At)
	}
	carried := segment.Carried([]segment.Renewal{{At: 500, TTL: 9000}, {At: 300, TTL: 12000}, {At: 400, TTL: 6000}})
	if carried != (segment.Renewal{At: 300, TTL: 6000}) {
		t.Fatalf("carried %+v, want the earliest time and the shortest lifetime", carried)
	}
	if lowered := (segment.Renewal{At: 1, TTL: 100}).Lowered(200); lowered.TTL != 100 {
		t.Fatalf("lowered to %d by a longer lifetime, want it kept", lowered.TTL)
	}
	renewal := segment.Renewal{At: 0, TTL: 1000}
	if renewal.Due(249, 2000) || !renewal.Due(250, 2000) || !renewal.Due(150, 600) || renewal.Due(149, 600) {
		t.Fatal("a page falls due a quarter of the shorter of its lifetime and this round's after its renewal")
	}
	if renewal.Fallback(499) || !renewal.Fallback(500) {
		t.Fatal("a page falls back half its lifetime after its renewal")
	}
}

func TestAMarkReadsBackAndRefusesWhatIsNotOne(t *testing.T) {
	mark := segment.Mark{Version: execution.ApplyVersion{StateApplyEpoch: 3, EvaluationTime: 1200, SlotDigest: "slot"},
		Position: 1140, Pages: 2, Renewed: []segment.Renewal{{At: 10, TTL: 100}, {At: 20, TTL: 100}}}
	raw, err := segment.EncodeMark(mark)
	if err != nil {
		t.Fatal(err)
	}
	read, err := segment.DecodeMark(raw)
	if err != nil || !reflect.DeepEqual(read, mark) {
		t.Fatalf("read back %+v, %v, want %+v", read, err, mark)
	}
	for name, bad := range map[string]segment.Mark{
		"three pages":    {Pages: 3, Renewed: make([]segment.Renewal, 3)},
		"renewals short": {Pages: 2, Renewed: []segment.Renewal{{At: 1, TTL: 1}}},
		"no lifetime":    {Pages: 1, Renewed: []segment.Renewal{{At: 1}}},
	} {
		if _, err := segment.EncodeMark(bad); !errors.Is(err, segment.ErrMark) {
			t.Errorf("%s: %v, want ErrMark", name, err)
		}
	}
	if _, err := segment.DecodeMark([]byte(`{"schema":"other","pages":1,"renewed":[{"at":1,"ttl":1}]}`)); !errors.Is(err, segment.ErrMark) {
		t.Fatalf("another schema read as a mark: %v", err)
	}
}

// storedView is a found record of one series and two Levels, its points
// every 60 s up to last.
func storedView(t *testing.T, series execution.SeriesIdentityDigest, first, last int64) execution.RuntimeStateView {
	t.Helper()
	view := execution.RuntimeStateView{Identity: execution.StateKeyIdentity{SeriesIdentityDigest: series},
		Status: execution.StateFoundReady, LastProcessedEventTime: last,
		Levels: []execution.RuntimeLevelStateView{{LevelID: 1, HistoryCompleteness: execution.HistoryFull, LastProcessedEventTime: last, LastRealPointTime: last},
			{LevelID: 2, HistoryCompleteness: execution.HistoryFull, LastProcessedEventTime: last, LastRealPointTime: last}}}
	for at := first; at <= last; at += 60 {
		recordID, err := contract.DeriveRecordIDV2(string(series), at)
		if err != nil {
			t.Fatal(err)
		}
		view.History = append(view.History, execution.StateHistoryPoint{RecordID: recordID, SourceTime: at,
			Levels: []execution.StateLevelFact{{LevelID: 1, DetectFingerprint: "f1", Result: execution.LevelFactNormal},
				{LevelID: 2, DetectFingerprint: "f2", Result: execution.LevelFactNormal}}})
	}
	return view
}

// A member's read is its State's own points, then a normal point at every
// interval its segment covers after them up to the mark - each with the
// record id the series derives there and its Levels' fingerprints - and the
// processed and last real times read as the mark; the record retains what it
// would have. Written round by round, the record would be the same.
func TestAMembersReadIsItsPointsAndANormalPointWhereverItsSegmentCovers(t *testing.T) {
	series := digest("extend")
	stored := storedView(t, series, 600, 900)
	extended, outcome := segment.Extend(stored, segment.Coverage{Entry: segment.Entry{Digest: series, Start: 900},
		Position: 1200, Interval: 60, Retention: 8})
	if outcome != segment.Extended {
		t.Fatalf("outcome %s, want extended", outcome)
	}
	written := storedView(t, series, 780, 1200) // the eight points a record of every round keeps
	if !reflect.DeepEqual(extended.History, written.History) {
		t.Fatalf("extended history %+v,\nwant the record written every round %+v", extended.History, written.History)
	}
	if extended.LastProcessedEventTime != 1200 || extended.Levels[0].LastProcessedEventTime != 1200 || extended.Levels[1].LastRealPointTime != 1200 {
		t.Fatalf("extended times %+v, want the mark's position", extended)
	}
	if stored.LastProcessedEventTime != 900 || len(stored.History) != 6 {
		t.Fatal("Extend changed the view it was given")
	}
	// A Level that keeps no last real point - it does not end on no data -
	// keeps none.
	untracked := storedView(t, series, 600, 900)
	untracked.Levels[1].LastRealPointTime = 0
	if extended, _ := segment.Extend(untracked, segment.Coverage{Entry: segment.Entry{Digest: series, Start: 900},
		Position: 1200, Interval: 60, Retention: 8}); extended.Levels[1].LastRealPointTime != 0 || extended.Levels[0].LastRealPointTime != 1200 {
		t.Fatalf("last real points %d and %d, want the tracked one at the end and the untracked one zero",
			extended.Levels[0].LastRealPointTime, extended.Levels[1].LastRealPointTime)
	}
}

// The segment reaches no further than it covers: an open one to the mark,
// a closed one to its close; a mark that stopped is not "now", and what came
// after it is a gap. A State whose own points reach the end has nothing to
// extend, and a key not found is no read.
func TestASegmentIsReadOnlyAsFarAsItCovers(t *testing.T) {
	series := digest("reach")
	stored := storedView(t, series, 600, 900)
	closed, outcome := segment.Extend(stored, segment.Coverage{Entry: segment.Entry{Digest: series, Start: 900, Closed: 1020},
		Position: 1200, Interval: 60, Retention: 100})
	if outcome != segment.Extended || closed.LastProcessedEventTime != 1020 || closed.History[len(closed.History)-1].SourceTime != 1020 {
		t.Fatalf("closed segment read to %d (%s), want its close at 1020", closed.LastProcessedEventTime, outcome)
	}
	stopped, _ := segment.Extend(stored, segment.Coverage{Entry: segment.Entry{Digest: series, Start: 900},
		Position: 960, Interval: 60, Retention: 100})
	if stopped.LastProcessedEventTime != 960 {
		t.Fatalf("a mark at 960 read to %d", stopped.LastProcessedEventTime)
	}
	if view, outcome := segment.Extend(stored, segment.Coverage{Entry: segment.Entry{Digest: series, Start: 600},
		Position: 900, Interval: 60, Retention: 100}); outcome != segment.Within || !reflect.DeepEqual(view, stored) {
		t.Fatalf("a State up to the mark: %s, want within and the read as stored", outcome)
	}
	missing := execution.RuntimeStateView{Identity: stored.Identity, Status: execution.StateMissingWarming}
	if view, outcome := segment.Extend(missing, segment.Coverage{Entry: segment.Entry{Digest: series, Start: 900},
		Position: 1200, Interval: 60, Retention: 100}); outcome != segment.RefusedNotFound || len(view.History) != 0 {
		t.Fatalf("a key not found: %s with %d points, want refused and no point", outcome, len(view.History))
	}
}

// Points that cannot be made the way the record would hold them leave the
// read as stored - a gap, never normal - and say why.
func TestARecordExtendCannotMatchIsReadAsStored(t *testing.T) {
	series := digest("refuse")
	stored := storedView(t, series, 600, 900)
	cover := segment.Coverage{Entry: segment.Entry{Digest: series, Start: 900}, Position: 1200, Interval: 60, Retention: 100}
	offGrid := cover
	offGrid.Position = 1230
	noFingerprint := storedView(t, series, 600, 900)
	for index := range noFingerprint.History {
		noFingerprint.History[index].Levels = noFingerprint.History[index].Levels[:1]
	}
	clash := storedView(t, series, 600, 900)
	clash.History = append(clash.History, execution.StateHistoryPoint{RecordID: "legacy", SourceTime: 1200, Levels: clash.History[0].Levels})
	notASeries := storedView(t, series, 600, 900)
	notASeries.Identity.SeriesIdentityDigest = "not-a-digest"
	for _, refusal := range []struct {
		name  string
		view  execution.RuntimeStateView
		cover segment.Coverage
		want  segment.Extension
	}{
		{"off grid", stored, offGrid, segment.RefusedOffGrid},
		{"a Level with no fingerprint", noFingerprint, cover, segment.RefusedFingerprint},
		{"a stored point under another id", clash, cover, segment.RefusedRecordClash},
		{"a series no id derives for", notASeries, cover, segment.RefusedRecordID},
		{"no interval", stored, segment.Coverage{Entry: cover.Entry, Position: 1200, Retention: 100}, segment.RefusedBadCoverage},
	} {
		view, outcome := segment.Extend(refusal.view, refusal.cover)
		if outcome != refusal.want || !reflect.DeepEqual(view, refusal.view) {
			t.Errorf("%s: %s, want %s and the read as stored", refusal.name, outcome, refusal.want)
		}
	}
}
