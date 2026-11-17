// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package segment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The store a Plan's segments are kept in (014 B3 3.2): its mark and its
// member pages, beside its State on the target its State is routed to, read
// once a Slot before its State and written once a round before its Progress.

// PlanKey is one Plan's segments: the Plan, the piece of a split strategy
// it is (Shard, zero for a Plan not split), under one generation. Two pieces
// of one strategy are two Plans' segments - mark, pages, cache entries and
// the shadow's books - as they are two gap markers and two no-data
// memories: sharing them, each piece would read the others' members as its
// own and write a mark over theirs.
type PlanKey struct {
	Plan       execution.PlanIdentity
	Shard      execution.ShardRef
	Generation execution.StateGeneration
}

// Lifetime is what a Plan's segment keys live by: its State's lifetime, by
// the same retention and horizon (the keys go with the State they stand
// for), and its evaluation interval, how often its round reads and writes
// them.
type Lifetime struct {
	Retention []execution.StateRetentionRequirement
	Horizon   int64
	Interval  time.Duration
}

// ReadPlan is one Plan a Slot reads the segments of.
type ReadPlan struct {
	Key PlanKey
	Lifetime
}

// ReadRequest reads the segments of a Slot's due Plans, under the owner
// fence the Slot holds.
type ReadRequest struct {
	Contract   execution.FrozenExecutionContractRef
	OwnerFence execution.OwnerFence
	Plans      []ReadPlan
}

// Stored is one Plan's segments as read: its mark's bytes, nil when it has
// none, and its pages', one for each page the mark names, nil for one
// missing; Source and Reason say where they were read from and why. TTL is
// the life the Plan's keys are written with this round, its State's: what a
// page's renewal says its members' keys were given.
type Stored struct {
	Key    PlanKey
	Mark   []byte
	Pages  [][]byte
	Source string
	Reason string
	TTL    time.Duration
}

// Members is the members of the pages that read, whatever else did not: a
// Plan started over for its stored segments counts what it let go.
func (stored Stored) Members() int {
	members := 0
	for _, raw := range stored.Pages {
		if raw == nil {
			continue
		}
		if entries, err := DecodePage(raw); err == nil {
			members += len(entries)
		}
	}
	return members
}

// ReadResult is the Slot's segments, one a Plan in request order.
type ReadResult struct {
	Plans []Stored
}

// PlanWrite is one Plan's round of its segments: the pages that changed, by
// number, the mark, the count of pages it names and the count the mark
// before named - the pages past the count are removed and the rest kept
// alive with the mark.
type PlanWrite struct {
	Key PlanKey
	Lifetime
	Pages      map[int][]byte
	Mark       []byte
	PageCount  int
	PriorCount int
}

// WriteRequest writes a Slot's Plans' rounds under the fence of its writes.
type WriteRequest struct {
	Contract execution.FrozenExecutionContractRef
	Fence    execution.StateApplyFence
	Plans    []PlanWrite
}

// WriteResult is each Plan's write's result, in request order.
type WriteResult struct {
	Plans []string
}

// PageRenewal is a page due for its renewal (3.5): the open members whose
// State keys it gives the Plan's lifetime.
type PageRenewal struct {
	Page    int
	Members []execution.SeriesIdentityDigest
}

// PlanRenewal is one Plan's pages due this round.
type PlanRenewal struct {
	Key PlanKey
	Lifetime
	Pages []PageRenewal
}

// RenewRequest renews a Slot's due pages under the fence of its writes,
// before their marks are written.
type RenewRequest struct {
	Contract execution.FrozenExecutionContractRef
	Fence    execution.StateApplyFence
	Plans    []PlanRenewal
}

// PlanRenewed is how one Plan's renewal came back: Renewed, by page in
// request order, whether every segment carrying its members answered - only
// then does its renewal time move on (3.5 rule 3); Missing, the keys already
// gone; OverBudget, the segments that ran longer than RenewScriptBudget.
type PlanRenewed struct {
	Renewed    []bool
	Missing    int
	OverBudget int
}

// RenewResult is each Plan's renewal, in request order.
type RenewResult struct {
	Plans []PlanRenewed
}

// Store reads, writes and renews the Plans' segments. A write's or a
// renewal's failure is a Plan's result, not an error; an error is a request
// it could not take.
type Store interface {
	ReadSegments(context.Context, ReadRequest) (ReadResult, error)
	WriteSegments(context.Context, WriteRequest) (WriteResult, error)
	RenewSegments(context.Context, RenewRequest) (RenewResult, error)
}

// RenewKeys is the most member keys one renewal script carries, K_max:
// measured on the build host (014 B3 §6.5), 256 keys run at a p99.99 of
// 1,027 µs, 512 at 2,469, on the 2.5 ms line with no margin.
const RenewKeys = 256

// RenewScriptBudget is the longest a renewal script may run: half the 5 ms a
// script may hold Redis, the other half left to a shared host's tail. A
// segment over it is counted over_budget, and nothing else changes: K_max
// changes with a release, not at run time.
const RenewScriptBudget = 2500 * time.Microsecond

// The words of the segments' reads, closed: a read served from the cache
// (cache, hit), or by Redis (redis) and why - cold, nothing kept of it;
// fence_changed, what was kept was kept under another fence; evicted, the
// cache let it go for its budget, the pool's room or its life; unknown_result,
// a write of it did not apply whole - in doubt, refused, or refused by the
// fence after earlier chunks landed; renew_missing, a write or renewal found
// one of its keys gone; failed, the read itself failed and the Slot has no
// segments.
const (
	ReadHit           = "hit"
	ReadCold          = "cold"
	ReadFenceChanged  = "fence_changed"
	ReadEvicted       = "evicted"
	ReadUnknownResult = "unknown_result"
	ReadRenewMissing  = "renew_missing"
	ReadFailed        = "failed"

	SourceCache = "cache"
	SourceRedis = "redis"
)

// ReadReasons are the reasons of a read from Redis; a read from the cache is
// always a hit.
var ReadReasons = []string{ReadCold, ReadFenceChanged, ReadEvicted, ReadUnknownResult, ReadRenewMissing, ReadFailed}

// How a Plan's segment write came back, closed: applied; stale_owner and
// content_moved, the fence refused it; failed, refused otherwise or not sent;
// unknown, its outcome is not known.
const (
	WriteApplied      = "applied"
	WriteStaleOwner   = "stale_owner"
	WriteContentMoved = "content_moved"
	WriteFailed       = "failed"
	WriteUnknown      = "unknown"
)

// WriteResults lists them.
var WriteResults = []string{WriteApplied, WriteStaleOwner, WriteContentMoved, WriteFailed, WriteUnknown}

// ErrStored is segments that do not read back as a book.
var ErrStored = errors.New("segment: stored segments do not read")

// Book is the stored segments as a book and the mark they were read under;
// none stored (no mark) is an empty book and a zero mark. A page missing or
// unreadable, or a member on another page than its digest buckets to, is
// ErrStored: what was lost is not known, so the caller starts the Plan over.
func (stored Stored) Book() (*Book, Mark, bool, error) {
	if stored.Mark == nil {
		book, err := NewBook(nil)
		return book, Mark{}, false, err
	}
	mark, err := DecodeMark(stored.Mark)
	if err != nil {
		return nil, Mark{}, false, fmt.Errorf("%w: %v", ErrStored, err)
	}
	if len(stored.Pages) != mark.Pages {
		return nil, mark, true, fmt.Errorf("%w: %d pages for a mark of %d", ErrStored, len(stored.Pages), mark.Pages)
	}
	var entries []Entry
	for page, raw := range stored.Pages {
		if raw == nil {
			return nil, mark, true, fmt.Errorf("%w: page %d missing", ErrStored, page)
		}
		decoded, err := DecodePage(raw)
		if err != nil {
			return nil, mark, true, fmt.Errorf("%w: page %d: %v", ErrStored, page, err)
		}
		for _, entry := range decoded {
			if bucket, err := BucketOf(entry.Digest, mark.Pages); err != nil || bucket != page {
				return nil, mark, true, fmt.Errorf("%w: %s on page %d of %d", ErrStored, entry.Digest, page, mark.Pages)
			}
		}
		entries = append(entries, decoded...)
	}
	book, err := NewBook(entries)
	if err != nil {
		return nil, mark, true, fmt.Errorf("%w: %v", ErrStored, err)
	}
	if book.pages != mark.Pages {
		return nil, mark, true, fmt.Errorf("%w: %d members lay out over %d pages, not the mark's %d", ErrStored,
			book.Len(), book.pages, mark.Pages)
	}
	return book, mark, true, nil
}

// Renewals is the renewal table of a book's pages as its round leaves them,
// from the mark before (prior, none for a Plan's first round): a page kept
// keeps its own, a page of a count that changed carries its sources' (the
// earliest time, the shortest life), a page with no mark before is new and
// staggered. Lowered to ttl, the life this round gives the keys it writes.
func Renewals(prior Mark, hadMark bool, count int, plan string, now, ttl int64) []Renewal {
	renewed := make([]Renewal, count)
	for page := range renewed {
		if !hadMark {
			renewed[page] = NewPage(now, plan, page, ttl)
			continue
		}
		sources := Sources(page, prior.Pages, count)
		carried := make([]Renewal, len(sources))
		for index, source := range sources {
			carried[index] = prior.Renewed[source]
		}
		renewed[page] = Carried(carried).Lowered(ttl)
	}
	return renewed
}

// BookMemberBytes is the heap one member of a decoded book holds - its
// digest's 64-character string and its slot in the book's map - measured at
// 73 to 87 bytes on go1.26 from 3,000 to 95,760 members, either side of a
// page count's doubling (book_cost_test.go), and held to it there; and
// BookPlanBytes a book's own, its map's and dirty set's fixed part: what a
// Slot reserves for the books it reads (BookBytes).
const (
	BookMemberBytes = 96
	BookPlanBytes   = 512
)

// BookBytes is what a book of members members holds, at most.
func BookBytes(members int) uint64 {
	return uint64(members)*BookMemberBytes + BookPlanBytes
}
