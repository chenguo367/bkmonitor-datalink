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
	"fmt"
	"sort"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Book is one Plan's members, as its pages hold them: who joined and from
// where, who was closed and where, and which pages a round's changes rewrite.
// Not safe for concurrent use; a Plan's round changes its book alone.
type Book struct {
	members map[execution.SeriesIdentityDigest]Entry
	// pages is the count the pages were last laid out over, dirty the pages
	// changed since (Settle).
	pages int
	dirty map[int]bool
}

// NewBook is a book holding entries, as their pages were read: laid out over
// the count their number asks for, nothing yet changed.
func NewBook(entries []Entry) (*Book, error) {
	book := &Book{members: make(map[execution.SeriesIdentityDigest]Entry, len(entries)), dirty: map[int]bool{}}
	for _, entry := range entries {
		if _, err := digestOf(entry.Digest); err != nil {
			return nil, err
		}
		if _, twice := book.members[entry.Digest]; twice {
			return nil, fmt.Errorf("%w: %s twice", ErrPageOrder, entry.Digest)
		}
		book.members[entry.Digest] = entry
	}
	book.pages = book.layout()
	return book, nil
}

// Len is the members, open and closed.
func (book *Book) Len() int { return len(book.members) }

// Entry is a series' entry, when it is a member.
func (book *Book) Entry(digest execution.SeriesIdentityDigest) (Entry, bool) {
	entry, ok := book.members[digest]
	return entry, ok
}

// Each visits every member, in no order.
func (book *Book) Each(visit func(Entry)) {
	for _, entry := range book.members {
		visit(entry)
	}
}

// Join makes a series a member, its segment starting after start: the
// position of its last explicit point, the one the round just wrote.
func (book *Book) Join(digest execution.SeriesIdentityDigest, start int64) error {
	if _, member := book.members[digest]; member {
		return fmt.Errorf("segment: %s is a member already", digest)
	}
	if start < 0 {
		return fmt.Errorf("%w: %s starts %d", ErrPosition, digest, start)
	}
	if err := book.touch(digest); err != nil {
		return err
	}
	book.members[digest] = Entry{Digest: digest, Start: start}
	return nil
}

// Close ends an open member's segment at at, the last position it was read
// as normal: the series was not in the round after it.
func (book *Book) Close(digest execution.SeriesIdentityDigest, at int64) error {
	entry, member := book.members[digest]
	if !member || !entry.Open() {
		return fmt.Errorf("segment: %s has no open segment to close", digest)
	}
	// Zero is an open segment's: a source time of zero closes nothing.
	if at <= 0 || at < entry.Start {
		return fmt.Errorf("%w: %s starts %d, closed %d", ErrPosition, digest, entry.Start, at)
	}
	if err := book.touch(digest); err != nil {
		return err
	}
	entry.Closed = at
	book.members[digest] = entry
	return nil
}

// Remove takes a member out: written whole this round, its own State says
// what its segment did.
func (book *Book) Remove(digest execution.SeriesIdentityDigest) error {
	if _, member := book.members[digest]; !member {
		return nil
	}
	if err := book.touch(digest); err != nil {
		return err
	}
	delete(book.members, digest)
	return nil
}

// Expire removes the closed members whose segment ended before before - the
// position past which neither their window nor their key's life reaches -
// and reports how many.
func (book *Book) Expire(before int64) (int, error) {
	expired := 0
	for digest, entry := range book.members {
		if entry.Open() || entry.Closed >= before {
			continue
		}
		if err := book.touch(digest); err != nil {
			return expired, err
		}
		delete(book.members, digest)
		expired++
	}
	return expired, nil
}

// Pages is the members laid out over their pages, each page in digest order:
// the count their number asks for, doubled while a page would pass
// PageEntries.
func (book *Book) Pages() [][]Entry {
	count := book.layout()
	if count != book.pages {
		for page := 0; page < count; page++ {
			book.dirty[page] = true
		}
		book.pages = count
	}
	pages := make([][]Entry, count)
	for digest, entry := range book.members {
		page, _ := BucketOf(digest, count)
		pages[page] = append(pages[page], entry)
	}
	for _, page := range pages {
		sort.Slice(page, func(i, j int) bool { return page[i].Digest < page[j].Digest })
	}
	return pages
}

// Dirty is the pages changed since Settle, in order: the ones a round
// rewrites. A count that changed rewrites every page.
func (book *Book) Dirty() []int {
	if count := book.layout(); count != book.pages {
		dirty := make([]int, count)
		for page := range dirty {
			dirty[page] = page
		}
		return dirty
	}
	dirty := make([]int, 0, len(book.dirty))
	for page := range book.dirty {
		dirty = append(dirty, page)
	}
	sort.Ints(dirty)
	return dirty
}

// Settle forgets the changes: the pages they rewrote are written.
func (book *Book) Settle() {
	book.pages = book.layout()
	book.dirty = map[int]bool{}
}

// touch marks the page digest is on, at the count laid out now, changed.
func (book *Book) touch(digest execution.SeriesIdentityDigest) error {
	page, err := BucketOf(digest, book.pages)
	if err != nil {
		return err
	}
	book.dirty[page] = true
	return nil
}

// layout is the count the members would be laid out over: PageCount, and
// doubled while a page would hold more than PageEntries.
func (book *Book) layout() int {
	count := PageCount(len(book.members))
	for {
		sizes := make([]int, count)
		full := false
		for digest := range book.members {
			page, _ := BucketOf(digest, count)
			sizes[page]++
			if sizes[page] > PageEntries {
				full = true
				break
			}
		}
		if !full {
			return count
		}
		count *= 2
	}
}
