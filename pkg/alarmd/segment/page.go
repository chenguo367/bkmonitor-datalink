// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package segment is a Plan's stable segments (014 B3): the member pages of
// the series a Plan's rounds found steady and normal, and the mark that says
// up to which position every open member is read as normal without a write
// of its own. It holds the data and its rules only - the encodings, the
// bucketing, the page renewal table and the read rule - and talks to no
// store.
package segment

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// PageEntries bounds the entries of one page: about 40 bytes each, 160 KiB
// at most, under the 200 KiB a script carries. PageFill is the fill a Plan's
// page count is sized for, so that the spread of a hash over its pages
// leaves every page under the bound.
const (
	PageEntries = 4096
	PageFill    = 3072
)

// digestBytes is a series identity digest's length: the whole digest is
// kept, not a prefix of it, so a series outside the pages that shares a
// prefix with a member is never read as normal.
const digestBytes = 32

// Entry is one member of a page: the series, the source time its stable
// segment starts after (its State's last explicit point when it joined), and
// where it was closed - zero while it is open.
type Entry struct {
	Digest execution.SeriesIdentityDigest
	Start  int64
	Closed int64
}

// Open is whether the member's segment is still open.
func (entry Entry) Open() bool { return entry.Closed == 0 }

// Errors a page refuses with, by what is wrong with it.
var (
	ErrDigest    = errors.New("segment: a series identity digest is 64 lowercase hexadecimal characters")
	ErrPageFull  = errors.New("segment: a page holds at most 4096 entries")
	ErrPageBytes = errors.New("segment: not a member page")
	ErrPageOrder = errors.New("segment: a page's entries are in digest order, each once")
	ErrPosition  = errors.New("segment: a segment starts at a source time and closes after it")
)

const (
	pageMagic   = "SGP"
	pageVersion = 1
)

// EncodePage is a page's bytes: the entries in digest order, each its whole
// digest and its two source times.
func EncodePage(entries []Entry) ([]byte, error) {
	if len(entries) > PageEntries {
		return nil, fmt.Errorf("%w: %d", ErrPageFull, len(entries))
	}
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Digest < sorted[j].Digest })
	buffer := make([]byte, 0, len(pageMagic)+1+binary.MaxVarintLen64+len(sorted)*(digestBytes+2*binary.MaxVarintLen64))
	buffer = append(buffer, pageMagic...)
	buffer = append(buffer, pageVersion)
	buffer = binary.AppendUvarint(buffer, uint64(len(sorted)))
	var previous []byte
	for _, entry := range sorted {
		raw, err := digestOf(entry.Digest)
		if err != nil {
			return nil, err
		}
		if previous != nil && bytes.Compare(previous, raw) >= 0 {
			return nil, fmt.Errorf("%w: %s twice", ErrPageOrder, entry.Digest)
		}
		if entry.Start < 0 || entry.Closed < 0 || entry.Closed != 0 && entry.Closed < entry.Start {
			return nil, fmt.Errorf("%w: %s starts %d, closed %d", ErrPosition, entry.Digest, entry.Start, entry.Closed)
		}
		buffer = append(buffer, raw...)
		buffer = binary.AppendUvarint(buffer, uint64(entry.Start))
		buffer = binary.AppendUvarint(buffer, uint64(entry.Closed))
		previous = raw
	}
	return buffer, nil
}

// PageBytes is the bytes EncodePage writes for entries, counted without
// writing them.
func PageBytes(entries []Entry) int {
	size := len(pageMagic) + 1 + uvarintLen(uint64(len(entries)))
	for _, entry := range entries {
		size += digestBytes + uvarintLen(uint64(entry.Start)) + uvarintLen(uint64(entry.Closed))
	}
	return size
}

func uvarintLen(value uint64) int {
	length := 1
	for value >= 0x80 {
		value >>= 7
		length++
	}
	return length
}

// DecodePage reads a page back, refusing bytes that are not one rather than
// reading a part of them.
func DecodePage(raw []byte) ([]Entry, error) {
	if len(raw) < len(pageMagic)+1 || string(raw[:len(pageMagic)]) != pageMagic || raw[len(pageMagic)] != pageVersion {
		return nil, ErrPageBytes
	}
	rest := raw[len(pageMagic)+1:]
	count, read := binary.Uvarint(rest)
	if read <= 0 || count > PageEntries {
		return nil, fmt.Errorf("%w: entry count", ErrPageBytes)
	}
	rest = rest[read:]
	entries := make([]Entry, 0, count)
	var previous []byte
	for index := uint64(0); index < count; index++ {
		if len(rest) < digestBytes {
			return nil, fmt.Errorf("%w: entry %d cut short", ErrPageBytes, index)
		}
		digest := rest[:digestBytes]
		if previous != nil && bytes.Compare(previous, digest) >= 0 {
			return nil, fmt.Errorf("%w: entry %d", ErrPageOrder, index)
		}
		rest = rest[digestBytes:]
		start, read := binary.Uvarint(rest)
		if read <= 0 {
			return nil, fmt.Errorf("%w: entry %d start", ErrPageBytes, index)
		}
		rest = rest[read:]
		closed, read := binary.Uvarint(rest)
		if read <= 0 {
			return nil, fmt.Errorf("%w: entry %d closed", ErrPageBytes, index)
		}
		rest = rest[read:]
		if start > 1<<62 || closed > 1<<62 || closed != 0 && closed < start {
			return nil, fmt.Errorf("%w: entry %d", ErrPosition, index)
		}
		entries = append(entries, Entry{Digest: execution.SeriesIdentityDigest(hex.EncodeToString(digest)),
			Start: int64(start), Closed: int64(closed)})
		previous = digest
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrPageBytes, len(rest))
	}
	return entries, nil
}

func digestOf(digest execution.SeriesIdentityDigest) ([]byte, error) {
	if len(digest) != 2*digestBytes {
		return nil, fmt.Errorf("%w: %q", ErrDigest, digest)
	}
	for _, character := range []byte(digest) {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return nil, fmt.Errorf("%w: %q", ErrDigest, digest)
		}
	}
	raw, err := hex.DecodeString(string(digest))
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ErrDigest, digest)
	}
	return raw, nil
}

// PageCount is the pages a Plan's members are bucketed over: the least power
// of two that holds them at PageFill a page, one at least. A power of two, so
// that a count that doubles splits each page in two and one that halves
// joins two into one (Sources), and a renewal time carries over page by page.
func PageCount(members int) int {
	pages := 1
	for pages*PageFill < members {
		pages *= 2
	}
	return pages
}

// BucketOf is the page of pages a series is kept on: the top bits of its
// digest, so that the pages of a doubled count are each half of one before.
func BucketOf(digest execution.SeriesIdentityDigest, pages int) (int, error) {
	if pages < 1 || pages&(pages-1) != 0 {
		return 0, fmt.Errorf("segment: %d pages is not a power of two", pages)
	}
	raw, err := digestOf(digest)
	if err != nil {
		return 0, err
	}
	if pages == 1 {
		return 0, nil
	}
	bits := 0
	for 1<<bits < pages {
		bits++
	}
	return int(binary.BigEndian.Uint64(raw[:8]) >> (64 - bits)), nil
}

// Sources is the pages of a count from whose members page of a count to is
// made: the one page it was half of when the count doubled, the two it joins
// when it halved, itself when it did not change.
func Sources(page, from, to int) []int {
	switch {
	case to == from:
		return []int{page}
	case to > from:
		return []int{page / (to / from)}
	default:
		ratio := from / to
		sources := make([]int, 0, ratio)
		for source := page * ratio; source < (page+1)*ratio; source++ {
			sources = append(sources, source)
		}
		return sources
	}
}
