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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

func storeDigest(seed string) execution.SeriesIdentityDigest {
	sum := sha256.Sum256([]byte(seed))
	return execution.SeriesIdentityDigest(hex.EncodeToString(sum[:]))
}

// A round's renewal table: a page kept keeps its own; a count that doubled
// gives each half its source's, one that halved the earlier time and the
// shorter life of the two it joins; a Plan's first pages are new, staggered
// earlier than now by less than a quarter life; every one lowered to the
// life this round gives.
func TestARoundsRenewalsAreKeptCarriedOrNew(t *testing.T) {
	prior := Mark{Pages: 2, Renewed: []Renewal{{At: 100, TTL: 4000}, {At: 50, TTL: 9000}}}
	if kept := Renewals(prior, true, 2, "p", 1000, 8000); kept[0] != prior.Renewed[0] || kept[1] != (Renewal{At: 50, TTL: 8000}) {
		t.Fatalf("kept %v, want each page's own, lowered to 8000", kept)
	}
	if doubled := Renewals(prior, true, 4, "p", 1000, 9000); doubled[0] != prior.Renewed[0] || doubled[1] != prior.Renewed[0] ||
		doubled[2] != prior.Renewed[1] || doubled[3] != prior.Renewed[1] {
		t.Fatalf("doubled %v, want each half its source's", doubled)
	}
	if halved := Renewals(prior, true, 1, "p", 1000, 9000); halved[0] != (Renewal{At: 50, TTL: 4000}) {
		t.Fatalf("halved %v, want the earlier time and the shorter life", halved)
	}
	fresh := Renewals(Mark{}, false, 4, "p", 100_000, 8000)
	distinct := map[int64]bool{}
	for page, renewal := range fresh {
		if renewal.TTL != 8000 || renewal.At > 100_000 || renewal.At <= 100_000-2000 {
			t.Fatalf("new page %d renewal %+v, want staggered within a quarter life before now", page, renewal)
		}
		distinct[renewal.At] = true
	}
	if len(distinct) < 2 {
		t.Fatalf("new pages %v all fall due together", fresh)
	}
	// Two Plans' first pages, made in one round as after a restart, fall
	// due apart: staggered by the Plan, not by the page alone.
	if Renewals(Mark{}, false, 1, "p", 100_000, 8000)[0] == Renewals(Mark{}, false, 1, "q", 100_000, 8000)[0] {
		t.Fatal("two Plans' first pages fall due together")
	}
}

// Stored segments read as a book only whole: a mark naming more pages than
// read, a page missing, a page that does not decode or a member on another
// page than its digest buckets to are ErrStored; none stored is an empty
// book.
func TestStoredSegmentsReadAsABookOnlyWhole(t *testing.T) {
	empty, _, had, err := Stored{}.Book()
	if err != nil || had || empty.Len() != 0 {
		t.Fatalf("none stored: %v %v %v", empty, had, err)
	}
	series := storeDigest("1")
	page, err := EncodePage([]Entry{{Digest: series, Start: 60}})
	if err != nil {
		t.Fatal(err)
	}
	mark := func(pages int) []byte {
		renewed := make([]Renewal, pages)
		for index := range renewed {
			renewed[index] = Renewal{At: 1, TTL: 1}
		}
		raw, err := EncodeMark(Mark{Position: 60, Pages: pages, Renewed: renewed, Sizes: make([]int, pages)})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if book, read, had, err := (Stored{Mark: mark(1), Pages: [][]byte{page}}).Book(); err != nil || !had || book.Len() != 1 || read.Position != 60 {
		t.Fatalf("whole: %v %+v %v %v", book, read, had, err)
	}
	other, _ := EncodePage(nil)
	bucket, _ := BucketOf(series, 2)
	misplaced := [][]byte{page, other}
	if bucket == 0 {
		misplaced = [][]byte{other, page}
	}
	for name, stored := range map[string]Stored{
		"fewer pages than the mark names": {Mark: mark(2), Pages: [][]byte{page}},
		"a page missing":                  {Mark: mark(1), Pages: [][]byte{nil}},
		"a page that does not decode":     {Mark: mark(1), Pages: [][]byte{[]byte("not a page")}},
		"a member off its bucket":         {Mark: mark(2), Pages: misplaced},
		"a mark that does not decode":     {Mark: []byte("{}"), Pages: nil},
	} {
		if _, _, _, err := stored.Book(); !errors.Is(err, ErrStored) {
			t.Errorf("%s: %v, want ErrStored", name, err)
		}
	}
	if members := (Stored{Mark: mark(2), Pages: [][]byte{page, nil}}).Members(); members != 1 {
		t.Fatalf("members of the pages that read: %d, want 1", members)
	}
}
