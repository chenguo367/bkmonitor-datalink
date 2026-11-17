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
	"runtime"
	"testing"
)

// storedBookOf is a Plan's stored segments of members members, laid out
// over their pages as a round writes them.
func storedBookOf(t *testing.T, members int) Stored {
	t.Helper()
	book, err := NewBook(nil)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < members; index++ {
		if err := book.Join(storeDigest(fmt.Sprint("member-", index)), 1_800_000_000+int64(index)); err != nil {
			t.Fatal(err)
		}
	}
	pages := book.Pages()
	stored := Stored{}
	mark := Mark{Position: 1_800_000_000, Pages: len(pages), Renewed: make([]Renewal, len(pages)), Sizes: make([]int, len(pages))}
	for page, entries := range pages {
		raw, err := EncodePage(entries)
		if err != nil {
			t.Fatal(err)
		}
		stored.Pages = append(stored.Pages, raw)
		mark.Renewed[page], mark.Sizes[page] = Renewal{At: 1, TTL: 1}, len(raw)
	}
	stored.Mark, err = EncodeMark(mark)
	if err != nil {
		t.Fatal(err)
	}
	return stored
}

// A Slot reserves BookBytes for the books it decodes, and holds no more:
// the live heap of decoded books, at the sizes a Plan's membership takes on
// either side of a page count's doubling, is within BookMemberBytes a
// member - and not below half of it, so the reservation is no guess either.
func TestADecodedBookHoldsNoMoreThanItsReservation(t *testing.T) {
	// From a page's worth up: below it the book is smaller than what the
	// heap's reading moves by between two collections, and BookPlanBytes
	// covers it.
	for _, members := range []int{3_000, 6_200, 24_000, 95_760} {
		stored := storedBookOf(t, members)
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		book, _, _, err := stored.Book()
		if err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		perMember := float64(held) / float64(members)
		t.Logf("%d members: %d bytes held, %.1f a member, reserved %d (%s)", members, held, perMember,
			BookBytes(members), runtime.Version())
		if held > int64(BookBytes(members)) || perMember < BookMemberBytes/2 {
			t.Errorf("%d members hold %d bytes, %.1f a member: want within the %d reserved and above half of %d a member",
				members, held, perMember, BookBytes(members), BookMemberBytes)
		}
		runtime.KeepAlive(book)
	}
}
