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
	"reflect"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A mark's reading lists its page renewals up to the pages of the largest
// Query Group, 64, and their total: one of 64 pages lists them all, one of
// 128 the first 64 and says 128.
func TestAMarksReadingListsAtMostSixtyFourRenewalsAndTheirTotal(t *testing.T) {
	key := PlanKey{Plan: execution.PlanIdentity{TenantID: "t", BusinessID: "2", StrategyID: "9"}, Generation: "g"}
	for _, pages := range []int{1, 64, 128} {
		mark := Mark{Version: execution.ApplyVersion{EvaluationTime: 1_800_000_000}, Position: 1_799_999_940, Pages: pages,
			Renewed: make([]Renewal, pages), Sizes: make([]int, pages)}
		for page := range mark.Renewed {
			mark.Renewed[page] = Renewal{At: int64(page) + 1, TTL: 60_000}
		}
		raw, err := EncodeMark(mark)
		if err != nil {
			t.Fatal(err)
		}
		listed := min(pages, 64)
		want := PlanReading{Plan: key.Plan, Generation: key.Generation, Mark: MarkFound, Round: 1_800_000_000,
			Position: 1_799_999_940, Pages: pages, Renewed: mark.Renewed[:listed], RenewedTotal: pages}
		if got := ReadMark(key, raw); !reflect.DeepEqual(got, want) {
			t.Fatalf("a mark of %d pages read %+v, want %+v", pages, got, want)
		}
	}
	if got := ReadMark(key, nil); got.Mark != MarkAbsent || got.Pages != 0 || got.Renewed != nil {
		t.Fatalf("no mark read %+v, want absent", got)
	}
	if got := ReadMark(key, []byte(`{"schema":"segment-mark-v1","pages":3}`)); got.Mark != MarkUnreadable || got.Unreadable == "" {
		t.Fatalf("a mark of three pages read %+v, want unreadable with why", got)
	}
}
