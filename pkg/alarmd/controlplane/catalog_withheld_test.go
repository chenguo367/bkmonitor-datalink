// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import "testing"

// The pair is what can be acted on. A CONFIG_REJECTED strategy has stopped
// detecting and a STALE_CONFIG one is still running its last good Plan, so the
// same reason means two different things depending on which it is, and a count
// by reason alone cannot separate them.
func TestWithheldCountsThePairAndNotJustTheReason(t *testing.T) {
	composition := ComposeCatalog(Catalog{Dispositions: []ObjectDisposition{
		{SourceID: "1", Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"},
		{SourceID: "2", Disposition: DispositionStaleConfig, Reason: "NO_DATA_CONFIG_INVALID"},
		{SourceID: "3", Disposition: DispositionConfigRejected, Reason: "PLAN_INVALID"},
		{SourceID: "4", Disposition: DispositionAccepted},
	}})
	for key, want := range map[WithheldKey]int{
		{Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"}: 1,
		{Disposition: DispositionStaleConfig, Reason: "NO_DATA_CONFIG_INVALID"}:    1,
		{Disposition: DispositionConfigRejected, Reason: "PLAN_INVALID"}:           1,
	} {
		if got := composition.Withheld[key]; got != want {
			t.Fatalf("Withheld[%+v] = %d, want %d", key, got, want)
		}
	}
	// An accepted object was not withheld, so it is in no pair at all.
	for key := range composition.Withheld {
		if key.Disposition == DispositionAccepted {
			t.Fatalf("Withheld carries %+v; an accepted object was not withheld", key)
		}
	}
	// The partition adds up against the disposition counts it came from.
	var withheld int
	for _, count := range composition.Withheld {
		withheld += count
	}
	if want := 3; withheld != want {
		t.Fatalf("withheld total = %d, want %d", withheld, want)
	}
}

// The reason reaches the count as it was attached, with no list deciding
// whether it is allowed to. A list would have to be exactly the set this
// package writes, and the first version of it held twenty of the forty-odd -
// which would have filed most of a deployment's rejected objects under a label
// naming nothing while a guard reported all was well.
func TestWithheldCarriesTheReasonAsItWasAttached(t *testing.T) {
	const unusual = "A_REASON_NOBODY_LISTED"
	composition := ComposeCatalog(Catalog{Dispositions: []ObjectDisposition{
		{SourceID: "1", Disposition: DispositionConfigRejected, Reason: unusual},
	}})
	key := WithheldKey{Disposition: DispositionConfigRejected, Reason: unusual}
	if got := composition.Withheld[key]; got != 1 {
		t.Fatalf("Withheld[%+v] = %d, want the reason counted under its own name", key, got)
	}
	for held := range composition.Withheld {
		if held.Reason != unusual {
			t.Fatalf("Withheld carries %+v; the reason was rewritten on the way in", held)
		}
	}
}

// The withheld pairs must add up to the objects that were not accepted.
//
// This is what makes a zero readable. On the deployment this was written for,
// every pair reads zero because nothing is being withheld for a reason this
// release introduced - and a family whose expected value is zero cannot tell
// "nothing is wrong" from "nothing was computed". Read against the disposition
// counts it partitions, it can: the same pass over the same dispositions
// produces both, so a zero that adds up is a zero that was computed.
func TestWithheldPartitionsTheObjectsThatWereNotAccepted(t *testing.T) {
	composition := ComposeCatalog(Catalog{Dispositions: []ObjectDisposition{
		{SourceID: "1", Disposition: DispositionAccepted},
		{SourceID: "2", Disposition: DispositionAccepted},
		{SourceID: "3", Disposition: DispositionConfigRejected, Reason: "NO_DATA_CONFIG_INVALID"},
		{SourceID: "4", Disposition: DispositionUnsupported, Reason: "ALGORITHM_NOT_MIGRATED"},
		{SourceID: "5", Disposition: DispositionStaleConfig, Reason: "PLAN_INVALID"},
		{SourceID: "6", Disposition: Disposition("A_DISPOSITION_NOBODY_LISTED"), Reason: "PLAN_INVALID"},
	}})

	var objects, accepted int
	for disposition, count := range composition.Objects {
		objects += count
		if disposition == DispositionAccepted {
			accepted += count
		}
	}
	var withheld int
	for _, count := range composition.Withheld {
		withheld += count
	}
	if want := objects - accepted; withheld != want {
		t.Fatalf("withheld total = %d, want %d: the pairs do not partition the objects that were not "+
			"accepted, so a zero in the family cannot be read against the disposition counts", withheld, want)
	}

	// Including the object whose disposition this build does not name: it is
	// folded to other in both counts, so the partition holds rather than
	// quietly losing a member on one side of the comparison.
	key := WithheldKey{Disposition: DispositionOther, Reason: "PLAN_INVALID"}
	if got := composition.Withheld[key]; got != 1 {
		t.Fatalf("Withheld[%+v] = %d, want the unnamed disposition folded the same way Objects folds it", key, got)
	}
}
