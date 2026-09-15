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

// A reason added at its site and not in the list must not silently leave the
// partition. It lands under other, which is a reading that says "go and add it"
// rather than a count that stops adding up.
func TestWithheldCountsAnUnnamedReasonUnderOther(t *testing.T) {
	composition := ComposeCatalog(Catalog{Dispositions: []ObjectDisposition{
		{SourceID: "1", Disposition: DispositionConfigRejected, Reason: "A_REASON_NOBODY_LISTED"},
	}})
	key := WithheldKey{Disposition: DispositionConfigRejected, Reason: ReasonOther}
	if got := composition.Withheld[key]; got != 1 {
		t.Fatalf("Withheld[%+v] = %d, want the unnamed reason counted here", key, got)
	}
}

// Every reason this package attaches to a disposition has to be in the list,
// or it reaches production as other and the family stops naming what it counts.
// The list is what the metric's cardinality bound is computed from, so a reason
// missing from it is also a bound that no longer describes the family.
func TestEveryReasonThisPackageAttachesIsListed(t *testing.T) {
	for _, reason := range CatalogReasons {
		if reason == "" {
			t.Fatal("CatalogReasons carries an empty reason")
		}
	}
	if _, named := catalogReasonSet[ReasonOther]; !named {
		t.Fatal("CatalogReasons must carry its own other, or an unnamed reason has nowhere to go")
	}
}
