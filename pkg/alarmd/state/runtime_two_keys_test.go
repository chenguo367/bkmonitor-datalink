// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The migration from the envelope under runtime to the framed record under
// runtime3 has three shapes a series can be read in, and the write must land
// correctly from each of them:
//
//   - first write: only the envelope exists. The view is the envelope, the
//     write creates runtime3, and the envelope is left to its TTL.
//   - bounce: both exist and the envelope is newer, because an old binary
//     took the Query Group back during a rollout and wrote it. The view is
//     the envelope; the write still goes to runtime3, compared against
//     runtime3's own bytes, and continues runtime3's own revision count.
//   - steady: runtime3 exists and is the newer (or only) one. The view is
//     the framed record.
//
// Each case is written against the load and apply paths together, because
// the defect they guard - a compare-and-set that expects the envelope's
// revision on the framed key - is invisible to either path alone: the load
// reports a perfectly good view and the apply reports a perfectly good
// conflict.
func TestARecordIsReadFromEitherKeyAndWrittenToTheFramedOne(t *testing.T) {
	version := applyVersion()
	older := execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: 30, SlotDigest: "slot-0"}
	type seed struct {
		envelope  *execution.ApplyVersion
		envRev    uint64
		framed    *execution.ApplyVersion
		framedRev uint64
	}
	cases := []struct {
		name           string
		seed           seed
		wantView       execution.StateRepresentation
		wantViewRev    uint64
		wantWrittenRev uint64
	}{
		{name: "first write: only the envelope exists",
			seed: seed{envelope: &older, envRev: 7}, wantView: execution.StateRepresentationEnvelope, wantViewRev: 7, wantWrittenRev: 1},
		{name: "bounce: the envelope is newer than the framed record",
			seed:     seed{envelope: &version, envRev: 9, framed: &older, framedRev: 3},
			wantView: execution.StateRepresentationEnvelope, wantViewRev: 9, wantWrittenRev: 4},
		{name: "steady: the framed record is newer than the envelope",
			seed:     seed{envelope: &older, envRev: 9, framed: &version, framedRev: 3},
			wantView: execution.StateRepresentationFramed, wantViewRev: 3, wantWrittenRev: 4},
		{name: "steady: only the framed record exists",
			seed: seed{framed: &version, framedRev: 3}, wantView: execution.StateRepresentationFramed, wantViewRev: 3, wantWrittenRev: 4},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			backend := newPipelineMemoryBackend()
			store := newBatchStore(t, backend, nil)
			identity := seriesIdentity(0)
			envelopeKey, _ := RuntimeStateKeyV2("alarmd", identity)
			framedKey, _ := RuntimeStateKeyV3("alarmd", identity)
			if test.seed.envelope != nil {
				backend.values[envelopeKey], _ = encodeRuntime(seriesMutation(t, identity, *test.seed.envelope, 0, "env"), test.seed.envRev)
			}
			if test.seed.framed != nil {
				backend.values[framedKey], _ = encodeRuntimePacked(seriesMutation(t, identity, *test.seed.framed, 0, ""), test.seed.framedRev)
			}
			envelopeBefore := string(backend.values[envelopeKey])

			next := execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: 120, SlotDigest: "slot-2"}
			loaded, err := store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: frozenRef(),
				Items: []execution.StatePreflightItem{{Identity: identity, ApplyVersion: next}}})
			if err != nil {
				t.Fatal(err)
			}
			view := loaded.Items[0]
			if view.Representation != test.wantView || view.BlobRevision != test.wantViewRev {
				t.Fatalf("loaded view = %s at revision %d, want the %s at revision %d: the newer of the two records is the one read",
					view.Representation, view.BlobRevision, test.wantView, test.wantViewRev)
			}

			// The evaluator writes from the view it read: its expected revision
			// is the view's, whichever key that came from.
			mutation := seriesMutation(t, identity, next, view.BlobRevision, "")
			applied, err := store.ApplyRuntime(context.Background(), execution.StateApplyRequest{Contract: frozenRef(),
				Retention: testRetention(), Items: []execution.StateMutation{mutation}})
			if err != nil {
				t.Fatal(err)
			}
			if applied.Items[0].Status != execution.StateApplied {
				t.Fatalf("apply = %+v, want APPLIED: a write built from a view read off either key must land on the "+
					"framed key without meeting a conflict it manufactured itself", applied.Items[0])
			}
			written := decodeRuntime(backend.values[framedKey], identity, frozenRef(), next)
			if written.Representation != execution.StateRepresentationFramed || written.BlobRevision != test.wantWrittenRev ||
				written.PersistedApplyVersion != next {
				t.Fatalf("framed key after apply = %s revision %d version %+v, want framed revision %d at the applied version: "+
					"the revision continues the framed key's own count", written.Representation, written.BlobRevision,
					written.PersistedApplyVersion, test.wantWrittenRev)
			}
			if string(backend.values[envelopeKey]) != envelopeBefore {
				t.Fatal("the envelope was written; it is read only and expires on its own")
			}
		})
	}
}

// A frozen series is renewed under the key its record lives in, and an item
// that does not say which is refused by name rather than renewed under a
// guess.
func TestAFrozenSeriesIsRenewedUnderTheKeyItsRecordLivesIn(t *testing.T) {
	backend := &casMemoryBackend{values: make(map[string][]byte)}
	router, err := NewFixedRouter("target", backend)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewExecutionStore(ExecutionStoreOptions{Prefix: "alarmd", Router: router, MaxValueBytes: 4096,
		MaxItemsPerCall: 4, MinTTL: time.Minute, MaxTTL: time.Hour, RestartMargin: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	identity := stateIdentityV2()
	envelopeKey, _ := RuntimeStateKeyV2("alarmd", identity)
	framedKey, _ := RuntimeStateKeyV3("alarmd", identity)
	ttl, err := store.runtimeTTL(testRetention())
	if err != nil {
		t.Fatal(err)
	}
	runningOut := writtenAt().Add(ttl - time.Second)
	for _, test := range []struct {
		name           string
		representation execution.StateRepresentation
		wantKey        string
		wantOutcome    execution.FrozenRenewalOutcome
	}{
		{name: "envelope", representation: execution.StateRepresentationEnvelope, wantKey: envelopeKey, wantOutcome: execution.FrozenRenewalRenewed},
		{name: "framed", representation: execution.StateRepresentationFramed, wantKey: framedKey, wantOutcome: execution.FrozenRenewalRenewed},
		{name: "unsaid", representation: "", wantKey: "", wantOutcome: execution.FrozenRenewalFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend.renewals = nil
			backend.values[envelopeKey], _ = encodeRuntime(seriesMutation(t, identity, applyVersion(), 0, ""), 1)
			backend.values[framedKey], _ = encodeRuntimePacked(seriesMutation(t, identity, applyVersion(), 0, ""), 1)
			backend.writeTTLs = map[string]time.Duration{envelopeKey: ttl, framedKey: ttl}
			result, err := store.RenewFrozenRuntime(context.Background(), execution.FrozenStateRenewalRequest{
				Contract: frozenRef(), Retention: testRetention(), Now: runningOut,
				Items: []execution.FrozenSeriesState{{Identity: identity, LastApplied: applyVersion().EvaluationTime,
					Representation: test.representation}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.Items[0].Outcome != test.wantOutcome {
				t.Fatalf("outcome = %s, want %s", result.Items[0].Outcome, test.wantOutcome)
			}
			if test.wantKey == "" {
				if len(backend.renewals) != 0 {
					t.Fatalf("an item that does not say which key holds its record renewed %+v; a guess that lands on "+
						"the other key reports renewed while the record runs out", backend.renewals)
				}
				return
			}
			if len(backend.renewals) != 1 || backend.renewals[0].Key != test.wantKey {
				t.Fatalf("renewals = %+v, want exactly the %s key %q", backend.renewals, test.name, test.wantKey)
			}
		})
	}
}

// A conflict met on the framed key is named in the framed key's own revision
// space. The write was built from an envelope at revision 9 and expected the
// framed key to be missing; another writer created it at revision 1 with a
// different statement. That is a move on the framed key (0 -> 1), and
// reading it against the envelope's 9 would call it a reset (9 -> 1) - a
// kind that says a key was recreated lower, which nothing did.
func TestAConflictOnTheFramedKeyIsNamedInItsOwnRevisionSpace(t *testing.T) {
	backend := newPipelineMemoryBackend()
	store := newBatchStore(t, backend, fixedFenceKeys{testFenceKeys()})
	identity := seriesIdentity(0)
	envelopeKey, _ := RuntimeStateKeyV2("alarmd", identity)
	framedKey, _ := RuntimeStateKeyV3("alarmd", identity)
	older := execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: 30, SlotDigest: "slot-0"}
	next := execution.ApplyVersion{StateApplyEpoch: 1, EvaluationTime: 120, SlotDigest: "slot-2"}
	backend.values[envelopeKey], _ = encodeRuntime(seriesMutation(t, identity, older, 0, "env"), 9)

	mutation := seriesMutation(t, identity, next, 9, "")
	if _, err := store.LoadRuntime(context.Background(), execution.StatePreflightRequest{Contract: frozenRef(),
		Items: preflightItems([]execution.StateMutation{mutation})}); err != nil {
		t.Fatal(err)
	}
	// Another writer lands the framed key first, at the same version with a
	// different statement.
	backend.values[framedKey], _ = encodeRuntimePacked(seriesMutation(t, identity, next, 0, "theirs"), 1)

	result, err := store.ApplyRuntimeFenced(context.Background(), execution.StateApplyRequest{Contract: frozenRef(),
		Retention: testRetention(), Items: []execution.StateMutation{mutation}}, testApplyFence())
	if err != nil {
		t.Fatal(err)
	}
	item := result.Items[0]
	if item.Status != execution.StateApplyVersionConflict || item.VersionConflict != execution.StateVersionConflictRevisionMoved {
		t.Fatalf("item = %+v, want VERSION_CONFLICT named revision_moved: the framed key went from missing to revision 1, "+
			"and the envelope's revision 9 is not a number that key ever had", item)
	}
}
