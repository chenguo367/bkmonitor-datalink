// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package viewstream_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream/pb"
)

// recordFieldsDesired assigns qg-1 to w1 and qg-2 to w2, each Assignment
// with the record's own fields, and names w1 alone as declaring the
// view-assignment capability.
func recordFieldsDesired() viewstream.Desired {
	desired := desiredAt(publicationA, map[string]string{"qg-1": "w1", "qg-2": "w2"},
		map[string]viewstream.Content{"qg-1": content("obj-1", "s1"), "qg-2": content("obj-2", "s2")})
	for identity, assignment := range desired.Assignments {
		assignment.AssignmentGeneration, assignment.ControlEpoch, assignment.PlacementReason = 5, 7, "RENDEZVOUS"
		assignment.AssignedAtMs, assignment.SegmentSkip, assignment.SegmentSkipNoData = 1_700_000_000_000, 7, 7
		desired.Assignments[identity] = assignment
	}
	desired.RecordFieldsTo, desired.ReadyKnown = map[string]bool{"w1": true}, true
	return desired
}

// The record's own fields reach the Worker that declares the capability and
// no other. A Worker that does not declare it decodes none of them and
// computes its view's digest over what it decodes: its view carries none of
// them, on the wire too, and its digest is the one the same view without
// them always had - so a binary that predates the fields installs it, and
// does not ask for a snapshot on every publication (015 R1, section 3).
func TestTheRecordsFieldsGoOnlyToAWorkerThatDeclaresThem(t *testing.T) {
	desired := recordFieldsDesired()
	capable, err := desired.Project("w1")
	if err != nil {
		t.Fatal(err)
	}
	if got := capable.Entries[0].Assignment; !got.Complete() || got.AssignmentGeneration != 5 || got.SegmentSkip != 7 {
		t.Fatalf("w1 declares the capability and was sent %+v, want the record's fields", got)
	}
	unaware, err := desired.Project("w2")
	if err != nil {
		t.Fatal(err)
	}
	sent := unaware.Entries[0].Assignment
	if sent.Complete() || sent.AssignmentGeneration != 0 || sent.ControlEpoch != 0 || sent.PlacementReason != "" ||
		sent.AssignedAtMs != 0 || sent.SegmentSkip != 0 || sent.SegmentSkipNoData != 0 {
		t.Fatalf("w2 does not declare the capability and was sent %+v, want none of the record's fields", sent)
	}
	// The digest a binary without the fields computes: the same entries
	// with the fields it cannot decode left out.
	plain := desiredAt(publicationA, map[string]string{"qg-1": "w1", "qg-2": "w2"},
		map[string]viewstream.Content{"qg-1": content("obj-1", "s1"), "qg-2": content("obj-2", "s2")})
	before, err := plain.Project("w2")
	if err != nil {
		t.Fatal(err)
	}
	if unaware.Version.Digest != before.Version.Digest {
		t.Fatalf("w2's digest = %s, the view without the fields digests to %s", unaware.Version.Digest, before.Version.Digest)
	}
	for _, chunk := range viewstream.SnapshotChunks(unaware, 0) {
		for _, entry := range chunk.Entries {
			wire := entry.Assignment
			bare := &pb.Assignment{DesiredWorkerId: wire.DesiredWorkerId, Revision: wire.Revision, ContentScope: wire.ContentScope,
				PendingContentScope: wire.PendingContentScope, EffectiveAtMs: wire.EffectiveAtMs, TimelineRecordRevision: wire.TimelineRecordRevision}
			if proto.Size(wire) != proto.Size(bare) {
				t.Fatalf("w2's wire Assignment is %d bytes, %d without the record's fields: it carries some", proto.Size(wire), proto.Size(bare))
			}
		}
	}
	// What w1 is sent decodes to what was projected, and validates against
	// its own digest.
	capable.Version.Revision = 1
	decoded, err := viewstream.AssembleSnapshot("w1", viewstream.SnapshotChunks(capable, 0))
	if err != nil {
		t.Fatalf("w1's snapshot does not decode: %v", err)
	}
	if got := decoded.Entries[0].Assignment; got != capable.Entries[0].Assignment {
		t.Fatalf("w1 decoded %+v, was sent %+v", got, capable.Entries[0].Assignment)
	}
}

// Who is sent the record's fields is part of what a desired set projects
// to: a Worker that starts declaring the capability changes its view, and a
// publication from outside the round, which knows no registrations, keeps
// the last round's set instead of taking the fields away from every Worker.
func TestWhoIsSentTheRecordsFieldsMovesTheFingerprint(t *testing.T) {
	desired := recordFieldsDesired()
	before := desired.Fingerprint()
	desired.RecordFieldsTo = map[string]bool{"w1": true, "w2": true}
	if desired.Fingerprint() == before {
		t.Fatal("a second Worker sent the record's fields left the fingerprint unchanged: the publisher would skip the round")
	}
	publisher, err := viewstream.NewPublisher(7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(recordFieldsDesired()); err != nil {
		t.Fatal(err)
	}
	outside := recordFieldsDesired()
	outside.RecordFieldsTo, outside.ReadyKnown = nil, false
	published, err := publisher.Publish(outside)
	if err != nil {
		t.Fatal(err)
	}
	if published.Changed {
		t.Fatalf("a publication from outside the round moved the views: %+v, want the last round's set to stand", published)
	}
	view, ok := publisher.Snapshot("w1")
	if !ok || !view.Entries[0].Assignment.Complete() {
		t.Fatalf("w1's view after it = %+v, want the record's fields still sent", view)
	}
}
