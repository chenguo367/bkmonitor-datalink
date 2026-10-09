// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
)

// viewPublishSource is the catalog as the view publication reads it: the
// published content by Query Group under one activation.
type viewPublishSource struct {
	published map[execution.QueryGroupIdentity]controlplane.ContentEntry
}

func (source viewPublishSource) LoadActivationHead(context.Context) (controlplane.ActivationState, error) {
	return controlplane.ActivationState{RecordRevision: 1, Current: controlplane.SnapshotPublicationRef{SnapshotRevision: "snap", PublicationEpoch: 1}}, nil
}

func (source viewPublishSource) LoadPublishedContent(context.Context, controlplane.SnapshotPublicationRef) (controlplane.PublishedContent, error) {
	return controlplane.PublishedContent{Groups: source.published}, nil
}

func (source viewPublishSource) ActivationBlocked(context.Context) ([]controlplane.BlockedQueryGroup, error) {
	return nil, nil
}

func (source viewPublishSource) DrainingContent(context.Context, execution.QueryGroupIdentity) (execution.ObjectDigest, []execution.OutputContextRef, bool, error) {
	return "", nil, false, nil
}

// viewPublishRuntime is a Leader's ownership runtime leading term 4 with a
// stream and the given catalog, recording what it observes.
func viewPublishRuntime(t *testing.T, source viewSource, observed *[]observability.Observation) (*productionPhaseTwoOwnership, *viewstream.Server, ownership.PublicationAuthority) {
	t.Helper()
	server, err := viewstream.NewServer(viewStreamAdmission{registry: nil, now: time.Now}, nil, viewstream.ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	authority := ownership.PublicationAuthority{Fence: execution.OwnerFence{
		QueryGroup: ownership.ControlLeaderIdentity, OwnerID: "leader", OwnerEpoch: 4, LeaseToken: "token",
	}, Deadline: time.Now().Add(time.Minute)}
	if err := server.Lead(authority.Fence.OwnerEpoch); err != nil {
		t.Fatal(err)
	}
	runtime := &productionPhaseTwoOwnership{authority: authority, dependencies: productionPhaseTwoOwnershipDependencies{
		ViewStream: server, ViewSource: source,
		Observer: observability.ObserverFunc(func(_ context.Context, observation observability.Observation) {
			*observed = append(*observed, observation)
		}),
	}}
	return runtime, server, authority
}

// decision-016 batch 4, section 2 check 3 and section 11 item 1: the entry
// previews the record's timeline revision, and the placement round stamps
// that number onto a record that had not said one without moving its
// record_revision (ownership's publishAssignmentScript; the cutover writes
// it the same way). The view the Worker installs must carry the stamped
// number. A view left at zero answers timeline_unsaid against a lease that
// brings it, and the Query Group is refused until something else in the
// fleet's desired set moves.
func TestATimelineRevisionStampedOntoARecordReachesTheWorkersView(t *testing.T) {
	var observed []observability.Observation
	source := viewPublishSource{published: map[execution.QueryGroupIdentity]controlplane.ContentEntry{"qg-a": {Digest: "obj-a"}}}
	runtime, server, authority := viewPublishRuntime(t, source, &observed)
	ctx := context.Background()
	unsaid := ownership.AssignmentRecord{QueryGroup: "qg-a", DesiredWorkerID: "w1", RecordRevision: 5, ContentScope: "obj-a"}
	runtime.publishView(ctx, authority, map[execution.QueryGroupIdentity]ownership.AssignmentRecord{"qg-a": unsaid}, nil)
	stamped := unsaid
	stamped.TimelineRecordRevision = 9
	runtime.publishView(ctx, authority, map[execution.QueryGroupIdentity]ownership.AssignmentRecord{"qg-a": stamped}, nil)
	if len(observed) != 0 {
		t.Fatalf("observed %+v, want two clean publications", observed)
	}

	view, ok := server.Snapshot("w1")
	if !ok || len(view.Entries) != 1 {
		t.Fatalf("w1 view = %+v (ok=%t), want qg-a", view, ok)
	}
	if got := view.Entries[0].Assignment.TimelineRecordRevision; got != 9 {
		t.Errorf("w1 view at revision %d previews timeline revision %d, the record says 9", view.Version.Revision, got)
	}
	gate := newViewExecutionGate()
	gate.attach(mapView{"qg-a": view.Entries[0]})
	lease := ownership.Lease{ContentScope: "obj-a", TimelineRecordRevision: 9}
	if hint, outcome, _ := gate.judgeAgainstView("qg-a", lease, true); outcome != viewGateExecutable || hint != 9 {
		t.Errorf("gate on the installed view = %s (hint %d) with the lease and the record at 9, want executable", outcome, hint)
	}
}
