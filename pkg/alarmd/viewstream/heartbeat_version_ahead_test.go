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
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream/pb"
)

// droppingDialer is the bufconn dialer with a transport that loses the next
// drop deltas the Leader sends, and nothing else: the stream stays up and
// the heartbeats go on.
type droppingDialer struct {
	*bufconnDialer
	drop    atomic.Int32
	dropped atomic.Int32
}

func (dialer *droppingDialer) dial(ctx context.Context, endpoint string) (pb.ControlServiceClient, func() error, error) {
	client, closer, err := dialer.bufconnDialer.dial(ctx, endpoint)
	if err != nil {
		return nil, nil, err
	}
	return droppingClient{ControlServiceClient: client, dialer: dialer}, closer, nil
}

type droppingClient struct {
	pb.ControlServiceClient
	dialer *droppingDialer
}

func (client droppingClient) Connect(ctx context.Context, opts ...grpc.CallOption) (pb.ControlService_ConnectClient, error) {
	stream, err := client.ControlServiceClient.Connect(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return droppingStream{ControlService_ConnectClient: stream, dialer: client.dialer}, nil
}

type droppingStream struct {
	pb.ControlService_ConnectClient
	dialer *droppingDialer
}

func (stream droppingStream) Recv() (*pb.LeaderMessage, error) {
	for {
		message, err := stream.ControlService_ConnectClient.Recv()
		if err != nil {
			return message, err
		}
		if _, delta := message.Body.(*pb.LeaderMessage_Delta); delta && stream.dialer.drop.Load() > 0 {
			stream.dialer.drop.Add(-1)
			stream.dialer.dropped.Add(1)
			continue
		}
		return message, nil
	}
}

// A delta the Leader sent that never arrived, on a stream that stays up and
// keeps its heartbeats, leaves the Worker behind with nothing to say so: no
// later delta comes until the Leader publishes again, and the version chain
// only catches it then. The Leader's heartbeat names the version it sent;
// ahead of the installed one past a heartbeat interval, the Worker asks for
// the snapshot by the path a delta that does not apply takes, counts it, and
// catches up (015 R1). A delta that did arrive leaves the heartbeat at the
// installed version and asks for nothing.
func TestAWorkerTheLeadersHeartbeatShowsBehindAsksForTheSnapshot(t *testing.T) {
	harness := startServer(t)
	if err := harness.server.Lead(7); err != nil {
		t.Fatal(err)
	}
	publish := func(object string) {
		t.Helper()
		if _, err := harness.server.Publish(context.Background(), desiredAt(publicationA, map[string]string{"qg-1": "w1"},
			map[string]viewstream.Content{"qg-1": content(object, "s1")})); err != nil {
			t.Fatal(err)
		}
	}
	publish("obj-1")
	dialer := &droppingDialer{bufconnDialer: &bufconnDialer{}}
	dialer.serveOn(t, "leader", harness.server)
	discovery := &scriptedDiscovery{}
	discovery.set("leader", true)
	observer := &sessionObserver{}
	const tick = 20 * time.Millisecond
	client, err := viewstream.NewClient(viewstream.ClientIdentity{WorkerID: "w1", Incarnation: "i1", StreamToken: "t1"}, discovery, nil, observer,
		viewstream.ClientOptions{Dial: dialer.dial, Tick: tick})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = client.Run(ctx) }()
	installedAt := func(revision uint64) func() bool {
		return func() bool {
			view, ok := client.Installed()
			return ok && view.Version.Revision == revision
		}
	}
	eventually(t, "the Worker installs the first revision", installedAt(1))

	// A delta that arrives: heartbeats past it ask for nothing.
	publish("obj-2")
	eventually(t, "the Worker installs the delta", installedAt(2))
	requested := client.Stats().SnapshotsRequested
	time.Sleep(10 * tick) // fixed-wait: heartbeats go by on the real clock; the cell is that none of them asks for a snapshot
	if stats := client.Stats(); stats.HeartbeatVersionAhead != 0 || stats.SnapshotsRequested != requested {
		t.Fatalf("after a delta that arrived: version ahead %d, snapshots %d (were %d), want no snapshot asked for",
			stats.HeartbeatVersionAhead, stats.SnapshotsRequested, requested)
	}

	// A delta that does not arrive: the heartbeat shows the Worker behind.
	dialer.drop.Store(1)
	publish("obj-3")
	eventually(t, "the transport loses the delta", func() bool { return dialer.dropped.Load() == 1 })
	eventually(t, "the Worker catches up to the revision it never received", installedAt(3))
	stats := client.Stats()
	if stats.HeartbeatVersionAhead != 1 || stats.SnapshotsRequested != requested+1 {
		t.Fatalf("after a lost delta: version ahead %d, snapshots %d (were %d), want one of each", stats.HeartbeatVersionAhead, stats.SnapshotsRequested, requested)
	}
	if behind := observer.count("install_behind", viewstream.FailureHeartbeatVersionAhead); behind != 1 {
		t.Fatalf("install_behind lines = %d, want one naming %s", behind, viewstream.FailureHeartbeatVersionAhead)
	}
	if stats.Installs["snapshot"] < 2 {
		t.Fatalf("installs = %v, want the revision caught up by a snapshot", stats.Installs)
	}
}
