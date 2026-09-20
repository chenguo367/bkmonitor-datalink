// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package viewstream_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/viewstream/pb"
)

// scriptedDiscovery answers with whatever endpoint the test set last.
type scriptedDiscovery struct {
	mu     sync.Mutex
	leader viewstream.LeaderEndpoint
	found  bool
	err    error
	asked  int
}

func (discovery *scriptedDiscovery) Leader(context.Context) (viewstream.LeaderEndpoint, bool, error) {
	discovery.mu.Lock()
	defer discovery.mu.Unlock()
	discovery.asked++
	return discovery.leader, discovery.found, discovery.err
}

func (discovery *scriptedDiscovery) set(endpoint string, found bool) {
	discovery.mu.Lock()
	defer discovery.mu.Unlock()
	discovery.leader, discovery.found = viewstream.LeaderEndpoint{WorkerID: "leader", ControlEpoch: 7, Endpoint: endpoint}, found
}

// countingProbe reports a fixed number missing and remembers what it was
// asked about.
type countingProbe struct {
	mu      sync.Mutex
	missing int
	asked   [][]execution.ObjectDigest
}

func (probe *countingProbe) MissingObjects(_ context.Context, objects []execution.ObjectDigest, _ []execution.OutputContextDigest) (int, error) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	probe.asked = append(probe.asked, append([]execution.ObjectDigest(nil), objects...))
	return probe.missing, nil
}

// bufconnDialer dials whichever listener the endpoint names, so a test can
// run two Leaders and move the client between them.
type bufconnDialer struct {
	mu        sync.Mutex
	listeners map[string]*bufconn.Listener
	servers   map[string]*grpc.Server
}

func (dialer *bufconnDialer) dial(_ context.Context, endpoint string) (pb.ControlServiceClient, func() error, error) {
	dialer.mu.Lock()
	listener, ok := dialer.listeners[endpoint]
	dialer.mu.Unlock()
	if !ok {
		return nil, nil, errors.New("no listener at " + endpoint)
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewControlServiceClient(conn), conn.Close, nil
}

// serveOn puts a ControlService behind a bufconn listener under a name. A
// name already served is taken over: the previous server is stopped, which
// ends every stream on it, the way a Leader restart does.
func (dialer *bufconnDialer) serveOn(t *testing.T, name string, service pb.ControlServiceServer) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	pb.RegisterControlServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	dialer.mu.Lock()
	if dialer.listeners == nil {
		dialer.listeners = map[string]*bufconn.Listener{}
		dialer.servers = map[string]*grpc.Server{}
	}
	previous := dialer.servers[name]
	dialer.listeners[name], dialer.servers[name] = listener, grpcServer
	dialer.mu.Unlock()
	if previous != nil {
		previous.Stop()
	}
}

func startClient(t *testing.T, discovery *scriptedDiscovery, probe viewstream.ObjectProbe, dialer *bufconnDialer, observer *sessionObserver) (*viewstream.Client, context.CancelFunc) {
	t.Helper()
	client, err := viewstream.NewClient(viewstream.ClientIdentity{WorkerID: "w1", Incarnation: "i1", StreamToken: "t1"}, discovery, probe, observer,
		viewstream.ClientOptions{Dial: dialer.dial, Sleep: func(ctx context.Context, wait time.Duration) error {
			// A test does not wait out the jitter; it waits a tick so the loop
			// cannot spin.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Millisecond):
				return nil
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = client.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return client, cancel
}

// The Worker finds the Leader, installs the snapshot it is sent and
// reports it; a publication that moves its projection arrives as a delta
// and is installed on top; one that does not arrives as an empty delta and
// is installed by receipt with the object count standing; the Leader's
// ledger counts each install; the probe is asked about exactly the objects
// each install brought.
func TestTheWorkerInstallsWhatTheLeaderSendsAndReportsIt(t *testing.T) {
	harness := startServer(t)
	ctx := context.Background()
	if err := harness.server.Lead(7); err != nil {
		t.Fatal(err)
	}
	first := desiredAt(publicationA, map[string]string{"qg-1": "w1", "qg-2": "w1", "qg-3": "w2"},
		map[string]viewstream.Content{"qg-1": content("obj-1", "s1"), "qg-2": content("obj-2", "s2"), "qg-3": content("obj-3", "s3")})
	if _, err := harness.server.Publish(ctx, first); err != nil {
		t.Fatal(err)
	}
	dialer := &bufconnDialer{}
	dialer.serveOn(t, "leader-a", harness.server)
	discovery := &scriptedDiscovery{}
	discovery.set("leader-a", true)
	probe := &countingProbe{missing: 2}
	observer := &sessionObserver{}
	client, _ := startClient(t, discovery, probe, dialer, observer)

	eventually(t, "the snapshot is installed", func() bool {
		view, ok := client.Installed()
		return ok && view.Version.Revision == 1 && len(view.Entries) == 2
	})
	eventually(t, "the Leader counts the install", func() bool {
		return harness.server.Stats().Counts.Installed == 1
	})
	stats := client.Stats()
	if !stats.Connected || stats.Leader.Endpoint != "leader-a" || stats.Installs["snapshot"] != 1 || stats.ObjectsMissing != 2 || stats.Installed.Revision != 1 {
		t.Fatalf("client stats after the snapshot = %+v", stats)
	}
	probe.mu.Lock()
	asked := len(probe.asked)
	firstAsk := probe.asked[0]
	probe.mu.Unlock()
	if asked != 1 || len(firstAsk) != 2 {
		t.Fatalf("probe asked %d times, first about %v; want once about the snapshot's two objects", asked, firstAsk)
	}

	// w1's content moves: a delta with one upsert; the probe is asked about
	// that one object; the Leader counts revision 2 installed.
	second := desiredAt(publicationA, map[string]string{"qg-1": "w1", "qg-2": "w1", "qg-3": "w2"},
		map[string]viewstream.Content{"qg-1": content("obj-1b", "s1"), "qg-2": content("obj-2", "s2"), "qg-3": content("obj-3", "s3")})
	probe.mu.Lock()
	probe.missing = 0
	probe.mu.Unlock()
	if _, err := harness.server.Publish(ctx, second); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the delta is installed", func() bool {
		view, ok := client.Installed()
		return ok && view.Version.Revision == 2
	})
	view, _ := client.Installed()
	if view.Entries[0].Content.ObjectDigest != "obj-1b" || len(view.Entries) != 2 || client.Stats().ObjectsMissing != 0 {
		t.Fatalf("view after the delta = %+v stats=%+v", view, client.Stats())
	}
	probe.mu.Lock()
	lastAsk := probe.asked[len(probe.asked)-1]
	probe.mu.Unlock()
	if len(lastAsk) != 1 || lastAsk[0] != "obj-1b" {
		t.Fatalf("probe asked about %v for the delta, want the one upserted object", lastAsk)
	}
	eventually(t, "revision 2 installed on the Leader", func() bool {
		stats := harness.server.Stats()
		return stats.Revision == 2 && stats.Counts.Installed == 1
	})
	// Only w2 moves: w1 gets an empty delta and installs revision 3 by
	// receipt, its object count standing.
	third := desiredAt(publicationA, map[string]string{"qg-1": "w1", "qg-2": "w1", "qg-3": "w2"},
		map[string]viewstream.Content{"qg-1": content("obj-1b", "s1"), "qg-2": content("obj-2", "s2"), "qg-3": content("obj-3b", "s3")})
	if _, err := harness.server.Publish(ctx, third); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the empty delta is installed", func() bool {
		view, ok := client.Installed()
		return ok && view.Version.Revision == 3
	})
	if stats := client.Stats(); stats.Installs["snapshot"] != 1 || stats.Installs["delta"] != 1 || stats.Installs["empty_delta"] != 1 || stats.Installed.Revision != 3 || len(stats.InstallFailures) != 0 || stats.SnapshotsRequested != 0 {
		t.Fatalf("client stats after three installs = %+v", stats)
	}
	if observer.count("installed", "") != 3 || observer.count("connected", "") != 1 {
		t.Fatalf("events = %+v", observer.events)
	}
}

// scriptedLeader is a ControlService a test writes the lines of: it sends
// what it is told to when a Hello arrives, and records what the Worker
// sends back.
type scriptedLeader struct {
	pb.UnimplementedControlServiceServer
	mu       sync.Mutex
	onHello  []*pb.LeaderMessage
	received []*pb.WorkerMessage
	replies  func(message *pb.WorkerMessage) []*pb.LeaderMessage
}

func (leader *scriptedLeader) Connect(stream pb.ControlService_ConnectServer) error {
	for {
		message, err := stream.Recv()
		if err == io.EOF || err != nil {
			return nil
		}
		leader.mu.Lock()
		leader.received = append(leader.received, message)
		var out []*pb.LeaderMessage
		if message.GetHello() != nil {
			out = append(out, leader.onHello...)
		}
		if leader.replies != nil {
			out = append(out, leader.replies(message)...)
		}
		leader.mu.Unlock()
		for _, reply := range out {
			if err := stream.Send(reply); err != nil {
				return err
			}
		}
	}
}

func (leader *scriptedLeader) got(kind string) int {
	leader.mu.Lock()
	defer leader.mu.Unlock()
	total := 0
	for _, message := range leader.received {
		switch kind {
		case "snapshot_request":
			if message.GetSnapshotRequest() != nil {
				total++
			}
		case "receipt_failed":
			if receipt := message.GetReceipt(); receipt != nil && receipt.Failure != "" {
				total++
			}
		case "receipt_installed":
			if receipt := message.GetReceipt(); receipt != nil && receipt.Installed {
				total++
			}
		}
	}
	return total
}

// A delta whose base is not what the Worker holds is not applied; the
// Worker reports the failure, asks for a snapshot, and installs the
// snapshot it gets. A tampered snapshot is refused whole and asked again.
// A Leader that refuses the stream sends the Worker back to discovery,
// which can name another Leader; the Worker connects there and installs.
func TestTheWorkerRefusesWhatDoesNotVerifyAndFollowsTheLeaderItIsSentTo(t *testing.T) {
	desired := desiredAt(publicationA, map[string]string{"qg-1": "w1"}, map[string]viewstream.Content{"qg-1": content("obj-1", "s1")})
	v2 := viewFrom(desired, "w1", 2)
	v3 := viewFrom(desired, "w1", 3)
	// A delta from revision 1 to 2 for a Worker that holds nothing, then --
	// on request -- the snapshot of revision 2; then a delta 2 -> 3 with a
	// forged target digest, then the good snapshot of 3.
	badBase := viewstream.DeltaToWire(viewstream.Delta{WorkerID: "w1", Base: viewstream.Version{ControlEpoch: 7, Revision: 1, Digest: "x"}, Target: v2.Version, Publication: publicationA})
	forged := viewstream.DeltaToWire(viewstream.Delta{WorkerID: "w1", Base: v2.Version, Target: viewstream.Version{ControlEpoch: 7, Revision: 3, Digest: "forged"}, Publication: publicationA})
	requests := 0
	scripted := &scriptedLeader{onHello: []*pb.LeaderMessage{{Body: &pb.LeaderMessage_Delta{Delta: badBase}}}}
	scripted.replies = func(message *pb.WorkerMessage) []*pb.LeaderMessage {
		if message.GetSnapshotRequest() == nil {
			return nil
		}
		requests++
		switch requests {
		case 1:
			out := []*pb.LeaderMessage{}
			for _, chunk := range viewstream.SnapshotChunks(v2, 0) {
				out = append(out, &pb.LeaderMessage{Body: &pb.LeaderMessage_Snapshot{Snapshot: chunk}})
			}
			return append(out, &pb.LeaderMessage{Body: &pb.LeaderMessage_Delta{Delta: forged}})
		case 2:
			out := []*pb.LeaderMessage{}
			for _, chunk := range viewstream.SnapshotChunks(v3, 0) {
				out = append(out, &pb.LeaderMessage{Body: &pb.LeaderMessage_Snapshot{Snapshot: chunk}})
			}
			return out
		}
		return nil
	}
	dialer := &bufconnDialer{}
	dialer.serveOn(t, "scripted", scripted)
	discovery := &scriptedDiscovery{}
	discovery.set("scripted", true)
	observer := &sessionObserver{}
	client, _ := startClient(t, discovery, nil, dialer, observer)
	eventually(t, "revision 3 installed after two snapshot requests", func() bool {
		view, ok := client.Installed()
		return ok && view.Version.Revision == 3
	})
	stats := client.Stats()
	if stats.SnapshotsRequested != 2 || stats.InstallFailures[viewstream.FailureDeltaBaseMismatch] != 1 || stats.InstallFailures[viewstream.FailureDeltaDigest] != 1 || stats.Installs["snapshot"] != 2 {
		t.Fatalf("client stats = %+v, want two snapshot requests, one base and one digest failure, two installs", stats)
	}
	if scripted.got("snapshot_request") != 2 || scripted.got("receipt_failed") != 2 || scripted.got("receipt_installed") != 2 {
		t.Fatalf("leader received: requests=%d failed receipts=%d installed receipts=%d", scripted.got("snapshot_request"), scripted.got("receipt_failed"), scripted.got("receipt_installed"))
	}

	// The Leader steps down: refused NOT_LEADER, the Worker goes back to
	// discovery, which now names a real Leader; the Worker installs there.
	harness := startServer(t)
	if err := harness.server.Lead(8); err != nil {
		t.Fatal(err)
	}
	newTerm := desired
	newTerm.ControlEpoch = 8
	if _, err := harness.server.Publish(context.Background(), newTerm); err != nil {
		t.Fatal(err)
	}
	dialer.serveOn(t, "leader-b", harness.server)
	refusing := &scriptedLeader{onHello: []*pb.LeaderMessage{{Body: &pb.LeaderMessage_Refusal{Refusal: &pb.Refusal{Reason: viewstream.RefusalNotLeader}}}}}
	// The scripted Leader is replaced by one that refuses: the open stream
	// ends with it, and the next connect meets the refusal.
	dialer.serveOn(t, "scripted", refusing)
	eventually(t, "the refusal is counted", func() bool { return client.Stats().Refusals[viewstream.RefusalNotLeader] >= 1 })
	discovery.set("leader-b", true)
	eventually(t, "the Worker installs from the new Leader", func() bool {
		view, ok := client.Installed()
		return ok && view.Version.ControlEpoch == 8 && view.Version.Revision == 1
	})
	if stats := client.Stats(); stats.Leader.Endpoint != "leader-b" || !stats.Connected {
		t.Fatalf("client stats after the move = %+v", stats)
	}
	if observer.count("disconnected", viewstream.RefusalNotLeader) < 1 {
		t.Fatalf("events = %+v, want a NOT_LEADER disconnect", observer.events)
	}
}

// Discovery that finds no Leader, or fails, costs a miss and a wait and
// nothing else; the Worker keeps trying and connects once a Leader appears.
func TestTheWorkerWaitsOutDiscoveryAndConnectsWhenALeaderAppears(t *testing.T) {
	harness := startServer(t)
	if err := harness.server.Lead(7); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.server.Publish(context.Background(), desiredAt(publicationA, map[string]string{"qg-1": "w1"}, map[string]viewstream.Content{"qg-1": content("obj-1", "s1")})); err != nil {
		t.Fatal(err)
	}
	dialer := &bufconnDialer{}
	dialer.serveOn(t, "leader-a", harness.server)
	discovery := &scriptedDiscovery{}
	observer := &sessionObserver{}
	client, _ := startClient(t, discovery, nil, dialer, observer)
	eventually(t, "discovery is asked more than once", func() bool { return client.Stats().DiscoveryMisses >= 3 })
	if _, ok := client.Installed(); ok {
		t.Fatal("installed a view without a Leader")
	}
	discovery.mu.Lock()
	discovery.err = errors.New("redis down")
	discovery.mu.Unlock()
	eventually(t, "a failing discovery is a miss too", func() bool { return observer.count("discovery_missed", "DISCOVERY_FAILED") >= 1 })
	discovery.mu.Lock()
	discovery.err = nil
	discovery.mu.Unlock()
	discovery.set("leader-a", true)
	eventually(t, "the Worker installs once a Leader appears", func() bool {
		view, ok := client.Installed()
		return ok && view.Version.Revision == 1
	})
}

// The reconnect wait is full jitter under an exponential ceiling: never
// above the ceiling for the attempt, never above the maximum, and not the
// same every time.
func TestTheReconnectWaitIsJitteredUnderAnExponentialCeiling(t *testing.T) {
	client, err := viewstream.NewClient(viewstream.ClientIdentity{WorkerID: "w1", Incarnation: "i1"}, &scriptedDiscovery{}, nil, nil, viewstream.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waits := viewstream.BackoffsForTest(client, 1, 40)
	if len(waits) != 40 {
		t.Fatal("no waits")
	}
	distinct := map[time.Duration]struct{}{}
	for _, wait := range waits {
		if wait < 0 || wait > viewstream.ReconnectMin {
			t.Fatalf("first attempt waited %s, want within [0, %s]", wait, viewstream.ReconnectMin)
		}
		distinct[wait] = struct{}{}
	}
	if len(distinct) < 10 {
		t.Fatalf("forty first-attempt waits took %d distinct values; the jitter is not there", len(distinct))
	}
	for attempt, ceiling := range map[int]time.Duration{2: 2 * time.Second, 5: 16 * time.Second, 6: viewstream.ReconnectMax, 20: viewstream.ReconnectMax} {
		for _, wait := range viewstream.BackoffsForTest(client, attempt, 20) {
			if wait > ceiling {
				t.Fatalf("attempt %d waited %s, over its ceiling %s", attempt, wait, ceiling)
			}
		}
	}
	if wait := viewstream.BackoffsForTest(client, 0, 1)[0]; wait != 0 {
		t.Fatalf("attempt 0 waited %s, want none", wait)
	}
}
