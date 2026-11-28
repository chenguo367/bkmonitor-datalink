// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package realtimeinput_test

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/xdg-go/scram"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/realtimeinput"
)

const innerTopic = "table_demo"

// collector gathers what a reader hands over.
type collector struct {
	mu       sync.Mutex
	messages []realtimeinput.Message
}

func (c *collector) handle(message realtimeinput.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, message)
}

func (c *collector) waitFor(t *testing.T, n int) []realtimeinput.Message {
	t.Helper()
	var out []realtimeinput.Message
	testwait.Until(t, fmt.Sprintf("%d messages read", n), func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.messages) < n {
			return false
		}
		out = append([]realtimeinput.Message(nil), c.messages...)
		return true
	})
	return out
}

// eventTimes are a recorded message's dtEventTimeStamp values, read off its expectation.
func eventTimes(t *testing.T, name string) []int64 {
	t.Helper()
	var out []int64
	for _, row := range loadExpect(t).Messages[name].Rows {
		out = append(out, int64(row["dtEventTimeStamp"].(float64)))
	}
	return out
}

func minMax(values []int64) (int64, int64) {
	low, high := values[0], values[0]
	for _, v := range values {
		if v < low {
			low = v
		}
		if v > high {
			high = v
		}
	}
	return low, high
}

// groupRequests are the requests of a consumer group and of offset commits: a reader that assigns sends none of them.
func groupRequests(broker *sarama.MockBroker) []string {
	var sent []string
	for _, exchange := range broker.History() {
		switch exchange.Request.(type) {
		case *sarama.JoinGroupRequest, *sarama.SyncGroupRequest, *sarama.HeartbeatRequest, *sarama.LeaveGroupRequest,
			*sarama.OffsetCommitRequest, *sarama.OffsetFetchRequest, *sarama.FindCoordinatorRequest, *sarama.ConsumerMetadataRequest:
			sent = append(sent, fmt.Sprintf("%T", exchange.Request))
		}
	}
	return sent
}

func plaintext(broker *sarama.MockBroker) realtimeinput.Cluster {
	return realtimeinput.Cluster{Name: "inner-demo", Brokers: []string{broker.Addr()}}
}

func TestTheReaderAssignsEveryPartitionByTimeWithoutAGroupOrACommit(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	from := time.UnixMilli(eventTimes(t, "k8s-1.bin")[0]).Add(-30 * time.Second)
	fromMs := from.UnixMilli()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()).SetLeader(innerTopic, 1, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetVersion(1).
			SetOffset(innerTopic, 0, fromMs, 1).SetOffset(innerTopic, 0, sarama.OffsetNewest, 3).SetOffset(innerTopic, 0, sarama.OffsetOldest, 0).
			SetOffset(innerTopic, 1, fromMs, -1).SetOffset(innerTopic, 1, sarama.OffsetNewest, 4).SetOffset(innerTopic, 1, sarama.OffsetOldest, 0),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).SetVersion(3).
			SetMessage(innerTopic, 0, 0, sarama.ByteEncoder("an older message the time placing skips")).
			SetMessage(innerTopic, 0, 1, sarama.ByteEncoder(readMessage(t, "k8s-1.bin"))).
			SetMessage(innerTopic, 0, 2, sarama.ByteEncoder(readMessage(t, "k8s-2.bin"))).
			SetHighWaterMark(innerTopic, 0, 3).SetHighWaterMark(innerTopic, 1, 4),
	})
	counts := realtimeinput.NewCounts()
	got := &collector{}
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, got.handle, counts)
	if err != nil {
		t.Fatal(err)
	}
	messages := got.waitFor(t, 2)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if messages[0].Partition != 0 || messages[0].Offset != 1 || messages[1].Offset != 2 {
		t.Fatalf("read %+v, want offsets 1 and 2 of partition 0", messages)
	}
	for i, name := range []string{"k8s-1.bin", "k8s-2.bin"} {
		if messages[i].Err != nil || len(messages[i].Rows) != len(eventTimes(t, name)) {
			t.Fatalf("message %d: %d rows, %v", i, len(messages[i].Rows), messages[i].Err)
		}
	}
	progress := reader.Progress(time.Now(), time.Minute)
	_, high := minMax(append(eventTimes(t, "k8s-1.bin"), eventTimes(t, "k8s-2.bin")...))
	if !progress[0].CaughtUp || !progress[0].Ready || progress[0].MaxEventTimeMs != high || progress[0].Gap != nil {
		t.Fatalf("partition 0 progress %+v, want caught up at %d", progress[0], high)
	}
	// Placed at its end, partition 1 is caught up before any message: nothing in it is at or after the time.
	if !progress[1].CaughtUp || !progress[1].Ready {
		t.Fatalf("partition 1 progress %+v, want placed at its end and caught up", progress[1])
	}
	snapshot := counts.Snapshot()
	if snapshot.Seeks[realtimeinput.SeekAtTime] != 1 || snapshot.Seeks[realtimeinput.SeekPastEnd] != 1 || snapshot.Decoded != 2 {
		t.Fatalf("counts %+v", snapshot)
	}
	if sent := groupRequests(broker); len(sent) != 0 {
		t.Fatalf("a reader that assigns sent %v", sent)
	}
}

func TestATimeOlderThanTheRetentionReadsFromTheOldestAndNamesTheGap(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	first, _ := minMax(eventTimes(t, "k8s-3.bin"))
	from := time.UnixMilli(first).Add(-time.Hour)
	fromMs := from.UnixMilli()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()),
		// The partition dropped its first 5 messages: the first one at or after the time is its oldest.
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetVersion(1).
			SetOffset(innerTopic, 0, fromMs, 5).SetOffset(innerTopic, 0, sarama.OffsetNewest, 6).SetOffset(innerTopic, 0, sarama.OffsetOldest, 5),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).SetVersion(3).
			SetMessage(innerTopic, 0, 5, sarama.ByteEncoder(readMessage(t, "k8s-3.bin"))).SetHighWaterMark(innerTopic, 0, 6),
	})
	counts := realtimeinput.NewCounts()
	got := &collector{}
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, got.handle, counts)
	if err != nil {
		t.Fatal(err)
	}
	got.waitFor(t, 1)
	progress := reader.Progress(time.Now(), time.Minute)
	_ = reader.Close()
	gap := progress[0].Gap
	if gap == nil || !gap.From.Equal(from) || gap.To.UnixMilli() != first {
		t.Fatalf("gap %+v, want from %v to %d", gap, from, first)
	}
	if counts.Snapshot().Seeks[realtimeinput.SeekPastRetention] != 1 {
		t.Fatalf("counts %+v", counts.Snapshot())
	}
}

func TestAPartitionThatAppearsIsPlacedByTheSameTime(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	from := time.UnixMilli(eventTimes(t, "k8s-1.bin")[0]).Add(-30 * time.Second)
	fromMs := from.UnixMilli()
	offsets := sarama.NewMockOffsetResponse(t).SetVersion(1).
		SetOffset(innerTopic, 0, fromMs, 0).SetOffset(innerTopic, 0, sarama.OffsetNewest, 1).SetOffset(innerTopic, 0, sarama.OffsetOldest, 0).
		SetOffset(innerTopic, 1, fromMs, 2).SetOffset(innerTopic, 1, sarama.OffsetNewest, 3).SetOffset(innerTopic, 1, sarama.OffsetOldest, 0)
	fetch := sarama.NewMockFetchResponse(t, 1).SetVersion(3).
		SetMessage(innerTopic, 0, 0, sarama.ByteEncoder(readMessage(t, "k8s-1.bin"))).
		SetMessage(innerTopic, 1, 0, sarama.ByteEncoder("an older message the time placing skips")).
		SetMessage(innerTopic, 1, 2, sarama.ByteEncoder(readMessage(t, "k8s-2.bin"))).
		SetHighWaterMark(innerTopic, 0, 1).SetHighWaterMark(innerTopic, 1, 3)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()),
		"OffsetRequest": offsets,
		"FetchRequest":  fetch,
	})
	got := &collector{}
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, got.handle, realtimeinput.NewCounts())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	got.waitFor(t, 1)
	// The binding is widened: the metadata now has a second partition.
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()).SetLeader(innerTopic, 1, broker.BrokerID()),
		"OffsetRequest": offsets,
		"FetchRequest":  fetch,
	})
	if err := reader.RefreshPartitions(); err != nil {
		t.Fatal(err)
	}
	messages := got.waitFor(t, 2)
	if messages[1].Partition != 1 || messages[1].Offset != 2 {
		t.Fatalf("the new partition was read from %d:%d, want 1:2", messages[1].Partition, messages[1].Offset)
	}
}

func TestAMessageThatDoesNotDecodeIsHandedOverWithItsReasonAndCounted(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	from := time.UnixMilli(1700000000000)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetVersion(1).
			SetOffset(innerTopic, 0, from.UnixMilli(), 0).SetOffset(innerTopic, 0, sarama.OffsetNewest, 1).SetOffset(innerTopic, 0, sarama.OffsetOldest, 0),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).SetVersion(3).
			SetMessage(innerTopic, 0, 0, sarama.ByteEncoder("hello")).SetHighWaterMark(innerTopic, 0, 1),
	})
	counts := realtimeinput.NewCounts()
	got := &collector{}
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, got.handle, counts)
	if err != nil {
		t.Fatal(err)
	}
	messages := got.waitFor(t, 1)
	_ = reader.Close()
	var failure *realtimeinput.DecodeFailure
	if !errors.As(messages[0].Err, &failure) || failure.Reason != realtimeinput.DecodeNotAvroContainer || len(messages[0].Rows) != 0 {
		t.Fatalf("message %+v", messages[0])
	}
	if counts.Snapshot().Failed[realtimeinput.DecodeNotAvroContainer] != 1 || counts.Snapshot().Decoded != 0 {
		t.Fatalf("counts %+v", counts.Snapshot())
	}
}

func TestSCRAMIsTheClusterSASLAndAuthenticatesAgainstAServer(t *testing.T) {
	for _, tc := range []struct {
		mechanism string
		server    scram.HashGeneratorFcn
	}{{realtimeinput.SASLSCRAMSHA256, scram.SHA256}, {realtimeinput.SASLSCRAMSHA512, scram.SHA512}} {
		t.Run(tc.mechanism, func(t *testing.T) {
			config, err := realtimeinput.ClientConfig(realtimeinput.Cluster{Name: "inner-demo", Brokers: []string{"192.0.2.1:9092"},
				SASL: tc.mechanism, Username: "demo-user", Password: "demo-password"}, "alarmd")
			if err != nil {
				t.Fatal(err)
			}
			if !config.Net.SASL.Enable || string(config.Net.SASL.Mechanism) != tc.mechanism || config.Net.SASL.Version != sarama.SASLHandshakeV1 {
				t.Fatalf("SASL %+v", config.Net.SASL)
			}
			// The client the configuration makes completes an exchange with a SCRAM server that knows the user.
			client, err := tc.server.NewClient("demo-user", "demo-password", "")
			if err != nil {
				t.Fatal(err)
			}
			stored := client.GetStoredCredentials(scram.KeyFactors{Salt: "demo-salt", Iters: 4096})
			server, err := tc.server.NewServer(func(user string) (scram.StoredCredentials, error) {
				if user != "demo-user" {
					return scram.StoredCredentials{}, errors.New("unknown user")
				}
				return stored, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			conversation := server.NewConversation()
			sasl := config.Net.SASL.SCRAMClientGeneratorFunc()
			if err := sasl.Begin(config.Net.SASL.User, config.Net.SASL.Password, config.Net.SASL.SCRAMAuthzID); err != nil {
				t.Fatal(err)
			}
			challenge := ""
			for !sasl.Done() {
				reply, err := sasl.Step(challenge)
				if err != nil {
					t.Fatal(err)
				}
				if sasl.Done() {
					break
				}
				if challenge, err = conversation.Step(reply); err != nil {
					t.Fatal(err)
				}
			}
			if !conversation.Valid() {
				t.Fatal("the server did not accept the client")
			}
		})
	}
}

func TestSCRAMIsWhatTheClientAsksTheBrokerForAndPlaintextAsksNothing(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"SaslHandshakeRequest":    sarama.NewMockSaslHandshakeResponse(t).SetEnabledMechanisms([]string{sarama.SASLTypeSCRAMSHA512}),
		"SaslAuthenticateRequest": sarama.NewMockSaslAuthenticateResponse(t).SetError(sarama.ErrSASLAuthenticationFailed),
		"MetadataRequest":         sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()),
	})
	cluster := realtimeinput.Cluster{Name: "inner-demo", Brokers: []string{broker.Addr()}, SASL: realtimeinput.SASLSCRAMSHA512,
		Username: "demo-user", Password: "demo-password"}
	if reader, err := realtimeinput.NewReader(cluster, "alarmd", innerTopic, time.Now, func(realtimeinput.Message) {}, realtimeinput.NewCounts()); err == nil {
		_ = reader.Close()
		t.Fatal("a refused authentication opened a reader")
	}
	var handshake *sarama.SaslHandshakeRequest
	var first *sarama.SaslAuthenticateRequest
	for _, exchange := range broker.History() {
		switch request := exchange.Request.(type) {
		case *sarama.SaslHandshakeRequest:
			handshake = request
		case *sarama.SaslAuthenticateRequest:
			if first == nil {
				first = request
			}
		}
	}
	if handshake == nil || handshake.Mechanism != sarama.SASLTypeSCRAMSHA512 {
		t.Fatalf("handshake %+v", handshake)
	}
	if first == nil || !strings.HasPrefix(string(first.SaslAuthBytes), "n,,n=demo-user,r=") {
		t.Fatalf("first authentication %+v", first)
	}

	plain := sarama.NewMockBroker(t, 2)
	defer plain.Close()
	plain.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(plain.Addr(), plain.BrokerID()).SetLeader(innerTopic, 0, plain.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetVersion(1).
			SetOffset(innerTopic, 0, 1700000000000, -1).SetOffset(innerTopic, 0, sarama.OffsetNewest, 0).
			SetOffset(innerTopic, 0, sarama.OffsetOldest, 0),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).SetVersion(3).SetHighWaterMark(innerTopic, 0, 0),
	})
	reader, err := realtimeinput.NewReader(plaintext(plain), "alarmd", innerTopic, func() time.Time { return time.UnixMilli(1700000000000) },
		func(realtimeinput.Message) {}, realtimeinput.NewCounts())
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	for _, exchange := range plain.History() {
		switch exchange.Request.(type) {
		case *sarama.SaslHandshakeRequest, *sarama.SaslAuthenticateRequest:
			t.Fatal("a PLAINTEXT cluster was asked for SASL")
		}
	}
}

func TestAClusterNamesASupportedSASLWithItsCredentials(t *testing.T) {
	for _, cluster := range []realtimeinput.Cluster{
		{Name: "plain-mechanism", Brokers: []string{"192.0.2.1:9092"}, SASL: "PLAIN", Username: "u", Password: "p"},
		{Name: "no-password", Brokers: []string{"192.0.2.1:9092"}, SASL: realtimeinput.SASLSCRAMSHA512, Username: "u"},
		{Name: "no-user", Brokers: []string{"192.0.2.1:9092"}, SASL: realtimeinput.SASLSCRAMSHA256, Password: "p"},
	} {
		if _, err := realtimeinput.ClientConfig(cluster, "alarmd"); err == nil {
			t.Fatalf("cluster %s was accepted", cluster.Name)
		}
	}
}

// A partition is caught up only while its end was confirmed lately: a partition that stopped answering is not taken as
// idle, which would let the wall clock carry its watermark on. A refresh confirms each read partition's end again.
func TestACaughtUpPartitionIsOneWhoseEndWasConfirmedLately(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	from := time.UnixMilli(eventTimes(t, "k8s-1.bin")[0]).Add(-30 * time.Second)
	fromMs := from.UnixMilli()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetVersion(1).
			SetOffset(innerTopic, 0, fromMs, -1).SetOffset(innerTopic, 0, sarama.OffsetNewest, 4).SetOffset(innerTopic, 0, sarama.OffsetOldest, 0),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).SetVersion(3).SetHighWaterMark(innerTopic, 0, 4),
	})
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, func(realtimeinput.Message) {},
		realtimeinput.NewCounts())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	freshness := time.Minute
	if progress := reader.Progress(time.Now(), freshness); !progress[0].CaughtUp {
		t.Fatalf("progress %+v, want caught up at its end", progress[0])
	}
	if progress := reader.Progress(time.Now().Add(2*freshness), freshness); progress[0].CaughtUp {
		t.Fatalf("progress %+v two freshness windows on, want not caught up: nothing confirmed its end since", progress[0])
	}
	if err := reader.RefreshPartitions(); err != nil {
		t.Fatal(err)
	}
	if progress := reader.Progress(time.Now(), freshness); !progress[0].CaughtUp {
		t.Fatalf("progress %+v after a refresh, want its end confirmed again", progress[0])
	}
}

// A partition whose offsets could not be listed is listed, not ready, so that a watermark taken over every partition waits
// for it; its placing is counted once.
func TestAPartitionWhosePlacingFailedIsListedNotReady(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	offsets := &sarama.OffsetResponse{Version: 1}
	offsets.AddTopicPartition(innerTopic, 0, 0)
	offsets.AddTopicPartition(innerTopic, 1, 0)
	offsets.Blocks[innerTopic][1].Err = sarama.ErrNotLeaderForPartition
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()).SetLeader(innerTopic, 1, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockWrapper(offsets),
		"FetchRequest":  sarama.NewMockFetchResponse(t, 1).SetVersion(3),
	})
	counts := realtimeinput.NewCounts()
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return time.UnixMilli(1700000000000) },
		func(realtimeinput.Message) {}, counts)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	progress := reader.Progress(time.Now(), time.Minute)
	if state, listed := progress[1]; !listed || state.Ready || state.CaughtUp {
		t.Fatalf("partition 1 progress %+v (listed %v), want listed and not ready", state, listed)
	}
	if failed := counts.Snapshot().Seeks[realtimeinput.SeekFailed]; failed != 1 {
		t.Fatalf("%d failed placings counted, want 1", failed)
	}
}

// A partition the client stops reading because its offset is gone is no longer read, counted by its reason, and placed
// by time again on the next refresh, where it reads on.
func TestAPartitionTheClientClosesIsPlacedAgainByTime(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	from := time.UnixMilli(eventTimes(t, "k8s-1.bin")[0]).Add(-30 * time.Second)
	fromMs := from.UnixMilli()
	gone := &sarama.FetchResponse{Version: 3}
	gone.AddError(innerTopic, 0, sarama.ErrOffsetOutOfRange)
	handlers := map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetVersion(1).
			SetOffset(innerTopic, 0, fromMs, 1).SetOffset(innerTopic, 0, sarama.OffsetNewest, 2).SetOffset(innerTopic, 0, sarama.OffsetOldest, 0),
		"FetchRequest": sarama.NewMockWrapper(gone),
	}
	broker.SetHandlerByMap(handlers)
	counts := realtimeinput.NewCounts()
	got := &collector{}
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, got.handle, counts)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	testwait.Until(t, "the closed partition counted", func() bool {
		return counts.Snapshot().Closed[realtimeinput.PartitionClosedOutOfRange] == 1
	})
	if progress := reader.Progress(time.Now(), time.Minute); progress[0].Ready || progress[0].CaughtUp {
		t.Fatalf("progress %+v, want the closed partition not ready", progress[0])
	}
	broker.SetHandlerByMap(withFetch(handlers, sarama.NewMockFetchResponse(t, 1).SetVersion(3).
		SetMessage(innerTopic, 0, 1, sarama.ByteEncoder(readMessage(t, "k8s-1.bin"))).SetHighWaterMark(innerTopic, 0, 2)))
	if err := reader.RefreshPartitions(); err != nil {
		t.Fatal(err)
	}
	got.waitFor(t, 1)
	if seeks := counts.Snapshot().Seeks[realtimeinput.SeekAtTime]; seeks != 2 {
		t.Fatalf("%d placings at the time, want the first and the one after the close", seeks)
	}
}

func TestSeekResultsAreClosed(t *testing.T) {
	want := "at_time,failed,past_end,past_retention"
	got := append([]string(nil), realtimeinput.SeekResults()...)
	sort.Strings(got)
	if strings.Join(got, ",") != want {
		t.Fatalf("results %v", got)
	}
}

// atItsEnd is a broker whose only partition is placed at its end: the time asked for is past every offset.
func atItsEnd(t *testing.T, fetch sarama.MockResponse) (*sarama.MockBroker, time.Time) {
	t.Helper()
	broker := sarama.NewMockBroker(t, 1)
	from := time.UnixMilli(eventTimes(t, "k8s-1.bin")[0]).Add(-30 * time.Second)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetVersion(1).
			SetOffset(innerTopic, 0, from.UnixMilli(), -1).SetOffset(innerTopic, 0, sarama.OffsetNewest, 4).SetOffset(innerTopic, 0, sarama.OffsetOldest, 0),
		"FetchRequest": fetch,
	})
	return broker, from
}

// An idle partition stays caught up from one refresh to the next: each refresh confirms its end again, so it is not
// taken for behind once the confirmation its placing made has aged past the freshness.
func TestARefreshConfirmsTheEndOfAnIdlePartition(t *testing.T) {
	broker, from := atItsEnd(t, sarama.NewMockFetchResponse(t, 1).SetVersion(3).SetHighWaterMark(innerTopic, 0, 4))
	defer broker.Close()
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, func(realtimeinput.Message) {},
		realtimeinput.NewCounts())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	freshness := 500 * time.Millisecond
	testwait.Until(t, "the placing's confirmation aged past the freshness", func() bool {
		return !reader.Progress(time.Now(), freshness)[0].CaughtUp
	})
	if err := reader.RefreshPartitions(); err != nil {
		t.Fatal(err)
	}
	if progress := reader.Progress(time.Now(), freshness); !progress[0].CaughtUp {
		t.Fatalf("progress %+v after a refresh, want its end confirmed again", progress[0])
	}
}

// A partition placed behind its end is not caught up before it reads there: its end is the newest offset when it was
// placed, not where it was placed.
func TestAPartitionPlacedBehindItsEndIsNotCaughtUp(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	from := time.UnixMilli(eventTimes(t, "k8s-1.bin")[0]).Add(-30 * time.Second)
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetVersion(1).
			SetOffset(innerTopic, 0, from.UnixMilli(), 1).SetOffset(innerTopic, 0, sarama.OffsetNewest, 2).SetOffset(innerTopic, 0, sarama.OffsetOldest, 0),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).SetVersion(3).SetHighWaterMark(innerTopic, 0, 2),
	})
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, func(realtimeinput.Message) {},
		realtimeinput.NewCounts())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if progress := reader.Progress(time.Now(), time.Minute); !progress[0].Ready || progress[0].CaughtUp {
		t.Fatalf("progress %+v, want placed and not caught up: one offset is still to read", progress[0])
	}
}

// A partition the client closed is not caught up, though it stood at its end with its end confirmed when it closed:
// nothing reads it until it is placed again.
func TestAClosedPartitionIsNotCaughtUpWhereverItStood(t *testing.T) {
	gone := &sarama.FetchResponse{Version: 3}
	gone.AddError(innerTopic, 0, sarama.ErrOffsetOutOfRange)
	broker, from := atItsEnd(t, sarama.NewMockWrapper(gone))
	defer broker.Close()
	counts := realtimeinput.NewCounts()
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, func(realtimeinput.Message) {}, counts)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	testwait.Until(t, "the closed partition counted", func() bool {
		return counts.Snapshot().Closed[realtimeinput.PartitionClosedOutOfRange] == 1
	})
	if progress := reader.Progress(time.Now(), time.Minute); progress[0].Ready || progress[0].CaughtUp {
		t.Fatalf("progress %+v, want the closed partition neither ready nor caught up", progress[0])
	}
}

// Closing the reader is not the client closing a partition: none of the reader's own closes is counted.
func TestTheReadersOwnCloseIsNotCounted(t *testing.T) {
	broker, from := atItsEnd(t, sarama.NewMockFetchResponse(t, 1).SetVersion(3).SetHighWaterMark(innerTopic, 0, 4))
	defer broker.Close()
	counts := realtimeinput.NewCounts()
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, func(realtimeinput.Message) {}, counts)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	for reason, closed := range counts.Snapshot().Closed {
		if closed != 0 {
			t.Errorf("%d closes counted as %s, want none: the reader closed its own partitions", closed, reason)
		}
	}
}

// A partition placed again after the client closed it keeps the latest event time it had read: a watermark taken over
// it does not go back before what was already read.
func TestAPartitionPlacedAgainKeepsTheLatestEventTimeItRead(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	from := time.UnixMilli(eventTimes(t, "k8s-1.bin")[0]).Add(-30 * time.Second)
	_, latest := minMax(eventTimes(t, "k8s-1.bin"))
	gone := &sarama.FetchResponse{Version: 3}
	gone.AddError(innerTopic, 0, sarama.ErrOffsetOutOfRange)
	handlers := map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(innerTopic, 0, broker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetVersion(1).
			SetOffset(innerTopic, 0, from.UnixMilli(), 1).SetOffset(innerTopic, 0, sarama.OffsetNewest, 2).SetOffset(innerTopic, 0, sarama.OffsetOldest, 0),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).SetVersion(3).
			SetMessage(innerTopic, 0, 1, sarama.ByteEncoder(readMessage(t, "k8s-1.bin"))).SetHighWaterMark(innerTopic, 0, 2),
	}
	broker.SetHandlerByMap(handlers)
	counts := realtimeinput.NewCounts()
	got := &collector{}
	reader, err := realtimeinput.NewReader(plaintext(broker), "alarmd", innerTopic, func() time.Time { return from }, got.handle, counts)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got.waitFor(t, 1)
	if read := reader.Progress(time.Now(), time.Minute)[0].MaxEventTimeMs; read != latest {
		t.Fatalf("latest event time %d after reading, want %d", read, latest)
	}
	broker.SetHandlerByMap(withFetch(handlers, sarama.NewMockWrapper(gone)))
	testwait.Until(t, "the closed partition counted", func() bool {
		return counts.Snapshot().Closed[realtimeinput.PartitionClosedOutOfRange] == 1
	})
	broker.SetHandlerByMap(withFetch(handlers, sarama.NewMockFetchResponse(t, 1).SetVersion(3).SetHighWaterMark(innerTopic, 0, 2)))
	if err := reader.RefreshPartitions(); err != nil {
		t.Fatal(err)
	}
	progress := reader.Progress(time.Now(), time.Minute)[0]
	if !progress.Ready || progress.MaxEventTimeMs != latest {
		t.Fatalf("progress %+v placed again, want ready with the latest event time %d it had read", progress, latest)
	}
}

// withFetch is handlers with its fetch answer replaced, as a new map: the broker reads the map it was given while it
// serves, so a test that changed it in place would race it.
func withFetch(handlers map[string]sarama.MockResponse, fetch sarama.MockResponse) map[string]sarama.MockResponse {
	out := make(map[string]sarama.MockResponse, len(handlers))
	for name, response := range handlers {
		out[name] = response
	}
	out["FetchRequest"] = fetch
	return out
}
