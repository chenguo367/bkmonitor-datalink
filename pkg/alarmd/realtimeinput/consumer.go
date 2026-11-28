// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package realtimeinput

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/xdg-go/scram"

	enginekafka "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/kafka"
)

// The SASL mechanisms a channel's Kafka cluster may name. None is PLAINTEXT.
const (
	SASLNone        = ""
	SASLSCRAMSHA256 = sarama.SASLTypeSCRAMSHA256
	SASLSCRAMSHA512 = sarama.SASLTypeSCRAMSHA512
)

// scramBrokerVersion is the protocol SCRAM needs: the client authenticates with SaslHandshake v1 and SaslAuthenticate,
// which brokers from 1.0.0 accept. A PLAINTEXT cluster is spoken to at the program's floor, which already has listing
// offsets by time (0.10.1.0).
const scramBrokerVersion = "1.0.0"

// Cluster is a channel's Kafka cluster as the values and the Secret give it, by the cluster name the channel record names;
// its credentials never come from Redis.
type Cluster struct {
	Name     string
	Brokers  []string
	SASL     string
	Username string
	Password string
}

// The outcomes of placing a partition by time. The set is closed.
const (
	// SeekAtTime: the partition holds data from the requested time; reading starts at its first message at or after it.
	SeekAtTime = "at_time"
	// SeekPastRetention: the requested time is older than what the partition keeps and data was dropped; reading starts at
	// the oldest message, and the stretch from the requested time to that message's data is a coverage gap.
	SeekPastRetention = "past_retention"
	// SeekPastEnd: nothing in the partition is at or after the requested time; reading starts at its end.
	SeekPastEnd = "past_end"
	// SeekFailed: the offsets could not be listed; the partition is not read until a later placing succeeds.
	SeekFailed = "failed"
)

// SeekResults is the closed set of seek outcomes.
func SeekResults() []string {
	return []string{SeekAtTime, SeekPastRetention, SeekPastEnd, SeekFailed}
}

// Message is one inner message read from a partition, decoded. Err is a named decode failure, with no rows.
type Message struct {
	Partition int32
	Offset    int64
	// CreateTime is the message's Kafka timestamp, for the input-to-event latency.
	CreateTime time.Time
	Rows       []Row
	Err        error
}

// CoverageGap is data a partition no longer holds: from the requested time to the event time of the first row read after
// it.
type CoverageGap struct {
	Partition int32
	From      time.Time
	To        time.Time
}

// PartitionProgress is how far a partition has been read: the latest event time seen, and whether the reader has reached
// the partition's high watermark, which is what lets an idle partition's watermark follow the wall clock (design 016
// section 3.3).
type PartitionProgress struct {
	MaxEventTimeMs int64
	CaughtUp       bool
	Gap            *CoverageGap
	// Ready says the partition is placed and being read; a partition whose placing failed, or that the client closed, is
	// listed and not ready until a later refresh places it.
	Ready bool
}

// The reasons the client stops reading a partition by itself. The set is closed.
const (
	// PartitionClosedOutOfRange: the partition no longer holds the offset being read (retention passed it, or the log
	// was truncated); the next refresh places it by time again.
	PartitionClosedOutOfRange = "offset_out_of_range"
	// PartitionClosedOther: the client closed the partition for another reason.
	PartitionClosedOther = "closed"
)

// PartitionClosedReasons is the closed set of the reasons a partition stops being read by itself.
func PartitionClosedReasons() []string {
	return []string{PartitionClosedOutOfRange, PartitionClosedOther}
}

// Counts are the reader's named counts.
type Counts struct {
	mu        sync.Mutex
	decoded   uint64
	failed    map[string]uint64
	seeks     map[string]uint64
	badPoints map[string]uint64
	closed    map[string]uint64
}

// CountsSnapshot is a copy of the counts.
type CountsSnapshot struct {
	Decoded   uint64
	Failed    map[string]uint64
	Seeks     map[string]uint64
	BadPoints map[string]uint64
	// Closed counts the partitions the client stopped reading by itself, by reason (PartitionClosedReasons).
	Closed map[string]uint64
}

// NewCounts makes counts with every name of every closed set at zero.
func NewCounts() *Counts {
	counts := &Counts{failed: map[string]uint64{}, seeks: map[string]uint64{}, badPoints: map[string]uint64{}, closed: map[string]uint64{}}
	for _, reason := range DecodeFailureReasons() {
		counts.failed[reason] = 0
	}
	for _, result := range SeekResults() {
		counts.seeks[result] = 0
	}
	for _, reason := range BadPointReasons() {
		counts.badPoints[reason] = 0
	}
	for _, reason := range PartitionClosedReasons() {
		counts.closed[reason] = 0
	}
	return counts
}

// ObservePartitionClosed counts a partition the client stopped reading by itself.
func (c *Counts) ObservePartitionClosed(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed[reason]++
}

// ObserveDecode counts a decoded message or its named failure.
func (c *Counts) ObserveDecode(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var failure *DecodeFailure
	switch {
	case err == nil:
		c.decoded++
	case errors.As(err, &failure):
		c.failed[failure.Reason]++
	}
}

// ObserveSeek counts a placing's outcome.
func (c *Counts) ObserveSeek(result string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seeks[result]++
}

// ObserveProjection counts a row that yields no point, by its named reason.
func (c *Counts) ObserveProjection(err error) {
	var bad *BadPoint
	if !errors.As(err, &bad) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.badPoints[bad.Reason]++
}

// Snapshot copies the counts.
func (c *Counts) Snapshot() CountsSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := CountsSnapshot{Decoded: c.decoded, Failed: map[string]uint64{}, Seeks: map[string]uint64{}, BadPoints: map[string]uint64{},
		Closed: map[string]uint64{}}
	for k, v := range c.failed {
		out.Failed[k] = v
	}
	for k, v := range c.seeks {
		out.Seeks[k] = v
	}
	for k, v := range c.badPoints {
		out.BadPoints[k] = v
	}
	for k, v := range c.closed {
		out.Closed[k] = v
	}
	return out
}

// ClientConfig is the sarama configuration for reading a cluster: no consumer group and no offset commits; SCRAM when the
// cluster names it.
func ClientConfig(cluster Cluster, clientID string) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.ClientID = clientID
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = false
	version := enginekafka.MinimumBrokerVersion
	switch cluster.SASL {
	case SASLNone:
	case SASLSCRAMSHA256, SASLSCRAMSHA512:
		if cluster.Username == "" || cluster.Password == "" {
			return nil, fmt.Errorf("realtime input cluster %q: %s needs a user and a password", cluster.Name, cluster.SASL)
		}
		version = scramBrokerVersion
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = true
		config.Net.SASL.Version = sarama.SASLHandshakeV1
		config.Net.SASL.Mechanism = sarama.SASLMechanism(cluster.SASL)
		config.Net.SASL.User = cluster.Username
		config.Net.SASL.Password = cluster.Password
		hash := scram.SHA512
		if cluster.SASL == SASLSCRAMSHA256 {
			hash = scram.SHA256
		}
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &scramClient{hash: hash} }
	default:
		return nil, fmt.Errorf("realtime input cluster %q: SASL mechanism %q is not supported", cluster.Name, cluster.SASL)
	}
	parsed, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, err
	}
	config.Version = parsed
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("realtime input cluster %q: %w", cluster.Name, err)
	}
	return config, nil
}

// scramClient is sarama's SCRAM client over xdg-go/scram.
type scramClient struct {
	hash         scram.HashGeneratorFcn
	conversation *scram.ClientConversation
}

func (c *scramClient) Begin(user, password, authzID string) error {
	client, err := c.hash.NewClient(user, password, authzID)
	if err != nil {
		return err
	}
	c.conversation = client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) { return c.conversation.Step(challenge) }

func (c *scramClient) Done() bool { return c.conversation.Done() }

// Reader reads every partition of one inner topic, assigned rather than joined: no consumer group, no committed offsets.
// Each partition is placed by time when it is first read, so a reader that starts or takes over reads from the earliest
// open period, less the margin, which the caller's From gives.
type Reader struct {
	client   sarama.Client
	consumer sarama.Consumer
	topic    string
	from     func() time.Time
	handle   func(Message)
	counts   *Counts

	mu       sync.Mutex
	progress map[int32]*partitionState
	open     map[int32]sarama.PartitionConsumer
	wg       sync.WaitGroup
	closed   bool
}

// partitionState is how far a partition has been read and what was last confirmed of its end.
type partitionState struct {
	maxEventTimeMs int64
	// next is the offset the reader reads next, high the partition's high watermark as last confirmed, at confirmedAt: by a
	// fetch that returned messages, by the placing, or by a refresh listing the partition's newest offset.
	next, high  int64
	confirmedAt time.Time
	ready       bool
	gap         *CoverageGap
}

// NewReader opens a client on the cluster and reads every partition the topic has now. handle is called from one
// goroutine per partition.
func NewReader(cluster Cluster, clientID, topic string, from func() time.Time, handle func(Message), counts *Counts) (*Reader, error) {
	config, err := ClientConfig(cluster, clientID)
	if err != nil {
		return nil, err
	}
	client, err := sarama.NewClient(cluster.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("realtime input cluster %q: %w", cluster.Name, err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	reader := &Reader{client: client, consumer: consumer, topic: topic, from: from, handle: handle, counts: counts,
		progress: map[int32]*partitionState{}, open: map[int32]sarama.PartitionConsumer{}}
	if err := reader.RefreshPartitions(); err != nil {
		_ = reader.Close()
		return nil, err
	}
	return reader, nil
}

// RefreshPartitions reads the topic's partitions from the cluster's metadata, which the channel record does not carry
// (bindings are widened in place), and starts reading any it is not reading yet, each placed by time. A partition whose
// placing failed, or that the client closed, is placed again. A partition being read has its end confirmed: its newest
// offset is listed, which is what keeps an idle partition caught up.
func (r *Reader) RefreshPartitions() error {
	if err := r.client.RefreshMetadata(r.topic); err != nil {
		return fmt.Errorf("realtime input topic %q metadata: %w", r.topic, err)
	}
	partitions, err := r.client.Partitions(r.topic)
	if err != nil {
		return fmt.Errorf("realtime input topic %q partitions: %w", r.topic, err)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	for _, partition := range partitions {
		r.mu.Lock()
		_, reading := r.open[partition]
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return nil
		}
		if reading {
			r.confirmEnd(partition)
			continue
		}
		r.startPartition(partition)
	}
	return nil
}

// confirmEnd lists a read partition's newest offset and keeps it, with the time, as its confirmed end.
func (r *Reader) confirmEnd(partition int32) {
	newest, err := r.client.GetOffset(r.topic, partition, sarama.OffsetNewest)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if state := r.progress[partition]; state != nil && state.ready {
		state.high, state.confirmedAt = newest, time.Now()
	}
}

func (r *Reader) startPartition(partition int32) {
	from := r.from()
	offset, gapFrom, newest, result := r.place(partition, from)
	var consumer sarama.PartitionConsumer
	if result != SeekFailed {
		var err error
		if consumer, err = r.consumer.ConsumePartition(r.topic, partition, offset); err != nil {
			consumer, result = nil, SeekFailed
		}
	}
	// One count per placing, whichever step failed.
	r.counts.ObserveSeek(result)
	r.mu.Lock()
	if r.closed {
		// Close took the reader's partitions already; this one was opened after it and is closed here.
		r.mu.Unlock()
		if consumer != nil {
			go func() {
				for range consumer.Messages() {
				}
			}()
			_ = consumer.Close()
		}
		return
	}
	state := &partitionState{}
	if previous := r.progress[partition]; previous != nil {
		state.maxEventTimeMs = previous.maxEventTimeMs
	}
	if consumer != nil {
		state.ready, state.next, state.high, state.confirmedAt = true, offset, newest, time.Now()
		if result == SeekPastRetention {
			state.gap = &CoverageGap{Partition: partition, From: gapFrom}
		}
		r.open[partition] = consumer
		r.wg.Add(1)
	}
	// A partition whose placing failed is listed, not ready: a watermark over every partition waits for it.
	r.progress[partition] = state
	r.mu.Unlock()
	if consumer != nil {
		go r.supervise(partition, consumer)
	}
}

// supervise reads a partition until its consumer closes, then, unless the reader closed it, takes it out of the reading
// so the next refresh places it by time again, and counts why the client closed it.
func (r *Reader) supervise(partition int32, consumer sarama.PartitionConsumer) {
	defer r.wg.Done()
	reasons := make(chan string, 1)
	go func() {
		reason := ""
		for err := range consumer.Errors() {
			// sarama retries a partition's fetches itself; an offset out of range it does not retry, and closes the partition.
			if errors.Is(err.Err, sarama.ErrOffsetOutOfRange) {
				reason = PartitionClosedOutOfRange
			}
		}
		reasons <- reason
	}()
	r.read(partition, consumer)
	reason := <-reasons
	r.mu.Lock()
	ours := r.closed
	if !ours && r.open[partition] == consumer {
		delete(r.open, partition)
		if state := r.progress[partition]; state != nil {
			state.ready = false
		}
	}
	r.mu.Unlock()
	if !ours {
		if reason == "" {
			reason = PartitionClosedOther
		}
		r.counts.ObservePartitionClosed(reason)
	}
}

// place finds where to start reading a partition for a time: the first message at or after it; the oldest one when the
// time is older than what the partition keeps and data was dropped (a coverage gap from the time on); the end when nothing
// is at or after it.
func (r *Reader) place(partition int32, from time.Time) (int64, time.Time, int64, string) {
	atTime, err := r.client.GetOffset(r.topic, partition, from.UnixMilli())
	if err != nil {
		return 0, time.Time{}, 0, SeekFailed
	}
	newest, err := r.client.GetOffset(r.topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, time.Time{}, 0, SeekFailed
	}
	if atTime < 0 || atTime >= newest {
		return newest, time.Time{}, newest, SeekPastEnd
	}
	oldest, err := r.client.GetOffset(r.topic, partition, sarama.OffsetOldest)
	if err != nil {
		return 0, time.Time{}, 0, SeekFailed
	}
	if atTime == oldest && oldest > 0 {
		return oldest, from, newest, SeekPastRetention
	}
	return atTime, time.Time{}, newest, SeekAtTime
}

func (r *Reader) read(partition int32, consumer sarama.PartitionConsumer) {
	for message := range consumer.Messages() {
		rows, err := DecodeInner(message.Value)
		r.counts.ObserveDecode(err)
		r.mu.Lock()
		state := r.progress[partition]
		for _, row := range rows {
			if row.EventTimeMs != nil && *row.EventTimeMs > state.maxEventTimeMs {
				state.maxEventTimeMs = *row.EventTimeMs
			}
		}
		if state.gap != nil && state.gap.To.IsZero() {
			if first := minEventTime(rows); first > 0 {
				state.gap.To = time.UnixMilli(first).UTC()
			}
		}
		// The fetch that returned the message also says the partition's high watermark.
		state.next, state.high, state.confirmedAt = message.Offset+1, consumer.HighWaterMarkOffset(), time.Now()
		r.mu.Unlock()
		r.handle(Message{Partition: partition, Offset: message.Offset, CreateTime: message.Timestamp, Rows: rows, Err: err})
	}
}

func minEventTime(rows []Row) int64 {
	var first int64
	for _, row := range rows {
		if row.EventTimeMs != nil && (first == 0 || *row.EventTimeMs < first) {
			first = *row.EventTimeMs
		}
	}
	return first
}

// Progress copies each partition's progress at now: a partition is caught up when the reader has reached a high
// watermark confirmed within freshness.
func (r *Reader) Progress(now time.Time, freshness time.Duration) map[int32]PartitionProgress {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[int32]PartitionProgress, len(r.progress))
	for partition, state := range r.progress {
		copied := PartitionProgress{MaxEventTimeMs: state.maxEventTimeMs, Ready: state.ready,
			CaughtUp: state.ready && state.next >= state.high && !state.confirmedAt.IsZero() && now.Sub(state.confirmedAt) <= freshness}
		if state.gap != nil {
			gap := *state.gap
			copied.Gap = &gap
		}
		out[partition] = copied
	}
	return out
}

// Close stops reading and closes the client. Nothing is committed: there is nothing to commit.
func (r *Reader) Close() error {
	r.mu.Lock()
	r.closed = true
	open := r.open
	r.open = map[int32]sarama.PartitionConsumer{}
	r.mu.Unlock()
	for _, consumer := range open {
		consumer.AsyncClose()
	}
	r.wg.Wait()
	var errs []error
	if r.consumer != nil {
		errs = append(errs, r.consumer.Close())
	}
	errs = append(errs, r.client.Close())
	return errors.Join(errs...)
}
