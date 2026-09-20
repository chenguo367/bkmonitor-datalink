// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package kafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

// produceAPIKey is the Kafka API key of the Produce request;
// produceVersionWithRecordBatches is the first Produce version that carries
// the record batch format and with it record headers (Kafka 0.11), and
// produceVersionBeforeRecordBatches the one the floor speaks.
const (
	produceAPIKey                     int16 = 0
	produceVersionWithRecordBatches   int16 = 3
	produceVersionBeforeRecordBatches int16 = 2
)

// ProtocolReasonUnsupportedByBroker is the one reason a negotiation names:
// a broker accepts no Produce version that carries record headers, so the
// producer speaks the floor and the standard RawEvent cannot be sent.
const ProtocolReasonUnsupportedByBroker = "PROTOCOL_UNSUPPORTED_BY_BROKER"

// BrokerProtocol is what one broker answered to ApiVersions about Produce.
// ID is the broker's node id when it said one, -1 when it was asked by
// address alone. A broker that did not answer has Answered false and Error
// set, and its versions are meaningless.
type BrokerProtocol struct {
	Address           string `json:"address"`
	ID                int32  `json:"id"`
	Answered          bool   `json:"answered"`
	ProduceMinVersion int16  `json:"produce_min_version"`
	ProduceMaxVersion int16  `json:"produce_max_version"`
	Error             string `json:"error,omitempty"`
}

// ProtocolNegotiation is the protocol the producer speaks and the answers
// it was decided from. It is a fact about the cluster at open time, kept
// for readers: the fleet page shows it, and a standard RawEvent refused for
// want of headers names it.
type ProtocolNegotiation struct {
	// Configured is the floor the producer is built with and falls back to
	// (MinimumBrokerVersion); Wanted the version the program has a use for
	// (RecordHeaderBrokerVersion); Negotiated what the producer speaks.
	Configured string `json:"configured"`
	Wanted     string `json:"wanted"`
	Negotiated string `json:"negotiated"`
	// ProduceVersion is the Produce request version the producer will send
	// under Negotiated; WantedProduceVersion the one record headers need.
	ProduceVersion       int16 `json:"produce_version"`
	WantedProduceVersion int16 `json:"wanted_produce_version"`
	// HeadersSupported is whether Negotiated carries record headers, that
	// is whether the standard RawEvent can be sent at all.
	HeadersSupported bool             `json:"headers_supported"`
	Brokers          []BrokerProtocol `json:"brokers"`
	// Reason is empty when every broker accepts the wanted version, else
	// ProtocolReasonUnsupportedByBroker.
	Reason string `json:"reason,omitempty"`

	version sarama.KafkaVersion
}

// Version is the protocol the producer is opened with.
func (negotiation ProtocolNegotiation) Version() sarama.KafkaVersion { return negotiation.version }

// String is the one line a log or a refusal carries.
func (negotiation ProtocolNegotiation) String() string {
	answers := make([]string, 0, len(negotiation.Brokers))
	for _, broker := range negotiation.Brokers {
		if !broker.Answered {
			answers = append(answers, fmt.Sprintf("%s did not answer (%s)", broker.Address, broker.Error))
			continue
		}
		answers = append(answers, fmt.Sprintf("%s produce v%d..v%d", broker.Address, broker.ProduceMinVersion, broker.ProduceMaxVersion))
	}
	return fmt.Sprintf("negotiated %s (configured %s, wanted %s, record headers %t; %s)",
		negotiation.Negotiated, negotiation.Configured, negotiation.Wanted, negotiation.HeadersSupported, strings.Join(answers, ", "))
}

// apiVersionsClient is the one call the negotiation makes of a broker.
type apiVersionsClient interface {
	ApiVersions(addr string, config *sarama.Config) (*sarama.ApiVersionsResponse, error)
}

type saramaAPIVersions struct{}

func (saramaAPIVersions) ApiVersions(addr string, config *sarama.Config) (*sarama.ApiVersionsResponse, error) {
	broker := sarama.NewBroker(addr)
	if err := broker.Open(config); err != nil {
		return nil, err
	}
	defer func() { _ = broker.Close() }()
	if connected, err := broker.Connected(); err != nil || !connected {
		if err == nil {
			err = errors.New("not connected")
		}
		return nil, err
	}
	return broker.ApiVersions(&sarama.ApiVersionsRequest{})
}

// NegotiateProtocol asks every broker which Produce versions it accepts and
// decides the protocol the producer will speak: RecordHeaderBrokerVersion
// when all of them accept the record batch format (Produce v3 or later),
// otherwise the configured floor. It asks every broker, not one, because a
// cluster is upgraded one broker at a time and the producer's partitions
// may lead on any of them; the newest version they all accept is the only
// one a whole batch can rely on.
//
// A broker that cannot be asked fails the negotiation: the answer is not
// known, and the producer is not opened on a guess. The partial answers
// are returned beside the error so a reader can say which broker did not
// answer. The lazy sink retries the open, so an unreachable broker at start
// is the same wait it always was, and the protocol is decided when the
// brokers answer.
func NegotiateProtocol(brokers []string, config *sarama.Config) (ProtocolNegotiation, error) {
	return negotiateProtocol(brokers, config, saramaAPIVersions{})
}

func negotiateProtocol(brokers []string, config *sarama.Config, client apiVersionsClient) (ProtocolNegotiation, error) {
	if len(brokers) == 0 || config == nil {
		return ProtocolNegotiation{}, errors.New("kafka: protocol negotiation needs brokers and a client configuration")
	}
	negotiation := ProtocolNegotiation{
		Configured: config.Version.String(), Wanted: RecordHeaderBrokerVersion, Negotiated: config.Version.String(),
		ProduceVersion: produceVersionBeforeRecordBatches, WantedProduceVersion: produceVersionWithRecordBatches,
		Brokers: make([]BrokerProtocol, 0, len(brokers)), version: config.Version,
	}
	var failed error
	headers := true
	for _, addr := range brokers {
		answer := BrokerProtocol{Address: addr, ID: -1, ProduceMinVersion: -1, ProduceMaxVersion: -1}
		response, err := client.ApiVersions(addr, config)
		if err == nil && (response == nil || response.Err != sarama.ErrNoError) {
			err = sarama.ErrUnknown
			if response != nil {
				err = response.Err
			}
		}
		if err != nil {
			answer.Error = err.Error()
			negotiation.Brokers = append(negotiation.Brokers, answer)
			if failed == nil {
				failed = fmt.Errorf("kafka: protocol negotiation: broker %s did not answer ApiVersions: %w", addr, err)
			}
			continue
		}
		answer.Answered = true
		for _, api := range response.ApiVersions {
			if api != nil && api.ApiKey == produceAPIKey {
				answer.ProduceMinVersion, answer.ProduceMaxVersion = api.MinVersion, api.MaxVersion
			}
		}
		if answer.ProduceMaxVersion < produceVersionWithRecordBatches {
			headers = false
		}
		negotiation.Brokers = append(negotiation.Brokers, answer)
	}
	if failed != nil {
		return negotiation, failed
	}
	if headers && recordHeaderBrokerVersion.IsAtLeast(config.Version) {
		negotiation.version = recordHeaderBrokerVersion
		negotiation.Negotiated = RecordHeaderBrokerVersion
		negotiation.ProduceVersion = produceVersionWithRecordBatches
	}
	negotiation.HeadersSupported = negotiation.version.IsAtLeast(recordHeaderBrokerVersion)
	if !negotiation.HeadersSupported {
		negotiation.Reason = ProtocolReasonUnsupportedByBroker
	}
	return negotiation, nil
}
