// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package config

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Shopify/sarama"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	enginekafka "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/kafka"
)

// The protocol the product is built with has to carry what the product
// sends. The standard RawEvent puts the tenant in a record header; a client
// built for 0.10.2.0 refuses such a record before any broker sees it
// ("Producing headers requires Kafka at least v0.11"), which is how a
// deployment whose Kafka was up produced nothing for 88 rounds. So this
// opens the real sink with the default configuration against a broker and
// sends one standard event: the produce request has to reach the broker, and
// on the header-capable protocol.
func TestDefaultKafkaProtocolCarriesTheStandardRawEventToTheBroker(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()

	cfg := Default()
	cfg.Kafka.Brokers = []string{broker.Addr()}
	coordinates := cfg.Kafka.TriggerEventCoordinates()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(coordinates.OutputTopic, 0, broker.BrokerID()),
		"ProduceRequest": sarama.NewMockProduceResponse(t).SetVersion(3),
	})

	sink, err := enginekafka.OpenTriggerEventSink(coordinates)
	if err != nil {
		t.Fatalf("OpenTriggerEventSink() with the default configuration = %v", err)
	}
	defer func() { _ = sink.Close() }()

	event := standardRawEventGolden(t)
	if err := sink.WriteBatch(context.Background(), []contract.TriggerEventV1{event}); err != nil {
		t.Fatalf("WriteBatch() of one standard RawEvent on the default protocol = %v, want it sent", err)
	}

	var produced *sarama.ProduceRequest
	for _, request := range broker.History() {
		if produce, ok := request.Request.(*sarama.ProduceRequest); ok {
			produced = produce
		}
	}
	if produced == nil {
		t.Fatal("the broker received no produce request: the client kept the event")
	}
	if produced.Version < 3 {
		t.Fatalf("produce request version = %d, want the record-batch protocol (3 or later) that carries headers", produced.Version)
	}
	if cfg.Kafka.BrokerVersion != enginekafka.MinimumBrokerVersion {
		t.Fatalf("default broker_version = %q, want the program's floor %q", cfg.Kafka.BrokerVersion, enginekafka.MinimumBrokerVersion)
	}
}

// And the same refusal cannot be met at runtime: a version below the floor
// is refused when the configuration is validated, naming the header.
func TestAKafkaProtocolBelowTheHeaderFloorIsRefusedAtValidation(t *testing.T) {
	cfg := Default()
	cfg.Kafka.Brokers = []string{"127.0.0.1:9092"}
	cfg.Kafka.BrokerVersion = "0.10.2.0"
	err := validatePhaseTwoKafkaOutput(cfg.Kafka)
	if err == nil || !strings.Contains(err.Error(), "record headers") {
		t.Fatalf("validatePhaseTwoKafkaOutput() with broker_version 0.10.2.0 = %v, want a refusal naming record headers", err)
	}
}

func standardRawEventGolden(t testing.TB) contract.TriggerEventV1 {
	t.Helper()
	payload, err := os.ReadFile("../contract/testdata/go-v2/trigger_event_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := contract.DecodeTriggerEventV1(payload)
	if err != nil {
		t.Fatal(err)
	}
	event, err := contract.BuildTriggerEventV1(contract.TriggerEventBuildInputV1{
		EventKind: legacy.EventKind, TenantID: legacy.TenantID, BusinessID: legacy.BusinessID, PlanRef: legacy.PlanRef,
		RecordRef: legacy.RecordRef, Observed: legacy.Observed, LevelResults: legacy.LevelResults,
		EvaluationTime: legacy.EvaluationTime, DetectPlanFingerprint: legacy.DetectPlanFingerprint,
		TriggerStateFingerprint: legacy.TriggerStateFingerprint, ExecutionID: legacy.Trace.ExecutionID,
		MaxEvidenceBytes: 64 << 10, DedupeMD5: "0260bae09d2ae3f75683bd06a76e9479",
		StrategyRef: &contract.StrategySnapshotRef{TenantID: legacy.TenantID, BusinessID: 2, StrategyID: 1001, Revision: 7},
	})
	if err != nil {
		t.Fatal(err)
	}
	event.WireFormat = contract.WireFormatStandardRawEvent
	return *event
}
