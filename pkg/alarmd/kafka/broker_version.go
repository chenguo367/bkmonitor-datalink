// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// MinimumBrokerVersion is the oldest Kafka protocol alarmd speaks, and the
// version its clients are built with: not a setting, a fact about what the
// program sends. It is set by the newest wire feature in use. The standard
// RawEvent carries the tenant in a record header (tenantHeader), and record
// headers exist from 0.11.0.0; on anything older the client refuses such a
// message before it reaches a broker -- "Producing headers requires Kafka
// at least v0.11" -- and a whole deployment's output stopped that way while
// its Kafka was up, because the refusal was reported as the broker not
// acknowledging. Consumer groups, the other feature in use, need 0.10.2.0
// and are covered. Raising this also moves every message, headers or not,
// to the record batch format brokers have accepted since 0.11.
//
// The three places that used to each spell a lower bound take it from
// here; a bound written three times is a bound that moves in two places.
// The parsed form below is derived from this string, not written a second
// time, for the same reason: a mutant that moved the string alone kept
// deciding by the old parsed value while its error text named the new one.
const MinimumBrokerVersion = "0.11.0.0"

var minimumBrokerVersion = mustParseKafkaVersion(MinimumBrokerVersion)

func mustParseKafkaVersion(value string) sarama.KafkaVersion {
	version, err := sarama.ParseKafkaVersion(value)
	if err != nil {
		panic("kafka: MinimumBrokerVersion is not a Kafka version: " + err.Error())
	}
	return version
}

// ValidateBrokerVersion parses a broker version and checks it against the
// range the program supports, naming the feature that sets the floor when
// it is below it. scope prefixes the error the way the caller's other
// errors are prefixed.
func ValidateBrokerVersion(scope, value string) (sarama.KafkaVersion, error) {
	version, err := sarama.ParseKafkaVersion(value)
	if err != nil {
		return sarama.KafkaVersion{}, fmt.Errorf("%s: broker_version %q: %w", scope, value, err)
	}
	if !version.IsAtLeast(minimumBrokerVersion) {
		return sarama.KafkaVersion{}, fmt.Errorf(
			"%s: broker_version %q is below %s, the oldest protocol that carries record headers; "+
				"the standard RawEvent puts the tenant in one and cannot be sent on an older protocol",
			scope, value, MinimumBrokerVersion,
		)
	}
	if !sarama.MaxVersion.IsAtLeast(version) {
		return sarama.KafkaVersion{}, fmt.Errorf(
			"%s: broker_version %q is above %s, the newest protocol this client speaks", scope, value, sarama.MaxVersion,
		)
	}
	return version, nil
}
