// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import (
	"errors"
	"fmt"
	"slices"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// ProviderRealtime is a Query Group whose input is not queried: its result
// table's realtime items are read from the inner Kafka topic Databus cleans
// them into, one period a Slot (016). Its Slot's source is the period
// source, not a query provider.
const ProviderRealtime ProviderKind = "realtime"

// RealtimePeriodSeconds is the period a realtime Query Group's Slot is: the
// one aggregation interval a realtime item may have (016 section 5.4, any
// other refused REALTIME_INTERVAL_UNSUPPORTED).
const RealtimePeriodSeconds = 60

// RealtimeInputFacts is a realtime Query Group's input
// (QueryPlanFacts.Realtime): the topic of one result table.
type RealtimeInputFacts struct {
	// ResultTableID is the result table whose realtime items the group's
	// Plans are.
	ResultTableID string
	// Cluster is the Kafka cluster the table's channel record names; its
	// brokers and credentials are the deployment's, from its values and
	// Secret, never from the record.
	Cluster string
	// Topic is the inner topic the table's rows are cleaned into.
	Topic string
	// SourceKind is the clean rules the table's rows were written by, one of
	// contract.RealtimeSourceKinds: how a row projects to its series'
	// labels. A channel whose source is not resolved yet is not compiled.
	SourceKind string
	// PeriodSeconds is the period one Slot of the group is
	// (RealtimePeriodSeconds).
	PeriodSeconds uint32
	// The metrics the group references are not here: they are its Plans'
	// own (EvaluationPlanV2.Realtime.Metric), and change with every item
	// added or removed, which would move the group's revision with them.
}

// Validate reports why facts cannot be a realtime Query Group's input, nil
// when they can be.
func (facts RealtimeInputFacts) Validate() error {
	switch {
	case facts.ResultTableID == "" || facts.Cluster == "" || facts.Topic == "":
		return errors.New("alarmd execution: a realtime input names its result table, cluster and topic")
	case !slices.Contains(contract.RealtimeSourceKinds(), facts.SourceKind):
		return fmt.Errorf("alarmd execution: realtime source kind %q is not one of %v", facts.SourceKind, contract.RealtimeSourceKinds())
	case facts.PeriodSeconds != RealtimePeriodSeconds:
		return fmt.Errorf("alarmd execution: realtime period %d s, want %d s", facts.PeriodSeconds, RealtimePeriodSeconds)
	}
	return nil
}

// realtimeFacts reports why facts are not a realtime Query Group's
// (BuildQueryPlanFacts): its provider is ProviderRealtime exactly when it
// carries a realtime input, the input holds (RealtimeInputFacts.Validate),
// it names its tenant and business, and it carries none of what a query
// provider is asked - nothing reads them, and a group that set them would
// read as a query it is not.
func (facts QueryPlanFacts) realtimeFacts() error {
	switch {
	case facts.Provider != ProviderRealtime || facts.Realtime == nil:
		return errors.New("alarmd execution: a realtime input is the realtime provider's, and the realtime provider's input is one")
	case facts.TenantID == "" || facts.BusinessID == "":
		return errors.New("alarmd execution: a realtime query group names its tenant and business")
	case facts.ProviderRouteRef != "" || len(facts.QueryList) != 0 || facts.PromQL != nil || len(facts.TSDBMap) != 0 ||
		facts.MetricMerge != "" || facts.NamedOutputs != nil || facts.QueryStepMillis != 0:
		return errors.New("alarmd execution: a realtime query group carries no query a provider is asked")
	}
	return facts.Realtime.Validate()
}
