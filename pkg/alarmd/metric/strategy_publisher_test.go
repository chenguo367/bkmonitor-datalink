// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metric

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

type publisherSourceStub struct {
	report controlplane.PublisherReport
	read   bool
	counts []controlplane.PublisherReportCount
}

func (stub publisherSourceStub) PublisherReport() (controlplane.PublisherReport, bool) {
	return stub.report, stub.read
}

func (stub publisherSourceStub) PublisherReportCounts() []controlplane.PublisherReportCount {
	return stub.counts
}

func publisherLabels(t *testing.T, r *Recorder, family string) []map[string]string {
	t.Helper()
	var series []map[string]string
	for _, m := range gatherFamily(t, r, family) {
		labels := map[string]string{}
		for _, l := range m.Label {
			labels[l.GetName()] = l.GetValue()
		}
		series = append(series, labels)
	}
	return series
}

// A process that has not read the active set says nothing about the
// publisher; one whose publisher leaves no record says exactly that.
func TestStrategyPublisherInfoSaysNotProvidedAndIsSilentBeforeARead(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	r.SetStrategyPublisherSource(publisherSourceStub{})
	if got := gatherFamily(t, r, "bkmonitor_alarmd_strategy_publisher_info"); len(got) != 0 {
		t.Fatalf("a process that read nothing emitted %v", got)
	}
	r.SetStrategyPublisherSource(publisherSourceStub{read: true,
		report: controlplane.PublisherReport{State: controlplane.PublisherReportNotProvided}})
	got := publisherLabels(t, r, "bkmonitor_alarmd_strategy_publisher_info")
	if len(got) != 1 || got[0]["state"] != "not_provided" || got[0]["writer"] != "" || got[0]["version"] != "" {
		t.Fatalf("info = %v, want one not_provided series", got)
	}
}

func TestStrategyPublisherInfoNamesTheWriterAndCountsTheRecords(t *testing.T) {
	r := NewRecorder(BuildInfo{})
	r.SetStrategyPublisherSource(publisherSourceStub{read: true,
		report: controlplane.PublisherReport{State: controlplane.PublisherReportProvided, Writer: "example-publisher", Version: "5.3.0"},
		counts: []controlplane.PublisherReportCount{
			{Outcome: "published", Count: 3}, {Outcome: "blocked", Reason: "SPLIT_RECORDS_NOT_BACKFILLED", Count: 2}}})
	info := publisherLabels(t, r, "bkmonitor_alarmd_strategy_publisher_info")
	if len(info) != 1 || info[0]["state"] != "provided" || info[0]["writer"] != "example-publisher" || info[0]["version"] != "5.3.0" {
		t.Fatalf("info = %v", info)
	}
	counted := map[string]float64{}
	for _, m := range gatherFamily(t, r, "bkmonitor_alarmd_strategy_publisher_reports_total") {
		key := ""
		for _, l := range m.Label {
			key += l.GetName() + "=" + l.GetValue() + ";"
		}
		counted[key] = m.GetCounter().GetValue()
	}
	if counted["outcome=published;reason=;"] != 3 || counted["outcome=blocked;reason=SPLIT_RECORDS_NOT_BACKFILLED;"] != 2 || len(counted) != 2 {
		t.Fatalf("reports = %v", counted)
	}
}
