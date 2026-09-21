// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"context"
	"sort"
)

// What the sink did with the round's events, as the sink counts it.
//
// The event_acked line said "success, 14 events, 1 ms" for a round whose
// fourteen events were all recoveries, which the Python-compatible protocol
// has no message for: nothing was handed to the broker and the line read as
// a write. On a deployment whose brokers were refusing every write, those
// lines were the evidence that some writes still went through. They were
// not. The line now says how many messages the sink handed to its client and
// how many events produced none -- from the sink, which is the only party
// that knows, through the context the caller gave it.

// OutputWriteFacts is the sink's count for one batch. Published is the
// number of messages handed to the client; on a successful write it is the
// number acknowledged, on a failed one the number attempted. WithoutMessage
// is the events the protocol had no message for -- a zero-message batch is
// a success that reached no broker, and reads as one.
type OutputWriteFacts struct {
	Published      int64 `json:"messages_published"`
	WithoutMessage int64 `json:"events_without_message"`
	// WithoutMessageBy is WithoutMessage split by the event's wire format and
	// kind, as the sink decided each: which protocol had no message for
	// which kind of event. Under the Python-compatible protocol that is
	// every recovery -- an envelope assembled from the records and dropped
	// at the sink -- and how many of those a deployment builds is the
	// number that decides whether the open-alert gate should run for that
	// protocol too. Summed over the buckets it equals WithoutMessage; absent
	// on a report from a sink that gave no breakdown.
	WithoutMessageBy []OutputWithoutMessage `json:"events_without_message_by,omitempty"`
}

// OutputWithoutMessage is one bucket of events the protocol had no message
// for: the resolved wire format, the event kind, and how many.
type OutputWithoutMessage struct {
	Format    string `json:"format"`
	EventKind string `json:"event_kind"`
	Events    int64  `json:"events"`
}

// The event kinds the without-message metric is created for at startup;
// EventKindOther folds a kind this build does not name.
const EventKindOther = "_other"

// OutputEventKinds is every kind a metric cell is created for.
var OutputEventKinds = []string{"ABNORMAL", "RECOVERY", EventKindOther}

// NormalizeOutputEventKind folds an event kind onto the bounded label set.
func NormalizeOutputEventKind(kind string) string {
	switch kind {
	case "ABNORMAL", "RECOVERY":
		return kind
	default:
		return EventKindOther
	}
}

type outputWriteReport struct {
	facts    OutputWriteFacts
	reported bool
}

type outputWriteReportKey struct{}

// ContextWithOutputWriteReport gives the caller of a sink a place for the
// sink's count. The returned function is the facts once the sink reported
// them, or nil for a sink that did not -- an older sink, a fake -- which is
// left absent rather than read as zero messages.
func ContextWithOutputWriteReport(ctx context.Context) (context.Context, func() *OutputWriteFacts) {
	if ctx == nil {
		ctx = context.Background()
	}
	report := &outputWriteReport{}
	return context.WithValue(ctx, outputWriteReportKey{}, report), func() *OutputWriteFacts {
		if !report.reported {
			return nil
		}
		facts := report.facts
		return &facts
	}
}

// ReportOutputWrite is the sink's count of one batch: messages handed to its
// client, events that produced none, and the latter by format and kind. A
// no-op when the caller gave no place for it. The breakdown is sorted so two
// reports of the same batch read the same.
func ReportOutputWrite(ctx context.Context, published, withoutMessage int, withoutMessageBy []OutputWithoutMessage) {
	if ctx == nil {
		return
	}
	report, _ := ctx.Value(outputWriteReportKey{}).(*outputWriteReport)
	if report == nil {
		return
	}
	buckets := append([]OutputWithoutMessage(nil), withoutMessageBy...)
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Format != buckets[j].Format {
			return buckets[i].Format < buckets[j].Format
		}
		return buckets[i].EventKind < buckets[j].EventKind
	})
	report.facts = OutputWriteFacts{Published: int64(published), WithoutMessage: int64(withoutMessage), WithoutMessageBy: buckets}
	report.reported = true
}
