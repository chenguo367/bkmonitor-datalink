// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// An answer's values reach the Dataset as the bytes UQ wrote them, whatever
// they are - numbers the encoder writes, numbers written another way, a
// string, null - and a series is kept as one run of columns: its points
// cost their time, value and id, not a record each.
func TestAnAnswersValuesReadBackAsUQWroteThem(t *testing.T) {
	values := []string{`12.5`, `0`, `1e21`, `12.50`, `"1.5"`, `null`, `9007199254740993`, `-3`}
	points := make([]string, len(values))
	for index, value := range values {
		points[index] = fmt.Sprintf("[%d,%s]", 1700123460000+int64(index)*60000, value)
	}
	series := `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],"group_keys":["bk_target_ip"],` +
		`"group_values":["192.0.2.10"],"values":[` + strings.Join(points, ",") + `]}`
	sink := &collectingSink{}
	if _, err := fixtureClient(t, http.StatusOK, `{"series":[`+series+`],"is_partial":false}`, DefaultLimits()).
		Execute(context.Background(), validAttempt(t), sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) != 1 || sink.batches[0].Dataset.Len() != len(values) {
		t.Fatalf("%d batches; want one series of %d points", len(sink.batches), len(values))
	}
	dataset := sink.batches[0].Dataset
	for index, want := range values {
		record, _ := dataset.Record(index)
		if got, ok := record.Value("value"); !ok || string(got) != want {
			t.Fatalf("point %d read back as %q, want %s", index, got, want)
		}
	}
	// One point fewer - a number - costs its time, its value and its record
	// id less.
	shorter := `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],"group_keys":["bk_target_ip"],` +
		`"group_values":["192.0.2.10"],"values":[` + strings.Join(points[:len(points)-1], ",") + `]}`
	fewer := &collectingSink{}
	if _, err := fixtureClient(t, http.StatusOK, `{"series":[`+shorter+`],"is_partial":false}`, DefaultLimits()).
		Execute(context.Background(), validAttempt(t), fewer); err != nil {
		t.Fatal(err)
	}
	last, _ := dataset.Record(len(values) - 1)
	if step, want := dataset.RetainedBytes()-fewer.batches[0].Dataset.RetainedBytes(), uint64(16+len(last.RecordID())); step != want {
		t.Fatalf("one point more costs %d bytes, want %d", step, want)
	}
}

// measureAttempt is validAttempt over a window of points minutes.
func measureAttempt(t *testing.T, points int) execution.QueryAttempt {
	t.Helper()
	attempt := validAttempt(t)
	spec := attempt.Spec
	spec.Digest = ""
	window := execution.QueryWindow{Start: 1_700_120_000, End: 1_700_120_000 + int64(points)*60}
	spec.LogicalWindow, spec.ProviderRange, spec.AcceptedRange = window, window, window
	spec, err := execution.BuildPhysicalQuerySpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	attempt.Spec = spec
	return attempt
}

// measuredAnswer writes an answer of series series of points points each, one
// series at a time, the dimensions of a disk series.
func measuredAnswer(writer http.ResponseWriter, series, points int) {
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(`{"series":[`))
	var line strings.Builder
	for index := 0; index < series; index++ {
		line.Reset()
		if index > 0 {
			line.WriteByte(',')
		}
		fmt.Fprintf(&line, `{"name":"_result0","columns":["_time","_result"],"types":["int64","float64"],`+
			`"group_keys":["bk_target_ip","bk_target_cloud_id","device_name","mountpoint"],`+
			`"group_values":["192.0.2.%d","0","/dev/vd%c%d","/data%d"],"values":[`, index%256, 'a'+index%26, index%7, index)
		for point := 0; point < points; point++ {
			if point > 0 {
				line.WriteByte(',')
			}
			fmt.Fprintf(&line, "[%d,%d.%d]", (1_700_120_000+int64(point)*60)*1000, index%97, point%10)
		}
		line.WriteString("]}")
		_, _ = writer.Write([]byte(line.String()))
	}
	_, _ = writer.Write([]byte(`],"is_partial":false}`))
}

func newMeasureServer(t *testing.T, series, points int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		measuredAnswer(writer, series, points)
	}))
}

// heldSink keeps every series as a Slot does until its completion.
type heldSink struct{ datasets []*execution.Dataset }

func (sink *heldSink) ConsumeProviderSeries(_ context.Context, batch execution.ProviderSeriesBatch) error {
	sink.datasets = append(sink.datasets, batch.Dataset)
	return nil
}

// TestMeasureTheRetainedBytesAgainstTheHeap holds what the Datasets count to
// the heap they take: N series of P points read through the production
// decoder and kept, as a Slot keeps them, counted at their RetainedBytes and
// measured as the HeapAlloc they add once the decoder's garbage is
// collected. Their ratio must fall within [0.8, 1.25] at every shape. It is a
// measurement of hundreds of MiB, run on demand with
// ALARMD_MEASURE_RETAINED_HEAP=1; it prints the wire bytes beside it, the
// count the share read before.
func TestMeasureTheRetainedBytesAgainstTheHeap(t *testing.T) {
	if os.Getenv("ALARMD_MEASURE_RETAINED_HEAP") == "" {
		t.Skip("a measurement: set ALARMD_MEASURE_RETAINED_HEAP=1")
	}
	for _, series := range []int{10_000, 100_000} {
		for _, points := range []int{1, 5, 15, 30} {
			server := newMeasureServer(t, series, points)
			client, err := NewClientWithLimits(server.URL, "alarmd-shadow", server.Client(),
				Limits{MaxSeriesBytes: 8 << 20, MaxSeries: uint64(series), MaxRecords: uint64(series * points)})
			if err != nil {
				t.Fatal(err)
			}
			attempt := measureAttempt(t, points)
			attempt.DeadlineUnixMilli = time.Now().Add(10 * time.Minute).UnixMilli()
			sink := &heldSink{datasets: make([]*execution.Dataset, 0, series)}
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			completion, err := client.Execute(context.Background(), attempt, sink)
			if err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			var counted uint64
			for _, dataset := range sink.datasets {
				counted += dataset.RetainedBytes()
			}
			heap := after.HeapAlloc - before.HeapAlloc
			ratio := float64(counted) / float64(heap)
			t.Logf("series=%d points=%d counted=%d heap=%d ratio=%.3f wire=%d wire_ratio=%.3f", series, points, counted, heap, ratio,
				completion.Delivery.Bytes, float64(completion.Delivery.Bytes)/float64(heap))
			runtime.KeepAlive(sink)
			server.Close()
			if len(sink.datasets) != series || ratio < 0.8 || ratio > 1.25 {
				t.Errorf("series=%d points=%d: %d series held, counted %d of a heap of %d (ratio %.3f); want every series and a ratio within [0.8, 1.25]",
					series, points, len(sink.datasets), counted, heap, ratio)
			}
		}
	}
}
