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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// pausingSink takes each batch pause after it is handed it.
type pausingSink struct{ pause time.Duration }

func (sink pausingSink) ConsumeProviderSeries(context.Context, execution.ProviderSeriesBatch) error {
	time.Sleep(sink.pause) // fixed-wait: the Slot slow to take a frame, the wait the case counts
	return nil
}

// Each side of a shared-schema answer's frames counts what it waited for the
// other, on the answer's completion: a Slot slow to hand series on - its sink
// taking each frame's batch 30 ms after it came - leaves the frame reader
// waiting with frames parsed (consumer); an answer whose lines come 30 ms
// apart leaves the Slot waiting for each frame (reader). The side not waited
// on waits less. A legacy answer counts neither.
func TestEachSideOfASharedAnswersFramesCountsItsWaitForTheOther(t *testing.T) {
	const step = 30 * time.Millisecond
	series := make([]responseSeries, 6)
	for index := range series {
		series[index] = sharedTestSeries()
		series[index].GroupValues = []json.RawMessage{json.RawMessage(fmt.Sprintf(`"10.0.0.%d"`, index))}
	}
	// Six series frames, each after its schema frame, the version first and
	// the footer last: 14 lines.
	lines := strings.SplitAfter(sharedTestBody(t, series, map[string]any{"is_partial": false}), "\n")
	execute := func(media string, body []string, gap, pause time.Duration) execution.ProviderStats {
		t.Helper()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", media)
			for _, line := range body {
				if line == "" {
					continue
				}
				time.Sleep(gap) // fixed-wait: the answer's lines coming apart, the wait the case counts
				_, _ = io.WriteString(w, line)
				w.(http.Flusher).Flush()
			}
		}))
		defer server.Close()
		client, err := NewClientWithLimits(server.URL, "alarmd", server.Client(), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		done, err := client.Execute(context.Background(), validAttempt(t), pausingSink{pause: pause})
		if err != nil {
			t.Fatal(err)
		}
		return done.Stats
	}
	waits := func(stats execution.ProviderStats) (time.Duration, time.Duration) {
		return time.Duration(stats.SharedFrameWaitConsumerMicros) * time.Microsecond,
			time.Duration(stats.SharedFrameWaitReaderMicros) * time.Microsecond
	}

	// The reader holds one frame in the channel and one parsed beside it:
	// of the six frames handed on 30 ms each, it waits out at least three.
	consumer, reader := waits(execute(SharedSchemaMediaType, lines, 0, step))
	if consumer < 3*step || reader >= consumer {
		t.Fatalf("a slow Slot: the reader waited %v on it and it %v on the reader, want the reader 90 ms or more and the more",
			consumer, reader)
	}
	// Fourteen lines 30 ms apart: the Slot waits for nearly every one.
	consumer, reader = waits(execute(SharedSchemaMediaType, lines, step, 0))
	if reader < 7*step || consumer >= reader {
		t.Fatalf("a slow answer: the Slot waited %v on the reader and the reader %v on it, want the Slot 210 ms or more and the more",
			reader, consumer)
	}
	legacy := []string{`{"series":[` + identityTestSeriesJSON() + `],"is_partial":false}`}
	if consumer, reader := waits(execute("application/json", legacy, step, step)); consumer != 0 || reader != 0 {
		t.Fatalf("a legacy answer counted %v and %v of frame waits", consumer, reader)
	}
}
