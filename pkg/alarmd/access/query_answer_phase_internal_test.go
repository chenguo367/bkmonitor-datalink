// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package access

import (
	"errors"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// A query whose response's headers came back carries its wait and body to
// the histograms, a failed one too; one no response came back to carries
// neither, its wait whatever it says.
func TestAnAnsweredQuerysPhasesReachItsHistograms(t *testing.T) {
	completion := execution.ProviderCompletion{Stats: execution.ProviderStats{QueryMillis: 4500, Answered: true, WaitMillis: 3000, BodyMillis: 1500}}
	for _, failure := range []string{"", QueryFailureTimeout} {
		answer := queryAnswerOf(completion, nil, 4500*time.Millisecond, failure)
		if !answer.Answered || answer.Wait != 3*time.Second || answer.Body != 1500*time.Millisecond {
			t.Fatalf("an answered query's answer %+v (failure %q), want its 3 s wait and 1.5 s body", answer, failure)
		}
	}
	unanswered := execution.ProviderCompletion{Stats: execution.ProviderStats{QueryMillis: 2000, WaitMillis: 2000}}
	if failed := queryAnswerOf(unanswered, errors.New("reset"), 2*time.Second, QueryFailureTransport); failed.Answered || failed.Wait != 0 || failed.Body != 0 {
		t.Fatalf("an unanswered query's answer %+v, want no phases", failed)
	}
}

// A shared-schema answer's frame waits reach the counter as its completion
// says them, each side its own; an answer that failed has none to carry.
func TestASharedAnswersFrameWaitsReachTheCounter(t *testing.T) {
	completion := execution.ProviderCompletion{Stats: execution.ProviderStats{Answered: true,
		SharedFrameWaitConsumerMicros: 1_500_000, SharedFrameWaitReaderMicros: 250}}
	answer := queryAnswerOf(completion, nil, 4500*time.Millisecond, "")
	if answer.FrameWaitConsumer != 1500*time.Millisecond || answer.FrameWaitReader != 250*time.Microsecond {
		t.Fatalf("frame waits %v on the consumer and %v on the reader, want 1.5 s and 250 µs", answer.FrameWaitConsumer, answer.FrameWaitReader)
	}
	if failed := queryAnswerOf(execution.ProviderCompletion{}, errors.New("reset"), time.Second, QueryFailureTransport); failed.FrameWaitConsumer != 0 || failed.FrameWaitReader != 0 {
		t.Fatalf("a failed answer carried frame waits %+v", failed)
	}
}
