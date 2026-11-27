// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import "testing"

// A stream consuming a slice beside the one that dispatches them asks the
// dispatching stream to let the Slot's reads ahead go, as the apply side
// asks, and takes its answer: the reads ahead are the dispatch's, let go on
// its goroutine. The dispatching stream lets them go itself (015 C1
// appendix A 11).
func TestAConsumingStreamAsksTheDispatchToYieldTheReadsAhead(t *testing.T) {
	dispatch := &streamedExecution{slotStream: &slotStream{}}
	requests := make(chan yieldRequest)
	consumer := &streamedExecution{slotStream: dispatch.slotStream, yieldTo: requests}
	served := make(chan string, 1)
	go func() {
		request := <-requests
		served <- request.phase
		dispatch.serveYield(request)
	}()
	if consumer.yieldReadAhead("normal_input") {
		t.Fatal("the dispatch let no read ahead go, and the consumer was told to judge its reservation again")
	}
	// The dispatch noted the request before it answered, and the consumer
	// returned on the answer: nothing noted is a request never made.
	select {
	case phase := <-served:
		if phase != "normal_input" {
			t.Fatalf("the dispatch was asked to yield for %q, want the consumer's phase", phase)
		}
	default:
		t.Fatal("the consumer let the reads ahead go itself: the dispatch was never asked")
	}
	if dispatch.yieldReadAhead("normal_input") {
		t.Fatal("the dispatch, with no read ahead, says one was let go")
	}

	// A Slot ending while the consumer asks: the request no one serves ends,
	// nothing let go, and the consumer goes on.
	done := make(chan struct{})
	ended := &streamedExecution{slotStream: dispatch.slotStream, yieldTo: make(chan yieldRequest), yieldDone: done}
	close(done)
	if ended.yieldReadAhead("normal_input") {
		t.Fatal("a request the ended Slot never served said a read ahead was let go")
	}
}
