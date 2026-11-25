// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import "context"

// applyWindow cuts the window being filled and waits until it has landed,
// through the Slot's apply goroutine as a cut window is (windowed_pipeline.go):
// for the cases that drive one window at a time. The apply goroutine is
// joined after each, and the next window starts another.
func (coordinator *SlotExecutionCoordinator) applyWindow(ctx context.Context, stream *streamedExecution, final bool) error {
	if err := stream.takeLandings(ctx); err != nil {
		return err
	}
	task, err := stream.takeWindow(ctx, final)
	if err != nil {
		return err
	}
	if err := stream.handOver(ctx, applyTask{window: task}); err != nil {
		return err
	}
	err = stream.drainApply(ctx)
	stream.joinApply(ctx)
	stream.windows.pipe = nil
	return err
}
