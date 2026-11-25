// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// heldParts is a store's parts that hold the bytes they say.
type heldParts uint64

func (parts heldParts) RetainedBytes() uint64 { return uint64(parts) }

// A kept frame is charged the parts the store kept beside its bytes, as well
// as its bytes as allocated and the frame itself (014 next9 1): a frame
// without parts is charged as before.
func TestAKeptFrameIsChargedThePartsItCarries(t *testing.T) {
	bare := &execution.EncodedStateFrame{Bytes: make([]byte, 100, 128)}
	if got, want := frameRetainedBytes(bare), 128+frameHeaderBytes; got != want {
		t.Fatalf("a frame without parts is charged %d, want %d", got, want)
	}
	carried := &execution.EncodedStateFrame{Bytes: make([]byte, 100, 128), Parts: heldParts(368)}
	if got, want := frameRetainedBytes(carried), 128+frameHeaderBytes+368; got != want {
		t.Fatalf("a frame with parts of 368 bytes is charged %d, want %d", got, want)
	}
}
