// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
package segment_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// previousMark is the mark as the release before the skip decoded it.
type previousMark struct {
	Schema   string            `json:"schema"`
	Position int64             `json:"position"`
	Pages    int               `json:"pages"`
	Renewed  []segment.Renewal `json:"renewed"`
	Sizes    []int             `json:"sizes"`
}

// A mark says whether its round left a member unwritten: written, it reads
// back; a mark of a round that left none is the bytes it was before the
// stage; and the release before reads a mark that says so as it read every
// mark, the word aside.
func TestAMarkSaysWhetherItsRoundLeftAMemberUnwritten(t *testing.T) {
	mark := segment.Mark{Position: 120, Pages: 1, Renewed: []segment.Renewal{{At: 1, TTL: 60_000}}, Sizes: []int{10}}
	plain, err := segment.EncodeMark(mark)
	if err != nil {
		t.Fatal(err)
	}
	// The word is left out when it is false: such a mark is written with
	// the fields, and so the bytes, it had before the stage.
	if strings.Contains(string(plain), "skipped") {
		t.Fatalf("a mark of a round that left none is %s, want no word", plain)
	}
	mark.Skipped = true
	raw, err := segment.EncodeMark(mark)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"skipped":true`) {
		t.Fatalf("a mark of a round that left a member is %s, want the word", raw)
	}
	if back, err := segment.DecodeMark(raw); err != nil || !reflect.DeepEqual(back, mark) {
		t.Fatalf("the mark read back (%+v, %v), want %+v", back, err, mark)
	}
	var previous previousMark
	if err := json.Unmarshal(raw, &previous); err != nil || previous.Position != 120 || previous.Pages != 1 {
		t.Fatalf("the release before read (%+v, %v), want the mark", previous, err)
	}
}
