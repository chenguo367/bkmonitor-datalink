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
	"context"
	"regexp"
	"testing"
)

// The lookback's reads are the seven kinds of its design, each named once,
// none and a value outside the set named not at all; a context carries the
// kind it was given, and none carries nothing.
func TestTheLookbackReadKindsAreTheDesignsEachNamedOnce(t *testing.T) {
	want := []string{"recheck", "deep_recheck", "probation", "earlier", "floor_probe", "directed", "directed_early"}
	kinds := LookbackReadKinds()
	if len(kinds) != len(want) {
		t.Fatalf("%d kinds %v, want the design's %d", len(kinds), kinds, len(want))
	}
	named := map[string]bool{}
	shape := regexp.MustCompile(`^[a-z]+(_[a-z]+)*$`)
	for _, kind := range kinds {
		name, ok := kind.Name()
		if !ok || !shape.MatchString(name) || named[name] {
			t.Fatalf("kind %d is named %q (%v), want a name of its own", kind, name, ok)
		}
		named[name] = true
	}
	for _, name := range want {
		if !named[name] {
			t.Errorf("no kind is named %s", name)
		}
	}
	for _, kind := range []LookbackRead{LookbackReadNone, LookbackRead(len(kinds) + 1), LookbackRead(255)} {
		if name, ok := kind.Name(); ok {
			t.Errorf("kind %d is named %q, want none", kind, name)
		}
	}
	ctx := context.Background()
	if WithLookbackRead(ctx, LookbackReadNone) != ctx || LookbackReadFrom(ctx) != LookbackReadNone {
		t.Fatal("none, or a context given nothing, carries a kind")
	}
	for _, kind := range kinds {
		if got := LookbackReadFrom(WithLookbackRead(ctx, kind)); got != kind {
			t.Fatalf("a context given %d carries %d", kind, got)
		}
	}
}
