// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// A query's request targets render as an array of objects with their count
// and digest, the array kept to RequestTargetSample, each name cut and
// marked, the count never below what was given, and a digest that is not one
// dropped. A line without them carries none of the keys.
func TestRequestTargetsRenderAsBoundedObjects(t *testing.T) {
	render := func(observation Observation) map[string]any {
		t.Helper()
		var output bytes.Buffer
		limiter, err := NewScopedLogLimiter(ScopedLogLimiterConfig{Window: time.Hour, MaxEvents: 100, MaxScopes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		policy, err := NewScopedBoundedLogPolicy(limiter)
		if err != nil {
			t.Fatal(err)
		}
		NewLoggingObserver(New("alarmd", &output), policy).Observe(context.Background(), observation)
		var line map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(output.String())), &line); err != nil {
			t.Fatalf("line %q: %v", output.String(), err)
		}
		return line
	}
	var targets []RequestTarget
	for i := 0; i < 7; i++ {
		targets = append(targets, RequestTarget{TableID: strings.Repeat("t", RequestTargetBytes+1+i), FieldName: "value", SpaceUID: "bkcc__2"})
	}
	line := render(Observation{Component: ComponentAccess, Stage: StageQueryCompleted, Result: ResultDegraded, ReasonCode: "QUERY_TARGET_MISSING",
		RequestTargets: &RequestTargetsFacts{Targets: targets, Digest: "0123456789abcdef"}})
	listed, _ := line["request_targets"].([]any)
	if len(listed) != RequestTargetSample || line["request_target_count"] != float64(7) || line["request_targets_digest"] != "0123456789abcdef" {
		t.Fatalf("rendered %v of %v, digest %v; want five of seven", line["request_targets"], line["request_target_count"], line["request_targets_digest"])
	}
	for _, raw := range listed {
		target, _ := raw.(map[string]any)
		if table, _ := target["table_id"].(string); len(table) != RequestTargetBytes || !reflect.DeepEqual(target["truncated"], []any{"table_id"}) ||
			target["field_name"] != "value" || target["space_uid"] != "bkcc__2" {
			t.Fatalf("a target rendered as %v, want its table cut to %d bytes and marked", target, RequestTargetBytes)
		}
	}
	if odd := render(Observation{Component: ComponentAccess, Stage: StageQueryCompleted, Result: ResultDegraded, ReasonCode: "QUERY_TARGET_MISSING",
		RequestTargets: &RequestTargetsFacts{Targets: targets[:1], Count: 1, Digest: "NOT A DIGEST"}}); odd["request_targets_digest"] != "" {
		t.Fatalf("a digest that is not one rendered as %v", odd["request_targets_digest"])
	}
	bare := render(Observation{Component: ComponentAccess, Stage: StageQueryCompleted, Result: ResultDegraded, ReasonCode: "QUERY_EMPTY"})
	for _, key := range []string{"request_targets", "request_target_count", "request_targets_digest"} {
		if _, present := bare[key]; present {
			t.Fatalf("a line without request targets carries %s: %v", key, bare)
		}
	}
}

// A target's truncated names keys and nothing else: only table_id,
// field_name and space_uid, each once, in that order, whatever it was given,
// and the keys its own values were cut under.
func TestATargetsTruncatedNamesOnlyItsKeysOnceInOrder(t *testing.T) {
	given := RequestTarget{TableID: "system.cpu", FieldName: strings.Repeat("f", RequestTargetBytes+1), SpaceUID: "bkcc__2",
		Truncated: []string{"space_uid", "a word", "table_id", "table_id"}}
	normalized := normalizeRequestTargets(&RequestTargetsFacts{Targets: []RequestTarget{given}, Count: 1})
	if got := normalized.Targets[0].Truncated; !reflect.DeepEqual(got, []string{"table_id", "field_name", "space_uid"}) {
		t.Fatalf("truncated = %v, want the three keys once each, in order, and no other word", got)
	}
}
