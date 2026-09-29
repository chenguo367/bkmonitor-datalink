// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package storecensus

import (
	"strings"
	"testing"
)

// Keys that differ only in which thing they are about are one family; keys
// of different kinds are not.
func TestAKeysFamilyIsItsKindNotItsInstance(t *testing.T) {
	for key, want := range map[string]string{
		"alarmd:control:object-catalog:qgobj:" + strings.Repeat("ab", 32): "alarmd:control:object-catalog:qgobj:*",
		"alarmd:control:object-catalog:activation":                        "alarmd:control:object-catalog:activation",
		"alarmd:state:{qg-1}:series:0123456789abcdef":                     "alarmd:state:*:series:*",
		"bkmonitor:strategy:12345":                                        "bkmonitor:strategy:*",
		"bkmonitor:biz:-20371:cache":                                      "bkmonitor:biz:*:cache",
		"bkmonitor:deadbeef:cache":                                        "bkmonitor:deadbeef:cache",
		"celery":                                                          "celery",
		"a:b:c:d:e:f:g:h:i:j":                                             "a:b:c:d:e:f:g:h:*",
		"alarmd:qg:550e8400-e29b-41d4-a716-446655440000:state":            "alarmd:qg:*:state",
		"alarmd:state:{qg.1:a}:series":                                    "alarmd:state:*:series",
		"celery-task-meta-550e8400-e29b-41d4-a716-446655440000":           "celery-task-meta-*",
		"_kombu.binding.celery":                                           "_kombu.binding.celery",
		// Not a UUID at the end: the whole long token is one instance.
		"celery-task-meta-550e8400xe29bx41d4xa716x446655440000": "*",
		"celery-task-meta-zzzzzzzz-e29b-41d4-a716-446655440000": "*",
	} {
		if got := FamilyOf(key); got != want {
			t.Errorf("FamilyOf(%q) = %q, want %q", key, got, want)
		}
	}
	if got := FamilyOf(strings.Repeat("x", 200)); got != "*" {
		t.Errorf("a long token = %q, want *", got)
	}
	if got := FamilyOf(strings.Repeat("word:", 5) + strings.Repeat("y", 39)); len(got) > maxFamilyName {
		t.Errorf("family name %q is %d bytes, past %d", got, len(got), maxFamilyName)
	}
}

// The platform's Python cache writes its keys with "." between segments,
// under a prefix of its application, platform and environment, and a
// cluster's name when it runs as one: those keys fold by kind as alarmd's
// do, and two of different kinds stay two.
func TestAPythonCacheKeysFamilyIsItsKind(t *testing.T) {
	md5 := strings.Repeat("0f", 16)
	for key, want := range map[string]string{
		"bk_monitorv3.ee.detect.result.1234.5678." + md5 + ".1":                 "bk_monitorv3.ee.detect.result.*.*.*.*",
		"bk_monitorv3.ee.default.detect.result.99.1." + md5 + ".2":              "bk_monitorv3.ee.default.detect.result.*.*.*.*",
		"bk_monitorv3.ee.detect.new_series.seen.1234.5678." + md5:               "bk_monitorv3.ee.detect.new_series.seen.*.*.*",
		"bk_monitorv3.ee.cache.strategy.1234":                                   "bk_monitorv3.ee.cache.strategy.*",
		"bk_monitorv3.ee.cache.strategy.5678":                                   "bk_monitorv3.ee.cache.strategy.*",
		"bk_monitorv3.ee.checkpoint.strategy_group_" + md5:                      "bk_monitorv3.ee.checkpoint.strategy_group_*",
		"bk_monitorv3.ee.trigger.lock.1234_5678":                                "bk_monitorv3.ee.trigger.lock.*_*",
		"bk_monitorv3.ee[stag].selfmonitor.redis.strategy_cost.snapshot.node_3": "bk_monitorv3.ee[stag].selfmonitor.redis.strategy_cost.snapshot.node_*",
	} {
		if got := FamilyOf(key); got != want {
			t.Errorf("FamilyOf(%q) = %q, want %q", key, got, want)
		}
	}
	if FamilyOf("bk_monitorv3.ee.detect.result.1.2."+md5+".1") == FamilyOf("bk_monitorv3.ee.detect.new_series.seen.1.2."+md5) {
		t.Error("detect.result and new_series.seen are one family, want two")
	}
}
