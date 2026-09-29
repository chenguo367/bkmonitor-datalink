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
		"a:b:c:d:e:f:g:h":                                                 "a:b:c:d:e:f:*",
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
