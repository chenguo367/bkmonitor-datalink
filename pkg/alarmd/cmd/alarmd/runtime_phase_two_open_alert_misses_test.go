// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/activecopy"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/linkdmanage"
)

// emptySearch is a link whose search finds no active alert.
type emptySearch struct{}

func (emptySearch) Search(context.Context, string, json.RawMessage, string, int) (linkdmanage.SearchPage, error) {
	return linkdmanage.SearchPage{}, nil
}

// The production wiring lets go of the copy's misses for the strategies the
// Query Group's rounds named here, and of no other strategy's; without a
// copy there is nothing to wire.
func TestALetGoQueryGroupsStrategiesHaveTheirOpenAlertMissesLetGoOf(t *testing.T) {
	at := time.Unix(1_800_000_000, 0)
	copy, err := activecopy.New(activecopy.Options{Link: emptySearch{}, EventSourceID: "alarmd", Now: func() time.Time { return at }})
	if err != nil {
		t.Fatal(err)
	}
	// Fingerprints as the link's are: an md5 in hex, which the copy asks for
	// and no other.
	for strategy, fingerprint := range map[string]string{"7": "0123456789abcdef0123456789abcdef", "8": "fedcba9876543210fedcba9876543210"} {
		copy.Holds("system", strategy, fingerprint)
	}
	copy.Pass(context.Background())
	strategiesFor := func(queryGroup string) []fleet.StrategyRef {
		if queryGroup != "query-group-1" {
			return nil
		}
		return []fleet.StrategyRef{{StrategyID: "7", BusinessID: "2"}}
	}
	forget := forgetOpenAlertMissesOf(copy, strategiesFor)
	forget(execution.QueryGroupIdentity("query-group-1"))
	if forgotten := copy.Stats().MissesForgotten; forgotten != 1 {
		t.Fatalf("let go of %d misses, want strategy 7's one", forgotten)
	}
	if forgetOpenAlertMissesOf(nil, strategiesFor) != nil {
		t.Fatal("wired a forget without a copy")
	}
}
