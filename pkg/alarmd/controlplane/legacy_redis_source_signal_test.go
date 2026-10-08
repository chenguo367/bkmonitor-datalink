// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// The Legacy source reports the cache manager's change signal as written: the
// integer second of the run that last changed something. Anything else it
// finds under that key is reported as no signal, which makes the reconciler
// read everything, as it did before the signal was consulted.
func TestLegacyRedisStrategySourceReadsTheChangeSignalAsWritten(t *testing.T) {
	ctx := context.Background()
	client := newControlplaneRedis(t)
	source := newRedisStrategySource(t, client)
	if signal, err := source.ChangeSignal(ctx); err != nil || signal != (controlplane.SourceChangeSignal{}) {
		t.Fatalf("ChangeSignal() without the key = (%+v, %v), want absent", signal, err)
	}
	if err := client.Set(ctx, "bkmonitor.cache.last_updated", "1700000000", 0).Err(); err != nil {
		t.Fatal(err)
	}
	signal, err := source.ChangeSignal(ctx)
	if err != nil || !signal.Present || signal.Value != "1700000000" || !signal.WrittenAt.Equal(time.Unix(1_700_000_000, 0)) {
		t.Fatalf("ChangeSignal() = (%+v, %v), want the written second", signal, err)
	}
	if signal.HoldsLastGood {
		t.Fatalf("ChangeSignal() without the writer's statement = %+v, want no statement", signal)
	}
	for _, unreadable := range []string{"", "not-a-second", "-5", "0", "1700000000.5"} {
		if err := client.Set(ctx, "bkmonitor.cache.last_updated", unreadable, 0).Err(); err != nil {
			t.Fatal(err)
		}
		if signal, err := source.ChangeSignal(ctx); err != nil || signal.Present {
			t.Fatalf("ChangeSignal() with %q = (%+v, %v), want absent", unreadable, signal, err)
		}
	}
}

// The writer's publication statement counts only as written for this very
// change signal. A statement left behind by a later last_updated - an older
// writer that published after it - or one that is unreadable, of another
// version, or says false, is no statement.
func TestLegacyRedisStrategySourceTakesTheWritersStatementOnlyForItsOwnSignal(t *testing.T) {
	ctx := context.Background()
	client := newControlplaneRedis(t)
	source := newRedisStrategySource(t, client)
	const statementKey = "bkmonitor.cache.publication_semantics"
	if err := client.Set(ctx, statementKey, `{"hold_last_good":true,"last_updated":1700000000,"version":1}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if signal, err := source.ChangeSignal(ctx); err != nil || signal != (controlplane.SourceChangeSignal{}) {
		t.Fatalf("ChangeSignal() with a statement and no last_updated = (%+v, %v), want absent", signal, err)
	}
	if err := client.Set(ctx, "bkmonitor.cache.last_updated", "1700000000", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if signal, err := source.ChangeSignal(ctx); err != nil || !signal.Present || !signal.HoldsLastGood {
		t.Fatalf("ChangeSignal() with the statement for this signal = (%+v, %v), want it held", signal, err)
	}
	for name, statement := range map[string]string{
		"written for an earlier signal": `{"hold_last_good":true,"last_updated":1699999999,"version":1}`,
		"another version":               `{"hold_last_good":true,"last_updated":1700000000,"version":2}`,
		"saying false":                  `{"hold_last_good":false,"last_updated":1700000000,"version":1}`,
		"without a version":             `{"hold_last_good":true,"last_updated":1700000000}`,
		"not JSON":                      `hold_last_good`,
		"empty":                         ``,
	} {
		if err := client.Set(ctx, statementKey, statement, 0).Err(); err != nil {
			t.Fatal(err)
		}
		if signal, err := source.ChangeSignal(ctx); err != nil || !signal.Present || signal.HoldsLastGood {
			t.Fatalf("ChangeSignal() with a statement %s = (%+v, %v), want the signal without it", name, signal, err)
		}
	}
	if err := client.Del(ctx, statementKey).Err(); err != nil {
		t.Fatal(err)
	}
	if signal, err := source.ChangeSignal(ctx); err != nil || !signal.Present || signal.HoldsLastGood {
		t.Fatalf("ChangeSignal() after the statement expired = (%+v, %v), want the signal without it", signal, err)
	}
}
