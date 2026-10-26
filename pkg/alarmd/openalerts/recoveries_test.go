// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package openalerts

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
)

// The record holds a close back only for a reopen since the read: a
// recovery of the fingerprint sent after the read began (or within the
// clock skew before it) and an ABNORMAL after that - the listed alert may
// have ended and the fingerprint's active alert be a new one. A recovery
// with no ABNORMAL after it, which a healthy series sends every cycle and
// which at another Level than the alert's is an orphan, holds nothing; nor
// does an ABNORMAL with no recovery before it, a recovery before the read,
// or another strategy's reopen. Neither steady stream writes to Redis. A
// record that cannot be read holds every close back and is counted. No key
// spells a tenant out.
func TestAReopenSinceTheReadHoldsTheCloseBack(t *testing.T) {
	server := redistest.Start(t)
	client := server.Client()
	ctx := testwait.Context(t)
	recoveries, err := NewRecoveries(client, "alarmd-test")
	if err != nil {
		t.Fatal(err)
	}
	fp := func(i int) string { return fmt.Sprintf("%032x", i) }
	readAt := time.Unix(1_800_000_000, 0)
	note := func(at time.Time, strategy, fingerprint string, recovery bool) {
		t.Helper()
		if err := recoveries.Note(ctx, at, []Sent{{TenantID: "tenant-a", StrategyID: strategy, Fingerprint: fingerprint, Recovery: recovery}}); err != nil {
			t.Fatal(err)
		}
	}
	// Recoveries every cycle with no ABNORMAL, and ABNORMALs every cycle
	// with no recovery: nothing written.
	for cycle := time.Duration(0); cycle < 5; cycle++ {
		note(readAt.Add(cycle*20*time.Second), "7", fp(1), true)
		note(readAt.Add(cycle*20*time.Second), "7", fp(2), false)
	}
	if keys := client.Keys(ctx, "*").Val(); len(keys) != 0 {
		t.Fatalf("steady recoveries and ABNORMALs wrote %v", keys)
	}
	// A recovery before the read (beyond the skew), the ABNORMAL after it.
	note(readAt.Add(-RecoverySkew-time.Minute), "7", fp(3), true)
	note(readAt.Add(time.Second), "7", fp(3), false)
	// A recovery within the skew before the read, then an ABNORMAL.
	note(readAt.Add(-RecoverySkew/2), "7", fp(4), true)
	note(readAt.Add(time.Second), "7", fp(4), false)
	// The race itself: recovered after the read began, fired again.
	note(readAt.Add(10*time.Second), "7", fp(5), true)
	note(readAt.Add(30*time.Second), "7", fp(5), false)
	// The same on another strategy.
	note(readAt.Add(10*time.Second), "8", fp(6), true)
	note(readAt.Add(30*time.Second), "8", fp(6), false)
	held := recoveries.Held(ctx, "tenant-a", "7", []string{fp(1), fp(2), fp(3), fp(4), fp(5), fp(6)}, readAt)
	if len(held) != 2 || held[fp(4)] != HeldReopenedSinceRead || held[fp(5)] != HeldReopenedSinceRead {
		t.Fatalf("held %v, want the two reopened since the read", held)
	}
	if stats := recoveries.Stats(); stats.Noted != 4 {
		t.Fatalf("stats %+v, want the four reopens noted and nothing else", stats)
	}
	for _, key := range client.Keys(ctx, "*").Val() {
		if strings.Contains(key, "tenant-a") {
			t.Fatalf("key %q spells the tenant out", key)
		}
	}
	server.Stop()
	held = recoveries.Held(ctx, "tenant-a", "7", []string{fp(1), fp(7)}, readAt)
	if len(held) != 2 || held[fp(7)] != HeldRecoveryUnknown {
		t.Fatalf("an unreadable record held %v, want every close as recovery_unknown", held)
	}
	note(readAt.Add(time.Minute), "7", fp(8), true)
	if err := recoveries.Note(ctx, readAt.Add(2*time.Minute), []Sent{{TenantID: "tenant-a", StrategyID: "7", Fingerprint: fp(8)}}); err == nil {
		t.Fatal("a reopen the record could not take reported no error")
	}
	if stats := recoveries.Stats(); stats.ReadFailed == 0 || stats.NoteFailed != 1 {
		t.Fatalf("stats %+v, want the failed read and the failed reopen counted", stats)
	}
}
