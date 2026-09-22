// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/openalerts"
)

type staticOpenAlertSource struct{ publication openalerts.Publication }

func (source staticOpenAlertSource) Read(context.Context, []openalerts.StrategyKey) (openalerts.Publication, error) {
	return source.publication, nil
}

// The replica's published facts about its copy: the age is absent until a
// publication has been read, and present as the seconds since once it has;
// the mode and the stale flag are the copy's own. The port adapter turns
// the Plans the worker names into the strategy keys the copy reads.
func TestOpenAlertSetFactsAndPortAdapter(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	now := func() time.Time { return at }
	source := &staticOpenAlertSource{}
	cache, err := openalerts.New(openalerts.Options{Source: source, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	facts := openAlertSetFactsSource(cache, now)()
	if facts == nil || facts.Mode != string(openalerts.ModeNeverLoaded) || facts.StaleBeyondBound || facts.AuthoritativeAgeSeconds != nil {
		t.Fatalf("facts before any read = %+v, want never_loaded, not stale, no age", facts)
	}
	// The reader's account before any read: no heartbeat and so no age,
	// cycle or publisher version -- not zeros; the reader's own version;
	// every answer word present at zero.
	if facts.Available || facts.HeartbeatAgeSeconds != nil || facts.CycleSeconds != 0 || facts.FingerprintVersion != "" ||
		facts.ReaderFingerprintVersion != openalerts.FingerprintVersion || facts.TrackedSets != 0 || facts.Members != 0 {
		t.Fatalf("account before any read = %+v, want nothing of the publisher and the reader's own version", facts)
	}
	for _, answer := range openalerts.Answers {
		if count, present := facts.Lookups[string(answer)]; !present || count != 0 {
			t.Fatalf("lookups before any lookup = %v, want every answer word at zero", facts.Lookups)
		}
	}

	port := openAlertCopyPort{cache: cache}
	port.TrackPlans("qg-test", []execution.PlanIdentity{{TenantID: "default", BusinessID: "2", StrategyID: "1001"}})
	source.publication = openalerts.Publication{
		Heartbeat: &openalerts.Heartbeat{PublishedAt: at, Cycle: time.Minute, FingerprintVersion: openalerts.FingerprintVersion},
		Sets:      map[openalerts.StrategyKey][]string{{TenantID: "default", StrategyID: "1001"}: {"f1"}},
	}
	cache.Refresh(context.Background())
	if !port.Contains("default", "1001", "f1") {
		t.Fatal("the tracked strategy's set was not read through the port")
	}
	at = at.Add(45 * time.Second)
	facts = openAlertSetFactsSource(cache, now)()
	if facts.Mode != string(openalerts.ModeAuthoritative) || facts.AuthoritativeAgeSeconds == nil || *facts.AuthoritativeAgeSeconds != 45 {
		t.Fatalf("facts after a read = %+v, want authoritative with age 45", facts)
	}
	if stats := cache.Stats(); stats.Tracked != 1 {
		t.Fatalf("tracked = %d, want the one Plan the worker named", stats.Tracked)
	}
	// The account after the read: the publisher's heartbeat by its own
	// clock, its cycle and version, one set tracked and loaded with its one
	// member, and the lookup above counted under the authoritative word.
	if !facts.Available || facts.UnavailableReason != "" || facts.HeartbeatAgeSeconds == nil || *facts.HeartbeatAgeSeconds != 45 ||
		facts.CycleSeconds != 60 || facts.FingerprintVersion != openalerts.FingerprintVersion ||
		facts.TrackedSets != 1 || facts.LoadedSets != 1 || facts.Members != 1 || facts.Lookups[string(openalerts.AnswerMember)] != 1 {
		t.Fatalf("account after a read = %+v, want available, heartbeat 45 s old, cycle 60, one set with one member, one authoritative_member lookup", facts)
	}
	// The publisher stops: past the staleness bound the copy is on its own
	// and the account says why, with the last heartbeat still dated.
	source.publication = openalerts.Publication{}
	at = at.Add(10 * time.Minute)
	cache.Refresh(context.Background())
	facts = openAlertSetFactsSource(cache, now)()
	if facts.Available || facts.Mode != string(openalerts.ModeSelfMaintained) || facts.UnavailableReason != string(openalerts.UnavailableHeartbeatMissing) ||
		facts.HeartbeatAgeSeconds == nil || *facts.HeartbeatAgeSeconds != 645 {
		t.Fatalf("account after the publisher stopped = %+v, want self_maintained for heartbeat_missing with the last heartbeat 645 s old", facts)
	}
}
