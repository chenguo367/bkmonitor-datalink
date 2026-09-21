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

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
)

// A changed platform horizon empties the candidate cache, so every Plan is
// recompiled under the new setting.
//
// The cache is keyed by the strategy document, and a platform default is
// exactly the input that changes while every document stays put. Without the
// horizon in the round key the new default reaches only the strategies whose
// own document happens to change next: every other Plan keeps compiling with
// the old horizon, its object bytes do not move, its digest does not move, and
// the setting reads as applied while doing nothing. Configuring the horizon
// through the platform default is the ordinary way to configure it, so that
// failure is the feature being off by default and looking on.
func TestAChangedPlatformHorizonRecompilesEveryPlan(t *testing.T) {
	ctx := context.Background()
	strategies := cacheTestStrategies(t)
	planner := &recordingPlanner{facts: queryFacts(t)}
	cache := controlplane.NewCandidateCache()

	build := func(horizon int64) {
		t.Helper()
		if _, err := controlplane.BuildCatalog(ctx, controlplane.BuildRequest{
			Strategies: strategies, Planner: planner, Cache: cache,
			NoDataPolicy: controlplane.NoDataPolicy{TrackingHorizonSeconds: horizon},
		}); err != nil {
			t.Fatal(err)
		}
	}

	build(600)
	if compiled, reused := cache.Stats(); compiled != 2 || reused != 0 {
		t.Fatalf("first round compiled=%d reused=%d, want everything compiled", compiled, reused)
	}
	// The control, and it has to come before the change: without it a cache
	// that never reused anything would pass the assertion below for the wrong
	// reason, and this case would say nothing about the horizon at all.
	build(600)
	if compiled, reused := cache.Stats(); compiled != 0 || reused != 2 {
		t.Fatalf("same horizon compiled=%d reused=%d, want everything reused", compiled, reused)
	}
	build(1200)
	if compiled, reused := cache.Stats(); compiled != 2 || reused != 0 {
		t.Fatalf("changed horizon compiled=%d reused=%d, want every Plan recompiled under the new default; "+
			"a reused Plan keeps the old horizon and nothing says so", compiled, reused)
	}
	// Back to no horizon at all is a change like any other. Zero is the value
	// the round key has to treat as a setting rather than as "unset", or
	// turning the horizon off would be the one change that does not take.
	build(0)
	if compiled, reused := cache.Stats(); compiled != 2 || reused != 0 {
		t.Fatalf("horizon removed compiled=%d reused=%d, want every Plan recompiled without it", compiled, reused)
	}
}
