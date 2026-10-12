// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"context"
)

// Observation is a fail-open side channel: an observer that panics must not
// take the path it observes down with it, and must not take the other
// observers down either. A fan-out (Multi) therefore gives each of its
// observers the observation on its own, and a panic in one is recovered
// there; the observers after it still receive the observation. Recovered
// around the whole fan-out instead, every observer after the panicking one
// silently missed every observation it panicked on.

// RecoverObserverPanic recovers a panic. It has to be the deferred call
// itself (defer RecoverObserverPanic()): recover stops a panic only when the
// deferred function calls it directly.
func RecoverObserverPanic() {
	_ = recover()
}

// observeIsolated gives one fan-out member the observation, recovering a
// panic in it so the members after it still receive theirs. The observation
// travels by pointer to the member's own Observe: it is large, and each
// member takes one copy of it, as before the isolation.
func observeIsolated(ctx context.Context, observer Observer, observation *Observation) {
	defer RecoverObserverPanic()
	observer.Observe(ctx, *observation)
}
