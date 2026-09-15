// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"errors"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// ErrLifetimeUnsupported is a routed backend that cannot renew a key's life.
//
// It is an error rather than a skip. A renewal nobody performs restores exactly
// the leak this mechanism exists to close, and it does so invisibly: the keys
// keep working, the Slot keeps passing, and the only symptom arrives as a Redis
// instance that has been growing for months.
var ErrLifetimeUnsupported = errors.New("state: routed backend cannot renew a key lifetime")

// RenewGenerationKey extends the life of a generation-scoped key that is being
// loaded, when it is running out.
//
// Renewal hangs on the load rather than on the write, and that is the whole
// design. A Plan still being evaluated loads its key every Slot, so it renews
// whether or not anything was written - and a memory that has not changed
// writes nothing, so a write-driven renewal would let a stable long absence,
// the one record that most needs to survive, be the one that expires. A Plan
// whose execution content changed stops loading the old generation's key
// entirely, and that key then ages out on its own.
//
// It is cheap because of the threshold, not because it is rare: the load
// happens every Slot and sends a command only when the remaining life has
// fallen below half.
func RenewGenerationKey(
	ctx context.Context, target StorageTarget, key string, retention []execution.StateRetentionRequirement,
	restartMargin, minimum, maximum time.Duration,
) error {
	backend, ok := target.Backend.(LifetimeBackend)
	if !ok {
		return ErrLifetimeUnsupported
	}
	requirements := make([]LevelRequirement, len(retention))
	for index, level := range retention {
		// Window facts left empty: the TTL formula reads only the retention,
		// and it must read exactly the retention the window was built from.
		requirements[index] = NewLevelRequirement(level, "", 0)
	}
	ttl, err := GenerationScopedTTL(requirements, restartMargin, minimum, maximum)
	if err != nil {
		return err
	}
	_, err = backend.RenewIfBelow(ctx, key, ttl, GenerationScopedRenewalThreshold(ttl))
	return err
}
