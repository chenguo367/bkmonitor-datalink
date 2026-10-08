// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package openalerts keeps this process's copy of the alert consumer's open
// alert set: which series, per strategy, the consumer still holds an alert
// on. The trigger asks it before a RECOVERY envelope goes (contract.OpenAlertSet).
//
// For each strategy this process owns, the copy reads the consumer's index
// set (IndexSource), reads it again when the consumer's change notices say it
// moved (Subscriber), and, with a reconciler bound, calibrates it against the
// consumer's own record (Reconciler). Between reads it adds what this process
// itself sent. A set that carries none of this process's own alerts once the
// consumer has had time to open them is taken to be keyed another way (see
// DisjointMinimum), and the gate answers by the UnavailablePolicy instead.
package openalerts

import (
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

const (
	// FingerprintVersion is the algorithm this reader computes fingerprints
	// under, published beside the copy's facts.
	FingerprintVersion = contract.MonitorDedupeMD5Version
	// RefreshInterval is how often the runtime runs Refresh.
	RefreshInterval = time.Minute
)

// StrategyKey identifies one strategy's set.
type StrategyKey struct {
	TenantID   string
	StrategyID string
}
