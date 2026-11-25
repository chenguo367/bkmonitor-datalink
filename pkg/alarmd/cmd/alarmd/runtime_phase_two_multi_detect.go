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
	"sync/atomic"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// multiDetectGate carries the release of items detected over several inputs
// (015 F1 6.2) from the round that reads the ready workers - the Leader's
// ownership round, every reconcile interval - to the round that builds the
// Catalog, the source round: such Plans are in the Catalog only while every
// ready worker declares ownership.CapabilityMultiDetect. A worker without it
// refuses their requirements, and with them the whole Slot of their Query
// Group, the single-query Plans that share it too.
//
// Until an ownership round of this process has recorded a ready set - a new
// Leader's source round may come first - or when the one it recorded is
// older than a Leader's ownership rounds come (fresh), the source round
// reads the registry itself once, rather than take no ready set for a fleet
// that cannot run them and withdraw every such Plan for a round on each
// failover, or take a ready set from before it last lost the Leader's place.
type multiDetectGate struct {
	current atomic.Pointer[multiDetectRecord]
	// list reads the ready workers when no ownership round has; now is the
	// clock their registrations are judged ready at.
	list func(context.Context, time.Time) ([]ownership.WorkerRegistration, ownership.ControlReadStats, error)
	now  func() time.Time
	// fresh is how old a record the source round takes: two reconcile
	// intervals, the time between two ownership rounds of a Leader with a
	// round to spare. Older, this process has not run one since - it lost
	// the Leader's place and may have it again - and the ready set it
	// recorded is another fleet's: a worker of the release before may have
	// joined meanwhile, and another Leader held the item for it. Zero takes
	// any record.
	fresh time.Duration
}

// multiDetectRecord is the gate for one ready set, and when it was read.
type multiDetectRecord struct {
	admission controlplane.MultiDetectAdmission
	at        time.Time
}

// record keeps the gate for the ready set an ownership round read.
func (gate *multiDetectGate) record(workers []ownership.WorkerRegistration) {
	if gate == nil {
		return
	}
	decided := ownership.MultiDetectAdmission(workers)
	admission := controlplane.MultiDetectAdmission{Admitted: decided.Admitted, Known: true,
		Unaware: append([]string(nil), decided.Unaware...)}
	gate.current.Store(&multiDetectRecord{admission: admission, at: gate.clock()})
}

func (gate *multiDetectGate) clock() time.Time {
	if gate.now != nil {
		return gate.now()
	}
	return time.Now()
}

// admission is the gate for a source round: the last ownership round's, else
// the registry's now. A registry it cannot read admits nothing, and the next
// round reads again.
func (gate *multiDetectGate) admission(ctx context.Context) controlplane.MultiDetectAdmission {
	if gate == nil {
		return controlplane.MultiDetectAdmission{}
	}
	at := gate.clock()
	if current := gate.current.Load(); current != nil && (gate.fresh <= 0 || at.Sub(current.at) <= gate.fresh) {
		return current.admission
	}
	if gate.list == nil {
		return controlplane.MultiDetectAdmission{}
	}
	workers, _, err := gate.list(ctx, at)
	if err != nil {
		return controlplane.MultiDetectAdmission{}
	}
	gate.record(workers)
	return gate.current.Load().admission
}
