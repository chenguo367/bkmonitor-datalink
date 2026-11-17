// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"context"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/segment"
)

// SetPlanSegments binds the read of an object's Plans' segments - each
// Plan's mark as Redis holds it and what this replica last did with them -
// for the object detail. Read on request only.
func (service *Service) SetPlanSegments(read func(ctx context.Context, queryGroup string) (segment.PlansReading, error)) {
	if service != nil {
		service.planSegments = read
	}
}

// PlanSegments is that read, when one is bound.
func (service *Service) PlanSegments(ctx context.Context, queryGroup string) (segment.PlansReading, bool, error) {
	if service == nil || service.planSegments == nil {
		return segment.PlansReading{}, false, nil
	}
	reading, err := service.planSegments(ctx, queryGroup)
	return reading, true, err
}

// SetLocalWake binds this replica's due index entry for an object, for the
// object detail: where a healthy object, which no column lists, stands in
// its cycle.
func (service *Service) SetLocalWake(wake func(queryGroup string) WakeFacts) {
	if service != nil {
		service.localWake = wake
	}
}

// LocalWake is this replica's due index entry for the object, when one is
// bound; Known false when the index holds none.
func (service *Service) LocalWake(queryGroup string) (WakeFacts, bool) {
	if service == nil || service.localWake == nil {
		return WakeFacts{}, false
	}
	return service.localWake(queryGroup), true
}
