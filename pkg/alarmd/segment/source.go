// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
package segment

import (
	"context"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// Source answers a series' coverage by its Plan's stable segments as one
// Slot read them before its own round (014 B3 3.2 rule 3): its entry, the
// Plan's mark and grid, when the series is a member.
type Source interface {
	Coverage(identity execution.StateKeyIdentity) (Coverage, bool)
}

type sourceKey struct{}

// WithSource is ctx carrying the Slot's segments, for every read of a
// stored record the Slot makes under it - its preflight, a cache hit, the
// first-seen and no-data pre-reads, a carried generation, the write path's
// conflict decode - which the State store extends by them.
func WithSource(ctx context.Context, source Source) context.Context {
	return context.WithValue(ctx, sourceKey{}, source)
}

// SourceFrom is the segments ctx carries, nil for none.
func SourceFrom(ctx context.Context) Source {
	source, _ := ctx.Value(sourceKey{}).(Source)
	return source
}
