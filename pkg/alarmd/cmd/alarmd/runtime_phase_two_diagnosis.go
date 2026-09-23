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
	"errors"
	"fmt"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/controlplane"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/fleet"
)

// diagnosisProgressBatch bounds one progress read of a diagnosis page, so a
// page of a large deployment is several bounded reads, not one unbounded.
const diagnosisProgressBatch = 500

// diagnosisUniverse reads the source's active set the way the control plane
// lists it, and classifies a failure into a reason word: the response goes to
// a CLI session, and a dependency's address is not part of a reason.
func diagnosisUniverse(source controlplane.StrategySource) fleet.UniverseReader {
	return func(ctx context.Context) ([]string, error) {
		if source == nil {
			return nil, errors.New("SOURCE_NOT_WIRED")
		}
		ids, err := source.ActiveStrategyIDs(ctx)
		switch {
		case err == nil:
			return ids, nil
		case errors.Is(err, controlplane.ErrLegacySourceIncomplete):
			return nil, fmt.Errorf("SOURCE_INCOMPLETE: the active strategy set is absent or does not decode")
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
			return nil, errors.New("SOURCE_READ_TIMEOUT")
		default:
			return nil, errors.New("SOURCE_UNREADABLE")
		}
	}
}

// progressBatchLoader is the one method a diagnosis reads progress through.
type progressBatchLoader interface {
	LoadProgressBatch(context.Context, []execution.ProgressIdentity) ([]execution.ProgressLoadResult, []error)
}

// diagnosisProgress reads the page's objects' persisted progress in bounded
// batches. An object whose own read failed or found nothing is left out, and
// the page says so per Plan; a batch that failed as a whole fails the page's
// progress, which the page then reports as unavailable.
func diagnosisProgress(store progressBatchLoader) fleet.ProgressReader {
	if store == nil {
		return nil
	}
	return func(queryGroups []string) (map[string]fleet.ProgressFacts, error) {
		out := make(map[string]fleet.ProgressFacts, len(queryGroups))
		ctx := context.Background()
		for start := 0; start < len(queryGroups); start += diagnosisProgressBatch {
			end := min(start+diagnosisProgressBatch, len(queryGroups))
			identities := make([]execution.ProgressIdentity, 0, end-start)
			for _, group := range queryGroups[start:end] {
				identities = append(identities, execution.ProgressIdentity{QueryGroup: execution.QueryGroupIdentity(group)})
			}
			results, errs := store.LoadProgressBatch(ctx, identities)
			failed := 0
			for index, result := range results {
				if errs[index] != nil {
					failed++
					continue
				}
				if result.Status != execution.ProgressFound || result.Progress == nil {
					continue
				}
				out[string(identities[index].QueryGroup)] = fleet.ProgressFacts{
					LastFullSlot: int64(result.Progress.LastFullSlot), NextSlot: int64(result.Progress.NextSlot)}
			}
			if failed == len(identities) && failed > 0 {
				return nil, errors.New("PROGRESS_UNREADABLE")
			}
		}
		return out, nil
	}
}
