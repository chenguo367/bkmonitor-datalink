// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package execution

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// RequestSpaceUID is the space a query's request is scoped to: the
// strategy's own space, or none for a Plan that reads every business of its
// tenant, which the provider then takes as unscoped.
func (facts QueryPlanFacts) RequestSpaceUID() string {
	if facts.GlobalBusiness {
		return ""
	}
	return facts.SpaceScope
}

// RequestTargetsOf is what a query asks its provider for, as the lines, the
// cooldown record and the pages name it (observability.RequestTargetsFacts):
// each table, field and space of its query list once, sorted, the first
// observability.RequestTargetSample of them with each name cut on a
// character boundary, how many there are, and the digest of the whole set as
// asked. Read from the request alarmd sends, never from the provider's
// error text. Nil for a query that names no table.
func RequestTargetsOf(facts QueryPlanFacts) *observability.RequestTargetsFacts {
	space := facts.RequestSpaceUID()
	targets := make([]observability.RequestTarget, 0, len(facts.QueryList))
	for _, clause := range facts.QueryList {
		if clause.TableID == "" && clause.FieldName == "" {
			continue
		}
		targets = append(targets, observability.RequestTarget{TableID: clause.TableID, FieldName: clause.FieldName, SpaceUID: space})
	}
	if len(targets) == 0 {
		return nil
	}
	slices.SortFunc(targets, func(a, b observability.RequestTarget) int {
		return cmp.Or(cmp.Compare(a.TableID, b.TableID), cmp.Compare(a.FieldName, b.FieldName), cmp.Compare(a.SpaceUID, b.SpaceUID))
	})
	targets = slices.CompactFunc(targets, observability.SameRequestTarget)
	named := &observability.RequestTargetsFacts{Count: len(targets), Digest: requestTargetsDigest(targets)}
	for _, target := range targets[:min(len(targets), observability.RequestTargetSample)] {
		named.Targets = append(named.Targets, observability.TruncatedRequestTarget(target))
	}
	return named
}

// requestTargetsDigest keys a set of targets: the first 64 bits, in hex, of
// the SHA-256 of the set, sorted and each once, encoded as a JSON array of
// [table, field, space] triples. The encoding is what keeps two sets apart
// whose names would join alike ("ab" and "c" against "a" and "bc"); the
// names are the uncut ones, so two tables alike in their first
// observability.RequestTargetBytes are two keys.
func requestTargetsDigest(sorted []observability.RequestTarget) string {
	triples := make([][3]string, 0, len(sorted))
	for _, target := range sorted {
		triples = append(triples, [3]string{target.TableID, target.FieldName, target.SpaceUID})
	}
	encoded, err := json.Marshal(triples)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:8])
}
