// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

import (
	"log/slog"
	"unicode/utf8"
)

// RequestTargetSample bounds the targets a line or a cooldown record names.
const RequestTargetSample = 5

// RequestTargetBytes bounds each name of a target as a line or record names
// it.
const RequestTargetBytes = 128

// requestTargetsDigestLen is the length of a digest, in hex digits.
const requestTargetsDigestLen = 16

// RequestTarget is one target a query asked its provider for: the table (a
// result table or a data label), the field and the space the request was
// scoped to - empty for a global business. Truncated names the keys whose
// value was cut to RequestTargetBytes on a character boundary, in the order
// table_id, field_name, space_uid, so a cut name is not read as another
// table and a page marks the one that was cut; absent when none was.
type RequestTarget struct {
	TableID   string   `json:"table_id"`
	FieldName string   `json:"field_name"`
	SpaceUID  string   `json:"space_uid"`
	Truncated []string `json:"truncated,omitempty"`
}

// The keys a RequestTarget's Truncated names.
const (
	RequestTargetTableID   = "table_id"
	RequestTargetFieldName = "field_name"
	RequestTargetSpaceUID  = "space_uid"
)

// SameRequestTarget says two targets name the same table, field and space.
func SameRequestTarget(a, b RequestTarget) bool {
	return a.TableID == b.TableID && a.FieldName == b.FieldName && a.SpaceUID == b.SpaceUID
}

// TruncatedRequestTarget is a target as a line, a record or a page names it:
// each name cut to RequestTargetBytes on a character boundary, and the keys
// cut named, with any it already named.
func TruncatedRequestTarget(target RequestTarget) RequestTarget {
	cut := map[string]bool{}
	for _, key := range target.Truncated {
		cut[key] = true
	}
	var was bool
	target.TableID, was = TruncateRequestTargetName(target.TableID)
	cut[RequestTargetTableID] = cut[RequestTargetTableID] || was
	target.FieldName, was = TruncateRequestTargetName(target.FieldName)
	cut[RequestTargetFieldName] = cut[RequestTargetFieldName] || was
	target.SpaceUID, was = TruncateRequestTargetName(target.SpaceUID)
	cut[RequestTargetSpaceUID] = cut[RequestTargetSpaceUID] || was
	target.Truncated = nil
	for _, key := range []string{RequestTargetTableID, RequestTargetFieldName, RequestTargetSpaceUID} {
		if cut[key] {
			target.Truncated = append(target.Truncated, key)
		}
	}
	return target
}

// RequestTargetsFacts is what a query that came back unavailable had asked
// for, on its query_completed and slot_completed lines and on the Query
// Group's cooldown record: the provider names the status, not the table it
// refused, so these are the request's targets, every one of them, not the
// refused one. Targets is the first RequestTargetSample of them, each once,
// sorted; Count is how many there are; Digest is over the whole set as asked,
// uncut, the key two Query Groups refused on the same targets are grouped
// by. It is a key, not a name: it is not shown on a page.
type RequestTargetsFacts struct {
	Targets []RequestTarget `json:"targets,omitempty"`
	Count   int             `json:"count,omitempty"`
	Digest  string          `json:"digest,omitempty"`
}

// TruncateRequestTargetName cuts a name to RequestTargetBytes on a character
// boundary, and says whether it cut.
func TruncateRequestTargetName(name string) (string, bool) {
	if len(name) <= RequestTargetBytes {
		return name, false
	}
	cut := RequestTargetBytes
	for cut > 0 && !utf8.RuneStart(name[cut]) {
		cut--
	}
	return name[:cut], true
}

func normalizeRequestTargets(facts *RequestTargetsFacts) *RequestTargetsFacts {
	if facts == nil {
		return nil
	}
	normalized := RequestTargetsFacts{Count: max(facts.Count, len(facts.Targets))}
	for _, target := range facts.Targets[:min(len(facts.Targets), RequestTargetSample)] {
		normalized.Targets = append(normalized.Targets, TruncatedRequestTarget(target))
	}
	if len(facts.Digest) == requestTargetsDigestLen && isLowerHex(facts.Digest) {
		normalized.Digest = facts.Digest
	}
	return &normalized
}

func isLowerHex(value string) bool {
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func appendRequestTargetsAttributes(attributes []slog.Attr, facts *RequestTargetsFacts) []slog.Attr {
	if facts == nil {
		return attributes
	}
	return append(attributes,
		slog.Any("request_targets", facts.Targets),
		slog.Int("request_target_count", facts.Count),
		slog.String("request_targets_digest", facts.Digest),
	)
}
