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
	"encoding/json"
	"sort"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

const (
	// noDataHeaderField holds everything about the record that is not a group.
	// One field rather than several, so a reader learns whether it may read the
	// rest in one lookup, and a writer proves the record is untouched by
	// comparing one value.
	noDataHeaderField = "_meta"
	// noDataGroupPrefix keeps group fields out of the header's namespace. A
	// group key is arbitrary text from the data, so without a prefix a group
	// could be named "_meta" and a record would lose its header to a dimension
	// value.
	noDataGroupPrefix = "g:"
	// noDataPresentValue is the whole stored value of a group seen in the round
	// the header names. Its last-seen time is the header's, so writing one per
	// group would write the same number once per group per round.
	noDataPresentValue = "p"
)

// noDataHashHeader is the record's one non-group field.
type noDataHashHeader struct {
	Schema           string                         `json:"schema"`
	Version          execution.NoDataMemorySchema   `json:"version"`
	Identity         execution.PlanNoDataIdentity   `json:"identity"`
	MarkerRevision   uint64                         `json:"marker_revision"`
	ApplyVersion     execution.ApplyVersion         `json:"apply_version"`
	MemoryDigest     execution.MutationDigest       `json:"memory_digest"`
	ScheduleRevision execution.PlanScheduleRevision `json:"schedule_revision"`
	RosterVersion    string                         `json:"roster_version"`
	PresentAsOf      int64                          `json:"present_as_of"`
}

// noDataHashVersion is the part of the header that has to be read before the
// rest of it may be. Same two-stage rule as the whole-memory record: a strict
// decode of a newer shape fails and reads as corrupt, a lenient one silently
// drops the fields that shape added.
type noDataHashVersion struct {
	Schema   string                       `json:"schema"`
	Version  execution.NoDataMemorySchema `json:"version"`
	Identity execution.PlanNoDataIdentity `json:"identity"`
}

// noDataGroupAbsenceValue is a group that was not seen in the round the header
// names.
type noDataGroupAbsenceValue struct {
	LastSeen    int64 `json:"last_seen,omitempty"`
	FirstAbsent int64 `json:"first_absent,omitempty"`
}

// decodeNoDataHash reads a hash record into the same snapshot shape the
// whole-memory record decodes to, so nothing above the store learns which
// representation it came from.
func decodeNoDataHash(
	fields map[string][]byte, identity execution.PlanNoDataIdentity,
) execution.NoDataMemorySnapshot {
	corrupt := execution.NoDataMemorySnapshot{Identity: identity, Status: execution.NoDataMemoryTerminal,
		ReasonCode: execution.ReasonCode(contract.ReasonStateCorrupt)}
	raw, present := fields[noDataHeaderField]
	if !present {
		// A hash with groups and no header is not a record with a default
		// header. Every write puts one there, so its absence means the record
		// was written by something that does not hold this shape, or partly
		// destroyed - and reading the groups without it would take a present
		// group's last-seen time from a round nobody stated.
		return execution.NoDataMemorySnapshot{Identity: identity, Status: execution.NoDataMemoryUnreadable,
			ReasonCode: execution.ReasonCode(contract.ReasonStateSchemaUnsupported)}
	}
	var version noDataHashVersion
	if err := json.Unmarshal(raw, &version); err != nil {
		return corrupt
	}
	if version.Schema != executionNoDataSchema || version.Identity != identity || version.Version == 0 {
		return corrupt
	}
	if !execution.NoDataMemoryReadable(version.Version) {
		return execution.NoDataMemorySnapshot{Identity: identity, Status: execution.NoDataMemoryUnreadable,
			SchemaVersion: version.Version,
			ReasonCode:    execution.ReasonCode(contract.ReasonStateSchemaUnsupported)}
	}
	var header noDataHashHeader
	if err := json.Unmarshal(raw, &header); err != nil {
		return corrupt
	}
	if header.MarkerRevision == 0 {
		return corrupt
	}
	groups := make([]execution.NoDataGroupMemory, 0, len(fields))
	for name, value := range fields {
		if name == noDataHeaderField {
			continue
		}
		key, isGroup := strings.CutPrefix(name, noDataGroupPrefix)
		if !isGroup || key == "" {
			// A field that is neither the header nor a group. Ignoring it would
			// be reading a record while pretending not to have seen part of it.
			return corrupt
		}
		group, ok := decodeNoDataGroupValue(key, value, header.PresentAsOf)
		if !ok {
			return corrupt
		}
		groups = append(groups, group)
	}
	sort.Slice(groups, func(left, right int) bool { return groups[left].GroupKey < groups[right].GroupKey })
	return execution.NoDataMemorySnapshot{
		Identity: identity, MarkerRevision: header.MarkerRevision,
		PersistedApplyVersion: header.ApplyVersion, PersistedMutationDigest: header.MemoryDigest,
		Status: execution.NoDataMemoryFound, SchemaVersion: header.Version,
		LastScheduleRevision: header.ScheduleRevision, RosterVersion: header.RosterVersion,
		PresentAsOf: header.PresentAsOf, Groups: groups,
	}
}

func decodeNoDataGroupValue(
	key string, value []byte, presentAsOf int64,
) (execution.NoDataGroupMemory, bool) {
	if string(value) == noDataPresentValue {
		if presentAsOf <= 0 {
			// Present against no round at all. The header and the group
			// disagree about whether this Plan has ever had data.
			return execution.NoDataGroupMemory{}, false
		}
		return execution.NoDataGroupMemory{GroupKey: key, LastSeen: presentAsOf}, true
	}
	var absent noDataGroupAbsenceValue
	if err := json.Unmarshal(value, &absent); err != nil {
		return execution.NoDataGroupMemory{}, false
	}
	if absent.LastSeen < 0 || absent.FirstAbsent < 0 ||
		(absent.LastSeen == 0 && absent.FirstAbsent == 0) {
		return execution.NoDataGroupMemory{}, false
	}
	return execution.NoDataGroupMemory{
		GroupKey: key, LastSeen: absent.LastSeen, FirstAbsent: absent.FirstAbsent,
	}, true
}

// encodeNoDataDelta turns one mutation into the fields to write and remove.
func encodeNoDataDelta(mutation execution.PlanNoDataMutation) ([]HashField, []string, error) {
	set := make([]HashField, 0, len(mutation.Set))
	for _, group := range mutation.Set {
		if group.Absent == nil {
			set = append(set, HashField{
				Name: noDataGroupPrefix + group.GroupKey, Value: []byte(noDataPresentValue),
			})
			continue
		}
		encoded, err := json.Marshal(noDataGroupAbsenceValue{
			LastSeen: group.Absent.LastSeen, FirstAbsent: group.Absent.FirstAbsent,
		})
		if err != nil {
			return nil, nil, err
		}
		set = append(set, HashField{Name: noDataGroupPrefix + group.GroupKey, Value: encoded})
	}
	deleted := make([]string, 0, len(mutation.Del))
	for _, key := range mutation.Del {
		deleted = append(deleted, noDataGroupPrefix+key)
	}
	return set, deleted, nil
}
