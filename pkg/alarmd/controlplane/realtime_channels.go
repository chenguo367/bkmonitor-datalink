// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
)

// RealtimeChannelsSuffix names the strategy cache hash of realtime channels (design 006 C1): <prefix>.realtime_channels,
// written by the strategy publisher in the transaction that writes the strategies.
const RealtimeChannelsSuffix = ".realtime_channels"

// realtimeChannelsSchema is the hash's __schema__ field and the value this reader reads.
const (
	realtimeChannelsSchemaField = "__schema__"
	realtimeChannelsSchema      = "1"
	realtimeChannelSchemaV1     = 1
	realtimeChannelFieldSep     = "|"
)

// The outcomes of reading the hash, and of each record in it. The set is closed.
const (
	// RealtimeChannelsOK: the hash is read under its schema; each record is accepted or refused on its own.
	RealtimeChannelsOK = "ok"
	// RealtimeChannelsAbsent: no hash. A writer that does not publish channels leaves none; with the channel hash written
	// in the strategies' transaction and always carrying __schema__, no hash is not "no channels".
	RealtimeChannelsAbsent = "absent"
	// RealtimeChannelsSchemaUnsupported: the hash has no __schema__ or one this reader does not read.
	RealtimeChannelsSchemaUnsupported = "schema_unsupported"
	// RealtimeChannelsFailed: the read itself failed.
	RealtimeChannelsFailed = "failed"
	// RealtimeChannelRecordMalformed: a record whose field is not <tenant>|<result table> or whose value is not the record.
	RealtimeChannelRecordMalformed = "record_malformed"
	// RealtimeChannelRecordSchemaVersionUnsupported: a record of a result table shape this reader does not read.
	RealtimeChannelRecordSchemaVersionUnsupported = "record_schema_version_unsupported"
	// RealtimeChannelRecordSourceKindUnsupported: a record naming a source kind outside the known ones.
	RealtimeChannelRecordSourceKindUnsupported = "record_source_kind_unsupported"
)

// RealtimeChannelReadResults is the closed set of outcomes, the hash's and the records'.
func RealtimeChannelReadResults() []string {
	return []string{RealtimeChannelsOK, RealtimeChannelsAbsent, RealtimeChannelsSchemaUnsupported, RealtimeChannelsFailed,
		RealtimeChannelRecordMalformed, RealtimeChannelRecordSchemaVersionUnsupported, RealtimeChannelRecordSourceKindUnsupported}
}

// realtimeSourceKinds are the source kinds a record may name: the contract's set, and empty for a channel whose source is
// not resolved yet.
var realtimeSourceKinds = func() map[string]bool {
	kinds := map[string]bool{contract.RealtimeSourceUnresolved: true}
	for _, kind := range contract.RealtimeSourceKinds() {
		kinds[kind] = true
	}
	return kinds
}()

// RealtimeChannelKey is a channel's place: the tenant and the result table.
type RealtimeChannelKey struct {
	Tenant      string
	ResultTable string
}

// RealtimeItemRef is one realtime item the channel's rule covers, as the strategy document names it.
type RealtimeItemRef struct {
	StrategyID int64  `json:"strategy_id"`
	ItemID     int64  `json:"item_id"`
	QueryMD5   string `json:"query_md5"`
}

// RealtimeRefusedItem is a realtime item the channel's rule left out, with the writer's reason.
type RealtimeRefusedItem struct {
	RealtimeItemRef
	Reason string `json:"reason"`
}

// RealtimeChannel is one result table's realtime channel. Topic and KafkaCluster are empty, and SourceKind may be, until
// the channel is built; AppliedAt is zero until a rule is read back as in effect.
type RealtimeChannel struct {
	Key           RealtimeChannelKey
	Topic         string
	KafkaCluster  string
	SchemaVersion int
	SourceKind    string
	Phase         string
	AppliedItems  []RealtimeItemRef
	AppliedAt     time.Time
	RefusedItems  []RealtimeRefusedItem
}

// RealtimeChannelRefusal is a record not read, by its hash field, with the outcome.
type RealtimeChannelRefusal struct {
	Field  string
	Result string
	Err    error
}

// RealtimeChannelsRead is one read of the hash: its outcome, the records accepted and the ones refused.
type RealtimeChannelsRead struct {
	Result   string
	Channels map[RealtimeChannelKey]RealtimeChannel
	Refused  []RealtimeChannelRefusal
}

// RealtimeChannelsHashReader is the client this reads with.
type RealtimeChannelsHashReader interface {
	HGetAll(context.Context, string) *redis.StringStringMapCmd
}

// ReadRealtimeChannels reads <prefix>.realtime_channels. A failed read is the outcome failed with its error; every other
// outcome comes back without one, a hash that is absent or of another schema carrying no channels, so the caller keeps
// what it had and the reason is named.
func ReadRealtimeChannels(ctx context.Context, reader RealtimeChannelsHashReader, prefix string) (RealtimeChannelsRead, error) {
	fields, err := reader.HGetAll(ctx, prefix+RealtimeChannelsSuffix).Result()
	if err != nil {
		return RealtimeChannelsRead{Result: RealtimeChannelsFailed}, fmt.Errorf("alarmd controlplane: read realtime channels: %w", err)
	}
	return DecodeRealtimeChannels(fields), nil
}

// DecodeRealtimeChannels decodes the hash's fields.
func DecodeRealtimeChannels(fields map[string]string) RealtimeChannelsRead {
	if len(fields) == 0 {
		return RealtimeChannelsRead{Result: RealtimeChannelsAbsent}
	}
	if fields[realtimeChannelsSchemaField] != realtimeChannelsSchema {
		return RealtimeChannelsRead{Result: RealtimeChannelsSchemaUnsupported}
	}
	read := RealtimeChannelsRead{Result: RealtimeChannelsOK, Channels: make(map[RealtimeChannelKey]RealtimeChannel, len(fields)-1)}
	names := make([]string, 0, len(fields))
	for name := range fields {
		if name != realtimeChannelsSchemaField {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		channel, result, err := decodeRealtimeChannel(name, fields[name])
		if err != nil {
			read.Refused = append(read.Refused, RealtimeChannelRefusal{Field: name, Result: result, Err: err})
			continue
		}
		read.Channels[channel.Key] = channel
	}
	return read
}

// realtimeChannelValue is the record as the writer encodes it. Members it does not list are not read.
type realtimeChannelValue struct {
	Topic         *string               `json:"topic"`
	KafkaCluster  *string               `json:"kafka_cluster"`
	SchemaVersion *int                  `json:"schema_version"`
	SourceKind    *string               `json:"source_kind"`
	Phase         *string               `json:"phase"`
	AppliedItems  []RealtimeItemRef     `json:"applied_items"`
	AppliedAt     *int64                `json:"applied_at"`
	RefusedItems  []RealtimeRefusedItem `json:"refused_items"`
}

func decodeRealtimeChannel(name, text string) (RealtimeChannel, string, error) {
	tenant, table, ok := strings.Cut(name, realtimeChannelFieldSep)
	if !ok || tenant == "" || table == "" || strings.Contains(table, realtimeChannelFieldSep) {
		return RealtimeChannel{}, RealtimeChannelRecordMalformed, fmt.Errorf("field %q is not <tenant>|<result table>", name)
	}
	var value realtimeChannelValue
	decoder := json.NewDecoder(bytes.NewReader([]byte(text)))
	if err := decoder.Decode(&value); err != nil {
		return RealtimeChannel{}, RealtimeChannelRecordMalformed, err
	}
	switch {
	case value.Topic == nil || value.KafkaCluster == nil || value.SchemaVersion == nil || value.SourceKind == nil || value.Phase == nil:
		return RealtimeChannel{}, RealtimeChannelRecordMalformed, errors.New("a member the record carries is missing")
	case *value.SchemaVersion != realtimeChannelSchemaV1:
		return RealtimeChannel{}, RealtimeChannelRecordSchemaVersionUnsupported, fmt.Errorf("schema_version %d", *value.SchemaVersion)
	case !realtimeSourceKinds[*value.SourceKind]:
		return RealtimeChannel{}, RealtimeChannelRecordSourceKindUnsupported, fmt.Errorf("source_kind %q", *value.SourceKind)
	}
	for _, item := range value.AppliedItems {
		if item.StrategyID <= 0 || item.ItemID <= 0 || item.QueryMD5 == "" {
			return RealtimeChannel{}, RealtimeChannelRecordMalformed, fmt.Errorf("applied item %+v", item)
		}
	}
	for _, item := range value.RefusedItems {
		if item.StrategyID <= 0 || item.ItemID <= 0 || item.QueryMD5 == "" || item.Reason == "" {
			return RealtimeChannel{}, RealtimeChannelRecordMalformed, fmt.Errorf("refused item %+v", item)
		}
	}
	channel := RealtimeChannel{
		Key:           RealtimeChannelKey{Tenant: tenant, ResultTable: table},
		Topic:         *value.Topic,
		KafkaCluster:  *value.KafkaCluster,
		SchemaVersion: *value.SchemaVersion,
		SourceKind:    *value.SourceKind,
		Phase:         *value.Phase,
		AppliedItems:  value.AppliedItems,
		RefusedItems:  value.RefusedItems,
	}
	if value.AppliedAt != nil && *value.AppliedAt > 0 {
		channel.AppliedAt = time.Unix(*value.AppliedAt, 0).UTC()
	}
	return channel, "", nil
}
