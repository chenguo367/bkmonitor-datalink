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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// executionNoDataSchema names the record and nothing else.
//
// The gap envelope writes "alarmd-plan-gap-v2", which states the record's kind
// and its shape in one string. This one does not, because the shape is already
// a field: two fields saying the same thing can disagree, and nothing would be
// checking. The kind is here and the shape is in Version, which is the field
// the readability rule reads.
const executionNoDataSchema = "alarmd-plan-no-data"

// noDataHeader is everything that has to be read before the body can be.
//
// Decoding happens in two stages and cannot be done in one. A strict decoder
// meeting a field a newer schema added would fail, turning a record this build
// simply cannot read into a corrupt one; a lenient decoder would drop those
// fields and hand back what is left, which is the payload the load contract
// refuses on the grounds that it is a guess. Reading the header first means the
// question "can this build read this record" is answered before anything tries.
type noDataHeader struct {
	Schema   string                       `json:"schema"`
	Version  execution.NoDataMemorySchema `json:"version"`
	Identity execution.PlanNoDataIdentity `json:"identity"`
}

type noDataEnvelope struct {
	Schema           string                         `json:"schema"`
	Version          execution.NoDataMemorySchema   `json:"version"`
	Identity         execution.PlanNoDataIdentity   `json:"identity"`
	MarkerRevision   uint64                         `json:"marker_revision"`
	ApplyVersion     execution.ApplyVersion         `json:"apply_version"`
	MutationDigest   execution.MutationDigest       `json:"mutation_digest"`
	ScheduleRevision execution.PlanScheduleRevision `json:"schedule_revision"`
	RosterVersion    string                         `json:"roster_version"`
	Groups           []execution.NoDataGroupMemory  `json:"groups"`
}

// LoadNoData reads one Plan's no-data memory per request item.
func (store *ExecutionStore) LoadNoData(
	ctx context.Context, request execution.NoDataLoadRequest,
) (execution.NoDataLoadResult, error) {
	if err := request.Contract.Validate(); err != nil || len(request.Items) == 0 ||
		len(request.Items) > store.options.MaxItemsPerCall {
		return execution.NoDataLoadResult{}, fmt.Errorf("state: invalid no-data load request")
	}
	result := execution.NoDataLoadResult{Items: make([]execution.NoDataMemorySnapshot, len(request.Items))}
	for index, item := range request.Items {
		if err := ctx.Err(); err != nil {
			return execution.NoDataLoadResult{}, err
		}
		snapshot, renewal := store.loadOneNoData(ctx, request, item)
		result.Items[index] = snapshot
		if renewal != nil {
			result.Renewals = append(result.Renewals, *renewal)
		}
	}
	return result, nil
}

func (store *ExecutionStore) loadOneNoData(
	ctx context.Context, request execution.NoDataLoadRequest, item execution.PlanNoDataLoadItem,
) (execution.NoDataMemorySnapshot, *execution.NoDataMemoryRenewal) {
	blob := store.loadOneNoDataBlob(ctx, request, item)
	hash, renewal, consulted := store.loadOneNoDataHash(ctx, request, item)
	if !consulted {
		return blob, renewal
	}
	return newerNoDataMemory(blob, hash), renewal
}

// newerNoDataMemory picks between the two representations a Plan may have
// during a rollout.
//
// By apply version, not by representation. A build that writes the hash never
// writes the whole-memory record, but a Plan can move back to a build that only
// knows the old one - a rollback, or a rebalance onto a worker not yet
// upgraded - and that build then writes a record newer than the hash. Reading
// the hash because it is the new shape would lose those rounds.
//
// A tie goes to the hash. One Slot writes one representation, so the two cannot
// state the same round; a tie means something that is not one Slot per round,
// and the hash is the shape the fleet is moving to.
//
// Only a record that was read is a candidate. An unreadable or failed read of
// either side is never the answer: the other side is used if it is a record,
// and if neither is, the failure itself is reported - the caller has to be able
// to tell a Plan with no memory from a Plan whose memory could not be read.
func newerNoDataMemory(blob, hash execution.NoDataMemorySnapshot) execution.NoDataMemorySnapshot {
	blobFound := blob.Status == execution.NoDataMemoryFound
	hashFound := hash.Status == execution.NoDataMemoryFound
	switch {
	case blobFound && hashFound:
		if execution.CompareApplyVersion(hash.PersistedApplyVersion, blob.PersistedApplyVersion) ==
			execution.ApplyVersionPersistedOlder {
			return blob
		}
		return hash
	case hashFound:
		return hash
	case blobFound:
		return blob
	case hash.Status != execution.NoDataMemoryMissing:
		// A hash that could not be read is reported even when the old record is
		// merely absent: the Plan's memory has moved to the hash, so an
		// unreadable one is the memory, not a missing one.
		return hash
	default:
		// Neither is a record. Nothing was read, so nothing names a shape: a
		// Plan before its first no-data round must not read as "still on the
		// old representation", which is the number the cleanup waits on.
		return blob
	}
}

func (store *ExecutionStore) loadOneNoDataBlob(
	ctx context.Context, request execution.NoDataLoadRequest, item execution.PlanNoDataLoadItem,
) execution.NoDataMemorySnapshot {
	snapshot := execution.NoDataMemorySnapshot{Identity: item.Identity, Status: execution.NoDataMemoryMissing}
	raw, err := store.readOneRenewing(ctx, item.Identity.Plan, item.Retention,
		func() (string, error) { return PlanNoDataKeyV2(store.options.Prefix, item.Identity) })
	if err != nil {
		var identityErr *IdentityError
		if errors.Is(err, ErrLifetimeUnsupported) {
			snapshot.Status = execution.NoDataMemoryTerminal
			snapshot.ReasonCode = execution.ReasonCode(contract.ReasonBackendCapabilityMissing)
		} else if errors.As(err, &identityErr) {
			snapshot.Status = execution.NoDataMemoryTerminal
			snapshot.ReasonCode = execution.ReasonCode(contract.ReasonStateCorrupt)
		} else {
			snapshot.Status = execution.NoDataMemoryUnavailable
			snapshot.ReasonCode = execution.ReasonCode(contract.ReasonRedisUnavailable)
		}
		return snapshot
	}
	if raw == nil {
		return snapshot
	}
	if len(raw) > store.options.MaxValueBytes {
		snapshot.Status = execution.NoDataMemoryTerminal
		snapshot.ReasonCode = execution.ReasonCode(contract.ReasonStateBudgetExceeded)
		return snapshot
	}
	return store.validatedNoData(request, item,
		named(decodeNoData(raw, item.Identity), execution.NoDataRepresentationWholeMemory))
}

// named says which record a snapshot was read from, and only when one was.
//
// Only a record that was read names a shape. A failed or unreadable one carries
// nothing but its status and its schema, which is the rule the load contract
// already holds: nothing may be taken out of a record this build could not
// read, and "which key answered" would be the first exception to it.
func named(
	snapshot execution.NoDataMemorySnapshot, representation execution.NoDataRepresentation,
) execution.NoDataMemorySnapshot {
	if snapshot.Status == execution.NoDataMemoryFound {
		snapshot.Representation = representation
	}
	return snapshot
}

// loadOneNoDataHash reads the per-group representation. The second return is
// false when the backend cannot hold one at all, which is not a failure of this
// Plan: the whole-memory record is still the memory there, and reporting a
// capability gap would pause a Plan the old path can serve.
func (store *ExecutionStore) loadOneNoDataHash(
	ctx context.Context, request execution.NoDataLoadRequest, item execution.PlanNoDataLoadItem,
) (execution.NoDataMemorySnapshot, *execution.NoDataMemoryRenewal, bool) {
	target, routeErr := store.options.Router.Route(item.Identity.Plan.TenantID, item.Identity.Plan.StrategyID)
	if routeErr != nil {
		return execution.NoDataMemorySnapshot{Identity: item.Identity, Status: execution.NoDataMemoryUnavailable,
			ReasonCode: execution.ReasonCode(contract.ReasonRedisUnavailable)}, nil, true
	}
	backend, ok := target.Backend.(NoDataHashBackend)
	if !ok {
		return execution.NoDataMemorySnapshot{}, nil, false
	}
	key, err := PlanNoDataHashKeyV2(store.options.Prefix, item.Identity)
	if err != nil {
		return execution.NoDataMemorySnapshot{Identity: item.Identity, Status: execution.NoDataMemoryTerminal,
			ReasonCode: execution.ReasonCode(contract.ReasonStateCorrupt)}, nil, true
	}
	fields, readErr := backend.ReadHash(ctx, key)
	if readErr != nil {
		return execution.NoDataMemorySnapshot{Identity: item.Identity, Status: execution.NoDataMemoryUnavailable,
			ReasonCode: execution.ReasonCode(contract.ReasonRedisUnavailable)}, nil, true
	}
	if len(fields) == 0 {
		return execution.NoDataMemorySnapshot{Identity: item.Identity, Status: execution.NoDataMemoryMissing}, nil, true
	}
	// Renewed before the record is decoded, and for a record this build cannot
	// read just as much as for one it can: a paused Plan's memory must not
	// expire while the rollback it is waiting out is still going on.
	renewal := store.renewNoDataHash(ctx, target, item, key)
	return store.validatedNoData(request, item,
		named(decodeNoDataHash(fields, item.Identity), execution.NoDataRepresentationPerGroup)), renewal, true
}

// renewNoDataHash keeps a record that was read alive the way a whole-memory one
// is kept alive, through the same renewal gate.
//
// It matters more here than it did there. The whole-memory record was rewritten
// whenever anything moved, and that write carried a lifetime; a hash is only
// touched for the groups that changed, so a Plan whose groups are all steady
// writes nothing for as long as that lasts and would let its memory expire
// underneath it. A failed renewal is not this Plan's failure to report - the
// record was read, and the read is what the caller asked for.
func (store *ExecutionStore) renewNoDataHash(
	ctx context.Context, target StorageTarget, item execution.PlanNoDataLoadItem, key string,
) *execution.NoDataMemoryRenewal {
	attempt, err := RenewGenerationKeyReporting(ctx, target, key, item.Retention,
		store.options.RestartMargin, store.options.MinTTL, store.options.MaxTTL, store.renewals)
	if !attempt.Asked && err == nil {
		// The gate answered from what this process already knows. Reporting it
		// would bury the attempts that reached the store under the ones that
		// did not, and it is only the former that say whether renewal works.
		return nil
	}
	renewal := &execution.NoDataMemoryRenewal{
		Identity: item.Identity, Renewed: attempt.Renewed, TTLSeconds: int64(attempt.TTL.Seconds()),
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrLifetimeUnsupported):
		// A backend that cannot renew leaks the lifetime of every memory it
		// holds. The whole-memory path reports this on every load and stops the
		// Slot; this path reports it and carries on, because stopping here
		// would take a Plan's threshold detection down over the lifetime of a
		// record that is still perfectly readable. The two differ on purpose
		// and the reason is that the old path had no way to say it at all.
		renewal.ReasonCode = execution.ReasonCode(contract.ReasonBackendCapabilityMissing)
	default:
		renewal.ReasonCode = execution.ReasonCode(contract.ReasonRedisUnavailable)
	}
	return renewal
}

func (store *ExecutionStore) validatedNoData(
	request execution.NoDataLoadRequest, item execution.PlanNoDataLoadItem,
	snapshot execution.NoDataMemorySnapshot,
) execution.NoDataMemorySnapshot {
	one := execution.NoDataLoadRequest{Contract: request.Contract, Items: []execution.PlanNoDataLoadItem{item}}
	if err := execution.ValidateNoDataLoad(one,
		execution.NoDataLoadResult{Items: []execution.NoDataMemorySnapshot{snapshot}}); err != nil {
		return execution.NoDataMemorySnapshot{Identity: item.Identity, Status: execution.NoDataMemoryTerminal,
			ReasonCode: execution.ReasonCode(contract.ReasonStateCorrupt)}
	}
	return snapshot
}

// decodeNoData reads a stored record, refusing to read its body until the
// header says this build understands its shape.
func decodeNoData(raw []byte, identity execution.PlanNoDataIdentity) execution.NoDataMemorySnapshot {
	corrupt := execution.NoDataMemorySnapshot{Identity: identity, Status: execution.NoDataMemoryTerminal,
		ReasonCode: execution.ReasonCode(contract.ReasonStateCorrupt)}
	var header noDataHeader
	if err := json.Unmarshal(raw, &header); err != nil {
		return corrupt
	}
	if header.Schema != executionNoDataSchema || header.Identity != identity || header.Version == 0 {
		return corrupt
	}
	if !execution.NoDataMemoryReadable(header.Version) {
		// The schema and nothing else. The body is in a shape this build has no
		// definition for, so anything taken out of it would be a guess with the
		// unrecognised fields already dropped.
		return execution.NoDataMemorySnapshot{Identity: identity, Status: execution.NoDataMemoryUnreadable,
			SchemaVersion: header.Version,
			ReasonCode:    execution.ReasonCode(contract.ReasonStateSchemaUnsupported)}
	}
	var envelope noDataEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return corrupt
	}
	if envelope.MarkerRevision == 0 {
		return corrupt
	}
	return execution.NoDataMemorySnapshot{
		Identity: identity, MarkerRevision: envelope.MarkerRevision,
		PersistedApplyVersion: envelope.ApplyVersion, PersistedMutationDigest: envelope.MutationDigest,
		Status: execution.NoDataMemoryFound, SchemaVersion: envelope.Version,
		LastScheduleRevision: envelope.ScheduleRevision, RosterVersion: envelope.RosterVersion,
		Groups: envelope.Groups,
	}
}

// ApplyNoData replaces one Plan's whole no-data memory per request item.
func (store *ExecutionStore) ApplyNoData(
	ctx context.Context, request execution.NoDataApplyRequest,
) (execution.NoDataApplyResult, error) {
	if err := request.Contract.Validate(); err != nil || len(request.Items) == 0 ||
		len(request.Items) > store.options.MaxItemsPerCall {
		return execution.NoDataApplyResult{}, fmt.Errorf("state: invalid no-data apply request")
	}
	result := execution.NoDataApplyResult{Items: make([]execution.NoDataApplyItemResult, len(request.Items))}
	for index, mutation := range request.Items {
		if err := ctx.Err(); err != nil {
			return execution.NoDataApplyResult{}, err
		}
		result.Items[index] = store.applyOneNoData(ctx, mutation)
	}
	return result, nil
}

func (store *ExecutionStore) applyOneNoData(
	ctx context.Context, mutation execution.PlanNoDataMutation,
) execution.NoDataApplyItemResult {
	item := execution.NoDataApplyItemResult{Identity: mutation.Identity}
	reject := func(reason string) execution.NoDataApplyItemResult {
		item.Status, item.ReasonCode = execution.NoDataRejected, execution.ReasonCode(reason)
		return item
	}
	tooLarge := func(record execution.NoDataRecordKind, size execution.NoDataRecordSize) execution.NoDataApplyItemResult {
		size.Record = record
		item.Size = &size
		return reject(contract.ReasonStateBudgetExceeded)
	}
	retry := func() execution.NoDataApplyItemResult {
		item.Status, item.ReasonCode = execution.NoDataRetryable, execution.ReasonCode(contract.ReasonRedisUnavailable)
		return item
	}
	conflict := func(kind execution.StateVersionConflictKind, persisted execution.MutationDigest,
	) execution.NoDataApplyItemResult {
		item.Status = execution.NoDataConflict
		item.Conflict = &execution.NoDataConflictFacts{
			Kind: kind, Persisted: persisted, Proposed: mutation.MemoryDigest,
		}
		return item
	}
	if err := mutation.ValidateDigest(); err != nil {
		return reject(contract.ReasonStateCorrupt)
	}
	if int(mutation.GroupCount) > store.options.MaxNoDataGroups {
		// The one bound left on a memory, and it is a guard rather than a
		// working limit: the representation has no size ceiling any more, so
		// this only catches an expected set that has run away. It is refused
		// the same way the byte bound was - named, counted, and not thrown -
		// because a Plan whose memory is refused goes on evaluating and only
		// stops remembering.
		return tooLarge(execution.NoDataRecordGroups, execution.NoDataRecordSize{
			Groups: int(mutation.GroupCount), Limit: store.options.MaxNoDataGroups,
		})
	}
	key, err := PlanNoDataHashKeyV2(store.options.Prefix, mutation.Identity)
	if err != nil {
		return reject(contract.ReasonStateCorrupt)
	}
	target, routeErr := store.options.Router.Route(mutation.Identity.Plan.TenantID, mutation.Identity.Plan.StrategyID)
	if routeErr != nil {
		return retry()
	}
	backend, ok := target.Backend.(NoDataHashBackend)
	if !ok {
		return reject(contract.ReasonBackendCapabilityMissing)
	}
	// The header and nothing else. Everything this write decides on is in it,
	// and reading the groups as well would double what every Plan transfers per
	// round -- on the very objects whose size this representation exists to
	// bring down, where the record the read would drag back is the thousands of
	// group fields the write is not touching.
	expectedHeader, readErr := backend.ReadHashField(ctx, key, noDataHeaderField)
	if readErr != nil {
		return retry()
	}
	if expectedHeader != nil {
		previous, _, _ := decodeNoDataHashHeader(expectedHeader, mutation.Identity)
		switch previous.Status {
		case execution.NoDataMemoryTerminal:
			return reject(string(previous.ReasonCode))
		case execution.NoDataMemoryUnreadable:
			// Not loaded, not applied to, not cleared. Overwriting a record
			// written by a newer build would destroy state that build is still
			// keeping, and it is the one case where the write has more to lose
			// than the round it belongs to.
			return reject(contract.ReasonStateSchemaUnsupported)
		}
		comparison := execution.CompareApplyVersion(previous.PersistedApplyVersion, mutation.ApplyVersion)
		if comparison == execution.ApplyVersionPersistedNewer {
			item.Status = execution.NoDataStale
			return item
		}
		if comparison == execution.ApplyVersionEqual {
			if previous.PersistedMutationDigest == mutation.MemoryDigest {
				item.Status = execution.NoDataAlreadyApplied
				return item
			}
			// One version, two statements. Not stale - nothing is newer - and
			// not applied - the memories differ - so it is named for what it
			// is, and it lasts one window: the next round carries a newer
			// apply version and passes on its own.
			return conflict(execution.StateVersionConflictSameVersionOtherStatement,
				previous.PersistedMutationDigest)
		}
		if previous.MarkerRevision != mutation.ExpectedMarkerRevision {
			// A delta is only meaningful against the revision it was derived
			// from, so this is a conflict rather than something to reconcile.
			kind := execution.StateVersionConflictRevisionMoved
			if previous.MarkerRevision < mutation.ExpectedMarkerRevision {
				kind = execution.StateVersionConflictRevisionReset
			}
			return conflict(kind, previous.PersistedMutationDigest)
		}
	} else if mutation.ExpectedMarkerRevision != 0 {
		// The caller read a record that is no longer there.
		return conflict(execution.StateVersionConflictMissing, "")
	}
	header, encodeErr := json.Marshal(noDataHashHeader{
		Schema: executionNoDataSchema, Version: mutation.SchemaVersion, Identity: mutation.Identity,
		MarkerRevision: mutation.ExpectedMarkerRevision + 1, ApplyVersion: mutation.ApplyVersion,
		MemoryDigest: mutation.MemoryDigest, ScheduleRevision: mutation.ScheduleRevision,
		RosterVersion: mutation.RosterVersion, PresentAsOf: mutation.PresentAsOf,
	})
	if encodeErr != nil {
		return reject(contract.ReasonStateCorrupt)
	}
	set, deleted, deltaErr := encodeNoDataDelta(mutation)
	if deltaErr != nil {
		return reject(contract.ReasonStateCorrupt)
	}
	write := HashDeltaWrite{
		Key: key, HeaderField: noDataHeaderField,
		ExpectedMissing: expectedHeader == nil, Header: header,
		Set: set, Del: deleted,
		// The floor, for the same reason a gap marker takes it: the load renews
		// to whatever this Plan needs, and this only has to keep the key from
		// being born without a lifetime at all.
		TTL: GenerationScopedFloor,
	}
	if expectedHeader != nil {
		write.ExpectedDigest = HeaderDigest(expectedHeader)
	}
	outcome, applyErr := backend.ApplyHashDelta(ctx, write)
	switch {
	case applyErr != nil:
		return retry()
	case outcome.Status == HashDeltaConflict:
		// Something wrote between the read above and this call. The header it
		// left is classified exactly as a fresh read would classify it, so the
		// answer does not depend on which of the two paths saw it.
		return conflict(raceConflictKind(outcome.Current, mutation), currentMemoryDigest(outcome.Current))
	default:
		item.Status = execution.NoDataApplied
	}
	return item
}

// raceConflictKind names a conflict discovered inside the write rather than by
// the read before it. The two have to name the same thing: a reader comparing
// counts across the two paths is asking one question.
func raceConflictKind(
	current []byte, mutation execution.PlanNoDataMutation,
) execution.StateVersionConflictKind {
	if len(current) == 0 {
		if mutation.ExpectedMarkerRevision == 0 {
			// The caller expected no record and one appeared.
			return execution.StateVersionConflictRevisionMoved
		}
		return execution.StateVersionConflictMissing
	}
	var header noDataHashHeader
	if err := json.Unmarshal(current, &header); err != nil {
		return execution.StateVersionConflictVersionIncomparable
	}
	switch {
	case header.MarkerRevision > mutation.ExpectedMarkerRevision:
		return execution.StateVersionConflictRevisionMoved
	case header.MarkerRevision < mutation.ExpectedMarkerRevision:
		return execution.StateVersionConflictRevisionReset
	default:
		return execution.StateVersionConflictSameVersionOtherStatement
	}
}

func currentMemoryDigest(current []byte) execution.MutationDigest {
	var header noDataHashHeader
	if err := json.Unmarshal(current, &header); err != nil {
		return ""
	}
	return header.MemoryDigest
}
