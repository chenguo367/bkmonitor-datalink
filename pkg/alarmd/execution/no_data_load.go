// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package execution

import (
	"errors"
	"fmt"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

type NoDataLoadStatus string

const (
	// NoDataMemoryMissing is a Plan with no stored memory. It is the normal
	// state before a Plan's first no-data round and it loads as empty memory.
	NoDataMemoryMissing NoDataLoadStatus = "MISSING"
	// NoDataMemoryFound is a stored memory this build can read. Zero groups is a
	// legitimate found record: the Plan remembers nothing, which is different
	// from never having written one because the two carry different marker
	// revisions.
	NoDataMemoryFound NoDataLoadStatus = "FOUND"
	// NoDataMemoryUnreadable is a record written in a schema newer than this
	// build understands, which happens on a rollback.
	//
	// It is a status rather than an error on purpose. During a rollback it is
	// every no-data Plan, every round, for as long as the rollback lasts, and an
	// error would fail the Plan's whole evaluation - taking its threshold
	// detection down with it over a no-data record nobody was asking about. So
	// the Plan is evaluated as usual and only its no-data detection pauses: the
	// record is not loaded, not applied to and not cleared, and the Slot says so
	// by name.
	//
	// The payload is refused with it. A reader that took the groups from a
	// record whose shape it does not know would be guessing at fields it has no
	// definition for, and the fields it silently dropped would be exactly the
	// ones the newer schema added.
	NoDataMemoryUnreadable NoDataLoadStatus = "UNREADABLE"
	// NoDataMemoryUnavailable is a retryable failure to read the record.
	NoDataMemoryUnavailable NoDataLoadStatus = "UNAVAILABLE"
	// NoDataMemoryTerminal is a deterministic failure to read the record.
	NoDataMemoryTerminal NoDataLoadStatus = "TERMINAL"
)

type NoDataLoadRequest struct {
	Contract FrozenExecutionContractRef
	Items    []PlanNoDataLoadItem
}

type NoDataApplyStatus string

const (
	NoDataApplied        NoDataApplyStatus = "APPLIED"
	NoDataAlreadyApplied NoDataApplyStatus = "ALREADY_APPLIED"
	NoDataStale          NoDataApplyStatus = "STALE_VERSION"
	NoDataConflict       NoDataApplyStatus = "CONFLICT"
	NoDataRetryable      NoDataApplyStatus = "RETRYABLE_IO"
	NoDataRejected       NoDataApplyStatus = "DETERMINISTIC_INVALID"
)

type NoDataApplyRequest struct {
	Contract FrozenExecutionContractRef
	Items    []PlanNoDataMutation
}

type NoDataApplyItemResult struct {
	Identity   PlanNoDataIdentity
	Status     NoDataApplyStatus
	ReasonCode ReasonCode
	// Size is set only by a refusal about how large the record is, and carries
	// the measurement that refusal made. STATE_BUDGET_EXCEEDED on its own says
	// a Plan's memory did not fit and leaves a reader with no way to tell a
	// record a little over the bound from one many times it, or to tell which
	// of the two records was measured -- and those are different situations
	// with different remedies.
	Size *NoDataRecordSize
}

// NoDataRecordKind names which record a size refusal measured.
type NoDataRecordKind string

const (
	// NoDataRecordStored is the record already in the store, measured before
	// it is decoded. A Plan can only reach this if the bound moved or another
	// build wrote the record, because no write of this build exceeds it.
	NoDataRecordStored NoDataRecordKind = "STORED"
	// NoDataRecordNext is the record this round would write.
	NoDataRecordNext NoDataRecordKind = "NEXT"
)

// NoDataRecordSize is the measurement behind a size refusal: which record, how
// many bytes it holds, and the bound it was measured against.
type NoDataRecordSize struct {
	Record NoDataRecordKind
	Bytes  int
	Limit  int
}

// NoDataRefusal is one shape a deterministic refusal takes, for the partition
// to pre-create and for a reader to bound the family by.
type NoDataRefusal struct {
	Reason ReasonCode
	Record NoDataRecordKind
}

// NoDataRefusals is every shape a deterministic no-data apply refusal takes.
//
// It is a closed list because every refusal in the store names its reason in
// that file, including the one that passes a stored record's reason through:
// the decoder produces exactly one terminal reason. That is what makes this
// safe to publish and to bound a metric label by, and the store's own test
// drives each condition and checks the pair it produced is here -- a list kept
// by hand beside code that can produce anything is the shape that reads as a
// bound while not being one.
var NoDataRefusals = []NoDataRefusal{
	{Reason: ReasonCode(contract.ReasonStateBudgetExceeded), Record: NoDataRecordStored},
	{Reason: ReasonCode(contract.ReasonStateBudgetExceeded), Record: NoDataRecordNext},
	{Reason: ReasonCode(contract.ReasonStateCorrupt)},
	{Reason: ReasonCode(contract.ReasonBackendCapabilityMissing)},
	{Reason: ReasonCode(contract.ReasonStateSchemaUnsupported)},
}

// NoDataWriteOutcomes is every outcome of a no-data memory write that is not a
// deterministic refusal.
//
// The refusals are reported on their own, with the store's reason and the
// measurement behind it; these are the rest, and they have to be reported too.
// A Plan whose write keeps landing on CONFLICT stores nothing, round after
// round, exactly as a refused one does -- and with only the refusals reported,
// that Plan is silent. The two lists together are every mutation the store was
// asked for, which is what makes "is this Plan's memory being kept" answerable
// rather than inferable from an absence of complaints.
var NoDataWriteOutcomes = noDataWriteOutcomes()

// NoDataApplyStatuses is every status an apply can return. The two lists that
// partition it are derived from this one rather than written beside it: two
// hand-kept lists whose union has to be this one is the shape where a status
// added later goes missing from both and nothing notices.
var NoDataApplyStatuses = []NoDataApplyStatus{
	NoDataApplied, NoDataAlreadyApplied, NoDataStale, NoDataConflict, NoDataRetryable, NoDataRejected,
}

func noDataWriteOutcomes() []NoDataApplyStatus {
	outcomes := make([]NoDataApplyStatus, 0, len(NoDataApplyStatuses)-1)
	for _, status := range NoDataApplyStatuses {
		if status == NoDataRejected {
			continue
		}
		outcomes = append(outcomes, status)
	}
	return outcomes
}

// NoDataWriteStored reports whether an outcome means the store now holds what
// the round wanted written.
//
// ALREADY_APPLIED counts: the record carries this round's own version and
// digest, so the memory the next round reads is the one this round produced.
// STALE_VERSION does not, and it is the one that reads like success and is not
// -- a newer record won, and what this round learned was dropped.
func NoDataWriteStored(status NoDataApplyStatus) bool {
	return status == NoDataApplied || status == NoDataAlreadyApplied
}

type NoDataApplyResult struct {
	Items []NoDataApplyItemResult
}

// NoDataMemorySnapshot is one Plan's stored no-data memory as it was read.
type NoDataMemorySnapshot struct {
	Identity                PlanNoDataIdentity
	MarkerRevision          uint64
	PersistedApplyVersion   ApplyVersion
	PersistedMutationDigest MutationDigest
	Status                  NoDataLoadStatus
	// SchemaVersion is the shape the record was written in. It is carried even
	// when the record is unreadable, and especially then: it is the only thing
	// that says which build wrote it.
	SchemaVersion        NoDataMemorySchema
	LastScheduleRevision PlanScheduleRevision
	RosterVersion        string
	Groups               []NoDataGroupMemory
	ReasonCode           ReasonCode
}

type NoDataLoadResult struct {
	Items []NoDataMemorySnapshot
}

func (result NoDataLoadResult) Find(identity PlanNoDataIdentity) (NoDataMemorySnapshot, bool) {
	for _, item := range result.Items {
		if item.Identity == identity {
			return item, true
		}
	}
	return NoDataMemorySnapshot{}, false
}

func noDataSnapshotHasPayload(snapshot NoDataMemorySnapshot) bool {
	return snapshot.MarkerRevision != 0 || snapshot.PersistedMutationDigest != "" ||
		snapshot.PersistedApplyVersion != (ApplyVersion{}) || snapshot.LastScheduleRevision != "" ||
		snapshot.RosterVersion != "" || len(snapshot.Groups) != 0
}

func ValidateNoDataLoad(request NoDataLoadRequest, result NoDataLoadResult) error {
	if len(result.Items) != len(request.Items) {
		return errors.New("alarmd execution: invalid no-data load result cardinality")
	}
	wanted := make(map[PlanNoDataIdentity]struct{}, len(request.Items))
	for _, item := range request.Items {
		if _, duplicate := wanted[item.Identity]; duplicate {
			return errors.New("alarmd execution: duplicate no-data load request identity")
		}
		wanted[item.Identity] = struct{}{}
	}
	seen := make(map[PlanNoDataIdentity]struct{}, len(result.Items))
	for _, item := range result.Items {
		if _, ok := wanted[item.Identity]; !ok {
			return errors.New("alarmd execution: no-data load returned an unknown identity")
		}
		if _, duplicate := seen[item.Identity]; duplicate {
			return errors.New("alarmd execution: no-data load returned a duplicate identity")
		}
		seen[item.Identity] = struct{}{}
		if err := validateNoDataSnapshot(item); err != nil {
			return err
		}
	}
	return nil
}

func validateNoDataSnapshot(item NoDataMemorySnapshot) error {
	reasonIsNone := item.ReasonCode == "" || item.ReasonCode == observability.ReasonNone
	switch item.Status {
	case NoDataMemoryMissing:
		if noDataSnapshotHasPayload(item) || item.SchemaVersion != 0 || !reasonIsNone {
			return errors.New("alarmd execution: missing no-data memory carries a persisted record")
		}
	case NoDataMemoryFound:
		if item.MarkerRevision == 0 || item.PersistedMutationDigest == "" || !reasonIsNone {
			return errors.New("alarmd execution: found no-data memory requires revision and digest and reason none")
		}
		if item.SchemaVersion == 0 || !NoDataMemoryReadable(item.SchemaVersion) {
			return fmt.Errorf("alarmd execution: found no-data memory has schema %d this build cannot read; "+
				"it is UNREADABLE, not FOUND", item.SchemaVersion)
		}
		if err := item.PersistedApplyVersion.Validate(); err != nil {
			return fmt.Errorf("alarmd execution: invalid no-data apply version: %w", err)
		}
		if item.LastScheduleRevision == "" {
			return errors.New("alarmd execution: found no-data memory requires its last plan schedule revision")
		}
		if item.RosterVersion == "" {
			return errors.New("alarmd execution: found no-data memory requires the roster version it was decided against")
		}
		return validateNoDataGroups(item.Groups)
	case NoDataMemoryUnreadable:
		// The schema is the one fact kept, because it is what names the build
		// that wrote the record. Everything else is refused: a payload in a
		// shape this build has no definition for is not evidence, it is a guess
		// with the fields it did not recognise already dropped.
		if noDataSnapshotHasPayload(item) {
			return errors.New("alarmd execution: unreadable no-data memory carries a payload this build cannot read")
		}
		if NoDataMemoryReadable(item.SchemaVersion) {
			return fmt.Errorf("alarmd execution: no-data memory schema %d is readable by this build and must not be "+
				"reported unreadable", item.SchemaVersion)
		}
		if err := requireReasonClass(item.ReasonCode, contract.ReasonClassDeterministic); err != nil {
			return err
		}
	case NoDataMemoryUnavailable:
		if noDataSnapshotHasPayload(item) {
			return errors.New("alarmd execution: unavailable no-data memory carries trusted payload")
		}
		if err := requireReasonClass(item.ReasonCode, contract.ReasonClassRetryable); err != nil {
			return err
		}
	case NoDataMemoryTerminal:
		if noDataSnapshotHasPayload(item) {
			return errors.New("alarmd execution: terminal no-data memory carries trusted payload")
		}
		if err := requireReasonClass(item.ReasonCode, contract.ReasonClassDeterministic); err != nil {
			return err
		}
	default:
		return errors.New("alarmd execution: unknown no-data load status")
	}
	return nil
}

func validateNoDataGroups(groups []NoDataGroupMemory) error {
	seen := make(map[string]struct{}, len(groups))
	previous := ""
	for index, group := range groups {
		if group.GroupKey == "" {
			return errors.New("alarmd execution: stored no-data group requires a key")
		}
		if _, duplicate := seen[group.GroupKey]; duplicate {
			return errors.New("alarmd execution: stored no-data memory contains a duplicate group")
		}
		seen[group.GroupKey] = struct{}{}
		if index > 0 && group.GroupKey < previous {
			return errors.New("alarmd execution: stored no-data memory is not in canonical group order")
		}
		previous = group.GroupKey
		if group.LastSeen < 0 || group.FirstAbsent < 0 {
			return errors.New("alarmd execution: stored no-data timestamps must not be negative")
		}
		if group.LastSeen == 0 && group.FirstAbsent == 0 {
			return errors.New("alarmd execution: stored no-data group remembers nothing")
		}
	}
	return nil
}
