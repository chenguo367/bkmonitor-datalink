// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// noDataStageMarkerField names the attempt a staging key belongs to. It is
// outside the group namespace, so no group can be named it, and it is not
// the header, so a staging key is never mistaken for a record; the swap
// removes it before the key becomes one.
const noDataStageMarkerField = "_stage"

// The staging key's life. Every segment sets it again, so it only has to
// outlive the gap between two scripts of one write; what it bounds is how
// long a write abandoned part way - the worker gone, the Slot past its
// deadline - leaves its fields holding the server's memory. It is the time
// left to the write's own deadline, the Slot's that its context carries, with
// the floor beside it, so the key goes about when the attempt it belongs to
// can no longer finish. A write without a deadline, which only a test makes,
// takes the ceiling.
const (
	noDataStageTTLFloor   = 30 * time.Second
	noDataStageTTLCeiling = 10 * time.Minute
)

// errNoDataGroupEncoding is a group that could not be encoded, which is the
// statement's fault and not the server's.
var errNoDataGroupEncoding = errors.New("state: no-data group is not encodable")

// errNoDataGroupCount is a whole-record statement whose group count is not
// the number of groups it carries, the statement's fault as well: the swap
// would find the staging key short or long on every attempt.
var errNoDataGroupCount = errors.New("state: no-data whole record counts other groups than it carries")

func noDataStageTTL(ctx context.Context, now time.Time) time.Duration {
	deadline, bounded := ctx.Deadline()
	if !bounded {
		return noDataStageTTLCeiling
	}
	return min(max(deadline.Sub(now), 0)+noDataStageTTLFloor, noDataStageTTLCeiling)
}

// noDataStageMarker is the value an attempt's staging key carries: the fence
// epoch and attempt number its name already has, and a nonce of this write.
// The nonce is what makes the marker this write's and not only this name's:
// two writes under one name - a caller that holds no fence, or one attempt
// writing the same Plan twice - each find the other's marker and stop rather
// than add fields to it.
func noDataStageMarker(attempt execution.NoDataWriteAttempt) ([]byte, error) {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	return []byte(strconv.FormatUint(attempt.OwnerEpoch, 10) + ":" + strconv.FormatUint(uint64(attempt.AttemptNo), 10) +
		":" + hex.EncodeToString(nonce[:])), nil
}

// replaceNoDataRecord writes a whole memory into the attempt's staging key a
// segment at a time and then puts it in place of the formal key in one
// script, so a reader of the formal key sees the record it had or the new one
// whole, and never part of either.
//
// What it builds at a time is one segment's fields and arguments, not the
// memory's: the server is held one segment's script at a time, and so is the
// worker's memory beyond the statement itself, which the worker reserves at
// one segment (execution.NoDataFirstWriteSegmentGroups).
func (store *ExecutionStore) replaceNoDataRecord(
	ctx context.Context, backend NoDataHashBackend, key, stageKey string, mutation execution.PlanNoDataMutation,
	header, expectedHeader []byte, lifetime time.Duration,
	attempt execution.NoDataWriteAttempt,
) (HashDeltaOutcome, error) {
	// The swap's HLEN is the distinct groups and the marker. Set is strictly
	// ordered (execution.ValidatePlanNoDataMutation), so its length is its
	// distinct groups, and a statement derived from no record carries every
	// group, so that is GroupCount: the two are one number, bound here before
	// anything is staged.
	if int(mutation.GroupCount) != len(mutation.Set) {
		return HashDeltaOutcome{}, errNoDataGroupCount
	}
	marker, err := noDataStageMarker(attempt)
	if err != nil {
		return HashDeltaOutcome{}, err
	}
	ttl := noDataStageTTL(ctx, store.options.Now())
	for start := 0; start == 0 || start < len(mutation.Set); start += execution.NoDataFirstWriteSegmentGroups {
		end := min(start+execution.NoDataFirstWriteSegmentGroups, len(mutation.Set))
		fields, encodeErr := encodeNoDataGroups(mutation.Set[start:end])
		if encodeErr != nil {
			return HashDeltaOutcome{}, errors.Join(errNoDataGroupEncoding, encodeErr)
		}
		outcome, stageErr := backend.StageHashSegment(ctx, HashStageSegment{
			Key: stageKey, MarkerField: noDataStageMarkerField, Marker: marker,
			First: start == 0, Set: fields, TTL: ttl,
		})
		if stageErr != nil || outcome.Status != HashDeltaApplied {
			return outcome, stageErr
		}
	}
	swap := HashStageSwap{
		Key: key, StageKey: stageKey, HeaderField: noDataHeaderField,
		ExpectedMissing: expectedHeader == nil, Header: header,
		MarkerField: noDataStageMarkerField, Marker: marker,
		// The groups the memory holds, as its writer counted them, which is
		// a count of distinct groups and not of fields sent: a group two
		// segments both carry is one field. len(Set), as bound above.
		Fields: int(mutation.GroupCount) + 1,
		TTL:    lifetime,
	}
	if expectedHeader != nil {
		swap.ExpectedDigest = HeaderDigest(expectedHeader)
	}
	return backend.SwapStagedHash(ctx, swap)
}
