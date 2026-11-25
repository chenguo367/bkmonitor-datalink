// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package progress

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/ownership"
)

// The owner's record (0.7 B2-3): the Progress record as the Query Group's
// owner last read or wrote it, kept in memory under the fence it was read
// or written with, so that one Slot reads the key once rather than at its
// Next, its BeginSlot and its CommitProgress. Only the stored bytes are kept,
// never anything derived from them: the Slot each round runs is still worked
// out from these bytes and the Schedule catalog as it is now, so a timeline
// the Leader rewrote (a cutover, a prune, a repair, a reopen) is seen the
// next round, with or without a read of the key.
//
// It can be kept because the owner is the record's only writer. Writes rest
// on that too, unbounded by the rounds and the age a Next read is: a write
// judges the record it compares against - a Slot already past, a range in
// flight - from the owner's record, and only a Conflict reads the key again. Every write
// of it - BeginSlot, CommitProgress, BeginRange, CommitRange, SkipPrunedRange
// - is the owner's fenced compare-and-set, and each only moves the cursor
// forward; the Leader, the CLI and the admin pages only read it. A write's
// compare-and-set compares against these bytes, so a record that changed
// behind the owner costs the write a conflict, one real read and the write
// again, within the same call.

// Callers and sources of a Progress read, the closed vocabulary of
// progress_reads_total. A Next read is the slot source's; the others are
// the write a read is for.
const (
	ProgressReadNext   = "next"
	ProgressReadBegin  = "begin"
	ProgressReadCommit = "commit"
	ProgressReadSkip   = "skip"
	ProgressReadRange  = "range"

	ProgressReadRedis = "redis"
	ProgressReadCache = "cache"
)

// ProgressReadCallers and ProgressReadSources list them, for the metric's
// cells.
var (
	ProgressReadCallers = []string{ProgressReadNext, ProgressReadBegin, ProgressReadCommit, ProgressReadSkip, ProgressReadRange}
	ProgressReadSources = []string{ProgressReadRedis, ProgressReadCache}
)

// ownerRecordRounds and ownerRecordAge bound how long a Next read is served
// from the owner's record without reading the key: at most this many Next
// reads, counting the one that read it, or this long since it was read,
// whichever comes first; the read after that goes to the store. No writer
// today moves the cursor back or opens work without the owner writing, so
// the bound guards only a writer added later. Any new writer of the
// Progress record has to be a compare-and-set that only moves the cursor
// forward, or this record has to change first. A strategy edit does not
// reach the record at all: the next Slot's Segment - its query revision,
// object digest and declared content scope - is read from the catalog every
// round (scheduler ReadFrozenSchedule, declaredContentScope), and a fenced
// write carries the content scope and is refused CONTENT_MOVED once the
// content moved, which drops the record here; so the bytes kept are never
// paired with a derivation from before an edit. Nothing derived from the
// catalog - a Segment, a schedule, a next Slot - is kept, only the bytes and
// their fence.
const (
	ownerRecordRounds = 10
	ownerRecordAge    = 5 * time.Minute
)

// ownerRecord is one Query Group's record as its owner last read or wrote
// it. fence is the owner fence it was read or written under; a call under
// any other fence does not use it. rounds is the Next reads it has answered
// since readAt, the last time the key was really read, that one included.
type ownerRecord struct {
	fence   execution.OwnerFence
	raw     []byte
	missing bool
	rounds  int
	readAt  time.Time
}

type ownerRecords struct {
	mu      sync.Mutex
	records map[execution.QueryGroupIdentity]*ownerRecord
}

// LoadProgressFor is LoadProgress for the Query Group's owner, under the
// fence it holds: served from the owner's record while the fence is the one
// it was kept under and the bound (ownerRecordRounds, ownerRecordAge) holds,
// read from the store otherwise. Every other reader - the Leader, the CLI,
// a diagnosis - calls LoadProgress, which always reads the key.
func (store *Store) LoadProgressFor(ctx context.Context, identity execution.ProgressIdentity, fence execution.OwnerFence) (
	execution.ProgressLoadResult, error) {
	name, err := store.namespace(identity)
	if err != nil {
		return execution.ProgressLoadResult{}, err
	}
	if raw, missing, served := store.serveNext(identity.QueryGroup, fence); served {
		store.countRead(ProgressReadNext, ProgressReadCache)
		result, err := store.decodeLoaded(identity, raw, missing)
		if err != nil {
			store.Forget(identity.QueryGroup)
		}
		return result, err
	}
	raw, missing, err := store.readOwnerRecord(ctx, ProgressReadNext, identity.QueryGroup, name, fence)
	if err != nil {
		return execution.ProgressLoadResult{}, err
	}
	result, err := store.decodeLoaded(identity, raw, missing)
	if err != nil {
		store.Forget(identity.QueryGroup)
	}
	return result, err
}

// Forget drops the Query Group's record: its owner released it, so the next
// call for it, by whatever fence, reads the key.
func (store *Store) Forget(queryGroup execution.QueryGroupIdentity) {
	store.owned.mu.Lock()
	defer store.owned.mu.Unlock()
	delete(store.owned.records, queryGroup)
}

// serveNext is the record a Next read under fence is answered with, when
// it is one: kept under that fence and inside the bound. Serving it counts
// one more round against the bound.
func (store *Store) serveNext(queryGroup execution.QueryGroupIdentity, fence execution.OwnerFence) ([]byte, bool, bool) {
	store.owned.mu.Lock()
	defer store.owned.mu.Unlock()
	record := store.owned.records[queryGroup]
	if record == nil || record.fence != fence || record.rounds >= ownerRecordRounds ||
		store.options.Now().Sub(record.readAt) >= ownerRecordAge {
		return nil, false, false
	}
	record.rounds++
	return record.raw, record.missing, true
}

// BeginDeferrable reports whether request's BeginSlot may wait for the
// Slot's first write (014 ⑩ F5), the round committed without it when it
// writes nothing: only when the BeginSlot would do no more than write the
// Slot's unfinished projection onto a record already at it, judged from
// the owner's record without a read. That is, the owner's record is kept
// under request's fence - the record BeginSlot would start from, so
// deciding costs no GET - and it is a record, of this Query Group, whose
// NextSlot is request's Slot and that holds no unfinished Slot and no
// unfinished range. An unfinished Slot is an earlier attempt's evidence:
// a round committed over it without beginning would erase it, unread, so
// its attempt begins as before and BeginSlot judges it. Anything else -
// no record kept, a record under another fence, no record at all, one
// that does not decode - is a BeginSlot now.
func (store *Store) BeginDeferrable(request execution.ProgressBeginRequest) bool {
	if store == nil {
		return false
	}
	store.owned.mu.Lock()
	record := store.owned.records[request.Identity.QueryGroup]
	var raw []byte
	kept := record != nil && record.fence == request.OwnerFence && !record.missing
	if kept {
		raw = record.raw
	}
	store.owned.mu.Unlock()
	if !kept {
		return false
	}
	current, err := decode(raw)
	if err != nil {
		return false
	}
	return current.Identity == request.Identity && current.UnfinishedSlot == nil && current.UnfinishedRange == nil &&
		current.NextSlot == request.Projection.Contract.Slot.EvaluationTime
}

// writeBase is the record a fenced write by caller starts from: the
// owner's record when it is kept under the write's fence, the key read now
// otherwise. cached says which, so the write knows whether a conflict may be
// the record having moved behind the owner rather than a real one.
func (store *Store) writeBase(ctx context.Context, caller string, queryGroup execution.QueryGroupIdentity, name string,
	fence execution.OwnerFence) (raw []byte, missing, cached bool, err error) {
	store.owned.mu.Lock()
	record := store.owned.records[queryGroup]
	if record != nil && record.fence == fence {
		raw, missing = record.raw, record.missing
		store.owned.mu.Unlock()
		store.countRead(caller, ProgressReadCache)
		return raw, missing, true, nil
	}
	store.owned.mu.Unlock()
	raw, missing, err = store.readOwnerRecord(ctx, caller, queryGroup, name, fence)
	return raw, missing, false, err
}

// readOwnerRecord reads the key and keeps what it read as the owner's
// record under fence, the bound starting again from this read. A read that
// fails drops the record.
func (store *Store) readOwnerRecord(ctx context.Context, caller string, queryGroup execution.QueryGroupIdentity, name string,
	fence execution.OwnerFence) ([]byte, bool, error) {
	store.countRead(caller, ProgressReadRedis)
	raw, missing, err := store.options.Control.ReadControl(ctx, queryGroup, name)
	if err != nil {
		store.Forget(queryGroup)
		return nil, false, err
	}
	store.owned.mu.Lock()
	defer store.owned.mu.Unlock()
	if store.owned.records == nil {
		store.owned.records = make(map[execution.QueryGroupIdentity]*ownerRecord)
	}
	store.owned.records[queryGroup] = &ownerRecord{fence: fence, raw: raw, missing: missing, rounds: 1, readAt: store.options.Now()}
	return raw, missing, nil
}

// settleWrite brings the owner's record in line with a fenced write's
// outcome. Applied, the record becomes the bytes written, when it still
// holds the ones the write compared against; the bound is left where it
// was, for a write is not a read. Any other outcome - a conflict, a refusal
// of the fence, an outcome the store could not report - drops it, and the
// next call reads the key.
func (store *Store) settleWrite(queryGroup execution.QueryGroupIdentity, fence execution.OwnerFence, status ownership.FencedCASStatus,
	expectedMissing bool, expected, written []byte) {
	store.owned.mu.Lock()
	defer store.owned.mu.Unlock()
	record := store.owned.records[queryGroup]
	if record == nil {
		return
	}
	if status != ownership.FencedCASApplied || record.fence != fence || record.missing != expectedMissing ||
		!bytes.Equal(record.raw, expected) {
		delete(store.owned.records, queryGroup)
		return
	}
	record.raw, record.missing = written, false
}

// fencedWrite runs write on the record a fenced write starts from
// (writeBase). A conflict met on the owner's record is read again from the
// store and written once more within the same call: the record may have
// moved behind the owner, and then the conflict is the record's and not the
// Slot's. A conflict on what was just read is the answer. A failed read is
// unread, the write's result for a store that did not answer; an error of
// write drops the record.
func fencedWrite[R any](ctx context.Context, store *Store, caller string, queryGroup execution.QueryGroupIdentity, name string,
	fence execution.OwnerFence, unread R, conflicted func(R) bool, write func(raw []byte, missing bool) (R, error)) (R, error) {
	raw, missing, cached, err := store.writeBase(ctx, caller, queryGroup, name, fence)
	if err != nil {
		return unread, nil
	}
	result, err := write(raw, missing)
	if err == nil && cached && conflicted(result) {
		store.Forget(queryGroup)
		if raw, missing, err = store.readOwnerRecord(ctx, caller, queryGroup, name, fence); err != nil {
			return unread, nil
		}
		result, err = write(raw, missing)
	}
	if err != nil {
		store.Forget(queryGroup)
	}
	return result, err
}

func (store *Store) countRead(caller, source string) {
	if store.options.Reads != nil {
		store.options.Reads(caller, source)
	}
}
