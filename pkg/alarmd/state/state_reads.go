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
	"sync/atomic"
)

// The purposes the store's MGETs are read for, the closed vocabulary of
// state_redis_reads_total and state_redis_read_keys_total (014 next9 1): a
// Slot's MGETs were about 2.25 times its admissions and nothing said which
// read what.
const (
	// StateReadRuntime is a Slot's State preflight: the keys the State cache
	// did not serve, in batches of the same target.
	StateReadRuntime = "runtime"
	// StateReadRuntimeVerify is a preflight batch carrying the keys a Plan's
	// sampled round reads back to check against what the cache served.
	StateReadRuntimeVerify = "runtime_verify"
	// StateReadCarry is the preflight's second pass: the previous
	// generation's key of each series an edited Plan carries.
	StateReadCarry = "carry"
	// StateReadGapLoad is a Slot's read of each due Plan's gap key.
	StateReadGapLoad = "gap_load"
	// StateReadGapApply is the read of a gap key before it is written.
	StateReadGapApply = "gap_apply"
	// StateReadSequential is the read of a State key a write goes key by key
	// for: no preflight witness, or a key twice in one request.
	StateReadSequential = "sequential"
	// StateReadSegmentMark and StateReadSegmentPage are a Slot's reads of
	// its Plans' stable segment marks and member pages the cache did not
	// serve.
	StateReadSegmentMark = "segment_mark"
	StateReadSegmentPage = "segment_page"
	// StateReadPlanSegments is the Plans' marks read for an object page,
	// outside any Slot.
	StateReadPlanSegments = "plan_segments"
	// StateReadOther is a read of a purpose this build does not name, which
	// should never be: non-zero is a defect.
	StateReadOther = "other"
)

// StateReadPurposes lists them.
var StateReadPurposes = []string{StateReadRuntime, StateReadRuntimeVerify, StateReadCarry, StateReadGapLoad, StateReadGapApply,
	StateReadSequential, StateReadSegmentMark, StateReadSegmentPage, StateReadPlanSegments, StateReadOther}

// StateReadStats is the store's MGETs by purpose: the calls that reached
// the backend, answered or not, and the keys they asked for.
type StateReadStats struct {
	Calls, Keys map[string]uint64
}

// stateReads counts the store's MGETs by purpose.
type stateReads struct {
	calls, keys map[string]*atomic.Uint64
}

func newStateReads() *stateReads {
	reads := &stateReads{calls: map[string]*atomic.Uint64{}, keys: map[string]*atomic.Uint64{}}
	for _, purpose := range StateReadPurposes {
		reads.calls[purpose], reads.keys[purpose] = &atomic.Uint64{}, &atomic.Uint64{}
	}
	return reads
}

// of is purpose's counters, a purpose outside the vocabulary's other's.
func (reads *stateReads) of(purpose string) (calls, keys *atomic.Uint64) {
	if calls = reads.calls[purpose]; calls == nil {
		purpose, calls = StateReadOther, reads.calls[StateReadOther]
	}
	return calls, reads.keys[purpose]
}

// count counts one call of purpose and keys keys of it.
func (reads *stateReads) count(purpose string, keys int) {
	if reads == nil {
		return
	}
	calls, counted := reads.of(purpose)
	calls.Add(1)
	counted.Add(uint64(keys))
}

// mgetter is what the store's MGETs are sent to: a target's Backend, or the
// compare-and-set backend a write reads through.
type mgetter interface {
	MGet(context.Context, []string) ([][]byte, error)
}

// mget is every MGET the store sends: counted under purpose, keys keys of
// it, before it is sent.
func (store *ExecutionStore) mget(ctx context.Context, backend mgetter, purpose string, keys []string) ([][]byte, error) {
	store.reads.count(purpose, len(keys))
	return backend.MGet(ctx, keys)
}

// mgetVerifying is a preflight batch's MGET: counted once, under
// runtime_verify when it carries a key its Plan's sampled round reads back to
// check (verify of them), under purpose otherwise, and its keys under each
// as they are.
func (store *ExecutionStore) mgetVerifying(ctx context.Context, backend mgetter, purpose string, keys []string, verify int) ([][]byte, error) {
	if verify <= 0 || store.reads == nil {
		return store.mget(ctx, backend, purpose, keys)
	}
	calls, verified := store.reads.of(StateReadRuntimeVerify)
	calls.Add(1)
	verified.Add(uint64(verify))
	_, rest := store.reads.of(purpose)
	rest.Add(uint64(len(keys) - verify))
	return backend.MGet(ctx, keys)
}

// StateReadStats reads the store's MGETs by purpose.
func (store *ExecutionStore) StateReadStats() StateReadStats {
	stats := StateReadStats{Calls: map[string]uint64{}, Keys: map[string]uint64{}}
	if store == nil || store.reads == nil {
		return stats
	}
	for _, purpose := range StateReadPurposes {
		stats.Calls[purpose], stats.Keys[purpose] = store.reads.calls[purpose].Load(), store.reads.keys[purpose].Load()
	}
	return stats
}
