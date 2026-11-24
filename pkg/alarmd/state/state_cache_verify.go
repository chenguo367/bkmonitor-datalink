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
	"bytes"
	"crypto/sha1"
	"reflect"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// The sampled round's check of the State cache (014 next7 3.6). With values
// read back kept, a stable member's State is not read from Redis on its
// ordinary rounds at all, and a cache that parted from the store would show
// in no other reading: the rules that let entries go are read from the code,
// not observed. Once a day, in its Plan's piece's sampled round - the round
// that writes and compares every member whole anyway - each value the cache
// would serve is read back all the same and compared: an entry's bytes with
// the bytes read, a compact member's digest with theirs. One that differs is
// let go and the round goes on with what was read; the first is reported by
// the field the two part at, never by either value. A compact member whose
// digest is the bytes' is rebuilt all the same and held to them: a value
// written is taken in unchecked (014 next9 1, the second step), and one that
// does not rebuild to the bytes it was taken in from is a defect of the
// compact form (writtenDiverged) besides a mismatch.

// verifyCached checks what the cache held of a series against raw, what its
// sampled round read back of it, counting the check and noting the first
// that differs on the pass.
func (store *ExecutionStore) verifyCached(request execution.StatePreflightRequest, item execution.StatePreflightItem, key string,
	raw []byte, cached cachedValue, pass *runtimeLoadPass) {
	kind, field, diverged := "", "", false
	switch {
	case cached.member != nil:
		kind = StateCacheEntryCompact
		field, diverged = compactMismatch(cached.member, item.Identity, raw)
	case cached.entry != nil:
		kind = entryKind(cached.entry)
		if !bytes.Equal(cached.entry.raw, raw) {
			field = bytesMismatch(cached.entry.raw, raw, item.Identity)
		}
	default:
		return
	}
	if field == "" {
		store.cache.verified[StateCacheVerifyMatch].Add(1)
		pass.verified.Matched++
		return
	}
	store.cache.verified[StateCacheVerifyMismatch].Add(1)
	pass.verified.Mismatched++
	if pass.verified.First == nil {
		pass.verified.First = &execution.StateCacheMismatch{Plan: item.Identity.Plan, Kind: kind, Field: field}
	}
	store.cache.dropState(key, request.Contract.Slot.QueryGroup, item.Identity, StateCacheDepartMismatch)
	if diverged {
		store.cache.writtenDiverged(item.Identity, field)
	}
}

// compactMismatch is where a compact member parts from raw, "" for none,
// and whether it parts from the very bytes it was taken in from - its digest
// theirs, its frame rebuilt to others: a defect of the compact form.
func compactMismatch(member *compactMember, identity execution.StateKeyIdentity, raw []byte) (string, bool) {
	if raw == nil {
		return "absent", false
	}
	diverged := sha1.Sum(raw) == member.witness
	if diverged {
		if rebuilt, err := member.frame(identity); err == nil && bytes.Equal(rebuilt, raw) {
			return "", false
		}
	}
	held, err := member.view(identity)
	if err != nil {
		return "bytes", diverged
	}
	stored, err := decodedForCompare(raw, identity)
	if err != nil {
		return "bytes", diverged
	}
	return viewMismatch(held, stored), diverged
}

// bytesMismatch is where cached bytes part from raw, which differ: the
// first field of their views that does, or the bytes alone.
func bytesMismatch(cached, raw []byte, identity execution.StateKeyIdentity) string {
	if raw == nil {
		return "absent"
	}
	held, heldErr := decodedForCompare(cached, identity)
	stored, storedErr := decodedForCompare(raw, identity)
	if heldErr != nil || storedErr != nil {
		return "bytes"
	}
	return viewMismatch(held, stored)
}

// viewMismatch is the name of the first field of a record's view the two
// differ in, "bytes" when they differ in none.
func viewMismatch(held, stored execution.RuntimeStateView) string {
	left, right := reflect.ValueOf(held), reflect.ValueOf(stored)
	for index := 0; index < left.NumField(); index++ {
		field := left.Type().Field(index)
		if !field.IsExported() {
			continue
		}
		if !reflect.DeepEqual(left.Field(index).Interface(), right.Field(index).Interface()) {
			return field.Name
		}
	}
	return "bytes"
}
