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
// the field the two part at, never by either value.

// verifyCached checks what the cache held of a series against raw, what its
// sampled round read back of it, counting the check and noting the first
// that differs on the pass.
func (store *ExecutionStore) verifyCached(request execution.StatePreflightRequest, item execution.StatePreflightItem, key string,
	raw []byte, cached cachedValue, pass *runtimeLoadPass) {
	kind, field := "", ""
	switch {
	case cached.member != nil:
		kind, field = StateCacheEntryCompact, compactMismatch(cached.member, item.Identity, raw)
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
}

// compactMismatch is where a compact member parts from raw, "" for none:
// the same bytes are the same view, checked as the member was taken in.
func compactMismatch(member *compactMember, identity execution.StateKeyIdentity, raw []byte) string {
	if raw == nil {
		return "absent"
	}
	if sha1.Sum(raw) == member.witness {
		return ""
	}
	held, err := member.view(identity)
	if err != nil {
		return "bytes"
	}
	stored, err := decodedForCompare(raw, identity)
	if err != nil {
		return "bytes"
	}
	return viewMismatch(held, stored)
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
