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
	"sync"
	"time"
)

// objectRenewalRounds is how many refresh rounds pass between renewals of
// the execution objects and output contexts: every fifth minute at the
// thirty-second refresh. The renewal also finds an object gone - its PEXPIRE
// answers 0 and the catalog forgets it, so the next publication writes it
// again - and it is that finding, not the TTL, that sets the cadence: a day's
// TTL renewed every five minutes is never near running out.
const objectRenewalRounds = 10

// catalogRenewals is what this process renewed of the catalog and when.
//
// A Schedule timeline is renewed when this process last renewed or wrote it
// more than half the TTL ago, or never. What it remembers is a lower bound
// of the time the key has left: every writer and every renewal of any
// process sets the full catalog TTL, and none shortens it, so a process that
// renews by its own memory never lets a key run out. A process that has just
// started remembers nothing and renews everything once.
type catalogRenewals struct {
	mu           sync.Mutex
	timelines    map[string]time.Time
	objectRounds uint64
	clock        func() time.Time
}

func (renewals *catalogRenewals) now() time.Time {
	if renewals.clock != nil {
		return renewals.clock()
	}
	return time.Now()
}

// timelinesDue is the keys of keys this process has not renewed or written
// within half the TTL: first the ones it never has, as many as first says,
// then the ones it did more than half the TTL ago. It forgets the keys no
// longer referenced, so what it remembers is bounded by the Query Groups the
// catalog still schedules.
func (renewals *catalogRenewals) timelinesDue(keys []string, ttl time.Duration) (due []string, first int, now time.Time) {
	renewals.mu.Lock()
	defer renewals.mu.Unlock()
	now = renewals.now()
	referenced := make(map[string]time.Time, len(keys))
	halfLife := make([]string, 0)
	for _, key := range keys {
		last, known := renewals.timelines[key]
		switch {
		case !known:
			due = append(due, key)
		case now.Sub(last) >= ttl/2:
			halfLife = append(halfLife, key)
		}
		if known {
			referenced[key] = last
		}
	}
	renewals.timelines = referenced
	first = len(due)
	return append(due, halfLife...), first, now
}

// renewed notes that keys were renewed or written at the given time. Only
// a write the store acknowledged is noted: a renewal missed leaves a key to
// be renewed early, a failed one noted would leave it to run out.
func (renewals *catalogRenewals) renewed(keys []string, at time.Time) {
	renewals.mu.Lock()
	defer renewals.mu.Unlock()
	if renewals.timelines == nil {
		renewals.timelines = make(map[string]time.Time, len(keys))
	}
	for _, key := range keys {
		renewals.timelines[key] = at
	}
}

// objectRenewalDue says whether this refresh round renews the execution
// objects and output contexts: the first round of the process, and every
// objectRenewalRounds-th after.
func (renewals *catalogRenewals) objectRenewalDue() bool {
	renewals.mu.Lock()
	defer renewals.mu.Unlock()
	due := renewals.objectRounds%objectRenewalRounds == 0
	renewals.objectRounds++
	return due
}

// SetCatalogRenewalClockForTest drives the half-life of the timeline
// renewals.
func SetCatalogRenewalClockForTest(repository *RedisCatalogRepository, now func() time.Time) {
	repository.renewals.mu.Lock()
	repository.renewals.clock = now
	repository.renewals.mu.Unlock()
}
