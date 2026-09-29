// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/redisfailure"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/storecensus"
)

// storeCensusTimeout bounds one store's census: a thousand draws and a
// thousand weighings, pipelined by the hundred, take milliseconds on a
// store that answers.
const storeCensusTimeout = 30 * time.Second

// censusStore is one store the process writes to, by the client label its
// operations are counted under.
type censusStore struct {
	name   string
	client redis.UniversalClient
}

// storeAt is a store and the connection it was opened with.
type storeAt struct {
	store      censusStore
	connection any
}

// distinctStores is each store once, under the first name it was given: two
// clients opened on one connection are one store, and a census of both would
// report its keys twice. A store with no client is not one this process
// opened.
func distinctStores(candidates ...storeAt) []censusStore {
	var stores []censusStore
	for index, candidate := range candidates {
		if candidate.store.client == nil {
			continue
		}
		distinct := true
		for _, earlier := range candidates[:index] {
			if earlier.store.client != nil && reflect.DeepEqual(earlier.connection, candidate.connection) {
				distinct = false
				break
			}
		}
		if distinct {
			stores = append(stores, candidate.store)
		}
	}
	return stores
}

// storeCensus is the latest census of each store, as the scrape reads it.
type storeCensus struct {
	stores []censusStore
	now    func() time.Time
	last   atomic.Pointer[[]storecensus.Result]
}

// measure takes a census of every store while leading and forgets the last
// one when not. A store whose census fails is left out of this one; the
// failure is counted under the store_census caller of its client.
func (census *storeCensus) measure(ctx context.Context, leading bool) {
	if !leading {
		census.last.Store(nil)
		return
	}
	results := make([]storecensus.Result, 0, len(census.stores))
	for _, store := range census.stores {
		measureCtx, cancel := context.WithTimeout(redisfailure.WithCaller(ctx, redisfailure.CallerStoreCensus), storeCensusTimeout)
		result, err := storecensus.Measure(measureCtx, store.client, store.name, census.now)
		cancel()
		if err == nil {
			results = append(results, result)
		}
	}
	census.last.Store(&results)
}

// read is the latest census of each store, none while not leading.
func (census *storeCensus) read() []storecensus.Result {
	if results := census.last.Load(); results != nil {
		return *results
	}
	return nil
}
