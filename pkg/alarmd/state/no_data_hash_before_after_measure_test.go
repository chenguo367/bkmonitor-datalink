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
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// beforeAfterGroups is n groups of one of two strategy shapes: a host-level
// one (two dimensions) or a disk-level one (four, one a long mount path, about
// three times the host key), present in the round the header names or absent.
func beforeAfterGroups(n int, wide, present bool) []execution.NoDataGroupMemory {
	groups := make([]execution.NoDataGroupMemory, n)
	for index := range groups {
		ip := fmt.Sprintf("192.0.%d.%d", index/256%256, index%256)
		key := "bk_target_cloud_id=0,bk_target_ip=" + ip + "," + contract.NoDataDimensionTag + "=true"
		if wide {
			key = "bk_target_cloud_id=0,bk_target_ip=" + ip + ",device_name=/dev/mapper/vg_data-lv_data_" +
				strconv.Itoa(index) + ",mount_point=/data/containers/overlay2/volumes/app-" + strconv.Itoa(index) +
				"/merged," + contract.NoDataDimensionTag + "=true"
		}
		groups[index] = execution.NoDataGroupMemory{GroupKey: key, LastSeen: noDataPresentAsOf}
		if !present {
			groups[index] = execution.NoDataGroupMemory{GroupKey: key, LastSeen: 900, FirstAbsent: 940}
		}
	}
	return groups
}

// TestMeasureNoDataHashAgainstTheWholeMemoryRecord compares one high-cardinality
// memory stored both ways on a real server: as the single-value record a build
// before the per-group representation wrote, and as the per-group record this
// build writes through its own store.
//
// It is a measurement and runs only with ALARMD_MEASURE_NO_DATA_CAPACITY=1;
// without it the test says so and skips. Run it once per Redis major
// (ALARMD_REDIS_SERVER). It logs the single-value record's bytes and MEMORY
// USAGE beside the per-group record's HGETALL bytes and MEMORY USAGE, and
// judges nothing. Decision-008 section 7.2 (b) expected the per-group record
// at most a third of the single-value record's bytes; the group keys are in
// both and are most of either, so it is not, and the section now carries
// these readings in place of that bound.
func TestMeasureNoDataHashAgainstTheWholeMemoryRecord(t *testing.T) {
	if os.Getenv(measureCapacityEnv) != "1" {
		t.Skipf("a measurement; set %s=1 to run it (once per Redis major)", measureCapacityEnv)
	}
	for _, groups := range []int{3200, 10000} {
		for _, wide := range []bool{false, true} {
			for _, present := range []bool{true, false} {
				shape := "host"
				if wide {
					shape = "disk"
				}
				presence := "absent"
				if present {
					presence = "present"
				}
				t.Run(fmt.Sprintf("%d %s groups %s", groups, shape, presence), func(t *testing.T) {
					server := startNoDataServer(t)
					ctx := context.Background()
					memory := beforeAfterGroups(groups, wide, present)
					mutation := firstNoDataWrite(t, 0, memory...)

					before, err := json.Marshal(noDataEnvelope{
						Schema: executionNoDataSchema, Version: execution.NoDataMemorySchemaV1,
						Identity: noDataIdentityV2(), MarkerRevision: 41, ApplyVersion: mutation.ApplyVersion,
						MutationDigest: mutation.MemoryDigest, ScheduleRevision: mutation.ScheduleRevision,
						RosterVersion: mutation.RosterVersion, Groups: memory,
					})
					if err != nil {
						t.Fatal(err)
					}
					blobKey, err := PlanNoDataKeyV2("alarmd", noDataIdentityV2())
					if err != nil {
						t.Fatal(err)
					}
					if err := server.client.Set(ctx, blobKey, before, time.Hour).Err(); err != nil {
						t.Fatal(err)
					}
					if got := storeNoData(t, server.store(t), mutation); got.Status != execution.NoDataApplied {
						t.Fatalf("write = %+v", got)
					}
					after := 0
					for name, value := range server.record(t) {
						after += len(name) + len(value)
					}
					blobUsage := server.client.MemoryUsage(ctx, blobKey, 0).Val()
					hashUsage := server.client.MemoryUsage(ctx, noDataHashKey(t), 0).Val()
					version := strings.TrimSpace(firstLines(server.client.Info(ctx, "server").Val(), "redis_version"))
					t.Logf("%s before %d bytes (MEMORY USAGE %d), after HGETALL %d bytes (MEMORY USAGE %d): "+
						"bytes %.2f, memory %.2f", version, len(before), blobUsage, after, hashUsage,
						float64(after)/float64(len(before)), float64(hashUsage)/float64(blobUsage))
				})
			}
		}
	}
}
