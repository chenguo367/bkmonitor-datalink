// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
)

// pooledSnapshot is a replica's snapshot shaped as a cooldown pool makes it:
// demoted objects, each with its strategies and its query cooldown, and
// no-data and anomaly rows beside them.
func pooledSnapshot(replica string, demoted int) Snapshot {
	snapshot := Snapshot{Replica: replica, TakenAt: now, Owned: demoted, Determined: demoted}
	for index := 0; index < demoted; index++ {
		row := anomaly(fmt.Sprintf("query-group-%06d", index))
		row.Kind, row.Replica = "QUERY_TARGET_MISSING", replica
		row.Strategies = []StrategyRef{{StrategyID: fmt.Sprint(100_000 + index), BusinessID: fmt.Sprint(2 + index%40)}}
		row.QueryCooldown = &observability.QueryCooldownFacts{Event: "entered", Until: now.Add(5 * time.Minute),
			LastQueryAt: now.Add(-time.Duration(index%300) * time.Second), Failures: uint32(1 + index%32),
			EnteredAt: now.Add(-9 * 24 * time.Hour), Source: "query_target_missing", LastExitReason: "probe_failed"}
		snapshot.Demoted = append(snapshot.Demoted, row)
		snapshot.OwnedObjects = append(snapshot.OwnedObjects, row.QueryGroup)
		if index%3 == 0 {
			noData := anomaly(fmt.Sprintf("query-group-%06d", index))
			noData.Kind, noData.Replica = "NO_DATA", replica
			snapshot.NoData = append(snapshot.NoData, noData)
		}
	}
	snapshot.TotalDemoted = len(snapshot.Demoted)
	return snapshot
}

// A snapshot is stored compressed, under its own key, behind a header that
// names the codec and its JSON's length; a load reads it back as written,
// and asks the memory line for what it holds decoded - its JSON's length
// times snapshotDecodedCharge, not what it is stored as.
func TestASnapshotIsStoredCompressedAndALoadIsAdmittedByWhatItHoldsDecoded(t *testing.T) {
	client := newFakeRedis()
	store := mustStore(t, client, time.Minute, 0)
	snapshot := pooledSnapshot("pod-a", 200)
	if err := store.Publish(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	stored := client.values[store.compressedSnapshotKey("pod-a")]
	if !strings.HasPrefix(stored, snapshotCodecMagic) || client.values[store.snapshotKey("pod-a")] != "" {
		t.Fatalf("stored %q under the compressed key and %d bytes under the JSON one, want the compressed one alone",
			stored[:min(len(stored), 16)], len(client.values[store.snapshotKey("pod-a")]))
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if length := binary.BigEndian.Uint64([]byte(stored[len(snapshotCodecMagic):snapshotCodecHeader])); length != uint64(len(raw)) ||
		len(stored) >= len(raw)/4 {
		t.Fatalf("the header says %d bytes of JSON and %d are stored, want %d and under a quarter of it", length, len(stored), len(raw))
	}
	var asked []uint64
	store.AdmitLoads(func(bytes uint64) bool {
		asked = append(asked, bytes)
		return true
	})
	loaded, err := store.Load(context.Background(), []string{"pod-a"})
	if err != nil || len(loaded) != 1 {
		t.Fatalf("loaded %d snapshots, error %v", len(loaded), err)
	}
	sameJSON(t, "snapshot", loaded[0], snapshot)
	if !reflect.DeepEqual(asked, []uint64{uint64(len(raw)) * snapshotDecodedCharge}) {
		t.Fatalf("the load asked the memory line for %v, want its JSON's %d bytes times %d", asked, len(raw), snapshotDecodedCharge)
	}
}

// Through a rolling update a replica on an earlier build still writes its
// snapshot as JSON under the key it always did: a load reads each replica
// from the key it was written to, in the order the replicas were named, and
// admits the JSON one by its stored length.
func TestASnapshotAnEarlierBuildWroteIsReadBesideACompressedOne(t *testing.T) {
	client := newFakeRedis()
	store := mustStore(t, client, time.Minute, 0)
	compressed := pooledSnapshot("pod-a", 20)
	if err := store.Publish(context.Background(), compressed); err != nil {
		t.Fatal(err)
	}
	earlier := pooledSnapshot("pod-b", 10)
	encoded, err := json.Marshal(earlier)
	if err != nil {
		t.Fatal(err)
	}
	client.values[store.snapshotKey("pod-b")] = string(encoded)
	raw, _ := json.Marshal(compressed)
	var asked []uint64
	store.AdmitLoads(func(bytes uint64) bool {
		asked = append(asked, bytes)
		return true
	})
	loaded, err := store.Load(context.Background(), []string{"pod-b", "pod-gone", "pod-a"})
	if err != nil || len(loaded) != 2 || loaded[0].Replica != "pod-b" || loaded[1].Replica != "pod-a" {
		t.Fatalf("loaded %d snapshots, error %v, want pod-b's then pod-a's", len(loaded), err)
	}
	sameJSON(t, "the earlier build's snapshot", loaded[0], earlier)
	sameJSON(t, "the compressed snapshot", loaded[1], compressed)
	if want := uint64(len(encoded)+len(raw)) * snapshotDecodedCharge; !reflect.DeepEqual(asked, []uint64{want}) {
		t.Fatalf("the load asked the memory line for %v, want %d", asked, want)
	}
}

// A compressed snapshot that does not inflate to the length its header says
// is no snapshot: the load fails naming the replica, as for one whose JSON
// does not decode.
func TestACompressedSnapshotThatDoesNotMatchItsHeaderFailsItsLoad(t *testing.T) {
	client := newFakeRedis()
	store := mustStore(t, client, time.Minute, 0)
	if err := store.Publish(context.Background(), pooledSnapshot("pod-a", 5)); err != nil {
		t.Fatal(err)
	}
	stored := []byte(client.values[store.compressedSnapshotKey("pod-a")])
	length := binary.BigEndian.Uint64(stored[len(snapshotCodecMagic):snapshotCodecHeader])
	binary.BigEndian.PutUint64(stored[len(snapshotCodecMagic):snapshotCodecHeader], length+1)
	client.values[store.compressedSnapshotKey("pod-a")] = string(stored)
	if loaded, err := store.Load(context.Background(), []string{"pod-a"}); err == nil || !strings.Contains(err.Error(), "decode snapshot for pod-a") {
		t.Fatalf("loaded %d snapshots, error %v, want the load failed naming pod-a", len(loaded), err)
	}
}

// A summarized publish writes the owned list whole only when its set differs
// from the one the store wrote last, and otherwise only extends its life; a
// list that is gone is written whole again, and the JSON snapshot an earlier
// build left is let go. A load takes a snapshot's owned list from the key,
// whatever the key holds.
func TestASummarizedPublishWritesTheOwnedListWholeOnlyWhenItsSetChanges(t *testing.T) {
	_, client := realRedis(t)
	ctx := context.Background()
	store := mustStore(t, client, time.Minute, 0)
	owned := func() string { return client.Get(ctx, store.ownedKey("pod-a")).Val() }
	client.Set(ctx, store.snapshotKey("pod-a"), `{"replica":"pod-a"}`, time.Minute)
	first := summarizedSnapshot("pod-a", 3)
	if _, err := store.PublishSummarized(ctx, first, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if want, _ := json.Marshal(first.OwnedObjects); owned() != string(want) || client.Exists(ctx, store.snapshotKey("pod-a")).Val() != 0 {
		t.Fatalf("owned list %s and %d JSON snapshots, want %s and the earlier build's let go", owned(), client.Exists(ctx,
			store.snapshotKey("pod-a")).Val(), want)
	}
	// A list the same publish would have written whole is kept as it is.
	client.Set(ctx, store.ownedKey("pod-a"), `["kept"]`, 5*time.Second)
	again := first
	again.TakenAt = now.Add(5 * time.Second)
	if _, err := store.PublishSummarized(ctx, again, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if ttl := client.PTTL(ctx, store.ownedKey("pod-a")).Val(); owned() != `["kept"]` || ttl <= 5*time.Second {
		t.Fatalf("owned list %s living %v after a publish of the same set, want it kept and its life the store's", owned(), ttl)
	}
	loaded, err := store.Load(ctx, []string{"pod-a"})
	if err != nil || len(loaded) != 1 || !reflect.DeepEqual(loaded[0].OwnedObjects, []string{"kept"}) {
		t.Fatalf("loaded %+v error %v, want the owned list read from its key", loaded, err)
	}
	changed := summarizedSnapshot("pod-a", 4)
	if _, err := store.PublishSummarized(ctx, changed, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if want, _ := json.Marshal(changed.OwnedObjects); owned() != string(want) {
		t.Fatalf("owned list %s after a publish of another set, want %s", owned(), want)
	}
	client.Del(ctx, store.ownedKey("pod-a"))
	if _, err := store.PublishSummarized(ctx, changed, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if want, _ := json.Marshal(changed.OwnedObjects); owned() != string(want) {
		t.Fatalf("owned list %s after a publish of the same set found it gone, want %s written whole", owned(), want)
	}
}

// BenchmarkSnapshotCodec compresses and inflates a replica's snapshot of
// about 2.2 MB of JSON shaped as a cooldown pool makes it, at the codec's
// level and at gzip's default, for the time each takes. Its rows repeat far
// more than a deployment's do, so the ratio it reports (stored/json) is no
// reading of one: a deployment's own snapshots came to 16 % at level 1
// (snapshotCodecLevel), where these come to a few percent.
func BenchmarkSnapshotCodec(b *testing.B) {
	snapshot := pooledSnapshot("pod-a", 3300)
	raw, err := json.Marshal(snapshot)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("snapshot of %d bytes of JSON", len(raw))
	for _, level := range []int{gzip.BestSpeed, gzip.DefaultCompression} {
		b.Run(fmt.Sprintf("compress-level-%d", level), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			var out bytes.Buffer
			for range b.N {
				out.Reset()
				writer, _ := gzip.NewWriterLevel(&out, level)
				_, _ = writer.Write(raw)
				_ = writer.Close()
			}
			b.ReportMetric(float64(out.Len())/float64(len(raw)), "stored/json")
		})
	}
	stored, err := encodeSnapshotPayload(raw)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encode", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		for range b.N {
			_, _ = encodeSnapshotPayload(raw)
		}
		b.ReportMetric(float64(len(stored))/float64(len(raw)), "stored/json")
	})
	b.Run("decode", func(b *testing.B) {
		b.SetBytes(int64(len(raw)))
		for range b.N {
			_, _ = decodeSnapshotPayload(stored)
		}
	})
}
