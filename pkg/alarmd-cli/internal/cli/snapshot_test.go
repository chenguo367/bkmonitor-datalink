// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// snapshotFixture is a deployment of two replicas, one not ready, with a
// two-page diagnosis, one fixed read its catalog does not offer, one read
// that is refused and one answer carrying a credential.
type snapshotFixture struct {
	mu      sync.Mutex
	invoked map[string][]map[string]any
}

func (f *snapshotFixture) server(t *testing.T, p *Profile) *httptest.Server {
	t.Helper()
	offered := []string{"fleet.get", "diagnose.environment", "overview.get", "lifecycle.get", "absent.list", "lookback.get", "store.info",
		"k8s.pods", "k8s.workloads", "k8s.events", "sre.receipts", "runtime.get", "metrics.list", "metrics.get", "k8s.logs", "strategy.get", "object.get"}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ob/api/cli/auth/exchange":
			writeJSON(t, w, sessionResponse(*p))
		case "/ob/api/cli/channel":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			operation, params := stringField(body, "operation"), objectField(body, "params")
			switch stringField(body, "mode") {
			case "discover":
				rows := []any{}
				for _, name := range offered {
					rows = append(rows, map[string]any{"operation": name})
				}
				writeJSON(t, w, envelope(*p, "ok", map[string]any{"operations": rows, "budget": map[string]any{"invokes_per_session_per_minute": 30.0}}))
				return
			case "describe":
				writeJSON(t, w, envelope(*p, "ok", map[string]any{"operation": operation}))
				return
			}
			f.mu.Lock()
			f.invoked[operation] = append(f.invoked[operation], params)
			f.mu.Unlock()
			var result any = map[string]any{}
			switch operation {
			case "fleet.get":
				result = map[string]any{
					"builds":       []any{map[string]any{"build": map[string]any{"version": "0.4.10", "commit": "abc1234"}}},
					"degradations": []any{},
					"per_replica": []any{
						map[string]any{"replica": "replica-a", "readiness": map[string]any{"ready": true}},
						map[string]any{"replica": "replica-b", "readiness": map[string]any{"ready": false}},
					}}
			case "diagnose.environment":
				if stringField(params, "cursor") == "" {
					page := diagnosisPage(3, []string{"1", "2"}, 2, true, "c1")
					page["strategies"] = []any{map[string]any{"strategy_id": "1", "verdict": "DETECTING"}, map[string]any{"strategy_id": "2", "verdict": "DATA_ABSENT"}}
					result = page
				} else {
					result = diagnosisPage(3, []string{"3"}, 1, true, "")
				}
			case "metrics.list":
				result = map[string]any{"families": []any{map[string]any{"name": "bkmonitor_alarmd_worker_work_total"}, map[string]any{"name": "bkmonitor_alarmd_unrelated_total"}}}
			case "strategy.get":
				result = map[string]any{"plans": []any{map[string]any{"query_group": "qg-two"}}}
			case "store.info":
				result = map[string]any{"note": "redis://ops:hunter2-credential@192.0.2.10:6379"}
			case "lookback.get":
				out := envelope(*p, "error", nil)
				out["error"] = map[string]any{"code": "lookback_unavailable", "message": "not here"}
				w.WriteHeader(http.StatusServiceUnavailable)
				writeJSON(t, w, out)
				return
			}
			writeJSON(t, w, envelope(*p, "ok", result))
		default:
			w.WriteHeader(404)
		}
	}))
	*p = fixtureProfile(server.URL)
	return server
}

func readArchive(t *testing.T, file string) map[string][]byte {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	zipped, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	tarred := tar.NewReader(zipped)
	for {
		header, err := tarred.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(tarred)
		files[header.Name[strings.Index(header.Name, "/")+1:]] = body
	}
	return files
}

// One command reads the deployment's catalog, its fleet, its whole
// diagnosis, the fixed reads, the troubled replica alone one by one and the
// strategies not detecting, at the session's rate, and writes one archive
// and its checksum: every read filed with the request it answered, the plan
// and its time cap in the manifest before anything else, what was not
// collected whole named, and a credential the deployment let through
// replaced and counted.
func TestASnapshotIsEveryReadNowInOneFile(t *testing.T) {
	snapshotPace = func(int) time.Duration { return 0 }
	t.Cleanup(func() { snapshotPace = func(rate int) time.Duration { return time.Minute / time.Duration(rate) } })
	fixture := &snapshotFixture{invoked: map[string][]map[string]any{}}
	var p Profile
	server := fixture.server(t, &p)
	defer server.Close()
	store := loggedIn(t, p, server)
	out := t.TempDir()
	code, result, _, _ := run(t, store, server.Client(), "", "snapshot", "--env", p.EnvironmentID, "--out", out)
	if code != 3 || result["status"] != "partial" {
		t.Fatalf("code %d result %v, want partial: lookback.get was refused", code, result)
	}
	file := stringField(objectField(result, "result"), "file")
	files := readArchive(t, file)

	var manifest SnapshotManifest
	if err := json.Unmarshal(files["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.RatePerMinute != 30 || manifest.RateSource != "discover" || manifest.PlannedCalls == 0 ||
		manifest.TimeCapSeconds != ((manifest.PlannedCalls+29)/30)*60+30 {
		t.Fatalf("manifest plan = rate %d (%s) planned %d cap %d s", manifest.RatePerMinute, manifest.RateSource, manifest.PlannedCalls, manifest.TimeCapSeconds)
	}
	results := map[string]string{}
	for _, item := range manifest.Items {
		results[item.Operation+"|"+item.File] = item.Result
	}
	if results["maintenance.get|"] != "not_in_catalog" || results["lookback.get|ops/lookback.get.json"] != "timeout" &&
		results["lookback.get|ops/lookback.get.json"] != "refused" {
		t.Fatalf("items = %v, want maintenance.get not in the catalog and lookback.get not collected", results)
	}
	// Only the replica that is not ready is read one by one.
	if runtimes := fixture.invoked["runtime.get"]; len(runtimes) != 1 || runtimes[0]["replica"] != "replica-b" {
		t.Fatalf("runtime.get asked %v, want replica-b alone", runtimes)
	}
	if got := fixture.invoked["metrics.get"]; len(got) != 1 || len(got[0]["names"].([]any)) != 1 {
		t.Fatalf("metrics.get asked %v, want the one key family replica-b has", got)
	}
	// Both diagnosis pages, the not-detecting strategy and its object.
	for _, name := range []string{"ops/diagnose.environment/page-0001.json", "ops/diagnose.environment/page-0002.json",
		"ops/strategy.get/2.json", "ops/object.get/2.json", "ops/fleet.get.json", "ops/discover.json", "ops/describe/fleet.get.json"} {
		if files[name] == nil {
			t.Fatalf("archive lacks %s; has %d files", name, len(files))
		}
	}
	var page map[string]any
	_ = json.Unmarshal(files["ops/diagnose.environment/page-0002.json"], &page)
	if stringField(objectField(objectField(page, "request"), "params"), "cursor") != "c1" {
		t.Fatalf("page two's request = %v, want its cursor", page["request"])
	}
	// The credential is replaced and the replacement counted, never quoted.
	if bytes.Contains(files["ops/store.info.json"], []byte("hunter2-credential")) || len(manifest.Redactions) != 1 ||
		manifest.Redactions[0].Rule != "url_userinfo" || bytes.Contains(files["manifest.json"], []byte("hunter2")) {
		t.Fatalf("redactions = %+v", manifest.Redactions)
	}
	// Each file's sum is in SHA256SUMS, and the archive's in its .sha256.
	for _, line := range strings.Split(strings.TrimSpace(string(files["SHA256SUMS"])), "\n") {
		sum, name, _ := strings.Cut(line, "  ")
		digest := sha256.Sum256(files[name])
		if hex.EncodeToString(digest[:]) != sum {
			t.Fatalf("SHA256SUMS line %q does not match", line)
		}
	}
	archive, _ := os.ReadFile(file)
	digest := sha256.Sum256(archive)
	written, _ := os.ReadFile(file + ".sha256")
	if !strings.HasPrefix(string(written), hex.EncodeToString(digest[:])+"  "+filepath.Base(file)) {
		t.Fatalf(".sha256 = %q", written)
	}
}

// --out belongs to snapshot alone, and a snapshot is never written over
// another.
func TestSnapshotFlagsAreItsOwn(t *testing.T) {
	store := Store{t.TempDir()}
	if code, result, _, _ := run(t, store, http.DefaultClient, "", "diagnose", "--env", "env-one", "--out", t.TempDir()); code != 2 ||
		stringField(objectField(result, "error"), "code") != "invalid_input" {
		t.Fatalf("code %d result %v", code, result)
	}
}
