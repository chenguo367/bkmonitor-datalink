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
	"regexp"
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
	// storeInfo replaces store.info's answer; malformed puts non-object
	// elements where objects are expected.
	storeInfo any
	malformed bool
	// lookback is how lookback.get answers: refused (the default), partial,
	// or error, an error status in a 200 answer; catalogWhole offers
	// maintenance.get too.
	lookback     string
	catalogWhole bool
}

func (f *snapshotFixture) server(t *testing.T, p *Profile) *httptest.Server {
	t.Helper()
	offered := []string{"fleet.get", "diagnose.environment", "overview.get", "lifecycle.get", "absent.list", "lookback.get", "store.info",
		"k8s.pods", "k8s.workloads", "k8s.events", "sre.receipts", "runtime.get", "metrics.list", "metrics.get", "k8s.logs", "strategy.get", "object.get"}
	if f.catalogWhole {
		offered = append(offered, "maintenance.get")
	}
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
				if f.malformed {
					rows = append(rows, "not an object", nil)
				}
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
				if f.malformed {
					health := result.(map[string]any)
					health["per_replica"] = append(health["per_replica"].([]any), nil, "replica-c")
					health["degradations"] = []any{7.0}
					health["builds"] = []any{"0.4.10"}
				}
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
				if f.malformed {
					result = map[string]any{"plans": []any{"qg-one", map[string]any{"query_group": "qg-two"}}}
				}
			case "store.info":
				result = map[string]any{"note": "redis://ops:hunter2-credential@192.0.2.10:6379"}
				if f.storeInfo != nil {
					result = f.storeInfo
				}
			case "lookback.get":
				if f.lookback == "partial" {
					// An answer whose evidence is scoped by the operation's
					// standing limitation: the read was collected whole.
					out := envelope(*p, "partial", map[string]any{"coverage": map[string]any{"owned": 1.0}})
					out["evidence"] = map[string]any{"complete": false, "limitations": []any{"Counts are the answering process's alone."}}
					writeJSON(t, w, out)
					return
				}
				if f.lookback == "error" {
					out := envelope(*p, "error", nil)
					out["error"] = map[string]any{"code": "lookback_unavailable", "message": "not here"}
					writeJSON(t, w, out)
					return
				}
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
	fixtureDigest = universeDigest([]string{"1", "2", "3"})
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

type noNetwork struct{ t *testing.T }

func (n noNetwork) RoundTrip(*http.Request) (*http.Response, error) {
	n.t.Error("a read from a snapshot went to the network")
	return nil, io.ErrUnexpectedEOF
}

// A snapshot is read with the commands the deployment is read with, in the
// same shapes and from the file alone: what it holds is answered with
// meta.snapshot, what it does not is not_in_snapshot with the params it was
// read with, and the diagnosis is proved over its stored pages as diagnose
// proves it live.
func TestASnapshotIsReadWithTheSameCommands(t *testing.T) {
	snapshotPace = func(int) time.Duration { return 0 }
	t.Cleanup(func() { snapshotPace = func(rate int) time.Duration { return time.Minute / time.Duration(rate) } })
	fixtureDigest = universeDigest([]string{"1", "2", "3"})
	fixture := &snapshotFixture{invoked: map[string][]map[string]any{}}
	var p Profile
	server := fixture.server(t, &p)
	store := loggedIn(t, p, server)
	_, written, _, _ := run(t, store, server.Client(), "", "snapshot", "--env", p.EnvironmentID, "--out", t.TempDir())
	server.Close()
	file := stringField(objectField(written, "result"), "file")
	offline := &http.Client{Transport: noNetwork{t}}
	reader := Store{t.TempDir()}

	code, result, _, _ := run(t, reader, offline, "", "discover", "--snapshot", file)
	if code != 0 || stringField(objectField(objectField(result, "meta"), "snapshot"), "file") != file {
		t.Fatalf("discover: code %d meta %v", code, result["meta"])
	}
	if code, result, _, _ = run(t, reader, offline, "", "invoke", "strategy.get", "--snapshot", file, "--input", `{"strategy_id":"2"}`); code != 0 ||
		result["status"] != "ok" {
		t.Fatalf("held read: code %d %v", code, result)
	}
	code, result, _, _ = run(t, reader, offline, "", "invoke", "strategy.get", "--snapshot", file, "--input", `{"strategy_id":"9"}`)
	asked, _ := objectField(result, "result")["asked_in_snapshot"].([]any)
	if code != 1 || stringField(objectField(result, "error"), "code") != "not_in_snapshot" || len(asked) != 1 ||
		stringField(asked[0].(map[string]any), "strategy_id") != "2" {
		t.Fatalf("read not held: code %d %v", code, result)
	}
	if code, _, _, _ = run(t, reader, offline, "", "describe", "fleet.get", "--snapshot", file); code != 0 {
		t.Fatalf("describe: code %d", code)
	}
	code, result, _, _ = run(t, reader, offline, "", "diagnose", "--snapshot", file)
	coverage := objectField(objectField(savedResult(t, result), "result"), "coverage")
	if code != 0 || coverage["holds"] != true {
		t.Fatalf("diagnose: code %d coverage %v", code, coverage)
	}
	if code, result, _, _ = run(t, reader, offline, "", "discover", "--snapshot", file, "--env", "another-env"); code != 2 {
		t.Fatalf("another environment: code %d %v", code, result)
	}
	// A command that cannot read a snapshot is refused before any file is
	// opened.
	if code, result, _, _ = run(t, reader, offline, "", "accept", "--snapshot", filepath.Join(t.TempDir(), "absent.tar.gz"), "--env", p.EnvironmentID); code != 2 ||
		stringField(objectField(result, "error"), "code") != "invalid_input" {
		t.Fatalf("accept from a snapshot: code %d %v", code, result)
	}
}

// A snapshot is never written over another: one whose file already exists
// is refused and the existing file is left as it was.
func TestASnapshotIsNeverWrittenOverAnother(t *testing.T) {
	snapshotPace = func(int) time.Duration { return 0 }
	at := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	snapshotClock = func() time.Time { return at }
	t.Cleanup(func() {
		snapshotPace = func(rate int) time.Duration { return time.Minute / time.Duration(rate) }
		snapshotClock = time.Now
	})
	fixtureDigest = universeDigest([]string{"1", "2", "3"})
	fixture := &snapshotFixture{invoked: map[string][]map[string]any{}}
	var p Profile
	server := fixture.server(t, &p)
	defer server.Close()
	store := loggedIn(t, p, server)
	out := t.TempDir()
	existing := filepath.Join(out, "alarmd-snapshot-"+p.EnvironmentID+"-20261014T030000Z.tar.gz")
	if err := os.WriteFile(existing, []byte("earlier"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, result, _, _ := run(t, store, server.Client(), "", "snapshot", "--env", p.EnvironmentID, "--out", out)
	kept, _ := os.ReadFile(existing)
	if code != 1 || stringField(objectField(result, "error"), "code") != "snapshot_failed" || string(kept) != "earlier" {
		t.Fatalf("code %d result %v kept %q", code, result, kept)
	}
}

func takeSnapshot(t *testing.T, fixture *snapshotFixture) (string, map[string]any, int) {
	t.Helper()
	snapshotPace = func(int) time.Duration { return 0 }
	t.Cleanup(func() { snapshotPace = func(rate int) time.Duration { return time.Minute / time.Duration(rate) } })
	fixtureDigest = universeDigest([]string{"1", "2", "3"})
	var p Profile
	server := fixture.server(t, &p)
	t.Cleanup(server.Close)
	store := loggedIn(t, p, server)
	code, result, _, _ := run(t, store, server.Client(), "", "snapshot", "--env", p.EnvironmentID, "--out", t.TempDir())
	return stringField(objectField(result, "result"), "file"), result, code
}

func manifestOf(t *testing.T, file string) SnapshotManifest {
	t.Helper()
	var manifest SnapshotManifest
	if err := json.Unmarshal(readArchive(t, file)["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func itemOf(manifest SnapshotManifest, operation string) SnapshotItem {
	for _, item := range manifest.Items {
		if item.Operation == operation {
			return item
		}
	}
	return SnapshotItem{}
}

// An answer that would take the snapshot past its byte cap is not kept: the
// read is recorded as skipped by the bound, and the archive is one its own
// reader opens.
func TestAnAnswerPastTheByteCapIsSkippedAndTheSnapshotStaysReadable(t *testing.T) {
	saved := snapshotMaxBytes
	snapshotMaxBytes = 64 << 10
	t.Cleanup(func() { snapshotMaxBytes = saved })
	file, _, code := takeSnapshot(t, &snapshotFixture{invoked: map[string][]map[string]any{}, storeInfo: map[string]any{"note": strings.Repeat("x", 80<<10)}})
	if code != 3 {
		t.Fatalf("code %d, want 3: a read was skipped", code)
	}
	if item := itemOf(manifestOf(t, file), "store.info"); item.Result != "skipped_by_bound" || item.Reason != "byte cap reached" {
		t.Fatalf("store.info item = %+v", item)
	}
	reader := Store{t.TempDir()}
	if code, result, _, _ := run(t, reader, &http.Client{Transport: noNetwork{t}}, "", "discover", "--snapshot", file); code != 0 {
		t.Fatalf("the snapshot does not open offline: code %d %v", code, result)
	}
}

// One element of the wrong shape, in the catalog, the fleet, the
// diagnosis or a strategy, is skipped; the run goes on and writes its file.
func TestAMalformedElementIsSkippedNotFatal(t *testing.T) {
	file, result, code := takeSnapshot(t, &snapshotFixture{invoked: map[string][]map[string]any{}, malformed: true})
	if code != 3 || file == "" {
		t.Fatalf("code %d result %v, want a written partial snapshot", code, result)
	}
	if manifest := manifestOf(t, file); len(manifest.Builds) != 0 || itemOf(manifest, "object.get").Result != "ok" {
		t.Fatalf("builds %v object.get %+v", manifest.Builds, itemOf(manifest, "object.get"))
	}
}

// A private key with no END line is redacted to the end of its string, not
// of the file: the answer stays readable JSON and reads back offline.
func TestAPrivateKeyWithoutItsEndIsRedactedWithinItsString(t *testing.T) {
	file, _, _ := takeSnapshot(t, &snapshotFixture{invoked: map[string][]map[string]any{},
		storeInfo: map[string]any{"key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANkey-material", "after": "kept"}})
	data := readArchive(t, file)["ops/store.info.json"]
	if !json.Valid(data) || bytes.Contains(data, []byte("key-material")) || !bytes.Contains(data, []byte(`"after": "kept"`)) {
		t.Fatalf("store.info = %s", data)
	}
	if item := itemOf(manifestOf(t, file), "store.info"); item.Result != "ok" {
		t.Fatalf("store.info item = %+v", item)
	}
}

// Should the second pass ever leave an answer unreadable, the answer is kept
// as a placeholder and counted as not whole; and a reader meeting a file
// that does not parse names it rather than leaving it out.
func TestABrokenAnswerIsAPlaceholderAndAnUnreadableFileIsNamed(t *testing.T) {
	saved := snapshotRules
	snapshotRules = append(append(snapshotRules[:0:0], snapshotRules...), struct {
		name    string
		pattern *regexp.Regexp
		keep    string
	}{"breaks_json", regexp.MustCompile(`"note"`), `"note`})
	t.Cleanup(func() { snapshotRules = saved })
	file, _, code := takeSnapshot(t, &snapshotFixture{invoked: map[string][]map[string]any{}, storeInfo: map[string]any{"note": "plain"}})
	if code != 3 || itemOf(manifestOf(t, file), "store.info").Result != "redaction_broke_json" {
		t.Fatalf("code %d store.info %+v", code, itemOf(manifestOf(t, file), "store.info"))
	}
	// An archive holding an ops file that does not parse.
	var buffer bytes.Buffer
	zipped := gzip.NewWriter(&buffer)
	tarred := tar.NewWriter(zipped)
	for name, body := range map[string]string{"s/manifest.json": `{"format":"` + snapshotFormat + `","environment_id":"env-one"}`, "s/ops/fleet.get.json": `{"status":`} {
		_ = tarred.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(body))})
		_, _ = tarred.Write([]byte(body))
	}
	_ = tarred.Close()
	_ = zipped.Close()
	broken := filepath.Join(t.TempDir(), "broken.tar.gz")
	_ = os.WriteFile(broken, buffer.Bytes(), 0o600)
	code, result, _, _ := run(t, Store{t.TempDir()}, &http.Client{Transport: noNetwork{t}}, "", "discover", "--snapshot", broken)
	if code != 1 || !strings.Contains(stringField(objectField(result, "error"), "message"), "ops/fleet.get.json") {
		t.Fatalf("code %d result %v, want the unreadable file named", code, result)
	}
}

// A read the deployment answered is collected, whatever the answer says of
// its own evidence: an operation whose standing limitation marks every
// answer partial is filed with that status and reason, and the snapshot is
// whole. Only a read that was not collected makes it incomplete.
func TestAPartialAnswerIsCollectedAndLeavesTheSnapshotWhole(t *testing.T) {
	snapshotPace = func(int) time.Duration { return 0 }
	t.Cleanup(func() { snapshotPace = func(rate int) time.Duration { return time.Minute / time.Duration(rate) } })
	fixtureDigest = universeDigest([]string{"1", "2", "3"})
	fixture := &snapshotFixture{invoked: map[string][]map[string]any{}, lookback: "partial", catalogWhole: true}
	var p Profile
	server := fixture.server(t, &p)
	defer server.Close()
	store := loggedIn(t, p, server)
	code, result, _, _ := run(t, store, server.Client(), "", "snapshot", "--env", p.EnvironmentID, "--out", t.TempDir())
	if code != 0 || result["status"] != "ok" {
		t.Fatalf("code %d result %v, want a whole snapshot: every read was collected", code, result)
	}
	files := readArchive(t, stringField(objectField(result, "result"), "file"))
	var manifest SnapshotManifest
	if err := json.Unmarshal(files["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	for _, item := range manifest.Items {
		if item.Operation == "lookback.get" && (item.Result != "partial" || item.Reason != "Counts are the answering process's alone.") {
			t.Fatalf("lookback.get item = %+v, want it filed partial with its reason", item)
		}
	}
}

// An operation the deployment's catalog does not offer was not collected:
// the snapshot is written and says so with exit 3, naming it.
func TestAReadTheCatalogLacksLeavesTheSnapshotIncomplete(t *testing.T) {
	snapshotPace = func(int) time.Duration { return 0 }
	t.Cleanup(func() { snapshotPace = func(rate int) time.Duration { return time.Minute / time.Duration(rate) } })
	fixtureDigest = universeDigest([]string{"1", "2", "3"})
	fixture := &snapshotFixture{invoked: map[string][]map[string]any{}, lookback: "partial"}
	var p Profile
	server := fixture.server(t, &p)
	defer server.Close()
	store := loggedIn(t, p, server)
	code, result, _, _ := run(t, store, server.Client(), "", "snapshot", "--env", p.EnvironmentID, "--out", t.TempDir())
	notWhole, _ := objectField(result, "result")["not_whole"].([]any)
	if code != 3 || result["status"] != "partial" || len(notWhole) != 1 || notWhole[0] != "maintenance.get" {
		t.Fatalf("code %d result %v, want exit 3 naming maintenance.get alone", code, result)
	}
}

// An answer with an error status was not collected, whatever its HTTP
// status: it is filed as refused with its code, and the snapshot is
// incomplete.
func TestAnErrorAnswerIsNotCollectedEvenWithASuccessfulStatus(t *testing.T) {
	snapshotPace = func(int) time.Duration { return 0 }
	t.Cleanup(func() { snapshotPace = func(rate int) time.Duration { return time.Minute / time.Duration(rate) } })
	fixtureDigest = universeDigest([]string{"1", "2", "3"})
	fixture := &snapshotFixture{invoked: map[string][]map[string]any{}, lookback: "error", catalogWhole: true}
	var p Profile
	server := fixture.server(t, &p)
	defer server.Close()
	store := loggedIn(t, p, server)
	code, result, _, _ := run(t, store, server.Client(), "", "snapshot", "--env", p.EnvironmentID, "--out", t.TempDir())
	if code != 3 {
		t.Fatalf("code %d result %v, want 3: lookback.get answered with an error", code, result)
	}
	files := readArchive(t, stringField(objectField(result, "result"), "file"))
	var manifest SnapshotManifest
	if err := json.Unmarshal(files["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	for _, item := range manifest.Items {
		if item.Operation == "lookback.get" && (item.Result != "refused" || item.Reason != "lookback_unavailable") {
			t.Fatalf("lookback.get item = %+v, want refused with its code", item)
		}
	}
}
