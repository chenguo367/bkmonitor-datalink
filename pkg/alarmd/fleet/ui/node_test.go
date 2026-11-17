// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package ui

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// nodeRequireEnv set to 1 fails a page test that finds no node rather than
// skipping it: a gate that skipped every page's script reports the same pass
// as one that ran them. Unset or empty requires nothing; any other value
// fails, because the likely misspellings - true, yes - would otherwise skip
// exactly as if it were unset (as redistest.RequireEnv for redis-server).
const nodeRequireEnv = "ALARMD_REQUIRE_NODE"

// pageNode is the node a page test runs the page's script with, the one on
// PATH. Without one the test is skipped, saying why - what this run does
// not execute - or fails when ALARMD_REQUIRE_NODE is 1.
func pageNode(t testing.TB, why string) string {
	t.Helper()
	require := os.Getenv(nodeRequireEnv)
	if require != "" && require != "1" {
		t.Fatalf("%s=%s: want 1 or unset", nodeRequireEnv, require)
		return ""
	}
	node, err := exec.LookPath("node")
	if err == nil {
		return node
	}
	if require == "1" {
		t.Fatalf("node is not on PATH and %s=1, so %s: %v", nodeRequireEnv, why, err)
		return ""
	}
	t.Skip("node is not on PATH, so " + why)
	return ""
}

// nodeRecordingTB keeps what pageNode asked of the test instead of stopping it.
type nodeRecordingTB struct {
	testing.TB
	fatal, skip string
}

func (tb *nodeRecordingTB) Helper() {}

func (tb *nodeRecordingTB) Fatalf(format string, args ...any) {
	tb.fatal = fmt.Sprintf(format, args...)
}

func (tb *nodeRecordingTB) Skip(args ...any) { tb.skip = fmt.Sprint(args...) }

// The node a page test runs: the one on PATH; none is a skip that says what
// is not executed, or a failure when the gate requires node; a requirement
// that is neither 1 nor unset fails, found node or not.
func TestAPageTestRunsTheNodeOnPathAndMissingIsASkipUnlessRequired(t *testing.T) {
	onPath := t.TempDir()
	found := filepath.Join(onPath, "node")
	if err := os.WriteFile(found, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	empty := t.TempDir()
	for _, test := range []struct {
		name, path, require string
		want                string
		fatal, skip         bool
	}{
		{name: "on PATH", path: onPath, want: found},
		{name: "on PATH while required", path: onPath, require: "1", want: found},
		{name: "none", path: empty, skip: true},
		{name: "none while required", path: empty, require: "1", fatal: true},
		{name: "none, required set to something else", path: empty, require: "true", fatal: true},
		{name: "on PATH, required set to something else", path: onPath, require: "yes", fatal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("PATH", test.path)
			t.Setenv(nodeRequireEnv, test.require)
			tb := &nodeRecordingTB{TB: t}
			got := pageNode(tb, "the page's script is not executed")
			if got != test.want || (tb.fatal != "") != test.fatal || (tb.skip != "") != test.skip {
				t.Fatalf("pageNode() = %q, fatal %q, skip %q; want %q, fatal %v, skip %v", got, tb.fatal, tb.skip, test.want, test.fatal, test.skip)
			}
			if test.skip && tb.skip != "node is not on PATH, so the page's script is not executed" {
				t.Fatalf("the skip says %q, not what is not executed", tb.skip)
			}
		})
	}
}
