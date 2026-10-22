// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package testwait

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// fixedWait is a wait bounded by a duration rather than by what it waits
// for: a sleep, a timer, a context on a constant timeout.
var fixedWait = regexp.MustCompile(`\btime\.(Sleep|After|NewTimer|Tick)\(|context\.With(Timeout|Deadline)\(context\.Background\(\),`)

// justification marks a fixed wait that is meant: something must not happen
// within the duration, or the duration is the behaviour under test.
const justification = "// fixed-wait:"

// No test in this module waits a fixed time for something that should
// happen: on a loaded host the time runs out first and the test fails for
// nothing. Such a wait is on the condition (Until, Expired, Context). A
// fixed wait that is meant - nothing must happen within it, or a slow peer
// is what the test simulates - says why on its line or the line above.
func TestNoTestWaitsAFixedTimeWithoutSayingWhy(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("%s is not the module root: %v", root, err)
	}
	var found []string
	scanned := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); path != root && (name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		scanned++
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		previous := ""
		for line := 1; scanner.Scan(); line++ {
			text := scanner.Text()
			if fixedWait.MatchString(text) && !strings.Contains(text, justification) && !strings.Contains(previous, justification) {
				relative, _ := filepath.Rel(root, path)
				found = append(found, relative+":"+strconv.Itoa(line)+": "+strings.TrimSpace(text))
			}
			previous = text
		}
		return scanner.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("no test files scanned; the walk is not reaching the module")
	}
	if len(found) > 0 {
		t.Fatalf("%d fixed waits without a %q reason - wait on the condition (testwait.Until, Expired, Context) "+
			"or say why the duration is enough:\n%s", len(found), justification, strings.Join(found, "\n"))
	}
}
