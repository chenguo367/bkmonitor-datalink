// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/testwait"
)

// A lookback read is requested as alarmd (lookback=<kind>), its kind's name
// as the design names it; a Slot's formal read, a recheck that names no
// kind and a kind outside the set are requested under no alarmd agent.
func TestALookbackReadNamesItsKindOnTheRequestAndAFormalReadDoesNot(t *testing.T) {
	var mu sync.Mutex
	var agents []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		agents = append(agents, r.Header.Get("User-Agent"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"series":[],"is_partial":false}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "alarmd", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	attempt := validAttempt(t)
	last := func() string {
		mu.Lock()
		defer mu.Unlock()
		return agents[len(agents)-1]
	}
	if _, err := client.Execute(context.Background(), attempt, &collectingSink{}); err != nil {
		t.Fatal(err)
	}
	if agent := last(); strings.HasPrefix(agent, "alarmd") {
		t.Fatalf("a formal read is requested as %q", agent)
	}
	ctx, cancel := context.WithCancel(testwait.Context(t))
	defer cancel()
	recheck := func(kind execution.LookbackRead) string {
		t.Helper()
		if _, err := client.Recheck(execution.WithLookbackRead(ctx, kind), attempt.Spec, &collectingSink{}); err != nil {
			t.Fatal(err)
		}
		return last()
	}
	for kind, name := range map[execution.LookbackRead]string{
		execution.LookbackReadRecheck: "recheck", execution.LookbackReadDeepRecheck: "deep_recheck",
		execution.LookbackReadProbation: "probation", execution.LookbackReadEarlier: "earlier",
		execution.LookbackReadFloorProbe: "floor_probe", execution.LookbackReadDirected: "directed",
		execution.LookbackReadDirectedEarly: "directed_early",
	} {
		if agent := recheck(kind); agent != "alarmd (lookback="+name+")" {
			t.Errorf("a %s read is requested as %q", name, agent)
		}
	}
	for _, kind := range []execution.LookbackRead{execution.LookbackReadNone, execution.LookbackRead(200)} {
		if agent := recheck(kind); strings.HasPrefix(agent, "alarmd") {
			t.Errorf("a recheck of kind %d is requested as %q, want no alarmd agent", kind, agent)
		}
	}
}
