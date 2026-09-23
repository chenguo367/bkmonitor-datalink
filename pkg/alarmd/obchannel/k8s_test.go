// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/k8sread"
)

// A read that did not happen reaches the CLI as its own failure code, never
// as a complete empty answer, and every answer says the path's boundary.
func TestK8sFailuresReachTheCLIByName(t *testing.T) {
	dir := t.TempDir()
	reader := k8sread.New(k8sread.Options{PodName: "p", Host: "127.0.0.1", Port: "1",
		TokenPath: filepath.Join(dir, "token"), CAPath: filepath.Join(dir, "ca"), NamespacePath: filepath.Join(dir, "ns")})
	ops := K8sOperations(reader)
	ids := map[string]Operation{}
	for _, op := range ops {
		ids[op.ID] = op
	}
	for _, id := range []string{"k8s.pods", "k8s.events", "k8s.logs"} {
		op, ok := ids[id]
		if !ok {
			t.Fatalf("no %s", id)
		}
		out := op.Run(context.Background(), Params{"pod": "p"})
		if out.Error == nil || out.Error.Code != "k8s_"+k8sread.CodeNoServiceAccount || out.Complete || out.Value != nil {
			t.Errorf("%s without a ServiceAccount: %+v", id, out)
		}
		if len(out.Limitations) == 0 || !strings.Contains(out.Limitations[0], "kubectl") {
			t.Errorf("%s does not say its boundary: %v", id, out.Limitations)
		}
		if op.Targetable {
			t.Errorf("%s is answered by the entry replica, not routed to one", id)
		}
	}
	if got := k8sOutcome(nil, errors.New("plain")); got.Error == nil || got.Error.Code != "k8s_"+k8sread.CodeAPIError {
		t.Errorf("an unnamed error is still a failure: %+v", got)
	}
	if got := k8sOutcome(k8sread.EventsResult{}, nil); !got.Complete || got.Error != nil {
		t.Errorf("a read that happened is complete: %+v", got)
	}
}
