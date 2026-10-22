// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package obchannel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/cliauth"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/internal/redistest"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/pod"
)

var changesBase = time.Date(2026, 10, 22, 2, 0, 0, 0, time.UTC)

func runChanges(t *testing.T, options ChangesOptions, params Params) (ChangesAnswer, Outcome) {
	t.Helper()
	if options.Now == nil {
		options.Now = func() time.Time { return changesBase.Add(4 * time.Hour) }
	}
	out := ChangesOperation(options).Run(context.Background(), params)
	if out.Error != nil {
		return ChangesAnswer{}, out
	}
	answer, ok := out.Value.(ChangesAnswer)
	if !ok {
		t.Fatalf("value %T", out.Value)
	}
	return answer, out
}

func lifecycleOf(events ...LifecycleEvent) func(context.Context) (LifecycleRead, error) {
	return func(context.Context) (LifecycleRead, error) {
		return LifecycleRead{Events: events, Cap: 64}, nil
	}
}

// What changed through the channel are rows - an image set is a release, a
// revert a rollback, an object written a write - with the replicas' starts
// and stops; a dry run and a read are not changes, and a command is counted
// by the hour rather than listed, node.exec once however many receipts it
// left. Each row says who by name, or that it was the deployment
// administrator, or that no one is known.
func TestChangesAreTheChannelsChangesAndTheReplicasStartsAndStops(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, "test.sre:{env}:receipts", nil, nil)
	at := func(minutes int) time.Time { return changesBase.Add(time.Duration(minutes) * time.Minute) }
	for _, action := range []SREAction{
		{Op: OpSetImage, Kind: "Deployment", Namespace: "blueking", Name: "uq", Result: SREResultDone, Operator: "alice", IssuedVia: cliauth.IssuedViaFront, At: at(10), SessionID: "s-1", RequestID: "r-1", Detail: "image x"},
		{Op: OpRevert, Kind: "Deployment", Namespace: "blueking", Name: "uq", Result: SREResultDone, IssuedVia: cliauth.IssuedViaAdminKey, At: at(20)},
		{Op: pod.OpEditConfigMapText, Kind: "ConfigMap", Namespace: "monitoring", Name: "alarmd", Result: SREResultFailed, At: at(30)},
		{Op: pod.OpDryRun, Kind: "Deployment", Namespace: "monitoring", Name: "alarmd", Result: SREResultDone, At: at(31)},
		{Op: OpNodeExec, Kind: "Node", Name: "master-1", ExecutionID: "e-1", At: at(35)},
		{Op: OpNodeEnded, Kind: "Node", Name: "master-1", ExecutionID: "e-1", At: at(36)},
		{Op: opPodExec, Kind: "Pod", Namespace: "monitoring", Name: "web-0/web", At: at(40)},
		{Op: opPodExec, Kind: "Pod", Namespace: "monitoring", Name: "web-0/web", At: at(70)},
		{Op: pod.OpReadSecret, Kind: "Secret", Namespace: "monitoring", Name: "s", At: at(41)},
	} {
		receipts.Record(action)
	}
	answer, out := runChanges(t, ChangesOptions{Receipts: receipts, Lifecycle: lifecycleOf(
		LifecycleEvent{Replica: "alarmd-1", Event: "stop", At: at(50), Build: "0.4.1", Reason: "signal"},
		LifecycleEvent{Replica: "alarmd-1", Event: "start", At: at(55), Build: "0.4.2"},
	)}, Params{"since": changesBase.Format(time.RFC3339), "until": at(120).Format(time.RFC3339)})
	if out.Error != nil || !out.Complete {
		t.Fatalf("outcome %+v", out)
	}
	var got []string
	for _, row := range answer.Rows {
		who := row.OperatorKind
		if row.Operator != "" {
			who += ":" + row.Operator
		}
		got = append(got, fmt.Sprintf("%s %s %s %s", row.At.Format("15:04"), row.Kind, row.Op+row.Replica, who))
	}
	want := []string{
		"02:55 start alarmd-1 ", "02:50 stop alarmd-1 ",
		"02:30 write configmap.edit_text unknown", "02:20 rollback workload.revert deployment_admin", "02:10 release workload.set_image user:alice",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("rows\n %s\nwant\n %s", strings.Join(got, "\n "), strings.Join(want, "\n "))
	}
	if answer.Rows[1].StopReason != "signal" || answer.Rows[0].Build != "0.4.2" || answer.Rows[4].Object.Name != "uq" || answer.Rows[4].Result != SREResultDone {
		t.Fatalf("row facts %+v", answer.Rows)
	}
	buckets, _ := json.Marshal(answer.Executions)
	if string(buckets) != `[{"start":"2026-10-22T03:00:00Z","node_exec":0,"pod_exec":1},{"start":"2026-10-22T02:00:00Z","node_exec":1,"pod_exec":1}]` || answer.ExecutionsReadBy != OpSREReceipts {
		t.Fatalf("executions %s by %q", buckets, answer.ExecutionsReadBy)
	}
	wire, _ := json.Marshal(answer)
	for _, private := range []string{"s-1", "r-1", "image x", "session", "request_id", "detail", "web-0"} {
		if strings.Contains(string(wire), private) {
			t.Fatalf("the answer carries %q: %s", private, wire)
		}
	}
}

// The answer says how far back each source reaches - its oldest entry, how
// many it keeps against its cap, whether it is full - and the time from
// which every read source still holds what it held.
func TestChangesSayHowFarBackTheyReach(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, "test.sre:{env}:receipts", nil, nil)
	for index := 0; index < MaxSREChangeReceipts+3; index++ {
		receipts.Record(SREAction{Op: pod.OpDeletePod, Kind: "Pod", Name: fmt.Sprint(index), At: changesBase.Add(time.Duration(index) * time.Second)})
	}
	receipts.Record(SREAction{Op: opPodExec, Kind: "Pod", Name: "p/c", At: changesBase.Add(-time.Hour)})
	answer, _ := runChanges(t, ChangesOptions{Receipts: receipts, Lifecycle: lifecycleOf(
		LifecycleEvent{Replica: "a", Event: "start", At: changesBase.Add(-2 * time.Hour)},
	)}, Params{"limit": json.Number("1")})
	changes := answer.Coverage.Changes
	if !changes.Read || !changes.Full || changes.Kept != MaxSREChangeReceipts || changes.Cap != MaxSREChangeReceipts ||
		!changes.OldestAt.Equal(changesBase.Add(3*time.Second)) || changes.RetentionSeconds != int64(SREReceiptRetention/time.Second) {
		t.Fatalf("change reach %+v", changes)
	}
	if executions := answer.Coverage.Executions; !executions.Read || executions.Full || executions.Kept != 1 || executions.Cap != MaxSREAccessReceipts {
		t.Fatalf("execution reach %+v", executions)
	}
	if lifecycle := answer.Coverage.Lifecycle; !lifecycle.Read || lifecycle.Cap != 64 || lifecycle.Full || !lifecycle.OldestAt.Equal(changesBase.Add(-2*time.Hour)) {
		t.Fatalf("lifecycle reach %+v", lifecycle)
	}
	if earliest := answer.Coverage.EarliestReadableAt; earliest == nil || !earliest.Equal(changesBase.Add(3*time.Second)) {
		t.Fatalf("earliest readable %v, want the full change list's oldest", earliest)
	}
	// The lifecycle start is in the default window too.
	if answer.Matched != MaxSREChangeReceipts+1 || len(answer.Rows) != 1 || answer.NextCursor == "" {
		t.Fatalf("matched %d rows %d cursor %q", answer.Matched, len(answer.Rows), answer.NextCursor)
	}
}

// A source that cannot be read is named as not read, and the answer is not
// complete: its rows or counts are missing, never zero.
func TestAChangesSourceThatDoesNotAnswerIsNamed(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, "test.sre:{env}:receipts", nil, nil)
	_ = client.Close()
	answer, out := runChanges(t, ChangesOptions{Receipts: receipts, Lifecycle: func(context.Context) (LifecycleRead, error) {
		return LifecycleRead{}, errors.New("down")
	}}, Params{})
	if out.Error != nil || out.Complete {
		t.Fatalf("outcome %+v", out)
	}
	coverage := answer.Coverage
	if coverage.Changes.Read || coverage.Changes.Reason != "store_unavailable" || coverage.Executions.Read || coverage.Lifecycle.Read ||
		coverage.Lifecycle.Reason != "store_unavailable" || coverage.EarliestReadableAt != nil || len(out.Limitations) != 3 {
		t.Fatalf("coverage %+v limitations %v", coverage, out.Limitations)
	}
	answer, out = runChanges(t, ChangesOptions{}, Params{})
	if out.Complete || answer.Coverage.Changes.Reason != "receipts_unavailable" || answer.Coverage.Lifecycle.Reason != "lifecycle_unavailable" {
		t.Fatalf("no sources: %+v", answer.Coverage)
	}
}

// Pages follow one another by time: every row once, in order, even when
// several share an instant and a newer change is written between pages.
func TestChangesPageByTimeWithoutRepeatOrGap(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, "test.sre:{env}:receipts", nil, nil)
	same := changesBase.Add(time.Hour)
	for index := 0; index < 5; index++ {
		receipts.Record(SREAction{Op: pod.OpDeletePod, Kind: "Pod", Namespace: "n", Name: fmt.Sprintf("p%d", index), At: same})
	}
	receipts.Record(SREAction{Op: pod.OpDeletePod, Kind: "Pod", Namespace: "n", Name: "older", At: same.Add(-time.Minute)})
	until := changesBase.Add(4 * time.Hour)
	params := Params{"limit": json.Number("2"), "since": changesBase.Format(time.RFC3339), "until": until.Format(time.RFC3339)}
	var seen []string
	for page := 0; page < 10; page++ {
		answer, out := runChanges(t, ChangesOptions{Receipts: receipts}, params)
		if out.Error != nil {
			t.Fatal(out.Error)
		}
		for _, row := range answer.Rows {
			seen = append(seen, row.Object.Name)
		}
		if page == 0 {
			receipts.Record(SREAction{Op: pod.OpDeletePod, Kind: "Pod", Namespace: "n", Name: "newer", At: same.Add(time.Minute)})
		}
		if answer.NextCursor == "" {
			break
		}
		params = Params{"limit": json.Number("2"), "cursor": answer.NextCursor, "since": changesBase.Format(time.RFC3339), "until": until.Format(time.RFC3339)}
	}
	if got := strings.Join(seen, ","); got != "p0,p1,p2,p3,p4,older" {
		t.Fatalf("paged %s", got)
	}
}

// The range is checked, and only what falls in it is read.
func TestChangesReadTheRangeAsked(t *testing.T) {
	client := redistest.Start(t).Client()
	receipts := NewSREReceipts(client, "test.sre:{env}:receipts", nil, nil)
	receipts.Record(SREAction{Op: pod.OpDeletePod, Kind: "Pod", Name: "in", At: changesBase.Add(30 * time.Minute)})
	receipts.Record(SREAction{Op: pod.OpDeletePod, Kind: "Pod", Name: "out", At: changesBase.Add(2 * time.Hour)})
	answer, _ := runChanges(t, ChangesOptions{Receipts: receipts}, Params{"since": changesBase.Format(time.RFC3339), "until": changesBase.Add(time.Hour).Format(time.RFC3339)})
	if len(answer.Rows) != 1 || answer.Rows[0].Object.Name != "in" {
		t.Fatalf("rows %+v", answer.Rows)
	}
	answer, _ = runChanges(t, ChangesOptions{Receipts: receipts, Now: func() time.Time { return changesBase.Add(25 * time.Hour) }}, Params{})
	if !answer.Since.Equal(changesBase.Add(time.Hour)) || len(answer.Rows) != 1 || answer.Rows[0].Object.Name != "out" {
		t.Fatalf("default window %v rows %+v", answer.Since, answer.Rows)
	}
	for name, params := range map[string]Params{
		"reversed":   {"since": changesBase.Add(time.Hour).Format(time.RFC3339), "until": changesBase.Format(time.RFC3339)},
		"too long":   {"since": changesBase.Add(-8 * 24 * time.Hour).Format(time.RFC3339), "until": changesBase.Format(time.RFC3339)},
		"not a time": {"since": "yesterday"},
		"bad cursor": {"cursor": "!!"},
	} {
		if _, out := runChanges(t, ChangesOptions{Receipts: receipts}, params); out.Error == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The admin page reads changes.list through the front surface.
func TestTheChangesPageIsAFrontRead(t *testing.T) {
	if !FrontReads[OpChangesList] {
		t.Fatal("changes.list is not a front read")
	}
}
