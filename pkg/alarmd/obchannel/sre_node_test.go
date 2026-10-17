package obchannel

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/node"
)

type fakeNodes struct {
	request node.ExecRequest
	result  node.Result
	err     error
}

func (f *fakeNodes) Exec(_ context.Context, r node.ExecRequest) (node.Result, error) {
	f.request = r
	return f.result, f.err
}
func (f *fakeNodes) Get(context.Context, node.GetRequest) (node.Result, error) {
	return f.result, f.err
}
func (f *fakeNodes) Cancel(context.Context, node.CancelRequest) (node.Result, error) {
	return f.result, f.err
}

func TestNodeSchemaAndRecoveryContract(t *testing.T) {
	f := &fakeNodes{result: node.Result{Record: node.Record{ExecutionID: "id", State: "running"}, Runner: json.RawMessage(`{"next_cursor":"page-2"}`)}}
	var actions []SREAction
	ops := nodeOperations(SREOptions{Nodes: f}, func(_ context.Context, _ time.Time, a SREAction, _ error) { actions = append(actions, a) })
	p := Params{"execution_id": "0199ed00-0000-7000-8000-000000000001", "mode": "host", "node": map[string]any{"name": "node-a", "uid": "uid-a"}, "argv": []any{"/bin/sh"}, "stdin": "private-script"}
	if err := validate(ops[0], p); err == nil {
		t.Fatal("host context missing but accepted")
	}
	p["cwd"] = "/srv"
	p["user"] = "root"
	if err := validate(ops[0], p); err != nil {
		t.Fatal(err)
	}
	out := ops[0].Run(WithCaller(context.Background(), Caller{Operator: "operator-a"}), p)
	if out.Error != nil || f.request.Caller.Operator != "operator-a" || f.request.Stdin != "private-script" || out.Next[0].Operation != OpNodeGet || out.Next[0].Params["output_cursor"] != "page-2" {
		t.Fatalf("lost node contract: %+v %+v", out, f.request)
	}
	if len(actions) != 1 || actions[0].ExecutionID != p["execution_id"] || actions[0].Argv != "" {
		t.Fatalf("unsafe audit: %+v", actions)
	}
	for _, op := range ops {
		if op.Targetable {
			t.Fatal("node operation routes to worker")
		}
	}
	f.result.State = "failed"
	if got := ops[1].Run(context.Background(), Params{"execution_id": p["execution_id"]}); got.Error == nil || got.Error.Code != "node_execution_failed" {
		t.Fatalf("exit failure lost: %+v", got)
	}
	p["env"] = map[string]any{"HOME": 42}
	if err := validate(ops[0], p); err == nil {
		t.Fatal("nonstring env accepted")
	}
}

func TestNodeReceiptsLinkOneExecutionAcrossRequests(t *testing.T) {
	f := SREReceiptFilter{ExecutionID: "execution-a"}
	for _, op := range []string{OpNodeExec, OpNodeCancel, OpNodeEnded} {
		if sreEffect(op) != SREEffectChange {
			t.Fatalf("%s audit lost from changes", op)
		}
		if !f.matches(SREAction{Op: op, ExecutionID: "execution-a", RequestID: op}) || f.matches(SREAction{Op: op, ExecutionID: "execution-b"}) {
			t.Fatalf("%s execution filter mismatched", op)
		}
	}
}
