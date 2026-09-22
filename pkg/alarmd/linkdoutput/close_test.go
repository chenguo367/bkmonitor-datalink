package linkdoutput

import (
	"encoding/json"
	"testing"
	"time"
)

func TestCloseUsesActiveSeverityAndStableIdentity(t *testing.T) {
	r := CloseRequest{TenantID: "tenant-test", Fingerprint: "0123456789abcdef0123456789abcdef", AlertInstanceID: "active-instance",
		Severity: "warning", StrategyID: 123, StrategyRevision: 4, BusinessID: 2, OccurredAt: time.Unix(1800000000, 0)}
	a, err := ConvertClose(r)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := ConvertClose(r)
	if a.EventID != b.EventID || string(a.Payload) != string(b.Payload) {
		t.Fatal("retry changed identity")
	}
	var payload wireEvent
	if err := json.Unmarshal(a.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.AlertID != r.Fingerprint || len(payload.Evaluations) != 1 || payload.Evaluations[0].Action != "closed" || payload.Evaluations[0].Severity != r.Severity || payload.Evaluations[0].ActionReason != CloseReasonInactive {
		t.Fatalf("wrong close: %+v", payload)
	}
	if len(payload.Values) != 0 {
		t.Fatal("closure fabricated a metric recovery")
	}
	r.AlertInstanceID = "next-instance"
	c, _ := ConvertClose(r)
	if a.EventID == c.EventID {
		t.Fatal("new instance reused event identity")
	}
	r.Severity = ""
	if _, err := ConvertClose(r); err == nil {
		t.Fatal("missing active severity accepted")
	}
}
