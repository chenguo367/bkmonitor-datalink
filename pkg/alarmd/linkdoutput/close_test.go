package linkdoutput

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
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

func TestClosePreservesNativeSignedBusinessIdentity(t *testing.T) {
	for _, businessID := range []int64{2, -42} {
		t.Run(strconv.FormatInt(businessID, 10), func(t *testing.T) {
			trigger := decision(func(event *contract.TriggerEventV1) {
				event.BusinessID = strconv.FormatInt(businessID, 10)
				event.StrategyRef.BusinessID = businessID
			})
			opened := convertRaw(t, trigger)
			closed, err := ConvertClose(CloseRequest{
				TenantID: trigger.TenantID, Fingerprint: opened.AlertID, AlertInstanceID: "active-instance",
				Severity: opened.Severity, StrategyID: trigger.StrategyRef.StrategyID,
				StrategyRevision: trigger.StrategyRef.Revision, BusinessID: businessID,
				OccurredAt: time.Unix(trigger.EvaluationTime+60, 0),
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range []Event{opened, closed} {
				var wire wireEvent
				if err := json.Unmarshal(event.Payload, &wire); err != nil {
					t.Fatal(err)
				}
				if wire.Labels.BusinessID != businessID || wire.AlertID != opened.AlertID {
					t.Fatalf("event changed business or alert identity: %s", event.Payload)
				}
			}
		})
	}
}

// The link's reconciliation carries no severity. An absent close without one
// closes at every built-in level, each exactly once - the consumer refuses a
// message that names a level twice - and the message is one the consumer's
// acceptance, as transcribed here, takes whole.
func TestAnAbsentCloseWithoutASeverityClosesAtEveryBuiltInLevel(t *testing.T) {
	r := CloseRequest{TenantID: "tenant-test", Fingerprint: "0123456789abcdef0123456789abcdef", AlertInstanceID: "active-instance",
		StrategyID: 123, StrategyRevision: 4, BusinessID: 2, OccurredAt: time.Unix(1800000000, 0), Reason: CloseReasonAbsent}
	event, err := ConvertClose(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkStandardPayload(event.Payload); err != nil {
		t.Fatalf("the consumer would refuse the every-level close: %v", err)
	}
	var payload wireEvent
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, evaluation := range payload.Evaluations {
		if evaluation.Action != ActionClosed || evaluation.ActionReason != CloseReasonAbsent || seen[evaluation.Severity] {
			t.Fatalf("an evaluation is not a single close at its level: %+v", payload.Evaluations)
		}
		seen[evaluation.Severity] = true
	}
	for _, name := range builtInSeverities {
		if !seen[name] {
			t.Fatalf("the level %q is not closed: %+v", name, payload.Evaluations)
		}
	}
	if len(payload.Evaluations) != len(builtInSeverities) {
		t.Fatalf("a level outside the built-in set was written: %+v", payload.Evaluations)
	}
	// With a known severity the absent close stays a single evaluation.
	r.Severity = "warning"
	known, err := ConvertClose(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(known.Payload, &payload); err != nil || len(payload.Evaluations) != 1 {
		t.Fatalf("a known severity was widened: %s", known.Payload)
	}
}

// Only the absent close may omit the severity. The inactive close closes the
// alerts of a strategy whose Plan is running, and its levels are known.
func TestAnInactiveCloseStillRequiresASeverity(t *testing.T) {
	r := CloseRequest{TenantID: "tenant-test", Fingerprint: "0123456789abcdef0123456789abcdef", AlertInstanceID: "active-instance",
		StrategyID: 123, StrategyRevision: 4, BusinessID: 2, OccurredAt: time.Unix(1800000000, 0)}
	if _, err := ConvertClose(r); err == nil {
		t.Fatal("an inactive close without a severity was accepted")
	}
	r.Reason = CloseReasonInactive
	if _, err := ConvertClose(r); err == nil {
		t.Fatal("an inactive close without a severity was accepted")
	}
}
