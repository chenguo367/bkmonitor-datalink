package linkdoutput

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const ActionClosed = "closed"
const CloseReasonInactive = "strategy_inactive"

// CloseRequest carries an active alert's identity and its actual severity,
// obtained from Linkd reconciliation. A SET member alone is insufficient.
// OccurredAt is a current maintenance decision, never a historical Slot.
type CloseRequest struct {
	TenantID, Fingerprint, AlertInstanceID, Severity string
	StrategyID, StrategyRevision, BusinessID         int64
	OccurredAt                                       time.Time
}

func ConvertClose(request CloseRequest) (Event, error) {
	if request.TenantID == "" || request.AlertInstanceID == "" || request.StrategyID <= 0 || request.StrategyRevision <= 0 || request.BusinessID <= 0 || request.OccurredAt.Unix() <= 0 {
		return Event{}, errors.New("close requires tenant, active instance, strategy revision, business and current time")
	}
	fingerprint, err := hex.DecodeString(request.Fingerprint)
	if err != nil || len(fingerprint) != 16 || hex.EncodeToString(fingerprint) != request.Fingerprint {
		return Event{}, errors.New("close requires the native source alert fingerprint")
	}
	if request.Severity == "" || len(request.Severity) > MaxSeverityBytes {
		return Event{}, errors.New("close requires the active alert severity")
	}
	// Stable within an attempt; retries use the same event. Later maintenance
	// rechecks current rules and uses a new current time rather than replaying
	// an old closure across a new active interval.
	idInput, _ := json.Marshal([]any{"alarmd-inactive-close-v1", request.TenantID, request.StrategyID,
		request.StrategyRevision, request.AlertInstanceID, request.Fingerprint, request.Severity, request.OccurredAt.Unix()})
	digest := sha256.Sum256(idInput)
	id := hex.EncodeToString(digest[:])
	wire := wireEvent{TenantID: request.TenantID, EventID: id, AlertID: request.Fingerprint,
		Title: "Strategy is outside its effective time", Content: "Close requested because the strategy is currently inactive; this does not indicate metric recovery.",
		Evaluations: []wireEvaluation{{Severity: request.Severity, Action: ActionClosed, ActionReason: CloseReasonInactive}},
		Dimensions:  map[string]json.RawMessage{}, OccurredAt: wireTime(request.OccurredAt.Unix()), ProducedAt: wireTime(request.OccurredAt.Unix()),
		Labels:    wireLabels{StrategyID: request.StrategyID, StrategyVersion: request.StrategyRevision, BusinessID: request.BusinessID},
		ExtraData: wireExtraData{EvaluationFamily: evaluationFamilyMetricAlgorithm},
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return Event{}, fmt.Errorf("encode close: %w", err)
	}
	return Event{EventID: id, AlertID: request.Fingerprint, TenantID: request.TenantID, Payload: payload, Severity: request.Severity, Action: ActionClosed}, nil
}
