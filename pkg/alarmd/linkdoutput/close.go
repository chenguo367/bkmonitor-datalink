package linkdoutput

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

const ActionClosed = "closed"
const CloseReasonInactive = "strategy_inactive"

// CloseReasonAbsent is the close for an alert whose strategy no longer
// exists: disabled or deleted, so nothing is left to evaluate it back to
// normal. It is its own reason and not a variant of the inactive one,
// because the two say different things to whoever reads the alert. Inactive
// means "outside its hours, it will be back"; absent means "there is no
// strategy behind this alert any more".
const CloseReasonAbsent = "strategy_absent"

// CloseRequest carries an active alert's identity and its severity. A SET
// member alone is insufficient. An empty severity is accepted for the absent
// close only, and closes at every built-in level; see closeAtEveryLevel.
// OccurredAt is a current maintenance decision, never a historical Slot.
//
// Reason says which close this is. Empty is CloseReasonInactive, which is
// what every caller sent before there was a second reason, and what keeps
// the event id of an inactive close identical across this change.
type CloseRequest struct {
	TenantID, Fingerprint, AlertInstanceID, Severity string
	StrategyID, StrategyRevision, BusinessID         int64
	OccurredAt                                       time.Time
	Reason                                           string
}

// closeText is what each reason puts on the event, and the salt its event id
// is derived under. The salts differ so that the same alert closed for two
// different reasons is two events; the inactive salt is the original string
// so that an inactive close keeps the id it has always had.
var closeText = map[string]struct{ salt, title, content string }{
	CloseReasonInactive: {"alarmd-inactive-close-v1", "Strategy is outside its effective time",
		"Close requested because the strategy is currently inactive; this does not indicate metric recovery."},
	CloseReasonAbsent: {"alarmd-absent-strategy-close-v1", "Strategy no longer exists",
		"Close requested because the strategy was disabled or deleted and nothing evaluates this alert any more; this does not indicate metric recovery."},
}

func ConvertClose(request CloseRequest) (Event, error) {
	// Match StrategySnapshotRef: negative IDs identify non-BKCC spaces; zero is unset.
	if request.TenantID == "" || request.AlertInstanceID == "" || request.StrategyID <= 0 || request.StrategyRevision <= 0 || request.BusinessID == 0 || request.OccurredAt.Unix() <= 0 {
		return Event{}, errors.New("close requires tenant, active instance, strategy revision, business and current time")
	}
	fingerprint, err := hex.DecodeString(request.Fingerprint)
	if err != nil || len(fingerprint) != 16 || hex.EncodeToString(fingerprint) != request.Fingerprint {
		return Event{}, errors.New("close requires the native source alert fingerprint")
	}
	reason := request.Reason
	if reason == "" {
		reason = CloseReasonInactive
	}
	text, known := closeText[reason]
	if !known {
		return Event{}, errors.New("close requires a reason this build can name")
	}
	if len(request.Severity) > MaxSeverityBytes || (request.Severity == "" && reason != CloseReasonAbsent) {
		return Event{}, errors.New("close requires the active alert severity")
	}
	evaluations := []wireEvaluation{{Severity: request.Severity, Action: ActionClosed, ActionReason: reason}}
	if request.Severity == "" {
		evaluations = closeAtEveryLevel(reason)
	}
	// Stable within an attempt; retries use the same event. Later maintenance
	// rechecks current rules and uses a new current time rather than replaying
	// an old closure across a new active interval.
	idInput, _ := json.Marshal([]any{text.salt, request.TenantID, request.StrategyID,
		request.StrategyRevision, request.AlertInstanceID, request.Fingerprint, request.Severity, request.OccurredAt.Unix()})
	digest := sha256.Sum256(idInput)
	id := hex.EncodeToString(digest[:])
	wire := wireEvent{TenantID: request.TenantID, EventID: id, AlertID: request.Fingerprint,
		Title: text.title, Content: text.content,
		Evaluations: evaluations,
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

// closeAtEveryLevel is a close whose severity is not known: one closed
// evaluation for each level this build names. The consumer applies the
// evaluation whose severity is the active alert's own and records the others
// as finding no alert at that level, which changes nothing; a severity it
// does not know would reject the whole event, which is why only the built-in
// names are written - the same names every triggered event of this build
// uses, so the consumer knows them wherever it created the alert.
//
// Only the absent close is allowed to ask for it. That close comes from the
// link's reconciliation, which carries no severity; the inactive close keeps
// requiring one, because what it closes has a Plan whose levels are known.
func closeAtEveryLevel(reason string) []wireEvaluation {
	levels := make([]uint32, 0, len(builtInSeverities))
	for level := range builtInSeverities {
		levels = append(levels, level)
	}
	sort.Slice(levels, func(i, j int) bool { return levels[i] < levels[j] })
	evaluations := make([]wireEvaluation, 0, len(levels))
	for _, level := range levels {
		evaluations = append(evaluations, wireEvaluation{Severity: builtInSeverities[level], Action: ActionClosed, ActionReason: reason})
	}
	return evaluations
}
