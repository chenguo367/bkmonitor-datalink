package execution

import (
	"testing"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/strategy"
)

// The mutation names what this round adds; the record it leaves behind is that
// merged into the history it was built against. So the contract proves the
// pair: the base is the history that was loaded, the addition is this batch's
// own points in order, and the bound is the Plan's.
func TestStateHistoryAdditionCarriesThisBatchAgainstTheLoadedRecord(t *testing.T) {
	loaded := []StateHistoryPoint{stateHistoryPoint("old", 10, LevelFactNormal)}
	mutation := StateMutation{
		AffectedRecords: []RecordAnchor{{RecordID: "current", SourceTime: 20}},
		Points:          []StateHistoryPoint{stateHistoryPoint("current", 20, LevelFactAnomalous)},
		RetentionPoints: 3,
		BaseHistory:     loaded,
	}
	if err := validateStateHistoryReplacement(loaded, mutation, 3); err != nil {
		t.Fatalf("valid addition rejected: %v", err)
	}
}

// Eviction is no longer the producer's to perform, so the producer no longer
// proves it: the addition is the same whether the merged record fits the bound
// or overflows it, and the bound travels for the store to apply.
func TestStateHistoryAdditionIsTheSameWhetherTheBoundEvictsOrNot(t *testing.T) {
	loaded := []StateHistoryPoint{
		stateHistoryPoint("oldest", 10, LevelFactNormal),
		stateHistoryPoint("old", 20, LevelFactAnomalous),
	}
	mutation := StateMutation{
		AffectedRecords: []RecordAnchor{{RecordID: "current", SourceTime: 30}},
		Points:          []StateHistoryPoint{stateHistoryPoint("current", 30, LevelFactNormal)},
		RetentionPoints: 2,
		BaseHistory:     loaded,
	}
	if err := validateStateHistoryReplacement(loaded, mutation, 2); err != nil {
		t.Fatalf("addition under an evicting bound rejected: %v", err)
	}
	merged, err := MergedHistory(mutation.BaseHistory, mutation.Points, mutation.RetentionPoints)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(merged) != 2 || merged[0].RecordID != "old" || merged[1].RecordID != "current" {
		t.Fatalf("the bound must evict the oldest of the merged record, got %+v", merged)
	}
}

// The bound is derived twice - by the producer from the compiled Plan, and
// here from the same Plan - and compared. One derivation would let the bound
// the write truncates by drift from the one the Plan asks for, and the only
// visible symptom would be points quietly missing from records.
func TestStateHistoryAdditionRejectsARetentionBoundThePlanDoesNotAskFor(t *testing.T) {
	loaded := []StateHistoryPoint{stateHistoryPoint("old", 10, LevelFactNormal)}
	mutation := StateMutation{
		AffectedRecords: []RecordAnchor{{RecordID: "current", SourceTime: 20}},
		Points:          []StateHistoryPoint{stateHistoryPoint("current", 20, LevelFactAnomalous)},
		RetentionPoints: 9,
		BaseHistory:     loaded,
	}
	if err := validateStateHistoryReplacement(loaded, mutation, 3); err == nil {
		t.Fatal("a mutation whose bound is not the Plan's must be rejected")
	}
	mutation.RetentionPoints = 0
	if err := validateStateHistoryReplacement(loaded, mutation, 3); err == nil {
		t.Fatal("a mutation carrying no bound must be rejected")
	}
}

func TestStateHistoryAdditionRejectsABaseThatIsNotTheLoadedHistory(t *testing.T) {
	loaded := []StateHistoryPoint{stateHistoryPoint("old", 10, LevelFactNormal)}
	mutation := StateMutation{
		AffectedRecords: []RecordAnchor{{RecordID: "current", SourceTime: 20}},
		Points:          []StateHistoryPoint{stateHistoryPoint("current", 20, LevelFactAnomalous)},
		RetentionPoints: 3,
	}
	if err := validateStateHistoryReplacement(loaded, mutation, 3); err == nil {
		t.Fatal("an absent base must be rejected while a history was loaded")
	}
	mutation.BaseHistory = []StateHistoryPoint{stateHistoryPoint("old", 10, LevelFactAnomalous)}
	if err := validateStateHistoryReplacement(loaded, mutation, 3); err == nil {
		t.Fatal("a base that rewrites a loaded point must be rejected")
	}
}

func TestStateHistoryAdditionRejectsAPointThisBatchDidNotEvaluate(t *testing.T) {
	mutation := StateMutation{
		AffectedRecords: []RecordAnchor{{RecordID: "current", SourceTime: 20}},
		Points: []StateHistoryPoint{
			stateHistoryPoint("current", 20, LevelFactAnomalous),
			stateHistoryPoint("invented", 30, LevelFactNormal),
		},
		RetentionPoints: 3,
	}
	if err := validateStateHistoryReplacement(nil, mutation, 3); err == nil {
		t.Fatal("a point outside this batch's anchors must be rejected")
	}
}

func TestStateHistoryAdditionRejectsPointsOutOfOrderOrRepeated(t *testing.T) {
	anchors := []RecordAnchor{{RecordID: "first", SourceTime: 20}, {RecordID: "second", SourceTime: 10}}
	descending := StateMutation{
		AffectedRecords: anchors,
		Points: []StateHistoryPoint{
			stateHistoryPoint("first", 20, LevelFactNormal),
			stateHistoryPoint("second", 10, LevelFactNormal),
		},
		RetentionPoints: 3,
	}
	if err := validateStateHistoryReplacement(nil, descending, 3); err == nil {
		t.Fatal("an addition out of order must be rejected")
	}
	repeated := StateMutation{
		AffectedRecords: []RecordAnchor{{RecordID: "first", SourceTime: 10}},
		Points: []StateHistoryPoint{
			stateHistoryPoint("first", 10, LevelFactNormal),
			stateHistoryPoint("first", 10, LevelFactNormal),
		},
		RetentionPoints: 3,
	}
	if err := validateStateHistoryReplacement(nil, repeated, 3); err == nil {
		t.Fatal("an addition naming one position twice must be rejected")
	}
}

// A point landing on a position the history already holds replaces it, so it
// has to carry what was stored there. Dropping a Level fact this way leaves the
// point in place and deletes the Level's past, which no count notices.
func TestStateHistoryAdditionMayNotDropAStoredFactAtAPositionItReplaces(t *testing.T) {
	loaded := []StateHistoryPoint{{RecordID: "same", SourceTime: 10, Levels: []StateLevelFact{
		{LevelID: 5, DetectFingerprint: "detect-v1", Result: LevelFactNormal},
		{LevelID: 6, DetectFingerprint: "detect-v1", Result: LevelFactNormal},
	}}}
	mutation := StateMutation{
		AffectedRecords: []RecordAnchor{{RecordID: "same", SourceTime: 10}},
		Points:          []StateHistoryPoint{stateHistoryPoint("same", 10, LevelFactNormal)},
		RetentionPoints: 3,
		BaseHistory:     loaded,
	}
	if err := validateStateHistoryReplacement(loaded, mutation, 3); err == nil {
		t.Fatal("an addition that drops a stored Level fact must be rejected")
	}
	mutation.Points = []StateHistoryPoint{{RecordID: "same", SourceTime: 10, Levels: []StateLevelFact{
		{LevelID: 5, DetectFingerprint: "detect-v1", Result: LevelFactNormal},
		{LevelID: 6, DetectFingerprint: "detect-v1", Result: LevelFactNormal},
		{LevelID: 7, DetectFingerprint: "detect-v1", Result: LevelFactAnomalous},
	}}}
	if err := validateStateHistoryReplacement(loaded, mutation, 3); err != nil {
		t.Fatalf("an addition carrying the stored facts and a new one must be accepted: %v", err)
	}
}

func TestGuardedOrInactiveUnknownMayPersistValidDetectFact(t *testing.T) {
	outcome := LevelOutcome{LevelID: 5, Outcome: LevelOutcomeUnknown, ReasonCode: ReasonCode(contract.ReasonHistoryWarming)}
	guarded := StateMutation{Levels: []RuntimeLevelStateMutation{{
		LevelID: 5, HistoryCompleteness: HistoryWarming, GapReasonCode: outcome.ReasonCode,
	}}}
	for _, fact := range []LevelFactResult{LevelFactNormal, LevelFactAnomalous} {
		if !stateFactMayAdvanceUnknown(fact, outcome, guarded, strategy.EffectiveTimeActive, true) {
			t.Fatalf("guarded %s fact must advance", fact)
		}
	}

	inactive := outcome
	inactive.ReasonCode = ReasonCode(contract.ReasonEffectiveTimeInactive)
	if !stateFactMayAdvanceUnknown(LevelFactNormal, inactive, StateMutation{}, strategy.EffectiveTimeInactive, true) {
		t.Fatal("INACTIVE must preserve a valid detect fact while suppressing business outcome")
	}
}

func TestUnknownWithoutExactAdvanceAuthorityIsRejected(t *testing.T) {
	outcome := LevelOutcome{LevelID: 5, Outcome: LevelOutcomeUnknown, ReasonCode: ReasonCode(contract.ReasonHistoryWarming)}
	if stateFactMayAdvanceUnknown(LevelFactNormal, outcome, StateMutation{}, strategy.EffectiveTimeActive, true) {
		t.Fatal("unguarded UNKNOWN must not advance state")
	}
	guarded := StateMutation{Levels: []RuntimeLevelStateMutation{{
		LevelID: 5, HistoryCompleteness: HistoryWarming, GapReasonCode: outcome.ReasonCode,
	}}}
	if stateFactMayAdvanceUnknown(LevelFactUnavailable, outcome, guarded, strategy.EffectiveTimeActive, true) ||
		stateFactMayAdvanceUnknown(LevelFactError, outcome, guarded, strategy.EffectiveTimeActive, true) {
		t.Fatal("UNAVAILABLE/ERROR facts must never advance history")
	}
	if stateFactMayAdvanceUnknown(LevelFactNormal, outcome, guarded, strategy.EffectiveTimeActive, false) {
		t.Fatal("PARTIAL/UNAVAILABLE input must never advance guarded UNKNOWN history")
	}
}

func TestUnavailableSeriesQualityFactDoesNotAuthorizeStateAdvance(t *testing.T) {
	plan := PlanIdentity{TenantID: "tenant", BusinessID: "2", StrategyID: "7"}
	series := SeriesIdentityDigest("series")
	outcome := LevelOutcome{
		Plan: plan, LevelID: 5, SeriesIdentityDigest: series,
		Record:  RecordAnchor{RecordID: "record", SourceTime: 10},
		Outcome: LevelOutcomeUnknown, ReasonCode: ReasonCode(contract.ReasonQueryUnavailable),
	}
	input := InternalExecution{Inputs: []NamedInputBinding{{
		Consumer: ConsumerRef{Plan: plan}, Role: InputRolePrimary,
		Completeness: CompletenessUnavailable, DataState: DataStateUnknown,
		Disposition: AccessUnavailable, ReasonCode: outcome.ReasonCode,
		Dataset: nil, View: nil,
		QualityFacts: []InputQualityFact{{
			ReasonCode: outcome.ReasonCode, ImpactScope: ImpactSeries,
			RecordID: outcome.Record.RecordID, SourceTime: outcome.Record.SourceTime, SeriesIdentity: series,
		}},
	}}}
	localized, err := validateLocalizedInputOutcome(input, plan, outcome)
	if err != nil || !localized {
		t.Fatalf("legal UNAVAILABLE series fact must localize its UNKNOWN outcome, localized=%v err=%v", localized, err)
	}
	if stateInputAllowsAdvance(input, outcome) {
		t.Fatal("legal UNAVAILABLE input must not authorize State advance")
	}
	guarded := StateMutation{Levels: []RuntimeLevelStateMutation{{
		LevelID: 5, HistoryCompleteness: HistoryWarming, GapReasonCode: outcome.ReasonCode,
	}}}
	if stateFactMayAdvanceUnknown(LevelFactNormal, outcome, guarded, strategy.EffectiveTimeActive, stateInputAllowsAdvance(input, outcome)) {
		t.Fatal("UNAVAILABLE input must be rejected by the State advance rule even with an exact guard")
	}
}

func stateHistoryPoint(recordID string, sourceTime int64, result LevelFactResult) StateHistoryPoint {
	return StateHistoryPoint{RecordID: recordID, SourceTime: sourceTime, Levels: []StateLevelFact{{
		LevelID: 5, DetectFingerprint: "detect-v1", Result: result,
	}}}
}
