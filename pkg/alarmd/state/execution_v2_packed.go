// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package state

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/contract"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

const executionStateSchemaV3 = "alarmd-runtime-state-v3"

// packedLevel is one Level's scalar state plus the detect fingerprint the
// envelope used to repeat on every point.
//
// The fingerprint is hoisted here rather than dropped because two readers
// compare it: the result contract compares the whole Level fact, and the
// load-time contract check compares the stored fingerprint against the current
// one. Hoisting is lossless because the key guarantees it: the state
// generation's closure covers each Level's detect fingerprint, so a Plan whose
// fingerprint moves is a different generation and a different key, and within
// one key a Level has exactly one.
type packedLevel struct {
	Mutation          execution.RuntimeLevelStateMutation `json:"mutation"`
	DetectFingerprint string                              `json:"detect_fingerprint"`
}

// packedHeader is the runtime envelope with the history taken out.
type packedHeader struct {
	Schema         string                     `json:"schema"`
	Identity       execution.StateKeyIdentity `json:"identity"`
	BlobRevision   uint64                     `json:"blob_revision"`
	ApplyVersion   execution.ApplyVersion     `json:"apply_version"`
	MutationDigest execution.MutationDigest   `json:"mutation_digest"`
	LastEventTime  int64                      `json:"last_event_time"`
	SeriesGuard    *execution.StateGuardFact  `json:"series_guard,omitempty"`
	Levels         []packedLevel              `json:"levels"`
	PointCount     int                        `json:"point_count"`
}

// ErrPackedContract is a write refused for disagreeing with what the framed
// record can represent. It is a deterministic refusal, never a retry: the same
// bytes would be refused again.
var ErrPackedContract = errors.New("state: runtime record cannot be framed")

// The rules a framed write can be refused by, as bounded names.
//
// A closed vocabulary rather than the error's sentence: the refusal reaches
// the admission line, and a line carrying free text cannot be grouped or
// counted, and carries whatever the values happened to be. Every rule below
// appears in exactly one refusal, and PackedRuleNames is what a reader may
// see - a rule added without a name here reaches the line as empty, which the
// case on that list refuses.
const (
	PackedRuleLevelNotInMutation   = "level_not_in_mutation"
	PackedRuleNoDetectFingerprint  = "no_detect_fingerprint"
	PackedRuleTwoFingerprints      = "two_fingerprints_for_one_level"
	PackedRuleDuplicateLevel       = "duplicate_level"
	PackedRuleSourceTimeNotRising  = "source_time_not_rising"
	PackedRuleRecordIDUnderivable  = "record_id_underivable"
	PackedRuleRecordIDNotDerived   = "record_id_not_derived"
	PackedRuleUnencodableFactState = "unencodable_fact_state"
	// PackedRuleMutationDigestMismatch is not one of the framing rules: the
	// store refuses the mutation before it frames anything, because the digest
	// the producer computed does not cover the content it sent. It is named
	// here because it reaches the line under the same reason as the eight, and
	// an unnamed ninth way is exactly what makes the other eight worth naming.
	PackedRuleMutationDigestMismatch = "mutation_digest_mismatch"
	// PackedRuleIdentityKeyUnderivable is the store refusing before it frames
	// or writes anything: the mutation's identity does not produce a key. Its
	// own name rather than sharing the digest's, because the two send a reader
	// to different places - one to what the producer computed, one to the
	// identity it computed it for.
	PackedRuleIdentityKeyUnderivable = "identity_key_underivable"
)

// PackedRuleNames is every rule a framed write can be refused by.
var PackedRuleNames = []string{
	PackedRuleLevelNotInMutation, PackedRuleNoDetectFingerprint, PackedRuleTwoFingerprints,
	PackedRuleDuplicateLevel, PackedRuleSourceTimeNotRising, PackedRuleRecordIDUnderivable,
	PackedRuleRecordIDNotDerived, PackedRuleUnencodableFactState, PackedRuleMutationDigestMismatch,
	PackedRuleIdentityKeyUnderivable,
}

// PackedContractRefusal is a framed write refused by one named rule. The
// sentence stays for a human reading the error; the rule is what the line
// carries.
type PackedContractRefusal struct {
	Rule   string
	Detail string
}

func (err *PackedContractRefusal) Error() string {
	return fmt.Sprintf("%s: %s (%s)", ErrPackedContract.Error(), err.Detail, err.Rule)
}

func (err *PackedContractRefusal) Unwrap() error { return ErrPackedContract }

// PackedRefusalRule is the rule that refused a framed write, empty when err is
// not one of those refusals.
func PackedRefusalRule(err error) string {
	var refusal *PackedContractRefusal
	if !errors.As(err, &refusal) || refusal == nil {
		return ""
	}
	return refusal.Rule
}

func packedRefusal(rule, format string, args ...any) error {
	return &PackedContractRefusal{Rule: rule, Detail: fmt.Sprintf(format, args...)}
}

// levelFingerprints picks each Level's one detect fingerprint out of the points
// and refuses a mutation whose points disagree with each other.
//
// Refused here rather than at the reader. The window already treats a point
// whose fingerprint differs from its Level's as an invariant violation, but it
// only says so when the record is read back, which is a round later and in a
// different Slot than the one that produced it. Hoisting the value makes the
// disagreement unrepresentable, so it has to be named where it is created.
func levelFingerprints(mutation execution.StateMutation, levels []execution.RuntimeLevelStateMutation) ([]string, error) {
	fingerprints := make([]string, len(levels))
	position := make(map[uint32]int, len(levels))
	for index, level := range levels {
		position[level.LevelID] = index
	}
	for _, point := range mutation.Points {
		for _, fact := range point.Levels {
			index, known := position[fact.LevelID]
			if !known {
				return nil, packedRefusal(PackedRuleLevelNotInMutation,
					"point at %d names Level %d, which the mutation does not carry", point.SourceTime, fact.LevelID)
			}
			if fact.DetectFingerprint == "" {
				return nil, packedRefusal(PackedRuleNoDetectFingerprint,
					"point at %d carries no detect fingerprint for Level %d", point.SourceTime, fact.LevelID)
			}
			if fingerprints[index] == "" {
				fingerprints[index] = fact.DetectFingerprint
				continue
			}
			if fingerprints[index] != fact.DetectFingerprint {
				return nil, packedRefusal(PackedRuleTwoFingerprints,
					"Level %d has two detect fingerprints in one record", fact.LevelID)
			}
		}
	}
	return fingerprints, nil
}

// encodeRuntimePacked writes the framed record.
func encodeRuntimePacked(mutation execution.StateMutation, revision uint64) ([]byte, error) {
	levels := append([]execution.RuntimeLevelStateMutation(nil), mutation.Levels...)
	sort.Slice(levels, func(i, j int) bool { return levels[i].LevelID < levels[j].LevelID })
	for index := 1; index < len(levels); index++ {
		if levels[index].LevelID == levels[index-1].LevelID {
			return nil, packedRefusal(PackedRuleDuplicateLevel, "Level %d appears twice", levels[index].LevelID)
		}
	}
	fingerprints, err := levelFingerprints(mutation, levels)
	if err != nil {
		return nil, err
	}
	last := int64(0)
	for _, level := range levels {
		if level.LastProcessedEventTime > last {
			last = level.LastProcessedEventTime
		}
	}
	header := packedHeader{
		Schema: executionStateSchemaV3, Identity: mutation.Identity, BlobRevision: revision,
		ApplyVersion: mutation.ApplyVersion, MutationDigest: mutation.MutationDigest, LastEventTime: last,
		SeriesGuard: mutation.SeriesGuard, Levels: make([]packedLevel, len(levels)), PointCount: len(mutation.Points),
	}
	for index, level := range levels {
		header.Levels[index] = packedLevel{Mutation: level, DetectFingerprint: fingerprints[index]}
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, 0, len(headerBytes)+packedFrameHeaderLen+binary.MaxVarintLen64)
	buffer = append(buffer, packedFrameMagic...)
	buffer = append(buffer, packedFrameSchemaV1, packedFrameCodecNone)
	buffer = appendUvarint(buffer, uint64(len(headerBytes)))
	buffer = append(buffer, headerBytes...)

	bitmapBytes := (len(levels) + 7) / 8
	index := make(map[uint32]int, len(levels))
	for position, level := range levels {
		index[level.LevelID] = position
	}
	previous := int64(0)
	for pointIndex, point := range mutation.Points {
		if point.SourceTime < 0 || (pointIndex > 0 && point.SourceTime <= previous) {
			return nil, packedRefusal(PackedRuleSourceTimeNotRising, "points must rise strictly by source time")
		}
		// The id is not stored. It is checked here against the derivation every
		// producer uses, so a producer that stops deriving it is refused where
		// it writes rather than read back as a different record later.
		expected, deriveErr := contract.DeriveRecordIDV2(string(mutation.Identity.SeriesIdentityDigest), point.SourceTime)
		if deriveErr != nil {
			return nil, packedRefusal(PackedRuleRecordIDUnderivable, "derive record id at %d: %v", point.SourceTime, deriveErr)
		}
		if point.RecordID != expected {
			return nil, packedRefusal(PackedRuleRecordIDNotDerived,
				"point at %d carries a record id the series identity and source time do not derive", point.SourceTime)
		}
		delta := uint64(point.SourceTime)
		if pointIndex > 0 {
			delta = uint64(point.SourceTime - previous)
		}
		buffer = appendUvarint(buffer, delta)
		present := make([]byte, bitmapBytes)
		valid := make([]byte, bitmapBytes)
		anomalous := make([]byte, bitmapBytes)
		for _, fact := range point.Levels {
			position := index[fact.LevelID]
			setBit(present, position, true)
			switch fact.Result {
			case execution.LevelFactNormal:
				setBit(valid, position, true)
			case execution.LevelFactAnomalous:
				setBit(valid, position, true)
				setBit(anomalous, position, true)
			case execution.LevelFactUnavailable:
			case execution.LevelFactError:
				setBit(anomalous, position, true)
			default:
				return nil, packedRefusal(PackedRuleUnencodableFactState, "Level %d fact result %q", fact.LevelID, fact.Result)
			}
		}
		buffer = append(buffer, present...)
		buffer = append(buffer, valid...)
		buffer = append(buffer, anomalous...)
		previous = point.SourceTime
	}
	return buffer, nil
}

// packedFrame reports whether these bytes are a framed record rather than the
// JSON envelope. The two are told apart by their first bytes, so a reader needs
// no version field to dispatch.
func packedFrame(raw []byte) bool {
	return len(raw) >= packedFrameHeaderLen && string(raw[:4]) == packedFrameMagic
}

// decodeRuntimePacked reads the framed record back into the same view the JSON
// envelope produces, reconstructing each point's record id and each fact's
// detect fingerprint.
func decodeRuntimePacked(raw []byte, identity execution.StateKeyIdentity) (execution.RuntimeStateView, error) {
	if !packedFrame(raw) {
		return execution.RuntimeStateView{}, fmt.Errorf("%w: not a framed record", ErrCorruptState)
	}
	if raw[4] != packedFrameSchemaV1 || raw[5] != packedFrameCodecNone {
		return execution.RuntimeStateView{}, fmt.Errorf("%w: frame schema %d codec %d", ErrUnsupportedState, raw[4], raw[5])
	}
	headerLen, rest, err := consumeUvarint(raw[packedFrameHeaderLen:])
	if err != nil {
		return execution.RuntimeStateView{}, err
	}
	if headerLen > uint64(len(rest)) {
		return execution.RuntimeStateView{}, fmt.Errorf("%w: truncated header", ErrCorruptState)
	}
	var header packedHeader
	if err := json.Unmarshal(rest[:headerLen], &header); err != nil {
		return execution.RuntimeStateView{}, fmt.Errorf("%w: header: %v", ErrCorruptState, err)
	}
	if header.Schema != executionStateSchemaV3 {
		return execution.RuntimeStateView{}, fmt.Errorf("%w: schema %q", ErrUnsupportedState, header.Schema)
	}
	if header.Identity != identity || header.BlobRevision == 0 {
		return execution.RuntimeStateView{}, fmt.Errorf("%w: identity or revision", ErrCorruptState)
	}
	rest = rest[headerLen:]

	bitmapBytes := (len(header.Levels) + 7) / 8
	levels := make([]execution.RuntimeLevelStateView, len(header.Levels))
	for index, level := range header.Levels {
		levels[index] = execution.RuntimeLevelStateView{LevelID: level.Mutation.LevelID,
			LevelStateCompatibility: level.Mutation.LevelStateCompatibility,
			HistoryCompleteness:     level.Mutation.HistoryCompleteness, GapReasonCode: level.Mutation.GapReasonCode,
			WarmupRequirementRef:   level.Mutation.WarmupRequirementRef,
			LastProcessedEventTime: level.Mutation.LastProcessedEventTime}
	}
	history := make([]execution.StateHistoryPoint, 0, header.PointCount)
	previous := int64(0)
	for point := 0; point < header.PointCount; point++ {
		delta, next, decodeErr := consumeUvarint(rest)
		if decodeErr != nil {
			return execution.RuntimeStateView{}, decodeErr
		}
		rest = next
		if len(rest) < 3*bitmapBytes {
			return execution.RuntimeStateView{}, fmt.Errorf("%w: truncated point %d", ErrCorruptState, point)
		}
		sourceTime := int64(delta)
		if point > 0 {
			sourceTime = previous + int64(delta)
		}
		present, valid, anomalous := rest[:bitmapBytes], rest[bitmapBytes:2*bitmapBytes], rest[2*bitmapBytes:3*bitmapBytes]
		rest = rest[3*bitmapBytes:]
		recordID, deriveErr := contract.DeriveRecordIDV2(string(identity.SeriesIdentityDigest), sourceTime)
		if deriveErr != nil {
			return execution.RuntimeStateView{}, fmt.Errorf("%w: derive record id: %v", ErrCorruptState, deriveErr)
		}
		facts := make([]execution.StateLevelFact, 0, len(header.Levels))
		for position, level := range header.Levels {
			if !bitSet(present, position) {
				continue
			}
			result := execution.LevelFactUnavailable
			switch {
			case bitSet(valid, position) && bitSet(anomalous, position):
				result = execution.LevelFactAnomalous
			case bitSet(valid, position):
				result = execution.LevelFactNormal
			case bitSet(anomalous, position):
				result = execution.LevelFactError
			}
			facts = append(facts, execution.StateLevelFact{LevelID: level.Mutation.LevelID,
				DetectFingerprint: level.DetectFingerprint, Result: result})
		}
		history = append(history, execution.StateHistoryPoint{RecordID: recordID, SourceTime: sourceTime, Levels: facts})
		previous = sourceTime
	}
	if len(rest) != 0 {
		return execution.RuntimeStateView{}, fmt.Errorf("%w: %d trailing bytes", ErrCorruptState, len(rest))
	}
	return execution.RuntimeStateView{Identity: identity, BlobRevision: header.BlobRevision,
		PersistedApplyVersion: header.ApplyVersion, PersistedMutationDigest: header.MutationDigest,
		LastProcessedEventTime: header.LastEventTime, SeriesGuard: header.SeriesGuard,
		Levels: levels, History: history}, nil
}

// runtimeViewSource says which key a loaded view came from, so the write side
// knows whether it is continuing a framed record or converting an envelope.
type runtimeViewSource uint8

const (
	runtimeViewNone runtimeViewSource = iota
	runtimeViewFramed
	runtimeViewEnvelope
)

// chooseRuntimeView picks between the two representations of one series.
//
// Not "prefer the new key". Ownership of a Query Group moves between replicas
// while a rollout is in progress, and an old binary that takes a Query Group
// back writes the envelope key after a new one has already written the framed
// one - so the envelope can legitimately be the newer of the two, and taking
// the framed one on sight would throw away every round the old owner ran.
//
// Equal versions go to the framed key, and that choice is not arbitrary. Two
// records at one version describe the same evaluation and hold the same facts,
// so either is correct to read; but taking the envelope means deriving the
// framed record from it again next round, and the round after, for as long as
// both exist. The migration would never converge while looking entirely
// healthy.
func chooseRuntimeView(framed, envelope *execution.RuntimeStateView) (execution.RuntimeStateView, runtimeViewSource) {
	switch {
	case framed == nil && envelope == nil:
		return execution.RuntimeStateView{}, runtimeViewNone
	case framed == nil:
		return *envelope, runtimeViewEnvelope
	case envelope == nil:
		return *framed, runtimeViewFramed
	}
	switch execution.CompareApplyVersion(framed.PersistedApplyVersion, envelope.PersistedApplyVersion) {
	case execution.ApplyVersionPersistedOlder:
		return *envelope, runtimeViewEnvelope
	default:
		// Newer, and equal. CompareApplyVersion is a total order: when the
		// epoch and evaluation time match it falls back to comparing the Slot
		// digests, which is deterministic and the same on every replica but
		// says nothing about which happened later - so it is usable to break a
		// tie and must not be read as "this one is more recent".
		return *framed, runtimeViewFramed
	}
}
