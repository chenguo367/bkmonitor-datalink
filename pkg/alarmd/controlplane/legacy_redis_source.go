package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var ErrLegacySourceIncomplete = errors.New("alarmd controlplane: legacy Redis source incomplete")

// ErrActiveStrategyIDInvalid marks an active set refused because one element
// is not a canonical positive integer. It wraps ErrLegacySourceIncomplete,
// which is what every caller already checks; the extra identity is for the
// reader who has to find out which element, and the error text names it.
var ErrActiveStrategyIDInvalid = errors.New("alarmd controlplane: active strategy identity is not a canonical positive integer")

// invalidActiveStrategyIDText bounds how much of a refused element the error
// repeats. The element comes from the store and could be anything; the
// reader needs enough to find it, not all of it.
const invalidActiveStrategyIDText = 64

// legacyStrategyMGetChunk bounds one MGET of strategy documents.
//
// A full read used to ask for every document in one MGET. Redis builds that
// reply in one go and every other client of the instance waits behind it, and
// the strategy cache is a shared instance - it can be the very database that
// holds alarmd's own runtime state. A strategy document is a few kilobytes,
// so ten thousand strategies would be a single reply of tens of megabytes;
// five hundred keys keep one reply to a few megabytes, which the instance
// serves between other commands. A read of no more than this many documents
// is one MGET, as before.
//
// One MGET was one instant of the store; chunks are several. A publication
// that lands between two chunks can leave one round with documents from both.
// Each is a document the writer published, and the round cannot mistake the
// mix for the current state for long: its change signal was read before the
// documents, so the next round finds it moved and reads them all again.
const legacyStrategyMGetChunk = 500

type legacyRedisCommands interface {
	Get(context.Context, string) *redis.StringCmd
	MGet(context.Context, ...string) *redis.SliceCmd
}

// LegacyRedisStrategySource adapts only the Python StrategyCacheManager String
// contract: <prefix>.strategy_ids, <prefix>.strategy_<id> and, as its change
// signal, <prefix>.last_updated. Legacy DTOs do not escape this adapter.
type LegacyRedisStrategySource struct {
	client          legacyRedisCommands
	strategyIDsKey  string
	strategyKeyStem string
	lastUpdatedKey  string
}

func NewLegacyRedisStrategySource(client redis.Cmdable, cachePrefix string) (*LegacyRedisStrategySource, error) {
	if client == nil || cachePrefix == "" || strings.ContainsAny(cachePrefix, "{} \t\r\n") {
		return nil, errors.New("alarmd controlplane: invalid legacy Redis strategy source")
	}
	return &LegacyRedisStrategySource{
		client: client, strategyIDsKey: cachePrefix + ".strategy_ids",
		strategyKeyStem: cachePrefix + ".strategy_",
		lastUpdatedKey:  cachePrefix + ".last_updated",
	}, nil
}

func (source *LegacyRedisStrategySource) ActiveStrategyIDs(ctx context.Context) ([]string, error) {
	if source == nil || source.client == nil {
		return nil, errors.New("alarmd controlplane: legacy Redis strategy source is required")
	}
	payload, err := source.client.Get(ctx, source.strategyIDsKey).Bytes()
	if errors.Is(err, redis.Nil) || (err == nil && len(payload) == 0) {
		return nil, ErrLegacySourceIncomplete
	}
	if err != nil {
		return nil, fmt.Errorf("alarmd controlplane: read legacy strategy active set: %w", err)
	}
	var rawIDs []json.RawMessage
	if err := json.Unmarshal(payload, &rawIDs); err != nil {
		return nil, fmt.Errorf("%w: decode active strategy set: %v", ErrLegacySourceIncomplete, err)
	}
	ids := make([]string, 0, len(rawIDs))
	for _, rawID := range rawIDs {
		text := string(rawID)
		id, err := strconv.ParseUint(text, 10, 64)
		if err != nil || id == 0 || strconv.FormatUint(id, 10) != text {
			if len(text) > invalidActiveStrategyIDText {
				text = text[:invalidActiveStrategyIDText] + "..."
			}
			return nil, fmt.Errorf("%w: %w: element %d of %d is %q",
				ErrLegacySourceIncomplete, ErrActiveStrategyIDInvalid, len(ids), len(rawIDs), text)
		}
		ids = append(ids, strconv.FormatUint(id, 10))
	}
	return ids, nil
}

func (source *LegacyRedisStrategySource) Strategies(ctx context.Context, ids []string) ([]SourceStrategy, error) {
	if source == nil || source.client == nil {
		return nil, errors.New("alarmd controlplane: legacy Redis strategy source is required")
	}
	if len(ids) == 0 {
		return []SourceStrategy{}, nil
	}
	keys := make([]string, len(ids))
	for index, id := range ids {
		parsed, err := strconv.ParseUint(id, 10, 64)
		if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != id {
			return nil, ErrObservationUnstable
		}
		keys[index] = source.strategyKeyStem + id
	}
	// Each chunk is turned into its strategies before the next is read: what
	// the read holds besides the documents, which are this round's working
	// set, is one chunk of replies, however many strategies there are.
	strategies := make([]SourceStrategy, 0, len(ids))
	for start := 0; start < len(keys); start += legacyStrategyMGetChunk {
		chunk := keys[start:min(start+legacyStrategyMGetChunk, len(keys))]
		read, err := source.client.MGet(ctx, chunk...).Result()
		if err != nil {
			return nil, fmt.Errorf("alarmd controlplane: read legacy strategy objects: %w", err)
		}
		if len(read) != len(chunk) {
			return nil, ErrObservationUnstable
		}
		for offset, value := range read {
			strategies = append(strategies, legacyStrategyOf(ids[start+offset], value))
		}
	}
	return strategies, nil
}

// legacyStrategyOf is one strategy document as the source read it, or the
// disposition that says why it cannot be used. The document is the copy
// legacyRedisBytes made of the reply, so the reply is not kept.
func legacyStrategyOf(id string, value interface{}) SourceStrategy {
	payload, ok := legacyRedisBytes(value)
	if !ok || len(payload) == 0 {
		return SourceStrategy{SourceID: id, SourceDisposition: &ObjectDisposition{
			SourceID: id, Scope: "STRATEGY", Disposition: DispositionSourceIncomplete,
			Reason: "SOURCE_OBJECT_INCOMPLETE",
		}}
	}
	strategy := SourceStrategy{SourceID: id, Document: json.RawMessage(payload)}
	var identityDTO struct {
		ID             int64           `json:"id"`
		BusinessID     int64           `json:"bk_biz_id"`
		TenantID       json.RawMessage `json:"bk_tenant_id"`
		SpaceUID       json.RawMessage `json:"space_uid"`
		GlobalBusiness json.RawMessage `json:"is_global_strategy"`
	}
	if err := json.Unmarshal(payload, &identityDTO); err != nil {
		strategy.SourceDisposition = &ObjectDisposition{
			SourceID: id, Scope: "STRATEGY", Disposition: DispositionConfigRejected,
			Reason: "STRATEGY_DOCUMENT_INVALID",
		}
		return strategy
	}
	if identityDTO.ID <= 0 || strconv.FormatInt(identityDTO.ID, 10) != id {
		strategy.SourceDisposition = &ObjectDisposition{
			SourceID: id, Scope: "STRATEGY", Disposition: DispositionConfigRejected,
			Reason: "STRATEGY_IDENTITY_INVALID",
		}
		return strategy
	}
	if identityDTO.BusinessID == 0 {
		strategy.SourceDisposition = &ObjectDisposition{
			SourceID: id, Scope: "STRATEGY", Disposition: DispositionConfigRejected,
			Reason: "STRATEGY_BUSINESS_IDENTITY_INVALID",
		}
		return strategy
	}
	tenantID, tenantOK := decodeRequiredIdentityString(identityDTO.TenantID)
	spaceUID, spaceOK := decodeRequiredIdentityString(identityDTO.SpaceUID)
	if !tenantOK || !spaceOK {
		// Which field, not only that one was. The source page samples
		// this disposition with the strategy id and the reason, and a
		// reader of 47 such rows could not tell whether the writer had
		// stopped filling the tenant, the space, or both.
		strategy.SourceDisposition = &ObjectDisposition{
			SourceID: id, Scope: "STRATEGY", Disposition: DispositionSourceIncomplete,
			Reason: "SOURCE_IDENTITY_UNAVAILABLE", FieldPath: missingIdentityFieldPath(tenantOK, spaceOK),
		}
		return strategy
	}
	global, globalOK := decodeGlobalBusiness(identityDTO.GlobalBusiness)
	if !globalOK {
		// Not read as false. A writer that meant true and spelled it
		// otherwise would have the strategy run as an ordinary one,
		// scoped to its own business's space: every other business's
		// data gone with nothing on the page to say so.
		strategy.SourceDisposition = &ObjectDisposition{
			SourceID: id, Scope: "STRATEGY", Disposition: DispositionConfigRejected,
			Reason: ReasonGlobalStrategyInvalid, FieldPath: "is_global_strategy",
		}
		return strategy
	}
	strategy.Identity = SourceIdentity{TenantID: tenantID, BusinessID: strconv.FormatInt(identityDTO.BusinessID, 10), SpaceScope: spaceUID, GlobalBusiness: global}
	return strategy
}

// ChangeSignal reads <prefix>.last_updated. The cache manager's incremental
// refresh writes it, as the integer second the run started, after it has
// written every strategy document of a run that found changes, and returns
// before touching it when a run finds none; its full refresh never writes it.
// So an unchanged value means no strategy was saved since the previous read,
// and says nothing about content the manager derives from other tables and
// rewrites in place. A marker that is absent, or whose value is not a
// positive integer, is reported as absent: the round then reads everything, as
// it did before the marker was consulted. A read that fails is not: the error
// is returned, and the refresh ends at its change-signal exit
// (SourceReconciler.observe) rather than reading every document without
// knowing whether the store answers.
func (source *LegacyRedisStrategySource) ChangeSignal(ctx context.Context) (SourceChangeSignal, error) {
	if source == nil || source.client == nil {
		return SourceChangeSignal{}, errors.New("alarmd controlplane: legacy Redis strategy source is required")
	}
	payload, err := source.client.Get(ctx, source.lastUpdatedKey).Result()
	if errors.Is(err, redis.Nil) {
		return SourceChangeSignal{}, nil
	}
	if err != nil {
		return SourceChangeSignal{}, fmt.Errorf("alarmd controlplane: read legacy strategy change signal: %w", err)
	}
	seconds, parseErr := strconv.ParseInt(strings.TrimSpace(payload), 10, 64)
	if parseErr != nil || seconds <= 0 {
		return SourceChangeSignal{}, nil
	}
	return SourceChangeSignal{Present: true, Value: payload, WrittenAt: time.Unix(seconds, 0)}, nil
}

// missingIdentityFieldPath names the identity field or fields a document did
// not carry usably, in the document's own key names.
func missingIdentityFieldPath(tenantOK, spaceOK bool) string {
	switch {
	case !tenantOK && !spaceOK:
		return "bk_tenant_id,space_uid"
	case !tenantOK:
		return "bk_tenant_id"
	default:
		return "space_uid"
	}
}

// ReasonGlobalStrategyInvalid refuses a strategy document whose
// is_global_strategy is present and is not a JSON boolean.
const ReasonGlobalStrategyInvalid = "STRATEGY_GLOBAL_INVALID"

// decodeGlobalBusiness reads the optional is_global_strategy. Absent is false;
// present, it must be true or false, and anything else - null, a string,
// a number - is refused rather than guessed.
func decodeGlobalBusiness(payload json.RawMessage) (bool, bool) {
	switch strings.TrimSpace(string(payload)) {
	case "":
		return false, true
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func decodeRequiredIdentityString(payload json.RawMessage) (string, bool) {
	if len(payload) == 0 {
		return "", false
	}
	var value string
	if err := json.Unmarshal(payload, &value); err != nil || value == "" || strings.TrimSpace(value) != value {
		return "", false
	}
	return value, true
}

func legacyRedisBytes(value interface{}) ([]byte, bool) {
	switch typed := value.(type) {
	case string:
		return []byte(typed), true
	case []byte:
		return append([]byte(nil), typed...), true
	default:
		return nil, false
	}
}
