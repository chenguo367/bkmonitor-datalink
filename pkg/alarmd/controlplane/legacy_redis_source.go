package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
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
//
// Beside the active set it reads <prefix>.publisher, the record a publisher
// may leave about its own last run, in the same MGET; see PublisherReport.
type LegacyRedisStrategySource struct {
	client          legacyRedisCommands
	strategyIDsKey  string
	strategyKeyStem string
	lastUpdatedKey  string

	publisherKey string
	now          func() time.Time
	mu           sync.Mutex
	publisher    *PublisherReport
	published    publisherCounter
}

func NewLegacyRedisStrategySource(client redis.Cmdable, cachePrefix string) (*LegacyRedisStrategySource, error) {
	if client == nil || cachePrefix == "" || strings.ContainsAny(cachePrefix, "{} \t\r\n") {
		return nil, errors.New("alarmd controlplane: invalid legacy Redis strategy source")
	}
	return &LegacyRedisStrategySource{
		client: client, strategyIDsKey: cachePrefix + ".strategy_ids",
		strategyKeyStem: cachePrefix + ".strategy_",
		lastUpdatedKey:  cachePrefix + ".last_updated",
		publisherKey:    cachePrefix + ".publisher", now: time.Now,
	}, nil
}

func (source *LegacyRedisStrategySource) ActiveStrategyIDs(ctx context.Context) ([]string, error) {
	if source == nil || source.client == nil {
		return nil, errors.New("alarmd controlplane: legacy Redis strategy source is required")
	}
	// One round trip for both: the publisher's record is read on every read
	// of the active set, and kept before the active set is judged, so a
	// round the active set refuses still says what the publisher said.
	values, err := source.client.MGet(ctx, source.strategyIDsKey, source.publisherKey).Result()
	if err != nil {
		return nil, fmt.Errorf("alarmd controlplane: read legacy strategy active set: %w", err)
	}
	if len(values) != 2 {
		return nil, fmt.Errorf("alarmd controlplane: read legacy strategy active set: %d values for 2 keys", len(values))
	}
	if !publisherReportUnrecorded(ctx) {
		source.notePublisher(decodePublisherReport(values[1], source.now()))
	}
	payload, ok := legacyRedisBytes(values[0])
	if values[0] == nil || (ok && len(payload) == 0) {
		return nil, ErrLegacySourceIncomplete
	}
	if !ok {
		return nil, fmt.Errorf("%w: active strategy set is a %T", ErrLegacySourceIncomplete, values[0])
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

func (source *LegacyRedisStrategySource) notePublisher(report PublisherReport) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.publisher = &report
	source.published.note(report)
}

// PublisherReport is the publisher's record as this process last read it,
// and false when it has not read the active set yet.
func (source *LegacyRedisStrategySource) PublisherReport() (PublisherReport, bool) {
	if source == nil {
		return PublisherReport{}, false
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.publisher == nil {
		return PublisherReport{}, false
	}
	return *source.publisher, true
}

// PublisherReportCounts is how many distinct records this process has read,
// by outcome and reason; see publisherCounter.
func (source *LegacyRedisStrategySource) PublisherReportCounts() []PublisherReportCount {
	if source == nil {
		return nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.published.snapshot()
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
	values := make([]interface{}, 0, len(keys))
	for start := 0; start < len(keys); start += legacyStrategyMGetChunk {
		chunk := keys[start:min(start+legacyStrategyMGetChunk, len(keys))]
		read, err := source.client.MGet(ctx, chunk...).Result()
		if err != nil {
			return nil, fmt.Errorf("alarmd controlplane: read legacy strategy objects: %w", err)
		}
		values = append(values, read...)
	}
	if len(values) != len(ids) {
		return nil, ErrObservationUnstable
	}
	strategies := make([]SourceStrategy, 0, len(ids))
	for index, value := range values {
		payload, ok := legacyRedisBytes(value)
		if !ok || len(payload) == 0 {
			strategies = append(strategies, SourceStrategy{SourceID: ids[index], SourceDisposition: &ObjectDisposition{
				SourceID: ids[index], Scope: "STRATEGY", Disposition: DispositionSourceIncomplete,
				Reason: "SOURCE_OBJECT_INCOMPLETE",
			}})
			continue
		}
		strategy := SourceStrategy{SourceID: ids[index], Document: append(json.RawMessage(nil), payload...)}
		var identityDTO struct {
			ID             int64           `json:"id"`
			BusinessID     int64           `json:"bk_biz_id"`
			TenantID       json.RawMessage `json:"bk_tenant_id"`
			SpaceUID       json.RawMessage `json:"space_uid"`
			GlobalBusiness json.RawMessage `json:"is_global_strategy"`
		}
		if err := json.Unmarshal(payload, &identityDTO); err != nil {
			strategy.SourceDisposition = &ObjectDisposition{
				SourceID: ids[index], Scope: "STRATEGY", Disposition: DispositionConfigRejected,
				Reason: "STRATEGY_DOCUMENT_INVALID",
			}
			strategies = append(strategies, strategy)
			continue
		}
		if identityDTO.ID <= 0 || strconv.FormatInt(identityDTO.ID, 10) != ids[index] {
			strategy.SourceDisposition = &ObjectDisposition{
				SourceID: ids[index], Scope: "STRATEGY", Disposition: DispositionConfigRejected,
				Reason: "STRATEGY_IDENTITY_INVALID",
			}
			strategies = append(strategies, strategy)
			continue
		}
		if identityDTO.BusinessID == 0 {
			strategy.SourceDisposition = &ObjectDisposition{
				SourceID: ids[index], Scope: "STRATEGY", Disposition: DispositionConfigRejected,
				Reason: "STRATEGY_BUSINESS_IDENTITY_INVALID",
			}
			strategies = append(strategies, strategy)
			continue
		}
		tenantID, tenantOK := decodeRequiredIdentityString(identityDTO.TenantID)
		spaceUID, spaceOK := decodeRequiredIdentityString(identityDTO.SpaceUID)
		if !tenantOK || !spaceOK {
			// Which field, not only that one was. The source page samples
			// this disposition with the strategy id and the reason, and a
			// reader of 47 such rows could not tell whether the writer had
			// stopped filling the tenant, the space, or both.
			strategy.SourceDisposition = &ObjectDisposition{
				SourceID: ids[index], Scope: "STRATEGY", Disposition: DispositionSourceIncomplete,
				Reason: "SOURCE_IDENTITY_UNAVAILABLE", FieldPath: missingIdentityFieldPath(tenantOK, spaceOK),
			}
			strategies = append(strategies, strategy)
			continue
		}
		global, globalOK := decodeGlobalBusiness(identityDTO.GlobalBusiness)
		if !globalOK {
			// Not read as false. A writer that meant true and spelled it
			// otherwise would have the strategy run as an ordinary one,
			// scoped to its own business's space: every other business's
			// data gone with nothing on the page to say so.
			strategy.SourceDisposition = &ObjectDisposition{
				SourceID: ids[index], Scope: "STRATEGY", Disposition: DispositionConfigRejected,
				Reason: ReasonGlobalStrategyInvalid, FieldPath: "is_global_strategy",
			}
			strategies = append(strategies, strategy)
			continue
		}
		strategy.Identity = SourceIdentity{TenantID: tenantID, BusinessID: strconv.FormatInt(identityDTO.BusinessID, 10), SpaceScope: spaceUID, GlobalBusiness: global}
		strategies = append(strategies, strategy)
	}
	return strategies, nil
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
