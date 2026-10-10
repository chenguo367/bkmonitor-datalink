package uq

import (
	"errors"
	"net/http"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// ClientOptions selects only a provider-local wire format. It is not part of
// the physical query, Plan, Slot or State identity. Empty means legacy only.
type ClientOptions struct{ SharedSchemaQueryGroups []string }

// NewClientWithOptions keeps existing constructors and their default behavior
// unchanged. Recheck, Range and diagnostics do not opt in even with this client.
func NewClientWithOptions(endpoint, querySource string, httpClient *http.Client, limits Limits, options ClientOptions) (*Client, error) {
	client, err := NewClientWithLimits(endpoint, querySource, httpClient, limits)
	if err != nil {
		return nil, err
	}
	client.sharedQueryGroups = make(map[execution.QueryGroupIdentity]struct{}, len(options.SharedSchemaQueryGroups))
	for _, group := range options.SharedSchemaQueryGroups {
		if group == "" || strings.TrimSpace(group) != group || len(group) > 256 || strings.ContainsAny(group, "\t\n\r ") {
			return nil, errors.New("alarmd access uq: invalid shared schema query group")
		}
		if group == "*" {
			if len(options.SharedSchemaQueryGroups) != 1 {
				return nil, errors.New("alarmd access uq: shared schema wildcard must be the only entry")
			}
			client.sharedAll = true
		} else {
			for _, c := range group {
				if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("_.:-", c)) {
					return nil, errors.New("alarmd access uq: invalid shared schema query group")
				}
			}
		}
		key := execution.QueryGroupIdentity(group)
		if _, duplicate := client.sharedQueryGroups[key]; duplicate {
			return nil, errors.New("alarmd access uq: duplicate shared schema query group")
		}
		client.sharedQueryGroups[key] = struct{}{}
	}
	return client, nil
}

func (client *Client) sharedFor(attempt execution.QueryAttempt) bool {
	if attempt.Spec.PlanFacts.PromQL != nil || attempt.Spec.PlanFacts.Normalization.Version == "uq-polling-normalization-v1" {
		return false
	}
	_, selected := client.sharedQueryGroups[attempt.Slot.QueryGroup]
	return client.sharedAll || selected
}
