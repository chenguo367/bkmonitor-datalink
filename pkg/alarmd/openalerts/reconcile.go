// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License.

package openalerts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
)

// Reconciliation is a complete alert-store observation. Missing and Suppressed
// are the index differences to preserve across ordinary SET reads.
type Reconciliation struct {
	Members    []string
	Missing    []string
	Suppressed []string
	Alerts     []Alert
}

// Severity is optional in older Console responses. Its absence never prevents
// membership recovery, but cannot be used as a fabricated close severity.
type Alert struct {
	AlertID       string `json:"alertId"`
	EventSourceID string `json:"eventSourceId"`
	Fingerprint   string `json:"fingerprint"`
	Severity      string `json:"severity,omitempty"`
}

type Reconciler interface {
	Reconcile(context.Context, StrategyKey) (Reconciliation, error)
}

// TargetBinding must be supplied from deployment configuration. It is checked
// against /targets and each response; the first target is never auto-selected.
type TargetBinding struct {
	EventSourceID string   `json:"eventSourceId"`
	HookName      string   `json:"hookName"`
	KeyPrefix     string   `json:"keyPrefix"`
	Address       string   `json:"address"`
	Database      int      `json:"database"`
	Sources       []string `json:"sources"`
}

type HTTPReconcilerOptions struct {
	BaseURL          string
	Username         string
	Password         string
	Client           *http.Client
	Binding          TargetBinding
	MaxResponseBytes int64
}

type HTTPReconciler struct{ options HTTPReconcilerOptions }

func NewHTTPReconciler(options HTTPReconcilerOptions) (*HTTPReconciler, error) {
	u, err := url.Parse(options.BaseURL)
	b := options.Binding
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		options.Client == nil || options.MaxResponseBytes <= 0 || options.Username == "" || options.Password == "" ||
		b.EventSourceID == "" || b.HookName == "" || !validPrefix(b.KeyPrefix) || b.Address == "" || b.Database < 0 || len(b.Sources) == 0 || len(b.Sources) > 64 {
		return nil, errors.New("alarmd openalerts: invalid reconciliation endpoint or binding")
	}
	b.Sources = append([]string(nil), b.Sources...)
	sort.Strings(b.Sources)
	found := false
	for i, source := range b.Sources {
		if source == "" || (i > 0 && source == b.Sources[i-1]) {
			return nil, errors.New("alarmd openalerts: invalid source scope")
		}
		found = found || source == b.EventSourceID
	}
	if !found {
		return nil, errors.New("alarmd openalerts: binding source is outside allowed scope")
	}
	options.Binding = b
	// Do not forward Basic Auth to another endpoint through a redirect.
	client := *options.Client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	options.Client = &client
	return &HTTPReconciler{options: options}, nil
}

func (reader *HTTPReconciler) get(ctx context.Context, path string, query url.Values, out any) error {
	return reader.getPath(ctx, "/local-api/strategy-index/"+path, query, out)
}

func (reader *HTTPReconciler) getPath(ctx context.Context, path string, query url.Values, out any) error {
	u := strings.TrimRight(reader.options.BaseURL, "/") + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return errors.New("alarmd openalerts: invalid reconcile request")
	}
	req.SetBasicAuth(reader.options.Username, reader.options.Password)
	response, err := reader.options.Client.Do(req)
	if err != nil {
		return errors.New("alarmd openalerts: reconcile request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return statusError(response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, reader.options.MaxResponseBytes+1))
	if err != nil || int64(len(data)) > reader.options.MaxResponseBytes {
		return ErrIncomplete
	}
	if json.Unmarshal(data, out) != nil {
		return ErrIncomplete
	}
	return nil
}

func (reader *HTTPReconciler) matches(target TargetBinding) bool {
	target.Sources = append([]string(nil), target.Sources...)
	sort.Strings(target.Sources)
	return reflect.DeepEqual(reader.options.Binding, target)
}

func (reader *HTTPReconciler) Reconcile(ctx context.Context, key StrategyKey) (Reconciliation, error) {
	if !validStrategyKey(key) {
		return Reconciliation{}, errors.New("alarmd openalerts: invalid strategy identity")
	}
	var targets []TargetBinding
	if err := reader.get(ctx, "targets", nil, &targets); err != nil {
		return Reconciliation{}, err
	}
	if len(targets) > 512 {
		return Reconciliation{}, ErrIncomplete
	}
	found := false
	for _, target := range targets {
		if target.EventSourceID == reader.options.Binding.EventSourceID && target.HookName == reader.options.Binding.HookName {
			if found || !reader.matches(target) {
				return Reconciliation{}, errors.New("alarmd openalerts: reconciliation binding changed")
			}
			found = true
		}
	}
	if !found {
		return Reconciliation{}, errors.New("alarmd openalerts: reconciliation binding missing")
	}
	var response struct {
		Target     TargetBinding `json:"target"`
		TenantID   string        `json:"tenantId"`
		StrategyID string        `json:"strategyId"`
		Key        string        `json:"key"`
		Complete   bool          `json:"complete"`
		Redis      struct {
			Complete bool `json:"complete"`
		} `json:"redis"`
		Alerts struct {
			Complete bool `json:"complete"`
		} `json:"alerts"`
		Rows []struct {
			Fingerprint string  `json:"fingerprint"`
			Status      string  `json:"status"`
			Alerts      []Alert `json:"alerts"`
		} `json:"rows"`
	}
	b := reader.options.Binding
	query := url.Values{"event_source_id": {b.EventSourceID}, "hook_name": {b.HookName}, "bk_tenant_id": {key.TenantID}, "strategy_id": {key.StrategyID}}
	if err := reader.get(ctx, "reconcile", query, &response); err != nil {
		return Reconciliation{}, err
	}
	if !response.Complete || !response.Redis.Complete || !response.Alerts.Complete || response.Rows == nil || len(response.Rows) > 10000 {
		return Reconciliation{}, ErrIncomplete
	}
	if !reader.matches(response.Target) || response.TenantID != key.TenantID || response.StrategyID != key.StrategyID || response.Key != b.KeyPrefix+":"+key.TenantID+":"+key.StrategyID {
		return Reconciliation{}, errors.New("alarmd openalerts: reconciliation identity mismatch")
	}
	result := Reconciliation{}
	seen := make(map[string]struct{}, len(response.Rows))
	activeCount := 0
	for _, row := range response.Rows {
		if row.Fingerprint == "" || len(row.Fingerprint) > 4096 || row.Alerts == nil {
			return Reconciliation{}, ErrIncomplete
		}
		if _, exists := seen[row.Fingerprint]; exists {
			return Reconciliation{}, ErrIncomplete
		}
		seen[row.Fingerprint] = struct{}{}
		for _, alert := range row.Alerts {
			activeCount++
			if activeCount > 5000 || alert.AlertID == "" || alert.Fingerprint != row.Fingerprint || !containsSource(b.Sources, alert.EventSourceID) {
				return Reconciliation{}, ErrIncomplete
			}
		}
		switch row.Status {
		case "matched", "missing_redis":
			if len(row.Alerts) == 0 {
				return Reconciliation{}, ErrIncomplete
			}
			result.Members = append(result.Members, row.Fingerprint)
			result.Alerts = append(result.Alerts, row.Alerts...)
			if row.Status == "missing_redis" {
				result.Missing = append(result.Missing, row.Fingerprint)
			}
		case "redis_only":
			if len(row.Alerts) != 0 {
				return Reconciliation{}, ErrIncomplete
			}
			result.Suppressed = append(result.Suppressed, row.Fingerprint)
		default:
			return Reconciliation{}, ErrIncomplete
		}
	}
	return result, nil
}

// statusError is a Console answer other than 200. It keeps the code so a
// caller for which one code is an answer rather than a failure - the alert
// record's 404 - can tell it apart.
type statusError int

func (code statusError) Error() string {
	return fmt.Sprintf("alarmd openalerts: reconcile HTTP status %d", int(code))
}

func containsSource(sources []string, value string) bool {
	index := sort.SearchStrings(sources, value)
	return index < len(sources) && sources[index] == value
}
