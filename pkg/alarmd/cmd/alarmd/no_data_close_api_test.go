// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/observability"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/state"
)

type recordedClose struct {
	tenant, strategy, fingerprint string
	at                            time.Time
}

type fakeCloseRecorder struct {
	closes []recordedClose
	err    error
}

func (recorder *fakeCloseRecorder) RecordNoDataManualClose(_ context.Context, tenant, strategy, fingerprint string, at time.Time) error {
	if recorder.err != nil {
		return recorder.err
	}
	recorder.closes = append(recorder.closes, recordedClose{tenant, strategy, fingerprint, at})
	return nil
}

func postNoDataClose(handler http.Handler, method, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, noDataClosePath, strings.NewReader(body)))
	return response
}

// The close is recorded at the time alarmd received it, and the 200 says
// nothing but that it was recorded.
func TestANoDataCloseIsRecordedAtItsReceiptTime(t *testing.T) {
	received := time.Unix(1_790_000_000, 0)
	recorder := &fakeCloseRecorder{}
	handler := newNoDataCloseAPI(recorder, func() time.Time { return received })
	for _, body := range []string{
		`{"bk_tenant_id":"system","strategy_id":123,"source_alert_id":"0123456789abcdef0123456789abcdef"}`,
		`{"bk_tenant_id":"system","strategy_id":"123","source_alert_id":"0123456789abcdef0123456789abcdef"}`,
	} {
		response := postNoDataClose(handler, http.MethodPost, body)
		if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "{}" {
			t.Fatalf("%s answered %d %s", body, response.Code, response.Body.String())
		}
	}
	want := recordedClose{"system", "123", "0123456789abcdef0123456789abcdef", received}
	if len(recorder.closes) != 2 || recorder.closes[0] != want || recorder.closes[1] != want {
		t.Fatalf("recorded %+v, want %+v twice", recorder.closes, want)
	}
}

// Each refusal is a 4xx of its own for a request that is truly invalid,
// named in the answer's code and counted; a store that fails answers 503,
// the link retries; nothing else is refused.
func TestANoDataCloseRefusesWhatItCannotRecord(t *testing.T) {
	valid := `{"bk_tenant_id":"s","strategy_id":1,"source_alert_id":"0123456789abcdef0123456789abcdef"}`
	for _, testCase := range []struct {
		name, method, body string
		err                error
		status             int
		code               string
	}{
		{"no tenant", http.MethodPost, `{"strategy_id":1,"source_alert_id":"0123456789abcdef0123456789abcdef"}`, nil, 400, noDataCloseIdentityInvalid},
		{"zero strategy", http.MethodPost, `{"bk_tenant_id":"s","strategy_id":0,"source_alert_id":"0123456789abcdef0123456789abcdef"}`, nil, 400, noDataCloseIdentityInvalid},
		{"not a fingerprint", http.MethodPost, `{"bk_tenant_id":"s","strategy_id":1,"source_alert_id":"abc"}`, nil, 400, noDataCloseIdentityInvalid},
		{"an unknown field", http.MethodPost, `{"bk_tenant_id":"s","strategy_id":1,"source_alert_id":"0123456789abcdef0123456789abcdef","closed_at":1}`, nil, 400, noDataCloseBodyNotObject},
		{"not JSON", http.MethodPost, `bk_tenant_id=s`, nil, 400, noDataCloseBodyNotObject},
		{"trailing data", http.MethodPost, valid + `{}`, nil, 400, noDataCloseBodyNotObject},
		{"too large", http.MethodPost, `{"bk_tenant_id":"` + strings.Repeat("s", maxNoDataCloseBodyBytes) + `"}`, nil, 400, noDataCloseBodyUnreadable},
		{"not a POST", http.MethodGet, ``, nil, 405, noDataCloseMethodNotAllowed},
		{"the store fails", http.MethodPost, valid, errors.New("connection refused"), 503, noDataCloseStoreUnavailable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			handler := newNoDataCloseAPI(&fakeCloseRecorder{err: testCase.err}, time.Now)
			response := postNoDataClose(handler, testCase.method, testCase.body)
			if response.Code != testCase.status || !strings.Contains(response.Body.String(), `"code":"`+testCase.code+`"`) {
				t.Fatalf("answered %d %s, want %d %s", response.Code, response.Body.String(), testCase.status, testCase.code)
			}
			if handler.Stats()[testCase.code] != 1 {
				t.Fatalf("counts %v, want one %s", handler.Stats(), testCase.code)
			}
		})
	}
}

// A strategy whose close hash is at its bound does not stop the user's
// close: a refusal would become the link's 502 and the close would fail
// there for a day. It answers 200, records nothing, and is counted.
func TestAFullCloseStoreDoesNotRefuseTheUsersClose(t *testing.T) {
	handler := newNoDataCloseAPI(&fakeCloseRecorder{err: fmt.Errorf("wrapped: %w", state.ErrNoDataManualClosesFull)}, time.Now)
	response := postNoDataClose(handler, http.MethodPost, `{"bk_tenant_id":"s","strategy_id":1,"source_alert_id":"0123456789abcdef0123456789abcdef"}`)
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "{}" || handler.Stats()[noDataCloseFullSkipped] != 1 ||
		handler.Stats()[noDataCloseRecorded] != 0 {
		t.Fatalf("answered %d %s, counts %v", response.Code, response.Body.String(), handler.Stats())
	}
}

// A process that serves the route with no internal listener says so once
// at startup and counts it; with one it says nothing.
func TestTheNoDataCloseRouteWithoutAnInternalListenerIsSaidAndCounted(t *testing.T) {
	for listen, want := range map[string]uint64{"": 1, "127.0.0.1:0": 0} {
		api := newNoDataCloseAPI(&fakeCloseRecorder{}, time.Now)
		served := false
		unserved := bindNoDataCloseAPI(api, listen, nil, observability.Discard(observability.ComponentRuntime), func(http.Handler) { served = true })
		if !served || unserved != (want == 1) || api.Stats()[noDataCloseInternalListenOff] != want {
			t.Fatalf("listen %q: served %v unserved %v counts %v", listen, served, unserved, api.Stats())
		}
	}
}
