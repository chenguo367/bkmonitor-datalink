// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package uq

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// phasedTransport answers after the provider's wait - the clock moved on by
// wait when the request is sent - with a body whose reading takes body more,
// the clock moved on again when it is read to its end.
type phasedTransport struct {
	offset     *atomic.Int64
	wait, body time.Duration
	answer     string
	status     int
	fail       error
}

func (transport phasedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.offset.Store(int64(transport.wait))
	if transport.fail != nil {
		return nil, transport.fail
	}
	status := transport.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(&clockedBody{reader: strings.NewReader(transport.answer), offset: transport.offset,
			done: transport.wait + transport.body}), Request: request}, nil
}

type clockedBody struct {
	reader *strings.Reader
	offset *atomic.Int64
	done   time.Duration
}

func (body *clockedBody) Read(p []byte) (int, error) {
	n, err := body.reader.Read(p)
	if errors.Is(err, io.EOF) {
		body.offset.Store(int64(body.done))
	}
	return n, err
}

// A query's time is split where its response's headers came in: the wait
// before them, the provider computing the query, and the body after, read
// and decoded; the two are the query's time, an upstream 504's too. A query
// no response came back to waited the whole of it and answered nothing.
func TestAQuerysTimeIsSplitAtItsAnswersFirstByte(t *testing.T) {
	base := time.Unix(1_700_123_500, 0)
	var offset atomic.Int64
	clientOf := func(transport phasedTransport) *Client {
		t.Helper()
		client, err := NewClientWithLimits("http://uq.invalid", "alarmd-shadow", &http.Client{Transport: transport}, DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		client.now = func() time.Time { return base.Add(time.Duration(offset.Load())) }
		return client
	}
	offset.Store(0)
	answered := clientOf(phasedTransport{offset: &offset, wait: 3 * time.Second, body: 1500 * time.Millisecond,
		answer: `{"series":[` + identityTestSeriesJSON() + `],"is_partial":false}`})
	completion, err := answered.Execute(context.Background(), validAttempt(t), &collectingSink{})
	if err != nil {
		t.Fatal(err)
	}
	if stats := completion.Stats; !stats.Answered || stats.WaitMillis != 3000 || stats.BodyMillis != 1500 || stats.QueryMillis != 4500 {
		t.Fatalf("an answered query's time %d ms split %d + %d, want 4500 as 3000 waiting and 1500 reading", stats.QueryMillis,
			stats.WaitMillis, stats.BodyMillis)
	}

	offset.Store(0)
	unanswered := clientOf(phasedTransport{offset: &offset, wait: 2 * time.Second, fail: errors.New("connection reset")})
	completion, err = unanswered.Execute(context.Background(), validAttempt(t), &collectingSink{})
	if err != nil {
		t.Fatal(err)
	}
	if stats := completion.Stats; stats.Answered || stats.WaitMillis != stats.QueryMillis || stats.QueryMillis != 2000 || stats.BodyMillis != 0 {
		t.Fatalf("an unanswered query's time %d ms split %d + %d (answered %v), want all of it waiting", stats.QueryMillis, stats.WaitMillis,
			stats.BodyMillis, stats.Answered)
	}

	// The provider answering that its backend ran out of time answered: its
	// wait is the provider's, before the headers.
	offset.Store(0)
	upstream := clientOf(phasedTransport{offset: &offset, wait: 30 * time.Second, body: 200 * time.Millisecond, status: http.StatusGatewayTimeout,
		answer: `{"error":"backend timeout"}`})
	completion, err = upstream.Execute(context.Background(), validAttempt(t), &collectingSink{})
	if err != nil {
		t.Fatal(err)
	}
	if stats := completion.Stats; !stats.Answered || stats.WaitMillis != 30000 || stats.WaitMillis+stats.BodyMillis != stats.QueryMillis {
		t.Fatalf("an upstream 504's time %d ms split %d + %d (answered %v), want 30000 waiting", stats.QueryMillis, stats.WaitMillis,
			stats.BodyMillis, stats.Answered)
	}
}
