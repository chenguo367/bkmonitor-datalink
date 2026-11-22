// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package observability

// What became of a Query Group's handover ledger (014 B4): what a process
// that lets a Query Group go leaves under its fence for the next owner,
// and the next owner reads once its lease is held. Closed: a metric label.
//
// On the writer: written; empty, nothing to hand over and nothing written;
// write_failed, the write did not land (the fence refused it, another write
// raced it, or Redis failed) and the next owner does as it did before the
// ledger existed. On the reader: loaded; absent, none left; stale, one left
// by an owner before the previous one - the previous one left none, and
// what an older owner held may have ended since; unreadable; read_failed.
const (
	HandoverLedgerWritten     = "written"
	HandoverLedgerEmpty       = "empty"
	HandoverLedgerWriteFailed = "write_failed"
	HandoverLedgerLoaded      = "loaded"
	HandoverLedgerAbsent      = "absent"
	HandoverLedgerStale       = "stale"
	HandoverLedgerUnreadable  = "unreadable"
	HandoverLedgerReadFailed  = "read_failed"
)

// HandoverLedgerResults is every result, for the metric that pre-creates them.
var HandoverLedgerResults = []string{HandoverLedgerWritten, HandoverLedgerEmpty, HandoverLedgerWriteFailed,
	HandoverLedgerLoaded, HandoverLedgerAbsent, HandoverLedgerStale, HandoverLedgerUnreadable, HandoverLedgerReadFailed}
