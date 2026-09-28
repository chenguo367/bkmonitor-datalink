// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package cli

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const stdoutBudget = 20 << 10

func sensitiveKey(key string) bool {
	k := strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(key))
	switch k {
	case "adminkey", "authorization", "proxyauthorization", "accesstoken", "refreshtoken", "token", "grantsecret", "grant", "grantcode", "logincode", "password", "passwd", "secret", "clientsecret", "cookie", "setcookie", "assertion", "identityassertion", "jwt", "xbkapijwt", "headers", "requestheaders", "responseheaders", "credentials":
		return true
	}
	return false
}

func redact(v any, secrets []string) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, value := range x {
			if sensitiveKey(k) {
				continue
			}
			out[redact(k, secrets).(string)] = redact(value, secrets)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = redact(x[i], secrets)
		}
		return out
	case string:
		for _, s := range secrets {
			if s != "" {
				x = strings.ReplaceAll(x, s, "[REDACTED]")
			}
		}
		// A server must not emit authorization bundles, even in free text.
		if strings.Contains(x, "alarmd-login-v1.") {
			return "[REDACTED authorization bundle]"
		}
		return x
	default:
		return v
	}
}

func (a *App) print(value any) int {
	data, err := json.Marshal(value)
	if err != nil {
		return 1
	}
	if len(data)+1 > stdoutBudget {
		data = []byte(`{"status":"error","error":{"code":"output_too_large","message":"local output exceeds 20 KiB"}}`)
		_, _ = fmt.Fprintln(a.Out, string(data))
		return 1
	}
	if _, err := fmt.Fprintln(a.Out, string(data)); err != nil {
		return 1
	}
	return 0
}

func (a *App) fail(code, message string, exit int) int {
	a.print(map[string]any{"status": "error", "summary": message, "error": map[string]any{"code": code, "message": message}, "evidence": map[string]any{"complete": false, "limitations": []string{message}}, "next_call": []any{}})
	return exit
}

func (a *App) emitResponse(raw map[string]any, secrets []string, httpStatus int) int {
	result := redact(raw, secrets).(map[string]any)
	if httpStatus < 200 || httpStatus >= 300 {
		result["status"] = "error"
	}
	meta := objectField(result, "meta")
	if meta == nil {
		meta = map[string]any{}
		result["meta"] = meta
	}
	if httpStatus < 200 || httpStatus >= 300 {
		meta["http_status"] = httpStatus
	}
	path, err := a.saveEvidence(result)
	if err != nil {
		return a.fail("evidence_write_failed", err.Error(), 1)
	}
	data, _ := json.Marshal(result)
	code := 0
	if stringField(result, "status") == "partial" {
		code = 3
	}
	if stringField(result, "status") == "error" || httpStatus < 200 || httpStatus >= 300 {
		code = 1
	}
	if len(data)+1 > stdoutBudget {
		result = map[string]any{"status": result["status"], "summary": result["summary"], "evidence": result["evidence"], "next_call": result["next_call"], "meta": meta, "result_omitted": true}
		if raw["error"] != nil {
			result["error"] = redact(raw["error"], secrets)
		}
		compact, _ := json.Marshal(result)
		if len(compact)+1 > stdoutBudget {
			result = map[string]any{"status": result["status"], "summary": "Response exceeds stdout budget; read meta.result_file for complete redacted evidence.", "meta": map[string]any{"result_file": path}, "result_omitted": true}
		}
	}
	if a.print(result) != 0 {
		return 1
	}
	return code
}

// saveEvidence writes one redacted record atomically to the private
// evidence directory and names the file in its meta.result_file.
func (a *App) saveEvidence(result map[string]any) (string, error) {
	meta := objectField(result, "meta")
	if meta == nil {
		meta = map[string]any{}
		result["meta"] = meta
	}
	dir := filepath.Join(a.Store.Dir, "results")
	if err := secureDir(dir); err != nil {
		return "", errors.New("cannot create the private evidence directory")
	}
	id := make([]byte, 12)
	if _, err := rand.Read(id); err != nil {
		return "", errors.New("cannot create an evidence identifier")
	}
	path, err := filepath.Abs(filepath.Join(dir, time.Now().UTC().Format("20060102T150405.000000000Z")+"-"+hex.EncodeToString(id)+".json"))
	if err != nil {
		return "", errors.New("cannot resolve evidence path")
	}
	meta["result_file"] = path
	data, err := json.Marshal(result)
	if err != nil || atomicWrite(path, append(data, '\n')) != nil {
		return "", errors.New("cannot save the complete redacted response")
	}
	return path, nil
}

func protocolError(err error) error {
	if err == nil {
		return nil
	}
	return errors.New("server response rejected: " + err.Error())
}
