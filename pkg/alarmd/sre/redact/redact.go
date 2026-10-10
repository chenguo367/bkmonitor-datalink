// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

// Package redact removes known deployment credentials and common credential
// fields from diagnostic evidence. Arbitrary application content requires its
// own domain masking; this contract does not promise to identify every secret.
package redact

import (
	"encoding/json"
	"regexp"
	"strings"
)

const Contract = "deployment_known_credentials_and_common_fields/v1"

var assignment = regexp.MustCompile(`(?i)(["']?(?:password|passwd|secret|client_secret|api_key|private_key|admin_key|access_token|refresh_token|authorization|cookie)["']?\s*[:=]\s*)(?:"[^"\r\n]*"|'[^'\r\n]*'|[^,\s}\]]+)`)
var bearer = regexp.MustCompile(`(?i)\b(Bearer|Basic)\s+[A-Za-z0-9+/_.=-]+`)

type Scrubber struct{ values []string }

func New(values ...string) *Scrubber {
	s := &Scrubber{}
	for _, value := range values {
		if value != "" {
			s.values = append(s.values, value)
		}
	}
	return s
}

func (s *Scrubber) Text(value string) string {
	for _, secret := range s.values {
		value = strings.ReplaceAll(value, secret, "[REDACTED]")
	}
	value = bearer.ReplaceAllString(value, "[REDACTED credential]")
	return assignment.ReplaceAllString(value, `${1}"[REDACTED]"`)
}

func (s *Scrubber) JSON(raw json.RawMessage) json.RawMessage {
	var value any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		return nil
	}
	encoded, err := json.Marshal(s.value(value))
	if err != nil {
		return nil
	}
	return encoded
}

func (s *Scrubber) value(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			normalized := strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(key))
			switch normalized {
			case "password", "passwd", "secret", "clientsecret", "apikey", "privatekey", "adminkey", "token", "accesstoken", "refreshtoken", "authorization", "proxyauthorization", "cookie", "setcookie", "credentials", "headers", "xbkapijwt", "identityassertion":
				out[key] = "[REDACTED]"
			default:
				out[s.Text(key)] = s.value(item)
			}
		}
		return out
	case []any:
		for i := range v {
			v[i] = s.value(v[i])
		}
		return v
	case string:
		return s.Text(v)
	default:
		return v
	}
}
