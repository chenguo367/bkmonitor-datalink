// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.
package pod

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/sre/redact"
)

// ConfigValueContract names how a ConfigMap value is shown: masked unless
// it is shown to be plain configuration.
const ConfigValueContract = "config_values_deny_by_default_v1"

// A ConfigMap is configuration, and configuration carries keys: crypt root
// keys and their IVs, instance keys, signing keys, rendered next to hosts and
// ports under names no list of credential words covers. Masking the values
// whose key names look like credentials showed every other value, and those
// keys with them.
//
// So a value is shown only when it is shown to be plain configuration, and
// otherwise masked as its size and fingerprint. The decision is made per
// value - per leaf of a YAML or JSON document, per line of other text - so
// a configuration file keeps its hosts, ports, switches and paths readable
// while the keys inside it are masked:
//
//   - a value under a credential-like name (configKey) is masked;
//   - a value that looks like key material (keyMaterial) is masked under any
//     name: a PEM block, a JWT, a run of base64 or hex of 16 bytes or more;
//   - otherwise it is shown only if it is plain (plainValue): a number, a
//     boolean, or text whose every word is short or reads like a word - a
//     host, a path, a duration - and not like a generated string.
//
// A line of other text - a Corefile directive, a hosts entry, a comment - is
// shown only when the whole line is plain and names nothing credential-like
// (configKey anywhere in it); every other line is masked.

// configKey is a name whose value is secret by what it names.
var configKey = regexp.MustCompile(`(?i)(passw|passwd|pwd|secret|token|credential|auth|cookie|session|private|crypt|cipher|sign|cert|salt|seed|nonce|apikey|api[_-]?key|access[_-]?key|(^|[_.\-])(key|iv|pin)([_.\-]|$)|(key|iv)$)`)

var (
	pemBlock   = regexp.MustCompile(`-----BEGIN [A-Z0-9 ]+-----`)
	lineKeyVal = regexp.MustCompile(`^(\s*(?:export\s+)?)([A-Za-z0-9_.\-]+)(\s*[:=]\s*)(.*)$`)
)

// keyMaterial says text holds something shaped like a key, whatever it is
// called (redact.KeyMaterial).
func keyMaterial(text string) bool { return redact.KeyMaterial(text) }

// plainValue says a scalar can be shown (redact.PlainValue).
func plainValue(text string) bool { return redact.PlainValue(text) }

// configMasker masks one ConfigMap value and counts what it masked.
type configMasker struct {
	fingerprints *fingerprints
	masked       int
	// splitPEM is set by a scalar holding part of a PEM block - its BEGIN
	// without its END, or an END alone - which the leaf rule cannot judge
	// whole (document).
	splitPEM bool
	// scalarPEM is set by a scalar holding a BEGIN line: a value whose PEM
	// header is in no scalar has it in a comment or a key, both of which a
	// document drops or shows unjudged (document).
	scalarPEM bool
}

func (m *configMasker) mask(text string) string {
	m.masked++
	return fmt.Sprintf("<redacted bytes=%d fingerprint=%s>", len(text), m.fingerprints.sum([]byte(text)))
}

// leaf is text under name, shown or masked.
func (m *configMasker) leaf(name, text string) string {
	if text == "" {
		return text
	}
	if referencePath(name, text) {
		return text
	}
	if configKey.MatchString(name) || !plainValue(text) {
		return m.mask(text)
	}
	return text
}

// referencePath says a value is a credential reference: a name ending in
// _file (password_file, admin_key_file) whose value alarmd would accept as
// a path and that reads as plain (redact.ShownCredentialPath). The path says where
// a credential is mounted, not what it is, and showing it is how a wrongly
// mounted key is diagnosed; so it is shown although the name reads like a
// credential. Any other value under such a name - the credential itself
// written where its path belongs, most often - is masked as any other.
func referencePath(name, text string) bool {
	return strings.HasSuffix(strings.ToLower(name), "_file") && redact.ShownCredentialPath(text)
}

// value is one ConfigMap value under its key, as it may be shown.
func (m *configMasker) value(key, text string) string {
	if !strings.ContainsAny(text, "\n") {
		return m.leaf(key, text)
	}
	// A value that is a PEM block, or a bundle of them, is key material
	// whole.
	if strings.HasPrefix(strings.TrimSpace(text), "-----BEGIN ") {
		return m.mask(text)
	}
	// A PEM block inside a document is one scalar, masked by the leaf rule
	// as key material whatever it is called; in other text its lines are
	// masked from BEGIN to END (lines). Neither masks the plain
	// configuration around it.
	if shown, ok := m.document(text); ok {
		return shown
	}
	return m.lines(text)
}

// document is text as a YAML (or JSON) document - every document of it, when
// it holds several - with every scalar decided on its own, or false when it
// is not one.
//
// A PEM block split across scalars, one line in each, is not a document the
// leaf rule can judge: its short last line and its END line read as plain
// alone. Such text is false too, and read line by line, where the block is
// masked from BEGIN to END - as is text whose PEM header is in no scalar but
// in a comment or a key, which the walk never judges.
func (m *configMasker) document(text string) (string, bool) {
	var nodes []*yaml.Node
	decoder := yaml.NewDecoder(strings.NewReader(text))
	for {
		var root yaml.Node
		err := decoder.Decode(&root)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", false
		}
		if len(root.Content) == 0 {
			continue
		}
		node := root.Content[0]
		if node.Kind != yaml.MappingNode && node.Kind != yaml.SequenceNode {
			return "", false
		}
		nodes = append(nodes, node)
	}
	if len(nodes) == 0 {
		return "", false
	}
	masked := m.masked
	for _, node := range nodes {
		m.walk(node, "")
	}
	if m.splitPEM || !m.scalarPEM && pemBlock.MatchString(text) {
		m.masked, m.splitPEM, m.scalarPEM = masked, false, false
		return "", false
	}
	m.scalarPEM = false
	if len(nodes) == 1 && (strings.HasPrefix(strings.TrimSpace(text), "{") || strings.HasPrefix(strings.TrimSpace(text), "[")) {
		var decoded any
		if err := nodes[0].Decode(&decoded); err == nil {
			if encoded, err := json.MarshalIndent(decoded, "", "  "); err == nil {
				return string(encoded), true
			}
		}
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	for _, node := range nodes {
		if err := encoder.Encode(node); err != nil {
			return "", false
		}
	}
	if err := encoder.Close(); err != nil {
		return "", false
	}
	return out.String(), true
}

func (m *configMasker) walk(node *yaml.Node, name string) {
	// A comment is free text; it is not shown.
	node.HeadComment, node.LineComment, node.FootComment = "", "", ""
	switch node.Kind {
	case yaml.MappingNode:
		for index := 0; index+1 < len(node.Content); index += 2 {
			keyNode := node.Content[index]
			keyNode.HeadComment, keyNode.LineComment, keyNode.FootComment = "", "", ""
			key := keyNode.Value
			value := node.Content[index+1]
			if value.Kind == yaml.ScalarNode {
				m.scalar(value, key)
				continue
			}
			m.walk(value, key)
		}
	case yaml.SequenceNode:
		for _, item := range node.Content {
			if item.Kind == yaml.ScalarNode {
				m.scalar(item, name)
				continue
			}
			m.walk(item, name)
		}
	case yaml.ScalarNode:
		m.scalar(node, name)
	case yaml.AliasNode, yaml.DocumentNode:
		for _, item := range node.Content {
			m.walk(item, name)
		}
	}
}

func (m *configMasker) scalar(node *yaml.Node, name string) {
	begins, ends := pemBlock.MatchString(node.Value), strings.Contains(node.Value, "-----END ")
	if begins != ends {
		m.splitPEM = true
	}
	m.scalarPEM = m.scalarPEM || begins
	if node.Tag == "!!bool" || node.Tag == "!!int" || node.Tag == "!!float" || node.Tag == "!!null" {
		if !configKey.MatchString(name) {
			return
		}
	}
	shown := m.leaf(name, node.Value)
	if shown != node.Value {
		node.Value, node.Tag, node.Style = shown, "!!str", 0
	}
	node.LineComment, node.HeadComment, node.FootComment = "", "", ""
}

// lines is text that is not a document, line by line: a line of key and
// value decided as a leaf, a blank kept, and any other line kept only when
// the whole of it is plain and no word in it names a credential. Every line
// of a PEM block, from its BEGIN line to its END line (or the end of the
// text), is masked: the block's last line can be a short run that reads as
// plain on its own.
func (m *configMasker) lines(text string) string {
	lines := strings.Split(text, "\n")
	inPEM := false
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if pemBlock.MatchString(line) {
			inPEM = true
		}
		if inPEM {
			if trimmed != "" {
				lines[index] = m.mask(line)
			}
			if strings.Contains(line, "-----END ") {
				inPEM = false
			}
			continue
		}
		if trimmed == "" {
			continue
		}
		if match := lineKeyVal.FindStringSubmatch(line); match != nil {
			lines[index] = match[1] + match[2] + match[3] + m.leaf(match[2], strings.Trim(match[4], `"'`))
			continue
		}
		if plainValue(trimmed) && !configKey.MatchString(trimmed) {
			continue
		}
		lines[index] = m.mask(line)
	}
	return strings.Join(lines, "\n")
}
