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

// ConfigValueContract names how a ConfigMap value is shown: in clear,
// except what is named or shaped like a credential.
const ConfigValueContract = "config_values_blacklist_v1"

// A ConfigMap is configuration, and the channel exists so that an agent can
// read and fix it without a person in the loop. A view that masks everything
// it cannot prove plain hides the very files an agent needs - a Corefile or
// a hosts file came back nearly every line masked - so the rule is a
// blacklist: a value is shown as it is, except
//
//   - a value under a credential-like name (configKey) - password, secret,
//     token, an API or access key, a private key - is masked whole;
//   - a part of any value, under any name, that is shaped like a credential
//     is masked where it stands (redact.CredentialSpans): a PEM block, a JWT,
//     a long run of base64 or hex that reads like key material, the userinfo
//     of a URL, an Authorization or Bearer value, a token under a known
//     provider prefix.
//
// The decision is made per value - per leaf of a YAML or JSON document, per
// line of other text, comments included - and a masked part keeps its size
// and fingerprint, so a host, a port or a path next to it stays readable.
// Secrets are not ConfigMaps: their values are never shown (secret.get).

// configKey is a name whose value is secret by what it names.
var configKey = regexp.MustCompile(`(?i)(passw|passwd|pwd|secret|token|credential|auth|cookie|session|private|crypt|cipher|sign|cert|salt|seed|nonce|apikey|api[_-]?key|access[_-]?key|(^|[_.\-])(key|iv|pin)([_.\-]|$)|(key|iv)$)`)

var (
	pemBlock   = regexp.MustCompile(`-----BEGIN [A-Z0-9 ]+-----`)
	lineKeyVal = regexp.MustCompile(`^([A-Za-z0-9_.\-]+)(\s*[:=]\s*)(.*)$`)
	// lineLeadPattern is a line's indentation, comment marker and export.
	lineLeadPattern = regexp.MustCompile(`^\s*(?:(?:#+|//|;)\s*)?(?:export\s+)?`)
)

// keyMaterial says text holds something shaped like a key, whatever it is
// called (redact.KeyMaterial).
func keyMaterial(text string) bool { return redact.KeyMaterial(text) }

// credentialShaped says text holds something shaped like a credential
// (redact.CredentialShaped).
func credentialShaped(text string) bool { return redact.CredentialShaped(text) }

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

// leaf is text under name: masked whole under a credential-like name,
// otherwise shown with its credential-shaped parts masked where they stand.
// Text of several lines - a file held in one value - is read as a value of
// its own (value).
func (m *configMasker) leaf(name, text string) string {
	if text == "" {
		return text
	}
	if referencePath(name, text) {
		return text
	}
	if configKey.MatchString(name) {
		return m.mask(text)
	}
	if strings.Contains(text, "\n") {
		return m.value(name, text)
	}
	return m.spans(text)
}

// spans is text with every credential-shaped part masked where it stands.
func (m *configMasker) spans(text string) string {
	found := redact.CredentialSpans(text)
	if len(found) == 0 {
		return text
	}
	var out strings.Builder
	last := 0
	for _, span := range found {
		out.WriteString(text[last:span[0]])
		out.WriteString(m.mask(text[span[0]:span[1]]))
		last = span[1]
	}
	out.WriteString(text[last:])
	return out.String()
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

// comments reads a node's comments as text of their own (lines): a
// comment is shown, with what it names or holds like a credential masked.
func (m *configMasker) comments(node *yaml.Node) {
	for _, comment := range []*string{&node.HeadComment, &node.LineComment, &node.FootComment} {
		if *comment != "" {
			*comment = m.lines(*comment)
		}
	}
}

func (m *configMasker) walk(node *yaml.Node, name string) {
	m.comments(node)
	switch node.Kind {
	case yaml.MappingNode:
		for index := 0; index+1 < len(node.Content); index += 2 {
			keyNode := node.Content[index]
			m.comments(keyNode)
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
		node.Value, node.Tag = shown, "!!str"
		if node.Style != yaml.LiteralStyle && node.Style != yaml.FoldedStyle {
			node.Style = 0
		}
	}
	m.comments(node)
}

// lines is text that is not a document, line by line, comments included: a
// line of key and value decided as a leaf; a directive whose name is
// credential-like (password, token...) with its arguments masked; any other
// line shown with its credential-shaped parts masked where they stand. Every
// line of a PEM block, from its BEGIN line to its END line (or the end of
// the text), is masked: the block's last line can be a short run that reads
// as plain on its own.
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
		prefix, rest := lineLead(line)
		if match := lineKeyVal.FindStringSubmatch(rest); match != nil {
			value := strings.Trim(match[3], `"'`)
			if shown := m.leaf(match[1], value); shown != value {
				lines[index] = prefix + match[1] + match[2] + shown
			}
			continue
		}
		if at := credentialWordEnd(rest); at > 0 {
			lines[index] = prefix + rest[:at] + " " + m.mask(strings.TrimSpace(rest[at:]))
			continue
		}
		lines[index] = prefix + m.spans(rest)
	}
	return strings.Join(lines, "\n")
}

// credentialWordEnd is where, in a line of words, the first word that names a
// credential (configKey) ends, when words follow it - "password hunter2",
// "api token abc" - so that what follows is masked; 0 when no such word has
// anything after it.
func credentialWordEnd(rest string) int {
	offset := 0
	fields := strings.Fields(rest)
	for index, field := range fields {
		at := strings.Index(rest[offset:], field) + offset
		offset = at + len(field)
		if index < len(fields)-1 && configKey.MatchString(strings.TrimRight(field, ":=")) {
			return offset
		}
	}
	return 0
}

// lineLead splits a line into its lead - indentation, a comment marker, an
// export - and the rest, so that a key and value or a directive is read the
// same in a comment as outside one.
func lineLead(line string) (string, string) {
	lead := lineLeadPattern.FindString(line)
	return lead, line[len(lead):]
}
