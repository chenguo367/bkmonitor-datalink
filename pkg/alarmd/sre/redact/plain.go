// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License.

package redact

import (
	"regexp"
	"strings"
	"unicode"
)

// The rules for telling plain configuration from a credential, shared by
// the SRE config view and by alarmd's loader. Accepting a credential
// reference and showing it are separate questions: the loader opens any
// well-formed absolute path (AcceptedCredentialPath), and a path is shown -
// in the view or in an error - only when it also reads as plain
// (ShownCredentialPath), so a value shaped like a key is never echoed.

var (
	pemBlock  = regexp.MustCompile(`-----BEGIN [A-Z0-9 ]+-----`)
	jwtLike   = regexp.MustCompile(`eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.`)
	base64Run = regexp.MustCompile(`[A-Za-z0-9+/_-]{22,}={0,2}`)
	hexRun    = regexp.MustCompile(`(?i)\b[0-9a-f]{32,}\b`)
	wordSplit = regexp.MustCompile(`[\s,;:/=@?&#()\[\]{}"'<>|.]+`)
)

// CredentialPathLimit is the longest credential reference accepted.
const CredentialPathLimit = 4096

// AcceptedCredentialPath says text is a well-formed path to open as a
// credential file: absolute, free of control characters and of leading or
// trailing whitespace, and at most CredentialPathLimit bytes. The commonest
// mistake with a reference is the credential itself written where its path
// belongs, and a credential almost never starts with "/". It does not ask
// whether the path reads as plain: /etc/secrets/Redis6Password is a path an
// operator may well choose.
func AcceptedCredentialPath(text string) bool {
	return strings.HasPrefix(text, "/") && len(text) <= CredentialPathLimit &&
		strings.TrimSpace(text) == text && !strings.ContainsFunc(text, unicode.IsControl)
}

// ShownCredentialPath says an accepted credential path may also be shown:
// it reads as plain configuration (PlainValue). A path that does not is
// opened but withheld wherever it would be printed.
func ShownCredentialPath(text string) bool {
	return AcceptedCredentialPath(text) && PlainValue(text)
}

// KeyMaterial says text holds something shaped like a key, whatever it is
// called: a PEM block, a JWT, a run of base64 or hex of 16 bytes or more.
func KeyMaterial(text string) bool {
	if pemBlock.MatchString(text) || jwtLike.MatchString(text) || hexRun.MatchString(text) {
		return true
	}
	for _, run := range base64Run.FindAllString(text, -1) {
		if mixedCase(run) && (hasDigit(run) || longestSegment(run) >= 20) {
			return true
		}
	}
	return false
}

// A run of base64 characters is key material when it reads like one: mixed
// case with a digit somewhere, or a long stretch with no separator. A path
// such as /etc/coredns/NodeHosts is also a long run of those characters,
// but of short words between slashes and with no digit; 16 random bytes in
// base64 almost never are.
func hasDigit(text string) bool {
	return strings.IndexFunc(text, unicode.IsDigit) >= 0
}

func longestSegment(run string) int {
	longest := 0
	for _, segment := range strings.FieldsFunc(run, func(r rune) bool { return r == '/' || r == '+' || r == '_' || r == '-' }) {
		longest = max(longest, len(segment))
	}
	return longest
}

func mixedCase(text string) bool {
	upper, lower := false, false
	for _, r := range text {
		upper = upper || unicode.IsUpper(r)
		lower = lower || unicode.IsLower(r)
	}
	return upper && lower
}

// generated says a word reads like a generated string rather than a word: a
// password, an identifier minted at random. Short words and words of one
// kind of character (a host label, a number, a constant) are not.
func generated(word string) bool {
	upper, lower, digit := false, false, false
	for _, r := range word {
		switch {
		case unicode.IsUpper(r):
			upper = true
		case unicode.IsLower(r):
			lower = true
		case unicode.IsDigit(r):
			digit = true
		}
	}
	return len(word) >= 8 && upper && lower && digit
}

// PlainValue says a scalar can be shown: short enough to read, and every
// word of it a word - a host, a path, a duration - and not like a generated
// string.
func PlainValue(text string) bool {
	if len(text) > 512 || strings.ContainsAny(text, "\r\n") || KeyMaterial(text) {
		return false
	}
	for _, word := range wordSplit.Split(text, -1) {
		if generated(word) {
			return false
		}
	}
	return true
}
