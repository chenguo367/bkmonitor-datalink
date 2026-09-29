// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package storecensus

import "strings"

const (
	// maxSegments is how many of a key's segments name its family; the rest
	// are one "*". Every key this process writes is named within them, and
	// so is the platform's longest cache key: a prefix of up to three
	// segments, three of kind and its instances.
	maxSegments = 8
	// maxFamilyName bounds a family's name, which is a metric label.
	maxFamilyName = 96
)

// FamilyOf is the family a key belongs to. A key is segments between its
// separators - ":" as alarmd and most Redis users write them, "." as the
// platform's Python cache writes its keys - and each segment that names one
// instance rather than a kind is written "*": a number, a digest, a hash
// tag, a UUID, a long token; inside a segment, each "_"-separated part that
// is one (strategy_group_<digest> is strategy_group_*). The separators are
// kept, so a family reads as its keys do. Keys that differ only in which
// Query Group, series or strategy they are about are one family.
func FamilyOf(key string) string {
	var name strings.Builder
	segments, start := 0, 0
	for index := 0; index <= len(key); index++ {
		if index < len(key) && key[index] == '{' {
			// A hash tag is one segment whatever it holds.
			if closing := strings.IndexByte(key[index:], '}'); closing > 0 {
				index += closing
			}
			continue
		}
		if index < len(key) && key[index] != ':' && key[index] != '.' {
			continue
		}
		if segments == maxSegments {
			name.WriteString("*")
			break
		}
		name.WriteString(kind(key[start:index]))
		segments++
		if index < len(key) {
			name.WriteByte(key[index])
		}
		start = index + 1
	}
	family := name.String()
	if len(family) > maxFamilyName {
		family = family[:maxFamilyName]
	}
	return family
}

// kind is a segment with its instances written "*": each of its
// "_"-separated parts that is one, and otherwise the whole of it when it is
// one - a long token or a hash tag that happens to hold a "_".
func kind(segment string) string {
	if !strings.Contains(segment, "_") {
		if instance(segment) {
			return "*"
		}
		return segment
	}
	parts := strings.Split(segment, "_")
	folded := false
	for index, part := range parts {
		if instance(part) {
			parts[index], folded = "*", true
		}
	}
	if !folded && instance(segment) {
		return "*"
	}
	return strings.Join(parts, "_")
}

// instance reports a segment that names one thing rather than a kind.
func instance(segment string) bool {
	switch {
	case segment == "":
		return false
	case strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}"):
		return true
	case len(segment) > 40:
		return true
	}
	digits, hexadecimal, hyphens := 0, 0, 0
	for index := 0; index < len(segment); index++ {
		char := segment[index]
		switch {
		case char >= '0' && char <= '9':
			digits++
			hexadecimal++
		case char >= 'a' && char <= 'f', char >= 'A' && char <= 'F':
			hexadecimal++
		case char == '-':
			hyphens++
		default:
			return false
		}
	}
	signed := segment[0] == '-' && hyphens == 1
	switch {
	case digits > 0 && digits+hyphens == len(segment) && (hyphens == 0 || signed):
		// A number of any length, signed or not.
		return true
	case segment[0] == '-' || segment[len(segment)-1] == '-':
		return false
	default:
		// A hexadecimal token long enough to be a digest or an identifier
		// rather than a word made of a to f, whole or in hyphenated groups
		// as a UUID is written.
		return hexadecimal >= 16
	}
}
