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
	// are one "*". Every key this process writes is named within them.
	maxSegments = 6
	// maxFamilyName bounds a family's name, which is a metric label.
	maxFamilyName = 96
)

// FamilyOf is the family a key belongs to: its colon-separated segments, each
// that names one instance rather than a kind - a number, a digest, a hash
// tag, a long token - written "*". Keys that differ only in which Query
// Group, series or strategy they are about are one family.
func FamilyOf(key string) string {
	segments := strings.Split(key, ":")
	if len(segments) > maxSegments {
		segments = append(segments[:maxSegments:maxSegments], "*")
	}
	for index, segment := range segments {
		if instance(segment) {
			segments[index] = "*"
		}
	}
	name := strings.Join(segments, ":")
	if len(name) > maxFamilyName {
		name = name[:maxFamilyName]
	}
	return name
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
	digits, hexadecimal := 0, true
	for index := 0; index < len(segment); index++ {
		char := segment[index]
		switch {
		case char >= '0' && char <= '9':
			digits++
		case char >= 'a' && char <= 'f', char >= 'A' && char <= 'F':
		case char == '-' && index == 0:
			hexadecimal = false
		default:
			return false
		}
	}
	// A number of any length, or a hexadecimal token long enough to be a
	// digest or an identifier rather than a word made of a to f.
	return digits == len(segment) || digits > 0 && segment[0] == '-' && digits == len(segment)-1 ||
		hexadecimal && len(segment) >= 16
}
