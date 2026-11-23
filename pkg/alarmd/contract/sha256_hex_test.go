// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package contract

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"
)

// The digest checked byte by byte answers as the regular expression it
// replaced, input for input (014 next8): at the boundaries of its length, of
// its character set and the empty string, and over random strings near
// them; and the entry every series a round reads is checked at,
// NewRecordIDDeriverV2, takes and refuses the same.
func TestTheDigestCheckAnswersAsTheExpressionItReplaced(t *testing.T) {
	replaced := regexp.MustCompile(`^[0-9a-f]{64}$`)
	valid := strings.Repeat("0123456789abcdef", 4)
	inputs := []string{"", valid, valid[:63], valid + "0", valid[:1], strings.ToUpper(valid), valid + "\n", "\n" + valid[1:],
		"é" + valid[2:], valid[:62] + "é", strings.Repeat("0", 64), strings.Repeat("f", 64), strings.Repeat("g", 64)}
	// Every byte at the edges of the two classes, and outside them, at the
	// first, a middle and the last place.
	for _, c := range []byte{0, ' ', '/', '0', '9', ':', '@', 'A', 'F', 'G', '`', 'a', 'f', 'g', 'z', 0x7f, 0x80, 0xff} {
		for _, at := range []int{0, 31, 63} {
			b := []byte(valid)
			b[at] = c
			inputs = append(inputs, string(b))
		}
	}
	random := rand.New(rand.NewSource(1))
	alphabet := []byte("0123456789abcdefABCDEFg-\x00\xc3\xa9")
	for range 2000 {
		b := make([]byte, 62+random.Intn(5))
		for index := range b {
			if random.Intn(8) == 0 {
				b[index] = alphabet[random.Intn(len(alphabet))]
			} else {
				b[index] = valid[random.Intn(16)]
			}
		}
		inputs = append(inputs, string(b))
	}
	accepted := 0
	for _, input := range inputs {
		want := replaced.MatchString(input)
		if got := isSHA256Hex(input); got != want {
			t.Fatalf("%q: the check says %t, the expression %t", input, got, want)
		}
		if _, err := NewRecordIDDeriverV2(input); (err == nil) != want {
			t.Fatalf("%q: NewRecordIDDeriverV2 error %v, the expression says %t", input, err, want)
		}
		if want {
			accepted++
		}
	}
	// Both sides of the boundary are among the inputs, as said above.
	if !isSHA256Hex(valid) || isSHA256Hex("") || isSHA256Hex(valid[:63]) || isSHA256Hex(valid+"0") ||
		isSHA256Hex(strings.ToUpper(valid)) || isSHA256Hex(strings.Repeat("g", 64)) || accepted < 10 {
		t.Fatalf("the boundaries: %d of %d inputs accepted", accepted, len(inputs))
	}
}
