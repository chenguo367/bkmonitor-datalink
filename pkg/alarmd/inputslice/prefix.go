// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package inputslice

import (
	"regexp"
	"slices"
	"strings"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// alphabet is what a split without a sample keeps apart after a prefix: the
// characters most dimension values are written in, digits, lower-case
// letters and the separators of addresses and names. Any other character
// after the prefix, and a value that ends where the prefix does, is in the
// rest of the split, as is a missing label in the root's: every value is in
// exactly one leaf whatever the values are.
const alphabet = "-.0123456789:_abcdefghijklmnopqrstuvwxyz"

// literal is every character a prefix may hold: the alphabet and upper-case
// letters, which a sample's common prefix and next characters take. Each is
// a literal to the store once escaped (regexp.QuoteMeta), so that the store
// reads a slice by its prefix - VictoriaMetrics reads a literal prefix as a
// scan of the label's values from it, where a pattern led by .* reads every
// value of the label, the fixed cost of a slice (015 C2 section 4.1).
const literal = alphabet + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

// A segment cut by prefix (byPrefix, tree.go) holds the values that start
// with its prefix, less those that start with its prefix followed by one of
// its except - the children of its splits kept apart. A split of such a
// segment at a longer prefix keeps some characters after it apart, each a
// child of one character more, and leaves the rest in one child of the
// segment's own prefix with those children added to except: the children
// are disjoint, and together the segment. prefix is of literal characters
// only; except is what follows prefix in the values in other children, each
// a literal of one character or more, none a prefix of another, sorted and
// joined by NUL (a character no literal holds).

const exceptSeparator = "\x00"

func (s segment) excepted() []string {
	if s.except == "" {
		return nil
	}
	return strings.Split(s.except, exceptSeparator)
}

// exceptOf is the except of a set of literals: each kept once, the ones
// another of them is a prefix of dropped (the shorter takes them), sorted.
func exceptOf(literals []string) string {
	sorted := slices.Clone(literals)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	kept := sorted[:0]
	for _, candidate := range sorted {
		// Sorted, a literal's prefixes among them come before it, the
		// shortest first, and the last one kept is the one to compare to.
		if len(kept) > 0 && strings.HasPrefix(candidate, kept[len(kept)-1]) {
			continue
		}
		kept = append(kept, candidate)
	}
	return strings.Join(kept, exceptSeparator)
}

// conditions is what the segment writes: its prefix as a match, and what it
// excepts as one non-match of the prefix followed by any of them, a bracket
// class when each is one character. Anchored at both ends, as every slice
// condition is (execution.InputSlice.Validate); a store that drops the
// anchors and keeps the literal before the rest reads the prefix as a
// prefix either way. The rest of the root's split writes the non-match
// alone, so a missing label - the empty value to the store's matcher and to
// UQ's own filter - is in it.
func (s segment) prefixConditions(dimensions []string) []execution.InputSliceCondition {
	conditions := s.prefixPatterns()
	for index := range conditions {
		conditions[index].Dimension = dimensions[s.dimension]
	}
	return conditions
}

// prefixPatterns is prefixConditions without the dimension's name.
func (s segment) prefixPatterns() []execution.InputSliceCondition {
	conditions := make([]execution.InputSliceCondition, 0, 2)
	prefix := regexp.QuoteMeta(s.prefix)
	if s.prefix != "" {
		conditions = append(conditions, execution.InputSliceCondition{Pattern: "^" + prefix + "(?s:.*)$"})
	}
	if s.except != "" {
		conditions = append(conditions, execution.InputSliceCondition{
			Negated: true, Pattern: "^" + prefix + alternatives(s.excepted()) + "(?s:.*)$",
		})
	}
	return conditions
}

// alternatives writes literals as one alternative of the pattern: a bracket
// class when each is one character, runs of three or more consecutive digits
// or letters as a range, and a group of the escaped literals otherwise.
func alternatives(literals []string) string {
	single := true
	for _, each := range literals {
		single = single && len(each) == 1
	}
	if !single {
		quoted := make([]string, 0, len(literals))
		for _, each := range literals {
			quoted = append(quoted, regexp.QuoteMeta(each))
		}
		return "(?:" + strings.Join(quoted, "|") + ")"
	}
	characters := make([]byte, 0, len(literals))
	for _, each := range literals {
		characters = append(characters, each[0])
	}
	slices.Sort(characters)
	var builder strings.Builder
	builder.WriteByte('[')
	for index := 0; index < len(characters); {
		end := index
		for end+1 < len(characters) && characters[end+1] == characters[end]+1 && sameRun(characters[end], characters[end+1]) {
			end++
		}
		if end-index >= 2 {
			builder.WriteByte(characters[index])
			builder.WriteByte('-')
			builder.WriteByte(characters[end])
		} else {
			for _, character := range characters[index : end+1] {
				if character == '-' {
					// A hyphen in a bracket class is a range unless escaped.
					builder.WriteString(`\-`)
					continue
				}
				builder.WriteByte(character)
			}
		}
		index = end + 1
	}
	builder.WriteByte(']')
	return builder.String()
}

// sameRun reports whether two characters are both digits, both lower-case
// or both upper-case letters: the runs a range is written for.
func sameRun(left, right byte) bool {
	class := func(character byte) int {
		switch {
		case '0' <= character && character <= '9':
			return 1
		case 'a' <= character && character <= 'z':
			return 2
		case 'A' <= character && character <= 'Z':
			return 3
		}
		return 0
	}
	return class(left) != 0 && class(left) == class(right)
}

// prefixHolds reports whether value is in the segment cut by prefix, a
// missing label read as the empty value. The characters of a prefix are
// ASCII, so a byte of a value that is not one of them - a byte of a longer
// character among them - is in no prefix.
func (s segment) prefixHolds(value string) bool {
	if !strings.HasPrefix(value, s.prefix) {
		return false
	}
	rest := value[len(s.prefix):]
	for _, excepted := range s.excepted() {
		if strings.HasPrefix(rest, excepted) {
			return false
		}
	}
	return true
}

// free is the alphabet characters a split of the segment at its own prefix
// can keep apart, in order: the ones it does not except outright.
func (s segment) free() string {
	excepted := s.excepted()
	var builder strings.Builder
	for index := 0; index < len(alphabet); index++ {
		if !slices.Contains(excepted, alphabet[index:index+1]) {
			builder.WriteByte(alphabet[index])
		}
	}
	return builder.String()
}

// splitBy is the children of the segment parent, each after kept, that cut
// it at the longer prefix parent.prefix+at and keep the characters apart
// after it (distinct literal characters, at excepted by none of parent's,
// at followed by none of them excepted outright): one child of each, its
// prefix at and the character, excepting what parent excepts within it; and
// the rest, parent less them.
func splitBy(kept []segment, parent segment, at, characters string) [][]segment {
	excepted := parent.excepted()
	children := make([][]segment, 0, len(characters)+1)
	restExcept := slices.Clone(excepted)
	for index := 0; index < len(characters); index++ {
		head := at + characters[index:index+1]
		var within []string
		for _, each := range excepted {
			if len(each) > len(head) && strings.HasPrefix(each, head) {
				within = append(within, each[len(head):])
			}
		}
		child := segment{dimension: parent.dimension, byPrefix: true, prefix: parent.prefix + head}
		if len(within) > 0 {
			child.except = exceptOf(within)
		}
		children = append(children, appendSegment(kept, child))
		restExcept = append(restExcept, head)
	}
	rest := segment{dimension: parent.dimension, byPrefix: true, prefix: parent.prefix, except: exceptOf(restExcept)}
	return append(children, appendSegment(kept, rest))
}

// canSplitAt reports whether the segment can be cut at its prefix and at
// keeping characters apart (splitBy): at is literal and excepted by none of
// its literals, and each character is literal, distinct, and at followed by
// it is excepted by none.
func (s segment) canSplitAt(at, characters string) bool {
	if characters == "" || !isLiteral(at) || !isLiteral(characters) {
		return false
	}
	excepted := s.excepted()
	for _, each := range excepted {
		if strings.HasPrefix(at, each) {
			return false
		}
	}
	seen := map[byte]bool{}
	for index := 0; index < len(characters); index++ {
		if seen[characters[index]] {
			return false
		}
		seen[characters[index]] = true
		head := at + characters[index:index+1]
		for _, each := range excepted {
			if strings.HasPrefix(head, each) {
				return false
			}
		}
	}
	return true
}

// isLiteral reports whether every character of text is a literal one.
func isLiteral(text string) bool {
	for index := 0; index < len(text); index++ {
		if strings.IndexByte(literal, text[index]) < 0 {
			return false
		}
	}
	return true
}

// Bound is the least and the greatest value of one dimension a slice read
// whole delivered, a missing label as the empty value: their longest common
// prefix is every value's, whatever order the store answered in - where a
// sample of a slice let go for its size is of the start of its stream only,
// the values of the least label order (015 C2 section 4.8).
type Bound struct {
	Low, High string
}

// Bounds is a slice's Bound by dimension.
type Bounds map[string]Bound

// boundedCut is the children of the segment parent cut by prefix, each after
// kept: with a bound, at its values' longest common literal prefix, keeping
// apart every alphabet character after it the segment does not except; with
// none, at its own prefix by the alphabet characters it does not except. False
// when that separates nothing: one value (the bound's least and greatest the
// same), or the least and the greatest in the same child.
func boundedCut(kept []segment, parent segment, bound *Bound) ([][]segment, bool) {
	if bound == nil {
		if free := parent.free(); free != "" {
			return splitBy(kept, parent, "", free), true
		}
		return nil, false
	}
	if bound.Low == bound.High || !strings.HasPrefix(bound.Low, parent.prefix) || !strings.HasPrefix(bound.High, parent.prefix) {
		return nil, false
	}
	low, high := bound.Low[len(parent.prefix):], bound.High[len(parent.prefix):]
	end := 0
	for end < len(low) && end < len(high) && low[end] == high[end] && strings.IndexByte(literal, low[end]) >= 0 {
		end++
	}
	at := low[:end]
	var characters strings.Builder
	for index := 0; index < len(alphabet); index++ {
		if parent.canSplitAt(at, alphabet[index:index+1]) {
			characters.WriteByte(alphabet[index])
		}
	}
	if characters.Len() == 0 || !parent.canSplitAt(at, characters.String()) {
		return nil, false
	}
	children := splitBy(kept, parent, at, characters.String())
	lowChild, highChild := -1, -1
	for index, child := range children {
		last := child[len(child)-1]
		if lowChild < 0 && last.prefixHolds(bound.Low) {
			lowChild = index
		}
		if highChild < 0 && last.prefixHolds(bound.High) {
			highChild = index
		}
	}
	if lowChild == highChild {
		return nil, false
	}
	return children, true
}

// splitOf is how children split parent, when they do: where they cut it and
// the characters they keep apart, and false when they are no split a tree
// makes - on the parent's own last segment, or on another dimension the
// parent is not cut on from that dimension's root, the parent's segments
// kept, a root that constrains nothing not carried.
func splitOf(parent []segment, children [][]segment, dimensions int) (string, string, bool) {
	if len(parent) == 0 || len(children) < 2 {
		return "", "", false
	}
	last := parent[len(parent)-1]
	if at, characters, ok := splitOn(parent[:len(parent)-1], last, children); ok {
		return at, characters, true
	}
	before := parent
	if !last.writesCondition() {
		before = parent[:len(parent)-1]
	}
	next := children[0][len(children[0])-1].dimension
	if next == last.dimension || next < 0 || next >= dimensions || cutOn(before, next) {
		return "", "", false
	}
	return splitOn(before, segment{dimension: next}, children)
}

// splitOn reports whether children are splitBy(kept, parent, at, characters)
// for some at and characters, and which.
func splitOn(kept []segment, parent segment, children [][]segment) (string, string, bool) {
	first := children[0]
	if len(first) != len(kept)+1 {
		return "", "", false
	}
	head := first[len(kept)].prefix
	if len(head) <= len(parent.prefix) || !strings.HasPrefix(head, parent.prefix) {
		return "", "", false
	}
	at := head[len(parent.prefix) : len(head)-1]
	var characters strings.Builder
	for _, child := range children[:len(children)-1] {
		if len(child) != len(kept)+1 {
			return "", "", false
		}
		cut := child[len(kept)]
		if len(cut.prefix) != len(head) || !strings.HasPrefix(cut.prefix, parent.prefix+at) {
			return "", "", false
		}
		characters.WriteByte(cut.prefix[len(cut.prefix)-1])
	}
	if !parent.canSplitAt(at, characters.String()) {
		return "", "", false
	}
	want := splitBy(kept, parent, at, characters.String())
	for index := range want {
		if !equalSegments(children[index], want[index]) {
			return "", "", false
		}
	}
	return at, characters.String(), true
}

func equalSegments(left, right []segment) bool {
	return slices.EqualFunc(left, right, segment.equal)
}
