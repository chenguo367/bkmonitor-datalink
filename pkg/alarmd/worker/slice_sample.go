// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import "github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/inputslice"

// sampleKeep is one dimension's sample of a slice's values: the
// sliceSampleValues values of the smallest hash the stream has carried so
// far (bottom-k), each with the series that carried it.
//
// The first values to arrive were the sample before: a stream ordered by its
// values - one whose values differ in their last character only, sorted by
// it - then gave a sample of its first few characters, and a split cut by it
// put the rest of the stream in one part (a slice let go for its size every
// round). A value's hash does not depend on where it comes in the stream, so
// the sample is spread over the whole of it, the same values for the same
// stream whatever its order.
//
// A value of the final sample is counted from its first series: when it
// first came, its hash was among the smallest of the values seen so far -
// it is among the smallest of all of them - so it was taken, and nothing
// smaller came after to put it out. A value put out later was never in the
// final sample. The sample is even over the values, not weighted by the
// series each carries: a value that carries many series is in it as any
// value is.
type sampleKeep struct {
	hashes []uint64
	heap   []int // sample indexes, the largest hash first
	where  []int // heap position by sample index
}

// note counts one series of value in the sample values, seen indexing them.
func (keep *sampleKeep) note(values *inputslice.Sample, dimension string, seen map[string]int, value string) {
	if at, taken := seen[value]; taken {
		(*values)[dimension][at].Series++
		return
	}
	hash := sampleHash(value)
	sampled := (*values)[dimension]
	if len(sampled) < sliceSampleValues {
		at := len(sampled)
		(*values)[dimension] = append(sampled, inputslice.SampledValue{Value: value, Series: 1})
		seen[value] = at
		keep.hashes, keep.where = append(keep.hashes, hash), append(keep.where, len(keep.heap))
		keep.heap = append(keep.heap, at)
		keep.up(len(keep.heap) - 1)
		return
	}
	top := keep.heap[0]
	if hash >= keep.hashes[top] {
		return
	}
	delete(seen, sampled[top].Value)
	sampled[top] = inputslice.SampledValue{Value: value, Series: 1}
	seen[value], keep.hashes[top] = top, hash
	keep.down(0)
}

func (keep *sampleKeep) swap(left, right int) {
	keep.heap[left], keep.heap[right] = keep.heap[right], keep.heap[left]
	keep.where[keep.heap[left]], keep.where[keep.heap[right]] = left, right
}

func (keep *sampleKeep) larger(left, right int) bool {
	return keep.hashes[keep.heap[left]] > keep.hashes[keep.heap[right]]
}

func (keep *sampleKeep) up(at int) {
	for at > 0 {
		parent := (at - 1) / 2
		if !keep.larger(at, parent) {
			return
		}
		keep.swap(at, parent)
		at = parent
	}
}

func (keep *sampleKeep) down(at int) {
	for {
		largest := at
		for _, child := range [2]int{2*at + 1, 2*at + 2} {
			if child < len(keep.heap) && keep.larger(child, largest) {
				largest = child
			}
		}
		if largest == at {
			return
		}
		keep.swap(at, largest)
		at = largest
	}
}

// sampleHash is FNV-1a over the value's bytes: fixed, so the same values make
// the same sample in every process and every round.
func sampleHash(value string) uint64 {
	hash := uint64(14695981039346656037)
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= 1099511628211
	}
	return hash
}
