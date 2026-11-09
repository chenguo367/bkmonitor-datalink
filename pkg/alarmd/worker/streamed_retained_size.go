// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package worker

import (
	"unsafe"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/execution"
)

// What a binding the Slot keeps holds beyond its Dataset: its slot in the
// streamed map - the key and the binding side by side, a control byte, the
// map filled to seven eighths - and the facts it carries. Its strings are
// the header's and the Dataset's, held whether it is kept or not.
var (
	streamedBindingBytes = (uint64(unsafe.Sizeof(streamedInputKey{})+unsafe.Sizeof(execution.NamedInputBinding{})) + 1) * 8 / 7
	qualityFactBytes     = uint64(unsafe.Sizeof(execution.InputQualityFact{}))
	inputTerminalBytes   = uint64(unsafe.Sizeof(execution.InputTerminal{}))
	partialEvidenceBytes = uint64(unsafe.Sizeof(execution.PartialEvidence{}))
	datasetViewBytes     = uint64(unsafe.Sizeof(execution.DatasetView{}))
)

// streamedRetainedSize is what a series batch holds while its Slot keeps it,
// as the structure says (no encoding, no read of the runtime): each binding
// as the Slot keeps it, each Dataset the batch and its bindings point at,
// once, at its RetainedBytes, and each binding's view of it.
func streamedRetainedSize(batch execution.SeriesExecutionBatch) uint64 {
	total := batch.Dataset.RetainedBytes()
	counted := []*execution.Dataset{batch.Dataset}
	for index := range batch.Inputs {
		binding := &batch.Inputs[index]
		total += streamedBindingBytes + uint64(cap(binding.QualityFacts))*qualityFactBytes +
			uint64(cap(binding.Terminals))*inputTerminalBytes
		if binding.PartialEvidence != nil {
			total += partialEvidenceBytes
		}
		if binding.View != nil {
			total += datasetViewBytes + 4*uint64(binding.View.Len())
		}
		if binding.Dataset != nil && !pointsAt(counted, binding.Dataset) {
			counted = append(counted, binding.Dataset)
			total += binding.Dataset.RetainedBytes()
		}
	}
	return total
}

func pointsAt(datasets []*execution.Dataset, dataset *execution.Dataset) bool {
	for _, counted := range datasets {
		if counted == dataset {
			return true
		}
	}
	return false
}
