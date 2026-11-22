// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2026 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package fleet

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// A replica's snapshot is written compressed: its JSON, a few megabytes on a
// replica holding a cooldown pool of hundreds of objects, written whole every
// publish, was the largest of what the fleet sent the runtime Redis, whose
// CPU follows the bytes it is sent; its field names repeat on every object,
// and gzip takes most of it away.
//
// The compressed snapshot is a header and a gzip stream of the JSON: the
// header is snapshotCodecMagic, which no JSON begins with, and the JSON's
// length, eight bytes big-endian, which a reader charges the memory line by
// before reading the snapshot (a snapshot is admitted by what it holds
// decoded, not by what it is stored as) and checks the stream against.
const (
	snapshotCodecMagic  = "AFZ\x01"
	snapshotCodecHeader = len(snapshotCodecMagic) + 8
	// snapshotCodecLevel trades the publish's CPU for bytes. Two replicas'
	// snapshots of about 2 MB read from a deployment came to 16 % of their
	// JSON at gzip's level 1 and 13.5 % at its default, the first in half
	// the time (15-22 ms against 33-35 ms in zlib): the default would save
	// a further 2.5 % of the JSON for the time of the whole compression
	// again, every publish. BenchmarkSnapshotCodec reads it in Go.
	snapshotCodecLevel = gzip.BestSpeed
)

var snapshotWriters = sync.Pool{New: func() any {
	writer, _ := gzip.NewWriterLevel(io.Discard, snapshotCodecLevel)
	return writer
}}

// encodeSnapshotPayload is the compressed form of a snapshot's JSON.
func encodeSnapshotPayload(raw []byte) ([]byte, error) {
	var out bytes.Buffer
	out.Grow(snapshotCodecHeader + len(raw)/6)
	out.WriteString(snapshotCodecMagic)
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(raw)))
	out.Write(length[:])
	writer := snapshotWriters.Get().(*gzip.Writer)
	defer snapshotWriters.Put(writer)
	writer.Reset(&out)
	if _, err := writer.Write(raw); err != nil {
		return nil, fmt.Errorf("alarmd fleet: compress snapshot: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("alarmd fleet: compress snapshot: %w", err)
	}
	return out.Bytes(), nil
}

// snapshotRawLength is the JSON length a compressed snapshot's header says,
// false for a value that is not one (an earlier build's JSON).
func snapshotRawLength(head []byte) (uint64, bool) {
	if len(head) < snapshotCodecHeader || string(head[:len(snapshotCodecMagic)]) != snapshotCodecMagic {
		return 0, false
	}
	return binary.BigEndian.Uint64(head[len(snapshotCodecMagic):snapshotCodecHeader]), true
}

// errSnapshotCodec is a compressed snapshot that does not decode to the
// length its header says.
var errSnapshotCodec = errors.New("alarmd fleet: compressed snapshot does not match its header")

// decodeSnapshotPayload is a stored snapshot's JSON: the value itself for
// an earlier build's, the stream inflated for a compressed one, which must
// come to the length its header says and no more.
func decodeSnapshotPayload(stored []byte) ([]byte, error) {
	length, compressed := snapshotRawLength(stored)
	if !compressed {
		return stored, nil
	}
	reader, err := gzip.NewReader(bytes.NewReader(stored[snapshotCodecHeader:]))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errSnapshotCodec, err)
	}
	raw := make([]byte, 0, length)
	out := bytes.NewBuffer(raw)
	if _, err := io.Copy(out, io.LimitReader(reader, int64(length)+1)); err != nil {
		return nil, fmt.Errorf("%w: %v", errSnapshotCodec, err)
	}
	if uint64(out.Len()) != length {
		return nil, fmt.Errorf("%w: %d bytes, the header says %d", errSnapshotCodec, out.Len(), length)
	}
	return out.Bytes(), nil
}
