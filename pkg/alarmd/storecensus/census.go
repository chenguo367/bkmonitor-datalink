// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package storecensus measures what a Redis the process stores into holds,
// by key family, from the store's side: keys drawn by the server, each
// weighed by the server, scaled to the server's key count. The process's own
// counters say how many commands and bytes it sends; they cannot say what
// the store keeps of them, how long, or next to what the store's other
// users keep. A design that adds keys starts from this.
//
// A store of at most SampleKeys keys is walked whole and every key weighed:
// the census is exact. A larger one is sampled: SampleKeys draws of
// RANDOMKEY, with replacement, each weighed with MEMORY USAGE, and a
// family's keys and bytes are its share of the draws times DBSIZE.
//
// The error, by the sample size N = SampleKeys. A family holding a share p
// of the keys is drawn n = p*N times on average, and its key estimate has a
// relative standard error of sqrt((1-p)/(p*N)): 3.1% at p = 0.5, 9.4% at
// p = 0.1, 31% at p = 0.01, and a family of one key in a thousand is drawn
// once or not at all. Its bytes carry, beside that, the spread of its keys'
// sizes: a relative standard error of sqrt((1-p+c*c)/(p*N)), c being the
// coefficient of variation of the family's key sizes - so a family of equal
// keys is as good as its key count, and one of few very large keys among
// many small ones is known only as well as the large ones happen to be
// drawn. Samples, reported with every family, is n: the error of a reading
// is read from it, 1/sqrt(n) at the least. MEMORY USAGE weighs an aggregate
// - a hash, a set - from a sample of its elements, as the server does.
package storecensus

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// SampleKeys is how many keys a census weighs.
	SampleKeys = 1024
	// MaxFamilies is how many families a census names; the rest are
	// OtherFamily. A family's name is built from key names, which the
	// store's other users choose, so the count is held here and not by them.
	MaxFamilies = 24
	// OtherFamily is the families past MaxFamilies, by bytes, together.
	OtherFamily = "_other"
	// Interval is how often a census runs.
	Interval = 10 * time.Minute
	// batch bounds one pipeline of a census.
	batch = 128
	// memorySamples is the MEMORY USAGE SAMPLES argument: the server's own
	// default, named.
	memorySamples = 5
)

// ErrUnsupported is a client a census does not measure: a cluster, whose
// keys and counts are per node.
var ErrUnsupported = errors.New("storecensus: a cluster client is not measured")

// Family is one family of keys as a census estimated it.
type Family struct {
	Name string
	// Samples is how many of the keys weighed were of this family: the
	// estimate below rests on these alone.
	Samples int
	Keys    float64
	Bytes   float64
}

// Result is one census of one store.
type Result struct {
	// Store is the name the process knows the store by: the client label
	// its operations are counted under.
	Store string
	At    time.Time
	// Keys is the store's key count (DBSIZE), Weighed the keys weighed and
	// Exact whether they were every key.
	Keys    int64
	Weighed int
	Exact   bool
	// Gone is keys drawn that no longer existed when weighed.
	Gone     int
	Families []Family
	Duration time.Duration
}

// Measure takes one census of the store client reaches.
func Measure(ctx context.Context, client redis.UniversalClient, store string, now func() time.Time) (Result, error) {
	if _, cluster := client.(*redis.ClusterClient); cluster {
		return Result{}, ErrUnsupported
	}
	started := now()
	result := Result{Store: store, At: started}
	keys, err := client.DBSize(ctx).Result()
	if err != nil {
		return Result{}, err
	}
	result.Keys = keys
	var drawn []string
	if keys <= SampleKeys {
		drawn, err = walk(ctx, client)
		result.Exact = true
	} else {
		drawn, err = draw(ctx, client)
	}
	if err != nil {
		return Result{}, err
	}
	sizes, err := weigh(ctx, client, drawn)
	if err != nil {
		return Result{}, err
	}
	families, weighed, gone := tally(drawn, sizes)
	result.Weighed, result.Gone = weighed, gone
	scale := 1.0
	if !result.Exact && weighed > 0 {
		scale = float64(keys) / float64(weighed)
	}
	for index := range families {
		families[index].Keys *= scale
		families[index].Bytes *= scale
	}
	result.Families = fold(families)
	result.Duration = now().Sub(started)
	return result, nil
}

// tally is the keys weighed by family, unscaled, and how many were weighed
// and how many had gone before they could be: a key gone is no part of any
// family, and does not dilute the share of those that were there.
func tally(drawn []string, sizes []int64) (families []Family, weighed, gone int) {
	byName := map[string]int{}
	for index, key := range drawn {
		if sizes[index] < 0 {
			gone++
			continue
		}
		weighed++
		name := FamilyOf(key)
		position, found := byName[name]
		if !found {
			position = len(families)
			byName[name] = position
			families = append(families, Family{Name: name})
		}
		families[position].Samples++
		families[position].Keys++
		families[position].Bytes += float64(sizes[index])
	}
	return families, weighed, gone
}

// walk is every key of a small store.
func walk(ctx context.Context, client redis.UniversalClient) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		page, next, err := client.Scan(ctx, cursor, "", batch).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, page...)
		if cursor = next; cursor == 0 || len(keys) > 2*SampleKeys {
			// A store that grew past its count while walked is walked no
			// further than twice the count it was walked for.
			return keys, nil
		}
	}
}

// draw is SampleKeys keys drawn by the server, with replacement.
func draw(ctx context.Context, client redis.UniversalClient) ([]string, error) {
	keys := make([]string, 0, SampleKeys)
	for len(keys) < SampleKeys {
		commands := make([]*redis.StringCmd, 0, batch)
		if _, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for range min(batch, SampleKeys-len(keys)) {
				commands = append(commands, pipe.RandomKey(ctx))
			}
			return nil
		}); err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		for _, command := range commands {
			// An emptied store answers nil: nothing to draw.
			key, err := command.Result()
			if errors.Is(err, redis.Nil) {
				return keys, nil
			}
			if err != nil {
				return nil, err
			}
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// weigh is each key's MEMORY USAGE, -1 for a key gone before it was weighed.
func weigh(ctx context.Context, client redis.UniversalClient, keys []string) ([]int64, error) {
	sizes := make([]int64, 0, len(keys))
	for start := 0; start < len(keys); start += batch {
		part := keys[start:min(start+batch, len(keys))]
		commands := make([]*redis.IntCmd, 0, len(part))
		if _, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, key := range part {
				commands = append(commands, pipe.MemoryUsage(ctx, key, memorySamples))
			}
			return nil
		}); err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		for _, command := range commands {
			size, err := command.Result()
			switch {
			case errors.Is(err, redis.Nil):
				sizes = append(sizes, -1)
			case err != nil:
				return nil, err
			default:
				sizes = append(sizes, size)
			}
		}
	}
	return sizes, nil
}

// fold names the MaxFamilies-1 largest families by bytes and folds the rest
// into OtherFamily, largest first.
func fold(families []Family) []Family {
	sort.Slice(families, func(i, j int) bool {
		if families[i].Bytes != families[j].Bytes {
			return families[i].Bytes > families[j].Bytes
		}
		return families[i].Name < families[j].Name
	})
	if len(families) <= MaxFamilies {
		return families
	}
	other := Family{Name: OtherFamily}
	for _, family := range families[MaxFamilies-1:] {
		other.Samples += family.Samples
		other.Keys += family.Keys
		other.Bytes += family.Bytes
	}
	return append(families[:MaxFamilies-1:MaxFamilies-1], other)
}
