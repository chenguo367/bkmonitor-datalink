// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2017-2025 Tencent. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package contract

import (
	"errors"
	"fmt"
	"strings"
)

// NoDataDimensionTag is the dimension Python adds to every no-data group so a
// no-data anomaly is a different object from the threshold anomaly on the same
// series. Its value is the Python boolean True, which is what count_md5 sees;
// this package states the name, and the identity the value takes is settled
// where the identity is built, not here.
const NoDataDimensionTag = "__NO_DATA_DIMENSION__"

// NoDataConfigV1 is an item's no-data detection setting, frozen with the Plan
// that carries it.
//
// Enablement is the presence of this section, not a field inside it. Python
// stores no_data_config as a dict with is_enabled beside the rest, and a Plan
// that carried NoDataConfigV1{...} with enablement switched off would read as
// configured while doing nothing - the shape that makes a reader look for the
// load that would wake it. Compilation therefore attaches this section only
// when Python says the item is enabled, and absent means no-data detection is
// not part of this Plan.
//
// One difference from Python is worth naming rather than discovering. Python's
// model default for no_data_config is {"is_enabled": True, "continuous": 5,
// "agg_dimension": []}, so a row created without the field is enabled; here an
// absent section is disabled. The two only disagree for a cached item that
// carries no no_data_config at all, which the strategy cache does not produce
// because the column has that default. Failing closed is the right way to be
// wrong about it: a strategy that should detect no-data and does not is a
// missing alert, while the reverse is an alert storm on every series of every
// strategy nobody configured.
type NoDataConfigV1 struct {
	// Continuous is how many consecutive absent periods raise the alert. It is
	// the trigger window and the threshold at once - Python sets
	// check_window_size and trigger_count to the same number - so the layer
	// that builds the synthetic series reads it for both.
	Continuous uint32 `json:"continuous"`
	// AggDimension names the dimensions a series is reduced to before absence
	// is judged. Empty is a setting rather than a gap: it reduces every series
	// to one group, which is how Python expresses "tell me when this item has
	// no data at all". It is Python's model default.
	AggDimension []string `json:"agg_dimension,omitempty"`
	// Level is the severity of a no-data anomaly, independent of the levels the
	// item's thresholds declare. Python defaults it to 2 when the field is
	// absent, and compilation applies that default rather than passing zero on.
	Level uint32 `json:"level"`
}

// Validate rejects a section that cannot produce a decision. A zero Continuous
// would make the trigger window empty, and a level outside the contract's range
// would produce an event no downstream stage can file.
func (config *NoDataConfigV1) Validate() error {
	if config == nil {
		return nil
	}
	if config.Continuous == 0 {
		return errors.New("no_data_config continuous must be positive")
	}
	if config.Level < 1 || config.Level > 3 {
		return fmt.Errorf("no_data_config level %d is outside 1..3", config.Level)
	}
	seen := make(map[string]struct{}, len(config.AggDimension))
	for _, dimension := range config.AggDimension {
		if dimension == "" || strings.TrimSpace(dimension) != dimension {
			return errors.New("no_data_config agg_dimension must be non-empty canonical text")
		}
		if dimension == NoDataDimensionTag {
			// The tag is added by the projection. Naming it as a source
			// dimension would make the group's own label an input to itself.
			return fmt.Errorf("no_data_config agg_dimension must not name %s", NoDataDimensionTag)
		}
		if _, duplicate := seen[dimension]; duplicate {
			return fmt.Errorf("no_data_config agg_dimension repeats %q", dimension)
		}
		seen[dimension] = struct{}{}
	}
	return nil
}
