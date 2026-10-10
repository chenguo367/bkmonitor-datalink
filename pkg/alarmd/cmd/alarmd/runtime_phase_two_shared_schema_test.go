package main

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/alarmd/config"
	"reflect"
	"testing"
)

func TestRuntimeReadsBackEffectiveSharedSchemaScope(t *testing.T) {
	cfg := config.Default()
	old := phaseTwoUQCodecFacts(cfg)
	if old.Mode != "legacy" || old.Scope != "ordinary_execute_non_polling" || len(old.QueryGroups) != 0 {
		t.Fatal(old)
	}
	cfg.PhaseTwo.Access.UQSharedSchemaQueryGroups = []string{"group-a"}
	selected := phaseTwoUQCodecFacts(cfg)
	cfg.PhaseTwo.Access.UQSharedSchemaQueryGroups[0] = "changed"
	if selected.Mode != "shared_schema_opt_in" || !reflect.DeepEqual(selected.QueryGroups, []string{"group-a"}) {
		t.Fatal("effective scope was not copied", selected)
	}
}
