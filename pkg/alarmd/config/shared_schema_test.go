package config

import "testing"

func TestSharedSchemaOptInDefaultsAndValidation(t *testing.T) {
	if len(Default().PhaseTwo.Access.UQSharedSchemaQueryGroups) != 0 {
		t.Fatal("shared schema enabled by default")
	}
	for _, groups := range [][]string{nil, {}, {"*"}, {"group-a", "group-b"}} {
		cfg := completePhaseTwoProductionConfig(validGoAccessConfigObject())
		cfg.PhaseTwo.Access.UQSharedSchemaQueryGroups = groups
		if err := cfg.PhaseTwo.validate(); err != nil {
			t.Fatalf("valid allowlist %v: %v", groups, err)
		}
	}
	for _, groups := range [][]string{{"*", "group-a"}, {"group-a", "group-a"}, {""}, {"bad group"}, {"**"}, {" group"}} {
		if err := validateSharedSchemaGroups(groups); err == nil {
			t.Fatalf("invalid allowlist accepted %v", groups)
		}
	}
}
