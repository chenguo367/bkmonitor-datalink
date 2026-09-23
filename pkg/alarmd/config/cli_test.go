package config

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCLIAdminKeyLoadsWithoutAppearingInJSON(t *testing.T) {
	key := strings.Repeat("k", 64)
	var cfg CLIConfig
	if err := yaml.Unmarshal([]byte("enabled: true\nadmin_key: "+key+"\n"), &cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.Enabled || cfg.AdminKey != key {
		t.Fatal("administrator key not loaded")
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), key) || strings.Contains(string(raw), "admin_key") || strings.Contains(string(raw), "AdminKey") {
		t.Fatal("administrator key in JSON evidence")
	}
	var legacy CLIConfig
	if err := yaml.Unmarshal([]byte("issuer_key: "+key+"\n"), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.AdminKey != "" {
		t.Fatal("removed host configuration enabled administrator authorization")
	}
}
