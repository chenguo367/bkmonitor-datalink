package config

import "testing"

func TestLinkdCapacityTracksContainer(t *testing.T) {
	small := DeriveLinkdCapacity(CapacityInputs{CPUBudget: 2, MemoryLimitBytes: 2 << 30})
	large := DeriveLinkdCapacity(CapacityInputs{CPUBudget: 8, MemoryLimitBytes: 8 << 30})
	if large.Bytes < small.Bytes*3 || large.Members < small.Members*3 || large.ReadBatch <= small.ReadBatch {
		t.Fatalf("capacity did not scale: %+v %+v", small, large)
	}
	if large.Bytes > (8<<30)/100 {
		t.Fatal("exceeded container share")
	}
}

func TestLinkdConfigurationBinding(t *testing.T) {
	if err := (LinkdConfig{}).Validate(); err != nil {
		t.Fatal(err)
	}
	c := LinkdConfig{ConsoleURL: "https://console.example.test", EventSourceID: "native-events", HookName: "active-index", Username: "reader", Password: "test-password"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.ConsoleURL = "https://reader:password@console.example.test"
	if err := c.Validate(); err == nil {
		t.Fatal("URL credentials accepted")
	}
	c.ConsoleURL = "https://console.example.test"
	c.HookName = ""
	if err := c.Validate(); err == nil {
		t.Fatal("missing source binding accepted")
	}
}
