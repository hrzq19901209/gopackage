package config

import "testing"

func TestConfig(t *testing.T) {
	config := LoadConfig("test.conf")
	if config.Master != "127.0.0.1:4204" {
		t.Error("expect %v, got %v", "127.0.0.1:4204", config.Master)
	}
}
