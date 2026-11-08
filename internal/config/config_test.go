package config

import (
	"testing"
	"time"
)

func TestLoadDefaultsAndOverrides(t *testing.T) {
	t.Setenv("GATEWAY_ADDR", ":9090")
	t.Setenv("KINESIS_SHARDS", "4")
	t.Setenv("SIMULATOR_TRUCK_COUNT", "250")
	t.Setenv("SIMULATOR_INTERVAL_MS", "1500")

	cfg := Load()

	if cfg.GatewayAddr != ":9090" {
		t.Fatalf("esperava GatewayAddr :9090, obteve %s", cfg.GatewayAddr)
	}
	if cfg.KinesisShards != 4 {
		t.Fatalf("esperava KinesisShards 4, obteve %d", cfg.KinesisShards)
	}
	if cfg.SimulatorTruckCount != 250 {
		t.Fatalf("esperava SimulatorTruckCount 250, obteve %d", cfg.SimulatorTruckCount)
	}
	if cfg.SimulatorInterval != 1500*time.Millisecond {
		t.Fatalf("esperava SimulatorInterval 1500ms, obteve %v", cfg.SimulatorInterval)
	}

	// Testa fallback quando inteiro é inválido
	t.Setenv("KINESIS_SHARDS", "invalido")
	cfgFallback := Load()
	if cfgFallback.KinesisShards != 2 {
		t.Fatalf("esperava fallback KinesisShards=2 para valor inválido, obteve %d", cfgFallback.KinesisShards)
	}
}

