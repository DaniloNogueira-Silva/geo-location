package config

import (
	"os"
	"strconv"
	"time"
)

// Config concentra todas as configurações dos serviços (Gateway, Lambda e Simulador).
type Config struct {
	// Servidor Gateway HTTP
	GatewayAddr string

	// AWS / MiniStack
	AWSRegion         string
	AWSEndpoint       string
	AWSAccessKey      string
	AWSSecretKey      string
	KinesisStreamName string
	KinesisShards     int32

	// Armazenamento Híbrido
	RedisAddr    string
	RedisPass    string
	RedisDB      int
	PostgresDSN  string

	// Simulador
	SimulatorGatewayURL string
	SimulatorTruckCount int
	SimulatorInterval   time.Duration
}

// Load carrega as configurações a partir de variáveis de ambiente com defaults prontos para o MiniStack local.
func Load() Config {
	return Config{
		GatewayAddr:         getEnv("GATEWAY_ADDR", ":8080"),
		AWSRegion:           getEnv("AWS_REGION", "us-east-1"),
		AWSEndpoint:         getEnv("AWS_ENDPOINT_URL", "http://localhost:4566"),
		AWSAccessKey:        getEnv("AWS_ACCESS_KEY_ID", "test"),
		AWSSecretKey:        getEnv("AWS_SECRET_ACCESS_KEY", "test"),
		KinesisStreamName:   getEnv("KINESIS_STREAM_NAME", "fleet-telemetry-stream"),
		KinesisShards:       int32(getEnvInt("KINESIS_SHARDS", 2)),
		RedisAddr:           getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPass:           getEnv("REDIS_PASSWORD", ""),
		RedisDB:             getEnvInt("REDIS_DB", 0),
		PostgresDSN:         getEnv("POSTGRES_DSN", "postgres://fleet_admin:fleet_password@localhost:5432/fleet_telemetry?sslmode=disable"),
		SimulatorGatewayURL: getEnv("SIMULATOR_GATEWAY_URL", "http://localhost:8080/telemetry"),
		SimulatorTruckCount: getEnvInt("SIMULATOR_TRUCK_COUNT", 1000),
		SimulatorInterval:   time.Duration(getEnvInt("SIMULATOR_INTERVAL_MS", 2000)) * time.Millisecond,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return fallback
}

