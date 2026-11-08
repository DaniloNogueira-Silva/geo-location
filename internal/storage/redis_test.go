package storage

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/google/uuid"
)

func setupTestRedis(t *testing.T) *RedisStore {
	t.Helper()
	cfg := config.Load()
	cfg.RedisDB = 1 // Usa DB 1 isolado para os testes reais

	ctx := context.Background()
	store, err := NewRedisStore(ctx, cfg)
	if err != nil {
		t.Fatalf("falha ao conectar no Redis real (%s) para teste: %v", cfg.RedisAddr, err)
	}

	if err := store.client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("falha ao limpar Redis DB de teste: %v", err)
	}

	t.Cleanup(func() {
		_ = store.client.FlushDB(ctx).Err()
		_ = store.Close()
	})

	return store
}

func TestRedisGeoSearchAndRadiusFiltering(t *testing.T) {
	store := setupTestRedis(t)
	ctx := context.Background()
	now := time.Now().UTC()

	events := []domain.TelemetryPayload{
		{
			EventID:      uuid.NewString(),
			TruckID:      "TRK-SP-CENTRO",
			RouteID:      "SP-RJ-DUTRA",
			Timestamp:    now,
			Location:     domain.GeoPoint{Latitude: -23.55052, Longitude: -46.633308}, // Praça da Sé, SP
			SpeedKmh:     60,
			HeadingDeg:   45,
			EngineTempC:  88,
			CargoTempC:   -18,
			FuelLevelPct: 80,
			Status:       domain.StatusMoving,
		},
		{
			EventID:      uuid.NewString(),
			TruckID:      "TRK-GUARULHOS",
			RouteID:      "SP-RJ-DUTRA",
			Timestamp:    now,
			Location:     domain.GeoPoint{Latitude: -23.4543, Longitude: -46.5337}, // Guarulhos (~14.7 km do centro de SP)
			SpeedKmh:     82,
			HeadingDeg:   70,
			EngineTempC:  91,
			CargoTempC:   -17.5,
			FuelLevelPct: 74,
			Status:       domain.StatusMoving,
		},
		{
			EventID:      uuid.NewString(),
			TruckID:      "TRK-RIO",
			RouteID:      "SP-RJ-DUTRA",
			Timestamp:    now,
			Location:     domain.GeoPoint{Latitude: -22.9068, Longitude: -43.1729}, // Rio de Janeiro (~360 km de SP)
			SpeedKmh:     75,
			HeadingDeg:   90,
			EngineTempC:  89,
			CargoTempC:   -18.2,
			FuelLevelPct: 50,
			Status:       domain.StatusMoving,
		},
	}

	updated, err := store.UpdateFleetPositionsBatch(ctx, events)
	if err != nil {
		t.Fatalf("erro em UpdateFleetPositionsBatch: %v", err)
	}
	if updated != 3 {
		t.Fatalf("esperava 3 caminhões atualizados no Redis GEOADD, obteve %d", updated)
	}

	// 1. Raio de 5km a partir do Centro de SP -> deve retornar apenas TRK-SP-CENTRO
	nearby5km, err := store.FindNearbyTrucks(ctx, -23.55052, -46.633308, 5.0, 10)
	if err != nil {
		t.Fatalf("erro em FindNearbyTrucks (5km): %v", err)
	}
	if len(nearby5km) != 1 || nearby5km[0].TruckID != "TRK-SP-CENTRO" {
		t.Fatalf("esperava apenas [TRK-SP-CENTRO] num raio de 5km, obteve %+v", nearby5km)
	}

	// 2. Raio de 25km a partir do Centro de SP -> deve retornar TRK-SP-CENTRO e TRK-GUARULHOS ordenados por distância
	nearby25km, err := store.FindNearbyTrucks(ctx, -23.55052, -46.633308, 25.0, 10)
	if err != nil {
		t.Fatalf("erro em FindNearbyTrucks (25km): %v", err)
	}
	if len(nearby25km) != 2 {
		t.Fatalf("esperava 2 caminhões num raio de 25km (SP + Guarulhos), obteve %d", len(nearby25km))
	}
	if nearby25km[0].TruckID != "TRK-SP-CENTRO" || nearby25km[1].TruckID != "TRK-GUARULHOS" {
		t.Fatalf("ordenação por distância incorreta: %+v", nearby25km)
	}
	if nearby25km[1].DistanceKm < 12.0 || nearby25km[1].DistanceKm > 17.0 {
		t.Fatalf("distância SP -> Guarulhos fora do esperado (~14.7km): %f km", nearby25km[1].DistanceKm)
	}
	if nearby25km[1].LastState == nil || nearby25km[1].LastState.SpeedKmh != 82 {
		t.Fatalf("estado hidratado do caminhão incorreto: %+v", nearby25km[1].LastState)
	}
}

func TestRedisOutOfOrderTimestampProtection(t *testing.T) {
	store := setupTestRedis(t)
	ctx := context.Background()

	t1200 := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	t1130Delayed := time.Date(2026, 11, 8, 11, 30, 0, 0, time.UTC)
	t1205Newer := time.Date(2026, 11, 8, 12, 5, 0, 0, time.UTC)

	// 1. Chega evento atual (12:00) em São José dos Campos
	evCurrent := domain.TelemetryPayload{
		EventID:      uuid.NewString(),
		TruckID:      "TRK-DELAY-TEST",
		Timestamp:    t1200,
		Location:     domain.GeoPoint{Latitude: -23.1791, Longitude: -45.8872},
		SpeedKmh:     88.0,
		HeadingDeg:   65.0,
		EngineTempC:  92.0,
		CargoTempC:   -18.0,
		FuelLevelPct: 70.0,
		Status:       domain.StatusMoving,
	}

	updated, err := store.UpdateFleetPositionsBatch(ctx, []domain.TelemetryPayload{evCurrent})
	if err != nil || updated != 1 {
		t.Fatalf("esperava updated=1 para primeiro evento, obteve updated=%d, err=%v", updated, err)
	}

	// 2. Chega pacote atrasado (11:30 - meia hora antes!) quando o caminhão ainda estava em São Paulo
	evDelayed := domain.TelemetryPayload{
		EventID:      uuid.NewString(),
		TruckID:      "TRK-DELAY-TEST",
		Timestamp:    t1130Delayed,
		Location:     domain.GeoPoint{Latitude: -23.5505, Longitude: -46.6333},
		SpeedKmh:     40.0,
		HeadingDeg:   60.0,
		EngineTempC:  85.0,
		CargoTempC:   -18.0,
		FuelLevelPct: 75.0,
		Status:       domain.StatusMoving,
	}

	updatedDelayed, err := store.UpdateFleetPositionsBatch(ctx, []domain.TelemetryPayload{evDelayed})
	if err != nil {
		t.Fatalf("erro inesperado ao processar pacote atrasado: %v", err)
	}
	if updatedDelayed != 0 {
		t.Fatalf("FALHA DE ARQUITETURA: pacote atrasado (11:30) sobrescreveu estado mais recente (12:00)! updated=%d", updatedDelayed)
	}

	// Verifica que o estado e a coordenada no Redis continuam sendo os das 12:00 (São José dos Campos)
	state, err := store.GetTruckState(ctx, "TRK-DELAY-TEST")
	if err != nil || state == nil {
		t.Fatalf("erro ao ler estado do caminhão: %v", err)
	}
	if !state.Timestamp.Equal(t1200) || math.Abs(state.Location.Latitude-(-23.1791)) > 0.001 || state.SpeedKmh != 88.0 {
		t.Fatalf("estado no Redis foi corrompido pelo evento atrasado: %+v", state)
	}

	// 3. Chega pacote mais novo (12:05) -> deve atualizar normalmente
	evNewer := evCurrent
	evNewer.EventID = uuid.NewString()
	evNewer.Timestamp = t1205Newer
	evNewer.Location = domain.GeoPoint{Latitude: -22.9500, Longitude: -45.5000}
	evNewer.SpeedKmh = 95.0

	updatedNewer, err := store.UpdateFleetPositionsBatch(ctx, []domain.TelemetryPayload{evNewer})
	if err != nil || updatedNewer != 1 {
		t.Fatalf("esperava updated=1 para evento mais recente (12:05), obteve %d, err=%v", updatedNewer, err)
	}

	stateAfter, err := store.GetTruckState(ctx, "TRK-DELAY-TEST")
	if err != nil || stateAfter == nil {
		t.Fatalf("erro ao ler estado atualizado: %v", err)
	}
	if !stateAfter.Timestamp.Equal(t1205Newer) || stateAfter.SpeedKmh != 95.0 {
		t.Fatalf("esperava estado das 12:05 com velocidade 95.0, obteve %+v", stateAfter)
	}
}

