package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/google/uuid"
)

func setupTestPostgres(t *testing.T) *PostgresStore {
	t.Helper()
	cfg := config.Load()
	ctx := context.Background()

	store, err := NewPostgresStore(ctx, cfg)
	if err != nil {
		t.Fatalf("falha ao conectar no PostgreSQL real para teste: %v", err)
	}

	t.Cleanup(func() {
		store.Close()
	})
	return store
}

func TestPostgresBatchInsertOrderingAndIdempotency(t *testing.T) {
	store := setupTestPostgres(t)
	ctx := context.Background()

	truckID := fmt.Sprintf("TRK-PG-%d", time.Now().UnixNano()%100000)
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), "DELETE FROM truck_telemetry WHERE truck_id = $1", truckID)
	})

	t0 := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	t5 := t0.Add(5 * time.Minute)
	t10 := t0.Add(10 * time.Minute)

	ev0 := domain.TelemetryPayload{
		EventID:      uuid.NewString(),
		TruckID:      truckID,
		RouteID:      "SP-BH-FERNAO-DIAS",
		Timestamp:    t0,
		Location:     domain.GeoPoint{Latitude: -23.5505, Longitude: -46.6333, AltitudeM: 760},
		SpeedKmh:     70,
		HeadingDeg:   30,
		EngineTempC:  88,
		CargoTempC:   -18,
		FuelLevelPct: 90,
		OdometerKm:   10000,
		Status:       domain.StatusMoving,
	}
	ev5 := ev0
	ev5.EventID = uuid.NewString()
	ev5.Timestamp = t5
	ev5.Location.Latitude = -23.4000
	ev5.SpeedKmh = 80
	ev5.OdometerKm = 10006

	ev10 := ev0
	ev10.EventID = uuid.NewString()
	ev10.Timestamp = t10
	ev10.Location.Latitude = -23.2500
	ev10.SpeedKmh = 85
	ev10.OdometerKm = 10013

	// 1. Envia lote propositalmente fora de ordem (t10, t0, t5)
	inserted, err := store.InsertBatch(ctx, []domain.TelemetryPayload{ev10, ev0, ev5})
	if err != nil {
		t.Fatalf("erro no InsertBatch: %v", err)
	}
	if inserted != 3 {
		t.Fatalf("esperava 3 registros inseridos no PostgreSQL, obteve %d", inserted)
	}

	// 2. Testa idempotência (retry da Lambda reenviando os mesmos 3 eventos + 1 evento inédito t15)
	ev15 := ev0
	ev15.EventID = uuid.NewString()
	ev15.Timestamp = t0.Add(15 * time.Minute)
	ev15.Location.Latitude = -23.1000
	ev15.SpeedKmh = 90

	insertedRetry, err := store.InsertBatch(ctx, []domain.TelemetryPayload{ev0, ev5, ev10, ev15})
	if err != nil {
		t.Fatalf("erro no InsertBatch de retry: %v", err)
	}
	if insertedRetry != 1 {
		t.Fatalf("idempotência falhou: esperava apenas 1 linha nova inserida (ignorando 3 duplicadas via ON CONFLICT), obteve %d", insertedRetry)
	}

	// 3. Consulta histórico e verifica que todos os 4 eventos estão presentes e ordenados por event_timestamp DESC
	history, err := store.GetTruckHistory(ctx, truckID, t0.Add(-1*time.Hour), t0.Add(2*time.Hour), 100)
	if err != nil {
		t.Fatalf("erro no GetTruckHistory: %v", err)
	}
	if len(history) != 4 {
		t.Fatalf("esperava 4 eventos no histórico do caminhão, obteve %d", len(history))
	}

	// Verifica ordenação cronológica reversa exata pelo timestamp gerado no caminhão (ev15 -> ev10 -> ev5 -> ev0)
	expectedOrder := []string{ev15.EventID, ev10.EventID, ev5.EventID, ev0.EventID}
	for i, expID := range expectedOrder {
		if history[i].EventID != expID {
			t.Fatalf("posição %d do histórico incorreta: esperava event_id %s, obteve %s", i, expID, history[i].EventID)
		}
	}
}

