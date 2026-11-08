package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/danil/geo-location/internal/observability"
	"github.com/danil/geo-location/internal/storage"
	"github.com/google/uuid"
)

func TestLambdaHandleKinesisEventHybridStorage(t *testing.T) {
	ctx := context.Background()
	cfg := config.Load()
	cfg.RedisDB = 3

	redisStore, err := storage.NewRedisStore(ctx, cfg)
	if err != nil {
		t.Fatalf("erro ao conectar ao Redis: %v", err)
	}
	t.Cleanup(func() { _ = redisStore.Close() })

	pgStore, err := storage.NewPostgresStore(ctx, cfg)
	if err != nil {
		t.Fatalf("erro ao conectar ao PostgreSQL: %v", err)
	}
	t.Cleanup(func() { pgStore.Close() })

	cwClient, _ := observability.NewCloudWatchClient(ctx, cfg)

	processor := &TelemetryProcessor{
		redisStore:    redisStore,
		postgresStore: pgStore,
		cwClient:      cwClient,
	}

	truckID := fmt.Sprintf("TRK-LMB-%d", time.Now().UnixNano()%100000)
	tCurrent := time.Now().UTC().Truncate(time.Millisecond)
	tOldDelayed := tCurrent.Add(-20 * time.Minute) // Evento atrasado (perda de sinal 4G anterior)

	evCurrent := domain.TelemetryPayload{
		EventID:      uuid.NewString(),
		TruckID:      truckID,
		RouteID:      "SP-CWB-REGIS-BITTENCOURT",
		Timestamp:    tCurrent,
		Location:     domain.GeoPoint{Latitude: -24.4961, Longitude: -47.8456}, // Registro
		SpeedKmh:     88.0,
		HeadingDeg:   210.0,
		EngineTempC:  92.0,
		CargoTempC:   -18.5,
		FuelLevelPct: 64.0,
		Status:       domain.StatusMoving,
	}

	evDelayed := domain.TelemetryPayload{
		EventID:      uuid.NewString(),
		TruckID:      truckID,
		RouteID:      "SP-CWB-REGIS-BITTENCOURT",
		Timestamp:    tOldDelayed,
		Location:     domain.GeoPoint{Latitude: -23.5505, Longitude: -46.6333}, // São Paulo (20 min antes)
		SpeedKmh:     60.0,
		HeadingDeg:   205.0,
		EngineTempC:  87.0,
		CargoTempC:   -18.0,
		FuelLevelPct: 70.0,
		Status:       domain.StatusMoving,
	}

	rawCurrent, _ := json.Marshal(evCurrent)
	rawDelayed, _ := json.Marshal(evDelayed)

	kinesisEvent := events.KinesisEvent{
		Records: []events.KinesisEventRecord{
			{
				EventID: "shardId-000000000000:1",
				Kinesis: events.KinesisRecord{
					PartitionKey: truckID,
					Data:         rawCurrent,
				},
			},
			{
				EventID: "shardId-000000000000:2",
				Kinesis: events.KinesisRecord{
					PartitionKey: truckID,
					Data:         rawDelayed,
				},
			},
			{
				// Registro corrompido: deve ser ignorado sem derrubar o lote inteiro
				EventID: "shardId-000000000000:3",
				Kinesis: events.KinesisRecord{
					PartitionKey: truckID,
					Data:         []byte(`{invalid-json`),
				},
			},
		},
	}

	if err := processor.HandleKinesisEvent(ctx, kinesisEvent); err != nil {
		t.Fatalf("erro em HandleKinesisEvent: %v", err)
	}

	// 1. No Redis (Tempo Real): DEVE manter a coordenada mais recente (Registro, tCurrent) e ignorar a atrasada
	state, err := redisStore.GetTruckState(ctx, truckID)
	if err != nil || state == nil {
		t.Fatalf("falha ao obter estado do caminhão no Redis pós-Lambda: %v", err)
	}
	if state.EventID != evCurrent.EventID || state.Location.Latitude != -24.4961 {
		t.Fatalf("Redis não manteve o evento mais recente: %+v", state)
	}

	// 2. No PostgreSQL (Histórico): DEVE conter AMBOS os eventos (o atual e o atrasado) ordenados pelo timestamp do caminhão
	history, err := pgStore.GetTruckHistory(ctx, truckID, tOldDelayed.Add(-1*time.Hour), tCurrent.Add(1*time.Hour), 10)
	if err != nil {
		t.Fatalf("falha ao consultar histórico no PostgreSQL pós-Lambda: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("esperava 2 eventos gravados no histórico do PostgreSQL, obteve %d", len(history))
	}
	if history[0].EventID != evCurrent.EventID || history[1].EventID != evDelayed.EventID {
		t.Fatalf("histórico no PostgreSQL fora de ordem: %+v", history)
	}
}

