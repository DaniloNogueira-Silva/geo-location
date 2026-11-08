package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsKinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/danil/geo-location/internal/kinesis"
	"github.com/danil/geo-location/internal/observability"
	"github.com/danil/geo-location/internal/storage"
	"github.com/google/uuid"
)

func TestGatewayEndpointsEndToEnd(t *testing.T) {
	ctx := context.Background()
	cfg := config.Load()
	cfg.KinesisStreamName = fmt.Sprintf("gateway-test-stream-%d", time.Now().UnixNano()%100000)
	cfg.KinesisShards = 1
	cfg.RedisDB = 2

	kinClient, err := kinesis.NewClient(ctx, cfg)
	if err != nil {
		t.Fatalf("erro ao criar cliente Kinesis: %v", err)
	}
	if err := kinClient.EnsureStreamExists(ctx); err != nil {
		t.Fatalf("erro ao criar stream no MiniStack: %v", err)
	}
	t.Cleanup(func() {
		_, _ = kinClient.Raw().DeleteStream(context.Background(), &awsKinesis.DeleteStreamInput{
			StreamName: aws.String(cfg.KinesisStreamName),
		})
	})

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

	_, handler := NewGatewayServer(kinClient, redisStore, pgStore, cwClient)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	truckID := fmt.Sprintf("TRK-GW-%d", time.Now().UnixNano()%10000)
	validPayload := domain.TelemetryPayload{
		EventID:      uuid.NewString(),
		TruckID:      truckID,
		RouteID:      "SP-RJ-DUTRA",
		Timestamp:    time.Now().UTC().Truncate(time.Millisecond),
		Location:     domain.GeoPoint{Latitude: -23.5505, Longitude: -46.6333, AltitudeM: 760},
		SpeedKmh:     84.5,
		HeadingDeg:   72.0,
		EngineTempC:  91.0,
		CargoTempC:   -18.0,
		FuelLevelPct: 78.0,
		OdometerKm:   120500.5,
		Status:       domain.StatusMoving,
	}

	// 1. POST /telemetry válido -> 202 Accepted
	bodyBytes, _ := json.Marshal(validPayload)
	resp, err := http.Post(ts.URL+"/telemetry", "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("erro no POST /telemetry: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("esperava 202 Accepted, obteve %d", resp.StatusCode)
	}

	// 2. POST /telemetry com JSON malformado -> 400 Bad Request
	respBadJSON, err := http.Post(ts.URL+"/telemetry", "application/json", bytes.NewReader([]byte(`{"truck_id":`)))
	if err != nil {
		t.Fatalf("erro na requisição malformada: %v", err)
	}
	respBadJSON.Body.Close()
	if respBadJSON.StatusCode != http.StatusBadRequest {
		t.Fatalf("esperava 400 Bad Request para JSON inválido, obteve %d", respBadJSON.StatusCode)
	}

	// 3. POST /telemetry com latitude fora do limite WGS84 -> 422 Unprocessable Entity
	invalidPayload := validPayload
	invalidPayload.Location.Latitude = 135.0
	invBytes, _ := json.Marshal(invalidPayload)
	respInvalid, err := http.Post(ts.URL+"/telemetry", "application/json", bytes.NewReader(invBytes))
	if err != nil {
		t.Fatalf("erro na requisição inválida: %v", err)
	}
	respInvalid.Body.Close()
	if respInvalid.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("esperava 422 Unprocessable Entity para latitude 135, obteve %d", respInvalid.StatusCode)
	}

	// 4. POST /telemetry/batch com lote de 2 eventos -> 202 Accepted
	ev2 := validPayload
	ev2.EventID = uuid.NewString()
	ev2.Timestamp = validPayload.Timestamp.Add(2 * time.Second)
	batchBytes, _ := json.Marshal([]domain.TelemetryPayload{validPayload, ev2})
	respBatch, err := http.Post(ts.URL+"/telemetry/batch", "application/json", bytes.NewReader(batchBytes))
	if err != nil {
		t.Fatalf("erro no POST /telemetry/batch: %v", err)
	}
	respBatch.Body.Close()
	if respBatch.StatusCode != http.StatusAccepted {
		t.Fatalf("esperava 202 Accepted no lote, obteve %d", respBatch.StatusCode)
	}

	// 5. Persiste os eventos no Redis e PostgreSQL e testa os endpoints de leitura do Gateway
	_, _ = redisStore.UpdateFleetPositionsBatch(ctx, []domain.TelemetryPayload{validPayload, ev2})
	_, _ = pgStore.InsertBatch(ctx, []domain.TelemetryPayload{validPayload, ev2})

	// GET /trucks/{id}/state
	respState, err := http.Get(fmt.Sprintf("%s/trucks/%s/state", ts.URL, truckID))
	if err != nil {
		t.Fatalf("erro no GET /trucks/{id}/state: %v", err)
	}
	defer respState.Body.Close()
	if respState.StatusCode != http.StatusOK {
		t.Fatalf("esperava 200 OK em /state, obteve %d", respState.StatusCode)
	}
	var stateOut domain.TelemetryPayload
	if err := json.NewDecoder(respState.Body).Decode(&stateOut); err != nil {
		t.Fatalf("erro ao decodificar /state: %v", err)
	}
	if stateOut.EventID != ev2.EventID {
		t.Fatalf("esperava o evento mais recente (%s) no /state, obteve %s", ev2.EventID, stateOut.EventID)
	}

	// GET /trucks/nearby
	respNearby, err := http.Get(fmt.Sprintf("%s/trucks/nearby?lat=-23.5505&lon=-46.6333&radius_km=5", ts.URL))
	if err != nil {
		t.Fatalf("erro no GET /trucks/nearby: %v", err)
	}
	defer respNearby.Body.Close()
	if respNearby.StatusCode != http.StatusOK {
		t.Fatalf("esperava 200 OK em /trucks/nearby, obteve %d", respNearby.StatusCode)
	}

	// GET /trucks/{id}/history
	respHist, err := http.Get(fmt.Sprintf("%s/trucks/%s/history?limit=10", ts.URL, truckID))
	if err != nil {
		t.Fatalf("erro no GET /trucks/{id}/history: %v", err)
	}
	defer respHist.Body.Close()
	if respHist.StatusCode != http.StatusOK {
		t.Fatalf("esperava 200 OK em /history, obteve %d", respHist.StatusCode)
	}
	var histOut struct {
		Count   int                       `json:"count"`
		History []domain.TelemetryPayload `json:"history"`
	}
	if err := json.NewDecoder(respHist.Body).Decode(&histOut); err != nil {
		t.Fatalf("erro ao decodificar /history: %v", err)
	}
	if histOut.Count != 2 {
		t.Fatalf("esperava 2 eventos no histórico do caminhão, obteve %d", histOut.Count)
	}

	// GET /health
	respHealth, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("erro no GET /health: %v", err)
	}
	defer respHealth.Body.Close()
	var healthOut struct {
		AcceptedTotal uint64 `json:"accepted_total"`
		RejectedTotal uint64 `json:"rejected_total"`
	}
	_ = json.NewDecoder(respHealth.Body).Decode(&healthOut)
	if healthOut.AcceptedTotal != 3 || healthOut.RejectedTotal != 2 {
		t.Fatalf("métricas de /health incorretas: esperava accepted=3, rejected=2; obteve %+v", healthOut)
	}
}

