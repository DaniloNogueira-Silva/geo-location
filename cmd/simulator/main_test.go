package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danil/geo-location/internal/domain"
)

func TestCalculateBearingCardinalDirections(t *testing.T) {
	// Norte (mesma longitude, latitude subindo) -> ~0 graus
	bNorth := calculateBearing(-23.0, -46.0, -22.0, -46.0)
	if math.Abs(bNorth-0.0) > 0.5 && math.Abs(bNorth-360.0) > 0.5 {
		t.Fatalf("esperava ~0° para Norte, obteve %f", bNorth)
	}

	// Leste (mesma latitude, longitude subindo) -> ~90 graus
	bEast := calculateBearing(0.0, -46.0, 0.0, -45.0)
	if math.Abs(bEast-90.0) > 0.5 {
		t.Fatalf("esperava ~90° para Leste, obteve %f", bEast)
	}

	// Sul (mesma longitude, latitude descendo) -> ~180 graus
	bSouth := calculateBearing(-22.0, -46.0, -23.0, -46.0)
	if math.Abs(bSouth-180.0) > 0.5 {
		t.Fatalf("esperava ~180° para Sul, obteve %f", bSouth)
	}

	// Oeste (mesma latitude, longitude descendo) -> ~270 graus
	bWest := calculateBearing(0.0, -45.0, 0.0, -46.0)
	if math.Abs(bWest-270.0) > 0.5 {
		t.Fatalf("esperava ~270° para Oeste, obteve %f", bWest)
	}
}

func TestTruckSimulatorOffline4GBufferAndBatchFlush(t *testing.T) {
	var singleCalls atomic.Int32
	var batchCalls atomic.Int32
	var receivedBatch []domain.TelemetryPayload

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/telemetry":
			singleCalls.Add(1)
			w.WriteHeader(http.StatusAccepted)
		case "/telemetry/batch":
			batchCalls.Add(1)
			_ = json.NewDecoder(r.Body).Decode(&receivedBatch)
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	sim := NewTruckSimulator("TRK-0042", brazilRoutes[0], 0.0)
	metrics := &SimulatorMetrics{}
	ctx := context.Background()
	t0 := time.Date(2026, 11, 8, 14, 0, 0, 0, time.UTC)
	interval := 2 * time.Second

	// 1. Ciclo Online normal -> deve chamar POST /telemetry
	sim.TickAndDispatch(ctx, ts.Client(), ts.URL+"/telemetry", t0, interval, metrics)
	if singleCalls.Load() != 1 || batchCalls.Load() != 0 {
		t.Fatalf("esperava 1 chamada em /telemetry e 0 em /batch, obteve single=%d, batch=%d", singleCalls.Load(), batchCalls.Load())
	}

	// 2. Simula perda de sinal 4G por 3 ciclos consecutivos
	sim.OfflineTicksRemaining = 3
	t1 := t0.Add(2 * time.Second)
	t2 := t0.Add(4 * time.Second)
	t3 := t0.Add(6 * time.Second)

	sim.TickAndDispatch(ctx, ts.Client(), ts.URL+"/telemetry", t1, interval, metrics)
	sim.TickAndDispatch(ctx, ts.Client(), ts.URL+"/telemetry", t2, interval, metrics)
	sim.TickAndDispatch(ctx, ts.Client(), ts.URL+"/telemetry", t3, interval, metrics)

	// Durante a perda de sinal, nenhuma requisição HTTP pode ter sido disparada e 3 eventos devem estar no buffer
	if singleCalls.Load() != 1 || batchCalls.Load() != 0 {
		t.Fatalf("não deveria ter enviado HTTP durante queda de sinal 4G: single=%d, batch=%d", singleCalls.Load(), batchCalls.Load())
	}
	if len(sim.OfflineBuffer) != 3 {
		t.Fatalf("esperava 3 eventos acumulados no buffer offline do caminhão, obteve %d", len(sim.OfflineBuffer))
	}

	// 3. Sinal 4G retorna no ciclo t4 -> deve enviar todos os 4 eventos (3 acumulados + atual) via POST /telemetry/batch
	t4 := t0.Add(8 * time.Second)
	sim.TickAndDispatch(ctx, ts.Client(), ts.URL+"/telemetry", t4, interval, metrics)

	if batchCalls.Load() != 1 {
		t.Fatalf("esperava 1 chamada a POST /telemetry/batch ao recuperar sinal 4G, obteve %d", batchCalls.Load())
	}
	if len(receivedBatch) != 4 {
		t.Fatalf("esperava lote com 4 eventos acumulados, obteve %d", len(receivedBatch))
	}
	if len(sim.OfflineBuffer) != 0 {
		t.Fatalf("buffer offline deveria ter sido limpo após 202 Accepted, mas restaram %d itens", len(sim.OfflineBuffer))
	}

	// Verifica que cada evento do lote preservou o timestamp exato gerado no caminhão durante a sombra 4G
	expectedTimestamps := []time.Time{t1, t2, t3, t4}
	for i, expTS := range expectedTimestamps {
		if !receivedBatch[i].Timestamp.Equal(expTS) {
			t.Fatalf("timestamp do item %d no lote incorreto: esperava %v, obteve %v", i, expTS, receivedBatch[i].Timestamp)
		}
	}
}

