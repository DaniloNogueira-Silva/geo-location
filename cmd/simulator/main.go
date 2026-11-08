package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/google/uuid"
)

// Route define uma rota rodoviária real com waypoints para interpolação realista de GPS.
type Route struct {
	ID        string
	Waypoints []domain.GeoPoint
}

var brazilRoutes = []Route{
	{
		ID: "SP-RJ-DUTRA",
		Waypoints: []domain.GeoPoint{
			{Latitude: -23.5505, Longitude: -46.6333, AltitudeM: 760}, // São Paulo
			{Latitude: -23.1791, Longitude: -45.8872, AltitudeM: 600}, // São José dos Campos
			{Latitude: -22.5200, Longitude: -44.1044, AltitudeM: 390}, // Volta Redonda
			{Latitude: -22.9068, Longitude: -43.1729, AltitudeM: 15},  // Rio de Janeiro
		},
	},
	{
		ID: "SP-BH-FERNAO-DIAS",
		Waypoints: []domain.GeoPoint{
			{Latitude: -23.5505, Longitude: -46.6333, AltitudeM: 760}, // São Paulo
			{Latitude: -22.9520, Longitude: -46.5419, AltitudeM: 850}, // Bragança Paulista
			{Latitude: -22.2308, Longitude: -45.9364, AltitudeM: 830}, // Pouso Alegre
			{Latitude: -19.9167, Longitude: -43.9345, AltitudeM: 852}, // Belo Horizonte
		},
	},
	{
		ID: "SP-CWB-REGIS-BITTENCOURT",
		Waypoints: []domain.GeoPoint{
			{Latitude: -23.5505, Longitude: -46.6333, AltitudeM: 760}, // São Paulo
			{Latitude: -24.4961, Longitude: -47.8456, AltitudeM: 25},  // Registro
			{Latitude: -25.4284, Longitude: -49.2733, AltitudeM: 934}, // Curitiba
		},
	},
	{
		ID: "SP-UBERLANDIA-ANHANGUERA",
		Waypoints: []domain.GeoPoint{
			{Latitude: -23.5505, Longitude: -46.6333, AltitudeM: 760}, // São Paulo
			{Latitude: -22.9099, Longitude: -47.0626, AltitudeM: 685}, // Campinas
			{Latitude: -21.1704, Longitude: -47.8103, AltitudeM: 546}, // Ribeirão Preto
			{Latitude: -18.9186, Longitude: -48.2772, AltitudeM: 863}, // Uberlândia
		},
	},
}

type SimulatorMetrics struct {
	sentTotal       atomic.Uint64
	accepted202     atomic.Uint64
	batchesFlushed  atomic.Uint64
	bufferedOffline atomic.Uint64
	failedTotal     atomic.Uint64
	totalLatencyUs  atomic.Uint64
}

// TruckSimulator encapsula o estado cinemático e o buffer offline 4G de um caminhão individual.
type TruckSimulator struct {
	TruckID               string
	Route                 Route
	SegmentIdx            int
	Progress              float64
	Direction             int
	FuelLevel             float64
	Odometer              float64
	CargoBaseTemp         float64
	OfflineProb           float64
	OfflineTicksRemaining int
	OfflineBuffer         []domain.TelemetryPayload
}

// NewTruckSimulator inicializa o estado de um caminhão em sua rota.
func NewTruckSimulator(truckID string, route Route, offlineProb float64) *TruckSimulator {
	return &TruckSimulator{
		TruckID:       truckID,
		Route:         route,
		SegmentIdx:    rand.IntN(len(route.Waypoints) - 1),
		Progress:      rand.Float64(),
		Direction:     1,
		FuelLevel:     60.0 + rand.Float64()*40.0,
		Odometer:      50000.0 + rand.Float64()*150000.0,
		CargoBaseTemp: -18.0,
		OfflineProb:   offlineProb,
	}
}

// NextTelemetry calcula o próximo ponto geográfico e telemetria no instante 'now' e avança o caminhão na rota.
func (t *TruckSimulator) NextTelemetry(now time.Time, interval time.Duration) domain.TelemetryPayload {
	p1 := t.Route.Waypoints[t.SegmentIdx]
	p2 := t.Route.Waypoints[t.SegmentIdx+1]
	if t.Direction == -1 {
		p1, p2 = p2, p1
	}

	lat := p1.Latitude + (p2.Latitude-p1.Latitude)*t.Progress + (rand.Float64()-0.5)*0.0002
	lon := p1.Longitude + (p2.Longitude-p1.Longitude)*t.Progress + (rand.Float64()-0.5)*0.0002
	alt := p1.AltitudeM + (p2.AltitudeM-p1.AltitudeM)*t.Progress

	speed := 72.0 + rand.Float64()*22.0
	status := domain.StatusMoving
	if rand.Float64() < 0.03 {
		speed = 0
		status = domain.StatusIdle
	}

	heading := calculateBearing(p1.Latitude, p1.Longitude, p2.Latitude, p2.Longitude)
	engineTemp := 86.0 + (speed/100.0)*8.0 + (rand.Float64()-0.5)*2.0
	cargoTemp := t.CargoBaseTemp + (rand.Float64()-0.5)*1.5

	if cargoTemp > -12.0 || engineTemp > 105.0 {
		status = domain.StatusAlert
	}

	payload := domain.TelemetryPayload{
		EventID:   uuid.NewString(),
		TruckID:   t.TruckID,
		RouteID:   t.Route.ID,
		Timestamp: now.UTC(),
		Location: domain.GeoPoint{
			Latitude:  math.Round(lat*1000000) / 1000000,
			Longitude: math.Round(lon*1000000) / 1000000,
			AltitudeM: math.Round(alt*10) / 10,
		},
		SpeedKmh:     math.Round(speed*10) / 10,
		HeadingDeg:   math.Round(heading*10) / 10,
		EngineTempC:  math.Round(engineTemp*10) / 10,
		CargoTempC:   math.Round(cargoTemp*10) / 10,
		FuelLevelPct: math.Round(t.FuelLevel*10) / 10,
		OdometerKm:   math.Round(t.Odometer*10) / 10,
		Status:       status,
	}

	// Avança o estado cinemático para o próximo tick
	hoursElapsed := interval.Hours()
	t.Odometer += speed * hoursElapsed
	t.FuelLevel = math.Max(5.0, t.FuelLevel-0.015)
	if t.FuelLevel <= 5.5 {
		t.FuelLevel = 100.0
	}

	t.Progress += 0.02 + rand.Float64()*0.015
	if t.Progress >= 1.0 {
		t.Progress = 0.0
		t.SegmentIdx++
		if t.SegmentIdx >= len(t.Route.Waypoints)-1 {
			t.SegmentIdx = 0
			t.Direction *= -1
		}
	}

	return payload
}

// TickAndDispatch gera a telemetria do ciclo atual e decide se armazena no buffer offline (queda de 4G)
// ou se despacha para o Gateway (individual ou descarregando lote acumulado em /telemetry/batch).
func (t *TruckSimulator) TickAndDispatch(
	ctx context.Context,
	client *http.Client,
	gatewayURL string,
	now time.Time,
	interval time.Duration,
	metrics *SimulatorMetrics,
) {
	payload := t.NextTelemetry(now, interval)

	// Simula entrada em zona de sombra 4G caso configurado
	if t.OfflineTicksRemaining == 0 && len(t.OfflineBuffer) == 0 && t.OfflineProb > 0 && rand.Float64() < t.OfflineProb {
		t.OfflineTicksRemaining = 3 + rand.IntN(4) // Fica de 3 a 6 ciclos acumulando eventos offline
	}

	if t.OfflineTicksRemaining > 0 {
		t.OfflineTicksRemaining--
		t.OfflineBuffer = append(t.OfflineBuffer, payload)
		metrics.bufferedOffline.Add(1)
		return
	}

	// Sinal 4G restabelecido e há eventos atrasados acumulados no caminhão: envia lote via POST /telemetry/batch
	if len(t.OfflineBuffer) > 0 {
		t.OfflineBuffer = append(t.OfflineBuffer, payload)
		batchURL := strings.TrimSuffix(gatewayURL, "/") + "/batch"
		if sendTelemetryBatchHTTP(ctx, client, batchURL, t.OfflineBuffer, metrics) {
			t.OfflineBuffer = t.OfflineBuffer[:0]
		}
		return
	}

	// Operação normal online: envia pacote individual via POST /telemetry
	sendTelemetryHTTP(ctx, client, gatewayURL, &payload, metrics)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := config.Load()

	trucksFlag := flag.Int("trucks", cfg.SimulatorTruckCount, "Quantidade de caminhões concorrentes (Goroutines)")
	intervalFlag := flag.Duration("interval", cfg.SimulatorInterval, "Intervalo de envio de telemetria por caminhão (ex: 2s)")
	gatewayURLFlag := flag.String("url", cfg.SimulatorGatewayURL, "URL do Gateway de Ingestão (POST /telemetry)")
	offlineProbFlag := flag.Float64("offline-prob", 0.05, "Probabilidade de queda temporária de sinal 4G por ciclo (0.0 a 1.0)")
	durationFlag := flag.Duration("duration", 0, "Duração máxima da simulação (0 = executa continuamente até Ctrl+C)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *durationFlag > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *durationFlag)
		defer cancel()
	}

	httpClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        2048,
			MaxIdleConnsPerHost: 2048,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	metrics := &SimulatorMetrics{}

	slog.Info("🚚 iniciando Simulador de Frota em Tempo Real",
		"trucks_goroutines", *trucksFlag,
		"interval", intervalFlag.String(),
		"offline_prob", *offlineProbFlag,
		"gateway_url", *gatewayURLFlag,
	)

	go reportMetricsLoop(ctx, metrics, *trucksFlag)

	var wg sync.WaitGroup
	wg.Add(*trucksFlag)

	for i := 1; i <= *trucksFlag; i++ {
		truckID := fmt.Sprintf("TRK-%04d", i)
		route := brazilRoutes[(i-1)%len(brazilRoutes)]

		go func(id string, r Route) {
			defer wg.Done()
			jitter := time.Duration(rand.IntN(max(1, int(intervalFlag.Milliseconds())))) * time.Millisecond
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitter):
			}

			runTruckGoroutine(ctx, httpClient, *gatewayURLFlag, id, r, *intervalFlag, *offlineProbFlag, metrics)
		}(truckID, route)
	}

	wg.Wait()
	slog.Info("simulação finalizada",
		"total_sent", metrics.sentTotal.Load(),
		"accepted_202", metrics.accepted202.Load(),
		"batches_flushed", metrics.batchesFlushed.Load(),
		"failed", metrics.failedTotal.Load(),
	)
}

func runTruckGoroutine(
	ctx context.Context,
	client *http.Client,
	gatewayURL string,
	truckID string,
	route Route,
	interval time.Duration,
	offlineProb float64,
	metrics *SimulatorMetrics,
) {
	sim := NewTruckSimulator(truckID, route, offlineProb)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		sim.TickAndDispatch(ctx, client, gatewayURL, time.Now().UTC(), interval, metrics)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sendTelemetryHTTP(
	ctx context.Context,
	client *http.Client,
	gatewayURL string,
	payload *domain.TelemetryPayload,
	metrics *SimulatorMetrics,
) bool {
	body, err := json.Marshal(payload)
	if err != nil {
		metrics.failedTotal.Add(1)
		return false
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL, bytes.NewReader(body))
	if err != nil {
		metrics.failedTotal.Add(1)
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	latencyUs := uint64(time.Since(start).Microseconds())
	metrics.sentTotal.Add(1)
	metrics.totalLatencyUs.Add(latencyUs)

	if err != nil {
		metrics.failedTotal.Add(1)
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusAccepted {
		metrics.accepted202.Add(1)
		return true
	}
	metrics.failedTotal.Add(1)
	return false
}

func sendTelemetryBatchHTTP(
	ctx context.Context,
	client *http.Client,
	batchURL string,
	batch []domain.TelemetryPayload,
	metrics *SimulatorMetrics,
) bool {
	body, err := json.Marshal(batch)
	if err != nil {
		metrics.failedTotal.Add(uint64(len(batch)))
		return false
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, batchURL, bytes.NewReader(body))
	if err != nil {
		metrics.failedTotal.Add(uint64(len(batch)))
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	latencyUs := uint64(time.Since(start).Microseconds())
	count := uint64(len(batch))
	metrics.sentTotal.Add(count)
	metrics.totalLatencyUs.Add(latencyUs)

	if err != nil {
		metrics.failedTotal.Add(count)
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusAccepted {
		metrics.accepted202.Add(count)
		metrics.batchesFlushed.Add(1)
		return true
	}
	metrics.failedTotal.Add(count)
	return false
}

func reportMetricsLoop(ctx context.Context, metrics *SimulatorMetrics, activeTrucks int) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastSent uint64
	lastTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			currentSent := metrics.sentTotal.Load()
			accepted := metrics.accepted202.Load()
			batches := metrics.batchesFlushed.Load()
			buffered := metrics.bufferedOffline.Load()
			failed := metrics.failedTotal.Load()
			totalLatUs := metrics.totalLatencyUs.Load()

			elapsed := now.Sub(lastTime).Seconds()
			rps := float64(currentSent-lastSent) / elapsed
			lastSent = currentSent
			lastTime = now

			var avgLatencyMs float64
			if currentSent > 0 {
				avgLatencyMs = (float64(totalLatUs) / float64(currentSent)) / 1000.0
			}

			slog.Info("📊 Métricas do Simulador",
				"active_trucks", activeTrucks,
				"throughput_rps", fmt.Sprintf("%.1f eventos/s", rps),
				"sent_total", currentSent,
				"accepted_202", accepted,
				"offline_batches_flushed", batches,
				"offline_events_buffered", buffered,
				"failed", failed,
				"avg_latency_ms", fmt.Sprintf("%.2f ms", avgLatencyMs),
			)
		}
	}
}

// calculateBearing calcula o azimute (0-360 graus) entre duas coordenadas geográficas.
func calculateBearing(lat1, lon1, lat2, lon2 float64) float64 {
	phi1 := lat1 * math.Pi / 180.0
	phi2 := lat2 * math.Pi / 180.0
	deltaLon := (lon2 - lon1) * math.Pi / 180.0

	y := math.Sin(deltaLon) * math.Cos(phi2)
	x := math.Cos(phi1)*math.Sin(phi2) - math.Sin(phi1)*math.Cos(phi2)*math.Cos(deltaLon)
	brng := math.Atan2(y, x) * 180.0 / math.Pi
	return math.Mod(brng+360.0, 360.0)
}
