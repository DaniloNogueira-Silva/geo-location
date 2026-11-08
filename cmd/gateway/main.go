package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/danil/geo-location/internal/kinesis"
	"github.com/danil/geo-location/internal/observability"
	"github.com/danil/geo-location/internal/storage"
)

type GatewayServer struct {
	kinesisClient *kinesis.Client
	redisStore    *storage.RedisStore
	postgresStore *storage.PostgresStore
	cwClient      *observability.CloudWatchClient

	acceptedTotal      atomic.Uint64
	rejectedTotal      atomic.Uint64
	kinesisErrorsTotal atomic.Uint64
}

// NewGatewayServer constrói o servidor e registra todas as rotas HTTP da API Edge.
func NewGatewayServer(
	kinClient *kinesis.Client,
	redisStore *storage.RedisStore,
	pgStore *storage.PostgresStore,
	cwClient *observability.CloudWatchClient,
) (*GatewayServer, http.Handler) {
	srv := &GatewayServer{
		kinesisClient: kinClient,
		redisStore:    redisStore,
		postgresStore: pgStore,
		cwClient:      cwClient,
	}

	mux := http.NewServeMux()
	// Endpoints de Ingestão (Edge API -> Kinesis -> 202 Accepted)
	mux.HandleFunc("POST /telemetry", srv.handlePostTelemetry)
	mux.HandleFunc("POST /telemetry/batch", srv.handlePostTelemetryBatch)

	// Endpoints de Consulta (Tempo Real no Redis + Histórico no PostgreSQL)
	mux.HandleFunc("GET /trucks/nearby", srv.handleGetNearbyTrucks)
	mux.HandleFunc("GET /trucks/{id}/state", srv.handleGetTruckState)
	mux.HandleFunc("GET /trucks/{id}/history", srv.handleGetTruckHistory)

	// Observabilidade e Saúde
	mux.HandleFunc("GET /health", srv.handleHealth)

	return srv, mux
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Inicializa cliente do Amazon Kinesis (MiniStack)
	kinClient, err := kinesis.NewClient(ctx, cfg)
	if err != nil {
		slog.Error("falha ao criar cliente Kinesis", "err", err)
		os.Exit(1)
	}
	if err := kinClient.EnsureStreamExists(ctx); err != nil {
		slog.Warn("não foi possível garantir o stream Kinesis na inicialização (verifique se o MiniStack está rodando)", "err", err)
	}

	// Inicializa CloudWatch para métricas de engenharia
	cwClient, err := observability.NewCloudWatchClient(ctx, cfg)
	if err != nil {
		slog.Warn("CloudWatch indisponível na inicialização", "err", err)
	}

	// Inicializa clientes de leitura para os endpoints de consulta (Redis e PostgreSQL)
	var redisStore *storage.RedisStore
	if rdb, err := storage.NewRedisStore(ctx, cfg); err != nil {
		slog.Warn("Redis indisponível na inicialização (endpoints /trucks/nearby retornarão 503 até conectar)", "err", err)
	} else {
		redisStore = rdb
		defer redisStore.Close()
	}

	var pgStore *storage.PostgresStore
	if pg, err := storage.NewPostgresStore(ctx, cfg); err != nil {
		slog.Warn("PostgreSQL indisponível na inicialização (endpoint /history retornará 503 até conectar)", "err", err)
	} else {
		pgStore = pg
		defer pgStore.Close()
	}

	_, mux := NewGatewayServer(kinClient, redisStore, pgStore, cwClient)

	httpServer := &http.Server{
		Addr:         cfg.GatewayAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("Gateway de Ingestão iniciado",
			"addr", cfg.GatewayAddr,
			"kinesis_stream", cfg.KinesisStreamName,
			"aws_endpoint", cfg.AWSEndpoint,
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("erro fatal no servidor HTTP", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("encerrando Gateway de Ingestão graciosamente...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

// handlePostTelemetry recebe o pacote HTTP do caminhão, valida em memória e publica no Amazon Kinesis.
// Não acessa banco de dados para garantir baixa latência e retorna 202 Accepted.
func (s *GatewayServer) handlePostTelemetry(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer r.Body.Close()

	var payload domain.TelemetryPayload
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	if err := dec.Decode(&payload); err != nil {
		s.rejectedTotal.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":   "payload JSON inválido",
			"details": err.Error(),
		})
		return
	}

	if err := payload.Validate(); err != nil {
		s.rejectedTotal.Add(1)
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"error":   "falha na validação de telemetria",
			"details": err.Error(),
		})
		return
	}

	seqNum, err := s.kinesisClient.PublishTelemetry(r.Context(), &payload)
	if err != nil {
		s.kinesisErrorsTotal.Add(1)
		slog.Error("falha ao despachar evento para o Kinesis",
			"truck_id", payload.TruckID,
			"event_id", payload.EventID,
			"err", err,
		)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "falha ao enfileirar evento no stream de ingestão",
		})
		return
	}

	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
	s.acceptedTotal.Add(1)

	if s.cwClient != nil {
		go func(lat float64) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = s.cwClient.RecordGatewayIngestion(ctx, 1, lat)
		}(latencyMs)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":          "ACCEPTED",
		"event_id":        payload.EventID,
		"truck_id":        payload.TruckID,
		"sequence_number": seqNum,
		"latency_ms":      latencyMs,
	})
}

// handlePostTelemetryBatch recebe múltiplos pacotes (ex: caminhão que recuperou sinal e enviou lote acumulado).
func (s *GatewayServer) handlePostTelemetryBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer r.Body.Close()

	var payloads []domain.TelemetryPayload
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2*1024*1024))
	if err := dec.Decode(&payloads); err != nil {
		s.rejectedTotal.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":   "lote JSON inválido",
			"details": err.Error(),
		})
		return
	}

	if len(payloads) == 0 || len(payloads) > 500 {
		s.rejectedTotal.Add(1)
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "o lote deve conter entre 1 e 500 eventos de telemetria",
		})
		return
	}

	for i := range payloads {
		if err := payloads[i].Validate(); err != nil {
			s.rejectedTotal.Add(1)
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
				"error":   "item inválido no lote",
				"index":   i,
				"details": err.Error(),
			})
			return
		}
	}

	published, err := s.kinesisClient.PublishTelemetryBatch(r.Context(), payloads)
	if err != nil {
		s.kinesisErrorsTotal.Add(1)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":   "falha no PutRecords do Kinesis",
			"details": err.Error(),
		})
		return
	}

	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
	s.acceptedTotal.Add(uint64(published))

	if s.cwClient != nil {
		go func(cnt int, lat float64) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = s.cwClient.RecordGatewayIngestion(ctx, cnt, lat)
		}(published, latencyMs)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":     "ACCEPTED",
		"received":   len(payloads),
		"published":  published,
		"latency_ms": latencyMs,
	})
}

// handleGetNearbyTrucks responde à consulta geoespacial em tempo real:
// Exemplo: GET /trucks/nearby?lat=-23.5505&lon=-46.6333&radius_km=10&limit=50
func (s *GatewayServer) handleGetNearbyTrucks(w http.ResponseWriter, r *http.Request) {
	if s.redisStore == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "Redis (ElastiCache) não está conectado",
		})
		return
	}

	q := r.URL.Query()
	lat, err := strconv.ParseFloat(q.Get("lat"), 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "parâmetro 'lat' inválido ou ausente"})
		return
	}
	lon, err := strconv.ParseFloat(q.Get("lon"), 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "parâmetro 'lon' inválido ou ausente"})
		return
	}

	radiusKm := 10.0
	if rStr := q.Get("radius_km"); rStr != "" {
		if parsed, err := strconv.ParseFloat(rStr, 64); err == nil && parsed > 0 {
			radiusKm = parsed
		}
	}

	limit := 50
	if lStr := q.Get("limit"); lStr != "" {
		if parsed, err := strconv.Atoi(lStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	trucks, err := s.redisStore.FindNearbyTrucks(r.Context(), lat, lon, radiusKm, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "erro na busca geoespacial",
			"details": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"center":    map[string]float64{"latitude": lat, "longitude": lon},
		"radius_km": radiusKm,
		"count":     len(trucks),
		"trucks":    trucks,
	})
}

// handleGetTruckState retorna a posição e telemetria mais recente de um caminhão específico no Redis.
func (s *GatewayServer) handleGetTruckState(w http.ResponseWriter, r *http.Request) {
	if s.redisStore == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "Redis (ElastiCache) não está conectado",
		})
		return
	}

	truckID := r.PathValue("id")
	state, err := s.redisStore.GetTruckState(r.Context(), truckID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "erro ao buscar estado do caminhão",
			"details": err.Error(),
		})
		return
	}
	if state == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error":    "caminhão não encontrado no cache de tempo real",
			"truck_id": truckID,
		})
		return
	}

	writeJSON(w, http.StatusOK, state)
}

// handleGetTruckHistory retorna o histórico de telemetria e rota armazenado no PostgreSQL (RDS).
func (s *GatewayServer) handleGetTruckHistory(w http.ResponseWriter, r *http.Request) {
	if s.postgresStore == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "PostgreSQL (RDS) não está conectado",
		})
		return
	}

	truckID := r.PathValue("id")
	q := r.URL.Query()

	to := time.Now().UTC().Add(1 * time.Hour)
	from := to.Add(-25 * time.Hour)

	if fromStr := q.Get("from"); fromStr != "" {
		if parsed, err := time.Parse(time.RFC3339, fromStr); err == nil {
			from = parsed
		}
	}
	if toStr := q.Get("to"); toStr != "" {
		if parsed, err := time.Parse(time.RFC3339, toStr); err == nil {
			to = parsed
		}
	}

	limit := 100
	if lStr := q.Get("limit"); lStr != "" {
		if parsed, err := strconv.Atoi(lStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	history, err := s.postgresStore.GetTruckHistory(r.Context(), truckID, from, to, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "erro ao consultar histórico no PostgreSQL",
			"details": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"truck_id": truckID,
		"from":     from,
		"to":       to,
		"count":    len(history),
		"history":  history,
	})
}

func (s *GatewayServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":               "UP",
		"accepted_total":       s.acceptedTotal.Load(),
		"rejected_total":       s.rejectedTotal.Load(),
		"kinesis_errors_total": s.kinesisErrorsTotal.Load(),
		"timestamp":            time.Now().UTC(),
	})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
