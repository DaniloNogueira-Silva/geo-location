package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore gerencia o armazenamento histórico de séries temporais no Amazon RDS (PostgreSQL).
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore cria o pool de conexões com o PostgreSQL e garante que a tabela histórica exista.
func NewPostgresStore(ctx context.Context, cfg config.Config) (*PostgresStore, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("DSN do PostgreSQL inválido: %w", err)
	}
	poolCfg.MaxConns = 25
	poolCfg.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("falha ao inicializar pool do PostgreSQL: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("falha no ping do PostgreSQL: %w", err)
	}

	store := &PostgresStore{pool: pool}
	if err := store.EnsureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return store, nil
}

// Close fecha o pool de conexões com o PostgreSQL.
func (s *PostgresStore) Close() {
	s.pool.Close()
}

// EnsureSchema garante a criação da tabela e índices caso o banco tenha sido iniciado sem o script init.sql.
func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	ddl := `
	CREATE TABLE IF NOT EXISTS truck_telemetry (
		event_id        UUID PRIMARY KEY,
		truck_id        VARCHAR(32) NOT NULL,
		route_id        VARCHAR(64),
		event_timestamp TIMESTAMPTZ NOT NULL,
		ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		latitude        DOUBLE PRECISION NOT NULL,
		longitude       DOUBLE PRECISION NOT NULL,
		altitude_m      DOUBLE PRECISION DEFAULT 0,
		speed_kmh       DOUBLE PRECISION NOT NULL,
		heading_deg     DOUBLE PRECISION NOT NULL,
		engine_temp_c   DOUBLE PRECISION NOT NULL,
		cargo_temp_c    DOUBLE PRECISION NOT NULL,
		fuel_level_pct  DOUBLE PRECISION NOT NULL,
		odometer_km     DOUBLE PRECISION DEFAULT 0,
		status          VARCHAR(16) NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_truck_telemetry_truck_time
		ON truck_telemetry (truck_id, event_timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_truck_telemetry_event_time
		ON truck_telemetry (event_timestamp DESC);
	`
	_, err := s.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("erro ao garantir schema no PostgreSQL: %w", err)
	}
	return nil
}

// InsertBatch realiza a inserção em lote (Batch Insert) ordenando pelo timestamp gerado no caminhão.
// Usa ON CONFLICT (event_id) DO NOTHING para garantir idempotência em caso de retries da AWS Lambda.
func (s *PostgresStore) InsertBatch(ctx context.Context, events []domain.TelemetryPayload) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}

	// Ordena os eventos pelo timestamp original do caminhão (decisão arquitetural)
	sorted := make([]domain.TelemetryPayload, len(events))
	copy(sorted, events)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})

	const insertSQL = `
		INSERT INTO truck_telemetry (
			event_id, truck_id, route_id, event_timestamp,
			latitude, longitude, altitude_m,
			speed_kmh, heading_deg, engine_temp_c, cargo_temp_c,
			fuel_level_pct, odometer_km, status
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7,
			$8, $9, $10, $11,
			$12, $13, $14
		)
		ON CONFLICT (event_id) DO NOTHING;
	`

	batch := &pgx.Batch{}
	for _, ev := range sorted {
		batch.Queue(
			insertSQL,
			ev.EventID,
			ev.TruckID,
			ev.RouteID,
			ev.Timestamp.UTC(),
			ev.Location.Latitude,
			ev.Location.Longitude,
			ev.Location.AltitudeM,
			ev.SpeedKmh,
			ev.HeadingDeg,
			ev.EngineTempC,
			ev.CargoTempC,
			ev.FuelLevelPct,
			ev.OdometerKm,
			string(ev.Status),
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	inserted := 0
	for i := 0; i < len(sorted); i++ {
		ct, err := br.Exec()
		if err != nil {
			return inserted, fmt.Errorf("erro ao executar item %d do batch insert no PostgreSQL: %w", i, err)
		}
		inserted += int(ct.RowsAffected())
	}

	return inserted, nil
}

// GetTruckHistory retorna o histórico de telemetria de um caminhão ordenado pelo timestamp do evento no veículo.
func (s *PostgresStore) GetTruckHistory(ctx context.Context, truckID string, from, to time.Time, limit int) ([]domain.TelemetryPayload, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}

	const query = `
		SELECT
			event_id::text,
			truck_id,
			COALESCE(route_id, ''),
			event_timestamp,
			latitude,
			longitude,
			altitude_m,
			speed_kmh,
			heading_deg,
			engine_temp_c,
			cargo_temp_c,
			fuel_level_pct,
			odometer_km,
			status
		FROM truck_telemetry
		WHERE truck_id = $1
		  AND event_timestamp >= $2
		  AND event_timestamp <= $3
		ORDER BY event_timestamp DESC
		LIMIT $4;
	`

	rows, err := s.pool.Query(ctx, query, truckID, from.UTC(), to.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("erro ao consultar histórico do caminhão %s: %w", truckID, err)
	}
	defer rows.Close()

	var history []domain.TelemetryPayload
	for rows.Next() {
		var ev domain.TelemetryPayload
		var statusStr string
		if err := rows.Scan(
			&ev.EventID,
			&ev.TruckID,
			&ev.RouteID,
			&ev.Timestamp,
			&ev.Location.Latitude,
			&ev.Location.Longitude,
			&ev.Location.AltitudeM,
			&ev.SpeedKmh,
			&ev.HeadingDeg,
			&ev.EngineTempC,
			&ev.CargoTempC,
			&ev.FuelLevelPct,
			&ev.OdometerKm,
			&statusStr,
		); err != nil {
			return nil, fmt.Errorf("erro ao ler linha do histórico: %w", err)
		}
		ev.Status = domain.TruckStatus(statusStr)
		history = append(history, ev)
	}

	if history == nil {
		history = []domain.TelemetryPayload{}
	}
	return history, rows.Err()
}

