-- Schema Histórico de Telemetria de Frotas (RDS / PostgreSQL)
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

-- Índice composto otimizado para Playback de Rotas e Séries Temporais por Caminhão
-- Ordenado pelo timestamp gerado no caminhão (event_timestamp) conforme decisão arquitetural
CREATE INDEX IF NOT EXISTS idx_truck_telemetry_truck_time
    ON truck_telemetry (truck_id, event_timestamp DESC);

-- Índice por janela temporal para relatórios analíticos e observabilidade
CREATE INDEX IF NOT EXISTS idx_truck_telemetry_event_time
    ON truck_telemetry (event_timestamp DESC);

