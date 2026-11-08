package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// TruckStatus representa o estado operacional do caminhão.
type TruckStatus string

const (
	StatusMoving  TruckStatus = "MOVING"
	StatusIdle    TruckStatus = "IDLE"
	StatusStopped TruckStatus = "STOPPED"
	StatusAlert   TruckStatus = "ALERT"
)

// GeoPoint representa uma coordenada geográfica WGS84.
type GeoPoint struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	AltitudeM float64 `json:"altitude_m,omitempty"`
}

// Validate verifica se as coordenadas estão dentro dos limites globais do WGS84.
func (g GeoPoint) Validate() error {
	if g.Latitude < -90.0 || g.Latitude > 90.0 {
		return fmt.Errorf("latitude inválida: %f (deve estar entre -90 e 90)", g.Latitude)
	}
	if g.Longitude < -180.0 || g.Longitude > 180.0 {
		return fmt.Errorf("longitude inválida: %f (deve estar entre -180 e 180)", g.Longitude)
	}
	if g.Latitude == 0 && g.Longitude == 0 {
		return errors.New("coordenada (0,0) inválida para a frota")
	}
	return nil
}

// TelemetryPayload representa o pacote de telemetria enviado pelo caminhão.
// Importante: O campo Timestamp reflete o relógio do hardware do caminhão no momento da coleta,
// garantindo ordenação correta mesmo quando há atraso de rede (buffer offline 4G).
type TelemetryPayload struct {
	EventID      string      `json:"event_id"`
	TruckID      string      `json:"truck_id"`
	RouteID      string      `json:"route_id,omitempty"`
	Timestamp    time.Time   `json:"timestamp"`
	Location     GeoPoint    `json:"location"`
	SpeedKmh     float64     `json:"speed_kmh"`
	HeadingDeg   float64     `json:"heading_deg"`
	EngineTempC  float64     `json:"engine_temp_c"`
	CargoTempC   float64     `json:"cargo_temp_c"`
	FuelLevelPct float64     `json:"fuel_level_pct"`
	OdometerKm   float64     `json:"odometer_km,omitempty"`
	Status       TruckStatus `json:"status"`
}

// Validate executa validação rápida e sem alocação pesada no Gateway de Ingestão.
func (t *TelemetryPayload) Validate() error {
	if strings.TrimSpace(t.EventID) == "" {
		return errors.New("event_id é obrigatório")
	}
	if strings.TrimSpace(t.TruckID) == "" {
		return errors.New("truck_id é obrigatório")
	}
	if t.Timestamp.IsZero() {
		return errors.New("timestamp do evento é obrigatório")
	}
	if err := t.Location.Validate(); err != nil {
		return err
	}
	if t.SpeedKmh < 0 || t.SpeedKmh > 250 {
		return fmt.Errorf("speed_kmh fora do limite: %f", t.SpeedKmh)
	}
	if t.FuelLevelPct < 0 || t.FuelLevelPct > 100 {
		return fmt.Errorf("fuel_level_pct deve estar entre 0 e 100: %f", t.FuelLevelPct)
	}
	if t.HeadingDeg < 0 || t.HeadingDeg > 360 {
		return fmt.Errorf("heading_deg deve estar entre 0 e 360: %f", t.HeadingDeg)
	}
	if t.Status == "" {
		t.Status = StatusMoving
	}
	return nil
}

// NearbyTruckResult representa a resposta de consulta geoespacial no Redis.
type NearbyTruckResult struct {
	TruckID    string             `json:"truck_id"`
	DistanceKm float64            `json:"distance_km"`
	Latitude   float64            `json:"latitude"`
	Longitude  float64            `json:"longitude"`
	LastState  *TelemetryPayload  `json:"last_state,omitempty"`
}

