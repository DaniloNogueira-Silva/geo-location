package domain

import (
	"testing"
	"time"
)

func TestTelemetryPayloadValidate(t *testing.T) {
	base := TelemetryPayload{
		EventID:   "550e8400-e29b-41d4-a716-446655440000",
		TruckID:   "TRK-0001",
		RouteID:   "SP-RJ-DUTRA",
		Timestamp: time.Now().UTC(),
		Location: GeoPoint{
			Latitude:  -23.5505,
			Longitude: -46.6333,
			AltitudeM: 760,
		},
		SpeedKmh:     85.0,
		HeadingDeg:   65.0,
		EngineTempC:  90.0,
		CargoTempC:   -18.0,
		FuelLevelPct: 75.0,
		Status:       StatusMoving,
	}

	t.Run("payload válido passa sem erros", func(t *testing.T) {
		p := base
		if err := p.Validate(); err != nil {
			t.Fatalf("esperava payload válido, mas retornou erro: %v", err)
		}
	})

	t.Run("status vazio assume StatusMoving por padrão", func(t *testing.T) {
		p := base
		p.Status = ""
		if err := p.Validate(); err != nil {
			t.Fatalf("erro inesperado: %v", err)
		}
		if p.Status != StatusMoving {
			t.Fatalf("esperava StatusMoving, obteve %s", p.Status)
		}
	})

	t.Run("rejeita event_id vazio", func(t *testing.T) {
		p := base
		p.EventID = "   "
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para event_id em branco")
		}
	})

	t.Run("rejeita truck_id vazio", func(t *testing.T) {
		p := base
		p.TruckID = ""
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para truck_id vazio")
		}
	})

	t.Run("rejeita timestamp zero", func(t *testing.T) {
		p := base
		p.Timestamp = time.Time{}
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para timestamp zero")
		}
	})

	t.Run("rejeita latitude fora de [-90, 90]", func(t *testing.T) {
		p := base
		p.Location.Latitude = -91.5
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para latitude < -90")
		}
		p.Location.Latitude = 90.001
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para latitude > 90")
		}
	})

	t.Run("rejeita longitude fora de [-180, 180]", func(t *testing.T) {
		p := base
		p.Location.Longitude = -181.0
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para longitude < -180")
		}
		p.Location.Longitude = 180.5
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para longitude > 180")
		}
	})

	t.Run("rejeita coordenada (0,0) Null Island", func(t *testing.T) {
		p := base
		p.Location.Latitude = 0
		p.Location.Longitude = 0
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para coordenada (0,0)")
		}
	})

	t.Run("rejeita velocidade negativa ou acima de 250 km/h", func(t *testing.T) {
		p := base
		p.SpeedKmh = -1.0
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para velocidade negativa")
		}
		p.SpeedKmh = 260.0
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para velocidade > 250")
		}
	})

	t.Run("rejeita fuel_level_pct fora de [0, 100]", func(t *testing.T) {
		p := base
		p.FuelLevelPct = -0.1
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para combustível negativo")
		}
		p.FuelLevelPct = 100.1
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para combustível > 100")
		}
	})

	t.Run("rejeita heading_deg fora de [0, 360]", func(t *testing.T) {
		p := base
		p.HeadingDeg = -5.0
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para heading < 0")
		}
		p.HeadingDeg = 360.1
		if err := p.Validate(); err == nil {
			t.Fatal("esperava erro para heading > 360")
		}
	})
}
