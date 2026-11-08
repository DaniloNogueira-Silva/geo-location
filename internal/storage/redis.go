package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/redis/go-redis/v9"
)

const (
	// Chave GeoSet do Redis que armazena as coordenadas de toda a frota ativa via GEOADD
	FleetGeoKey = "fleet:locations"
	// Prefixo da chave Hash/JSON que guarda o último estado completo de cada caminhão
	TruckStateKeyPrefix = "fleet:truck:"
)

// upsertTruckGeoLua garante que um pacote atrasado (timestamp menor que o já registrado no Redis)
// nunca sobrescreva uma posição mais recente do caminhão.
// KEYS[1] = fleet:locations (GeoSet)
// KEYS[2] = fleet:truck:<truck_id> (Hash com timestamp_ms e payload JSON)
// ARGV[1] = longitude
// ARGV[2] = latitude
// ARGV[3] = truck_id
// ARGV[4] = event_timestamp_ms
// ARGV[5] = payload_json
var upsertTruckGeoLua = redis.NewScript(`
local current_ts = redis.call('HGET', KEYS[2], 'timestamp_ms')
if current_ts and tonumber(current_ts) > tonumber(ARGV[4]) then
    return 0
end
redis.call('GEOADD', KEYS[1], ARGV[1], ARGV[2], ARGV[3])
redis.call('HSET', KEYS[2], 'timestamp_ms', ARGV[4], 'payload', ARGV[5])
return 1
`)

// RedisStore gerencia o armazenamento em tempo real e consultas geoespaciais no Amazon ElastiCache (Redis).
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore conecta ao Redis (ElastiCache) e valida a conexão.
func NewRedisStore(ctx context.Context, cfg config.Config) (*RedisStore, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPass,
		DB:       cfg.RedisDB,
		PoolSize: 50,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("falha ao conectar ao Redis (%s): %w", cfg.RedisAddr, err)
	}

	return &RedisStore{client: rdb}, nil
}

// Close encerra o pool de conexões do Redis.
func (s *RedisStore) Close() error {
	return s.client.Close()
}

// UpdateFleetPositionsBatch processa um lote de eventos vindos da Lambda.
// Dedup por caminhão mantendo apenas o evento de maior Timestamp do lote e aplica GEOADD + HSET atômico.
func (s *RedisStore) UpdateFleetPositionsBatch(ctx context.Context, events []domain.TelemetryPayload) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}

	// Seleciona apenas o evento mais recente por caminhão dentro do próprio lote
	latestByTruck := make(map[string]domain.TelemetryPayload, len(events))
	for _, ev := range events {
		existing, ok := latestByTruck[ev.TruckID]
		if !ok || !ev.Timestamp.Before(existing.Timestamp) {
			latestByTruck[ev.TruckID] = ev
		}
	}

	updatedCount := 0
	for _, ev := range latestByTruck {
		rawJSON, err := json.Marshal(ev)
		if err != nil {
			return updatedCount, fmt.Errorf("erro ao serializar estado do caminhão %s: %w", ev.TruckID, err)
		}

		stateKey := TruckStateKeyPrefix + ev.TruckID
		res, err := upsertTruckGeoLua.Run(
			ctx,
			s.client,
			[]string{FleetGeoKey, stateKey},
			ev.Location.Longitude,
			ev.Location.Latitude,
			ev.TruckID,
			ev.Timestamp.UnixMilli(),
			string(rawJSON),
		).Int()
		if err != nil {
			return updatedCount, fmt.Errorf("erro ao executar GEOADD/HSET para %s: %w", ev.TruckID, err)
		}
		updatedCount += res
	}

	return updatedCount, nil
}

// FindNearbyTrucks responde à pergunta: "Quais caminhões estão num raio de X km agora?"
// Utiliza GEOSEARCH do Redis e hidrata o resultado com o estado atual de cada caminhão.
func (s *RedisStore) FindNearbyTrucks(ctx context.Context, lat, lon, radiusKm float64, limit int) ([]domain.NearbyTruckResult, error) {
	if limit <= 0 {
		limit = 50
	}

	locations, err := s.client.GeoSearchLocation(ctx, FleetGeoKey, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  lon,
			Latitude:   lat,
			Radius:     radiusKm,
			RadiusUnit: "km",
			Sort:       "ASC",
			Count:      limit,
		},
		WithCoord: true,
		WithDist:  true,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("erro na consulta GEOSEARCH do Redis: %w", err)
	}

	if len(locations) == 0 {
		return []domain.NearbyTruckResult{}, nil
	}

	// Busca os estados detalhados em pipeline para evitar N+1 roundtrips
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(locations))
	for i, loc := range locations {
		cmds[i] = pipe.HGet(ctx, TruckStateKeyPrefix+loc.Name, "payload")
	}
	_, _ = pipe.Exec(ctx)

	results := make([]domain.NearbyTruckResult, 0, len(locations))
	for i, loc := range locations {
		item := domain.NearbyTruckResult{
			TruckID:    loc.Name,
			DistanceKm: loc.Dist,
			Latitude:   loc.Latitude,
			Longitude:  loc.Longitude,
		}
		if payloadStr, err := cmds[i].Result(); err == nil && payloadStr != "" {
			var state domain.TelemetryPayload
			if jsonErr := json.Unmarshal([]byte(payloadStr), &state); jsonErr == nil {
				item.LastState = &state
			}
		}
		results = append(results, item)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].DistanceKm < results[j].DistanceKm
	})

	return results, nil
}

// GetTruckState retorna o estado mais recente de um caminhão específico armazenado no Redis.
func (s *RedisStore) GetTruckState(ctx context.Context, truckID string) (*domain.TelemetryPayload, error) {
	payloadStr, err := s.client.HGet(ctx, TruckStateKeyPrefix+truckID, "payload").Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("erro ao consultar estado do caminhão %s no Redis: %w", truckID, err)
	}

	var state domain.TelemetryPayload
	if err := json.Unmarshal([]byte(payloadStr), &state); err != nil {
		return nil, fmt.Errorf("erro ao desserializar estado do caminhão %s: %w", truckID, err)
	}
	return &state, nil
}

