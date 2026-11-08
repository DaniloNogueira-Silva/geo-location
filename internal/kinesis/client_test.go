package kinesis

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsKinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/google/uuid"
)

func TestKinesisPublishAndReadFromMiniStack(t *testing.T) {
	ctx := context.Background()
	cfg := config.Load()
	cfg.KinesisStreamName = fmt.Sprintf("test-telemetry-stream-%d", time.Now().UnixNano()%100000)
	cfg.KinesisShards = 1

	client, err := NewClient(ctx, cfg)
	if err != nil {
		t.Fatalf("erro ao criar cliente Kinesis: %v", err)
	}

	if err := client.EnsureStreamExists(ctx); err != nil {
		t.Fatalf("falha ao criar stream %s no MiniStack (%s): %v", cfg.KinesisStreamName, cfg.AWSEndpoint, err)
	}
	t.Cleanup(func() {
		_, _ = client.Raw().DeleteStream(context.Background(), &awsKinesis.DeleteStreamInput{
			StreamName: aws.String(cfg.KinesisStreamName),
		})
	})

	// 1. Publica registro individual via PutRecord
	singleEvent := domain.TelemetryPayload{
		EventID:      uuid.NewString(),
		TruckID:      "TRK-KIN-01",
		RouteID:      "SP-RJ-DUTRA",
		Timestamp:    time.Now().UTC().Truncate(time.Millisecond),
		Location:     domain.GeoPoint{Latitude: -23.5505, Longitude: -46.6333},
		SpeedKmh:     80.5,
		HeadingDeg:   70.0,
		EngineTempC:  90.0,
		CargoTempC:   -18.0,
		FuelLevelPct: 85.0,
		Status:       domain.StatusMoving,
	}

	seqNum, err := client.PublishTelemetry(ctx, &singleEvent)
	if err != nil {
		t.Fatalf("erro em PublishTelemetry: %v", err)
	}
	if seqNum == "" {
		t.Fatal("esperava sequence_number não vazio do Kinesis")
	}

	// 2. Publica lote via PutRecords
	batchEvents := []domain.TelemetryPayload{
		singleEvent,
		singleEvent,
	}
	batchEvents[0].EventID = uuid.NewString()
	batchEvents[0].TruckID = "TRK-KIN-02"
	batchEvents[1].EventID = uuid.NewString()
	batchEvents[1].TruckID = "TRK-KIN-03"

	publishedCount, err := client.PublishTelemetryBatch(ctx, batchEvents)
	if err != nil {
		t.Fatalf("erro em PublishTelemetryBatch: %v", err)
	}
	if publishedCount != 2 {
		t.Fatalf("esperava 2 registros publicados no lote, obteve %d", publishedCount)
	}

	// 3. Lê os registros diretamente do Shard no MiniStack para comprovar que estão gravados no stream
	shardsOut, err := client.Raw().ListShards(ctx, &awsKinesis.ListShardsInput{
		StreamName: aws.String(cfg.KinesisStreamName),
	})
	if err != nil || len(shardsOut.Shards) == 0 {
		t.Fatalf("erro ao listar shards: %v", err)
	}

	iterOut, err := client.Raw().GetShardIterator(ctx, &awsKinesis.GetShardIteratorInput{
		StreamName:        aws.String(cfg.KinesisStreamName),
		ShardId:           shardsOut.Shards[0].ShardId,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatalf("erro ao obter ShardIterator: %v", err)
	}

	recordsOut, err := client.Raw().GetRecords(ctx, &awsKinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(10),
	})
	if err != nil {
		t.Fatalf("erro no GetRecords: %v", err)
	}
	if len(recordsOut.Records) != 3 {
		t.Fatalf("esperava ler exatamente 3 registros do shard no MiniStack, leu %d", len(recordsOut.Records))
	}

	var firstRead domain.TelemetryPayload
	if err := json.Unmarshal(recordsOut.Records[0].Data, &firstRead); err != nil {
		t.Fatalf("erro ao desserializar registro lido do Kinesis: %v", err)
	}
	if firstRead.EventID != singleEvent.EventID || firstRead.TruckID != "TRK-KIN-01" {
		t.Fatalf("dados lidos do Kinesis não batem com o enviado: %+v", firstRead)
	}
}

