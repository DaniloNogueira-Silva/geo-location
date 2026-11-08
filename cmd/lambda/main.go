package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsKinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
	"github.com/danil/geo-location/internal/kinesis"
	"github.com/danil/geo-location/internal/observability"
	"github.com/danil/geo-location/internal/storage"
)

// TelemetryProcessor processa lotes de eventos do Amazon Kinesis e persiste no armazenamento híbrido (Redis + PostgreSQL).
type TelemetryProcessor struct {
	redisStore    *storage.RedisStore
	postgresStore *storage.PostgresStore
	cwClient      *observability.CloudWatchClient
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisStore, err := storage.NewRedisStore(ctx, cfg)
	if err != nil {
		slog.Error("falha ao inicializar RedisStore na Lambda", "err", err)
		os.Exit(1)
	}
	defer redisStore.Close()

	pgStore, err := storage.NewPostgresStore(ctx, cfg)
	if err != nil {
		slog.Error("falha ao inicializar PostgresStore na Lambda", "err", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	cwClient, err := observability.NewCloudWatchClient(ctx, cfg)
	if err != nil {
		slog.Warn("CloudWatch indisponível na inicialização da Lambda", "err", err)
	}

	processor := &TelemetryProcessor{
		redisStore:    redisStore,
		postgresStore: pgStore,
		cwClient:      cwClient,
	}

	// Se estiver executando dentro do Runtime da AWS Lambda (MiniStack / AWS real), usa lambda.Start.
	// Caso contrário, executa o consumidor direto dos Shards do Kinesis para facilitar desenvolvimento e debug local.
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		slog.Info("iniciando handler no modo AWS Lambda Runtime (Event Source Mapping)")
		lambda.StartWithOptions(processor.HandleKinesisEvent, lambda.WithContext(ctx))
		return
	}

	slog.Info("AWS_LAMBDA_RUNTIME_API não detectada: iniciando em modo Worker/Consumer direto do Kinesis (MiniStack)",
		"stream", cfg.KinesisStreamName,
		"endpoint", cfg.AWSEndpoint,
	)
	if err := processor.RunLocalKinesisConsumer(ctx, cfg); err != nil {
		slog.Error("erro no consumidor local do Kinesis", "err", err)
		os.Exit(1)
	}
}

// HandleKinesisEvent é a assinatura oficial acionada pelo Event Source Mapping do Amazon Kinesis na AWS Lambda.
func (p *TelemetryProcessor) HandleKinesisEvent(ctx context.Context, kinesisEvent events.KinesisEvent) error {
	if len(kinesisEvent.Records) == 0 {
		return nil
	}

	batch := make([]domain.TelemetryPayload, 0, len(kinesisEvent.Records))
	for _, rec := range kinesisEvent.Records {
		var payload domain.TelemetryPayload
		if err := json.Unmarshal(rec.Kinesis.Data, &payload); err != nil {
			slog.Warn("registro corrompido ignorado no lote do Kinesis",
				"event_id", rec.EventID,
				"partition_key", rec.Kinesis.PartitionKey,
				"err", err,
			)
			continue
		}
		batch = append(batch, payload)
	}

	return p.ProcessBatchWithRetry(ctx, batch)
}

// ProcessBatchWithRetry escreve em paralelo no ElastiCache (Redis GEOADD) e no RDS (PostgreSQL Batch Insert)
// aplicando política de retry com backoff exponencial.
func (p *TelemetryProcessor) ProcessBatchWithRetry(ctx context.Context, batch []domain.TelemetryPayload) error {
	if len(batch) == 0 {
		return nil
	}

	start := time.Now()
	const maxAttempts = 3

	var (
		redisUpdated int
		pgInserted   int
		redisErr     error
		pgErr        error
	)

	var wg sync.WaitGroup
	wg.Add(2)

	// 1. Escrita de Tempo Real no ElastiCache (Redis GEOADD + Estado Atual)
	go func() {
		defer wg.Done()
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			redisUpdated, redisErr = p.redisStore.UpdateFleetPositionsBatch(ctx, batch)
			if redisErr == nil {
				return
			}
			slog.Warn("tentativa falhou ao atualizar Redis GEOADD", "attempt", attempt, "err", redisErr)
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		}
	}()

	// 2. Escrita Histórica em Lote no RDS (PostgreSQL Batch Insert)
	go func() {
		defer wg.Done()
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			pgInserted, pgErr = p.postgresStore.InsertBatch(ctx, batch)
			if pgErr == nil {
				return
			}
			slog.Warn("tentativa falhou no Batch Insert do PostgreSQL", "attempt", attempt, "err", pgErr)
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		}
	}()

	wg.Wait()

	if redisErr != nil {
		return fmt.Errorf("falha definitiva no Redis após %d tentativas: %w", maxAttempts, redisErr)
	}
	if pgErr != nil {
		return fmt.Errorf("falha definitiva no PostgreSQL após %d tentativas: %w", maxAttempts, pgErr)
	}

	durationMs := float64(time.Since(start).Microseconds()) / 1000.0
	if p.cwClient != nil {
		_ = p.cwClient.RecordLambdaBatch(ctx, len(batch), redisUpdated, pgInserted, durationMs)
	}

	slog.Info("lote do Kinesis processado com sucesso",
		"batch_size", len(batch),
		"redis_geoadd_updated", redisUpdated,
		"postgres_batch_inserted", pgInserted,
		"duration_ms", durationMs,
	)
	return nil
}

// RunLocalKinesisConsumer consome diretamente todos os Shards do stream no MiniStack em paralelo.
func (p *TelemetryProcessor) RunLocalKinesisConsumer(ctx context.Context, cfg config.Config) error {
	kinClient, err := kinesis.NewClient(ctx, cfg)
	if err != nil {
		return err
	}
	if err := kinClient.EnsureStreamExists(ctx); err != nil {
		return err
	}

	shardsOut, err := kinClient.Raw().ListShards(ctx, &awsKinesis.ListShardsInput{
		StreamName: aws.String(kinClient.StreamName()),
	})
	if err != nil {
		return fmt.Errorf("falha ao listar shards do stream %s: %w", kinClient.StreamName(), err)
	}

	slog.Info("iniciando workers para shards do Kinesis", "shard_count", len(shardsOut.Shards))

	var wg sync.WaitGroup
	for _, shard := range shardsOut.Shards {
		shardID := aws.ToString(shard.ShardId)
		wg.Add(1)
		go func(sID string) {
			defer wg.Done()
			p.consumeShard(ctx, kinClient, sID)
		}(shardID)
	}

	wg.Wait()
	return nil
}

func (p *TelemetryProcessor) consumeShard(ctx context.Context, kinClient *kinesis.Client, shardID string) {
	iterOut, err := kinClient.Raw().GetShardIterator(ctx, &awsKinesis.GetShardIteratorInput{
		StreamName:        aws.String(kinClient.StreamName()),
		ShardId:           aws.String(shardID),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		slog.Error("falha ao obter ShardIterator", "shard_id", shardID, "err", err)
		return
	}

	shardIterator := iterOut.ShardIterator
	slog.Info("consumindo shard do Kinesis", "shard_id", shardID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if shardIterator == nil {
			slog.Warn("shard fechado (iterator nil), encerrando leitura do shard", "shard_id", shardID)
			return
		}

		recordsOut, err := kinClient.Raw().GetRecords(ctx, &awsKinesis.GetRecordsInput{
			ShardIterator: shardIterator,
			Limit:         aws.Int32(100),
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("erro no GetRecords do Kinesis, aguardando 1s...", "shard_id", shardID, "err", err)
			time.Sleep(1 * time.Second)
			continue
		}

		shardIterator = recordsOut.NextShardIterator

		if len(recordsOut.Records) == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		batch := make([]domain.TelemetryPayload, 0, len(recordsOut.Records))
		for _, rec := range recordsOut.Records {
			var payload domain.TelemetryPayload
			if err := json.Unmarshal(rec.Data, &payload); err != nil {
				slog.Warn("payload inválido ignorado no shard", "shard_id", shardID, "err", err)
				continue
			}
			batch = append(batch, payload)
		}

		if err := p.ProcessBatchWithRetry(ctx, batch); err != nil {
			slog.Error("erro ao processar lote do shard", "shard_id", shardID, "err", err)
		}
	}
}
