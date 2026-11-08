package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsKinesis "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/danil/geo-location/internal/config"
	"github.com/danil/geo-location/internal/domain"
)

// Client encapsula o cliente do Amazon Kinesis configurado para o MiniStack/AWS.
type Client struct {
	raw        *awsKinesis.Client
	streamName string
	shards     int32
}

// NewClient inicializa o cliente do Amazon Kinesis apontando para o endpoint configurado (MiniStack por padrão).
func NewClient(ctx context.Context, cfg config.Config) (*Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKey, cfg.AWSSecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar config AWS: %w", err)
	}

	rawClient := awsKinesis.NewFromConfig(awsCfg, func(o *awsKinesis.Options) {
		if cfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpoint)
		}
	})

	return &Client{
		raw:        rawClient,
		streamName: cfg.KinesisStreamName,
		shards:     cfg.KinesisShards,
	}, nil
}

// Raw retorna o cliente oficial do AWS SDK v2 para uso avançado (ex: consumidor local de shards).
func (c *Client) Raw() *awsKinesis.Client {
	return c.raw
}

// StreamName retorna o nome do stream configurado.
func (c *Client) StreamName() string {
	return c.streamName
}

// EnsureStreamExists verifica se o stream existe no MiniStack e o cria caso ainda não tenha sido provisionado.
func (c *Client) EnsureStreamExists(ctx context.Context) error {
	_, err := c.raw.DescribeStreamSummary(ctx, &awsKinesis.DescribeStreamSummaryInput{
		StreamName: aws.String(c.streamName),
	})
	if err == nil {
		return nil
	}

	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		// Continua tentativa de criação caso o emulador retorne código genérico
		slog.Warn("stream não encontrado na verificação inicial, tentando criar", "stream", c.streamName, "err", err)
	}

	_, createErr := c.raw.CreateStream(ctx, &awsKinesis.CreateStreamInput{
		StreamName: aws.String(c.streamName),
		ShardCount: aws.Int32(c.shards),
	})
	if createErr != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(createErr, &inUse) {
			return fmt.Errorf("falha ao criar stream %s no Kinesis: %w", c.streamName, createErr)
		}
	}

	// Aguarda o stream ficar ACTIVE
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		out, descErr := c.raw.DescribeStreamSummary(ctx, &awsKinesis.DescribeStreamSummaryInput{
			StreamName: aws.String(c.streamName),
		})
		if descErr == nil && out.StreamDescriptionSummary != nil &&
			out.StreamDescriptionSummary.StreamStatus == types.StreamStatusActive {
			slog.Info("Kinesis stream ativo e pronto", "stream", c.streamName)
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return nil
}

// PublishTelemetry envia um único evento de telemetria para o Kinesis usando o TruckID como PartitionKey.
// Usar o TruckID garante que os eventos de um mesmo caminhão caiam sempre no mesmo Shard.
func (c *Client) PublishTelemetry(ctx context.Context, payload *domain.TelemetryPayload) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("erro ao serializar payload: %w", err)
	}

	out, err := c.raw.PutRecord(ctx, &awsKinesis.PutRecordInput{
		StreamName:   aws.String(c.streamName),
		PartitionKey: aws.String(payload.TruckID),
		Data:         data,
	})
	if err != nil {
		return "", fmt.Errorf("erro no PutRecord do Kinesis: %w", err)
	}

	return aws.ToString(out.SequenceNumber), nil
}

// PublishTelemetryBatch envia até 500 eventos em uma única chamada PutRecords do Kinesis.
func (c *Client) PublishTelemetryBatch(ctx context.Context, payloads []domain.TelemetryPayload) (int, error) {
	if len(payloads) == 0 {
		return 0, nil
	}

	entries := make([]types.PutRecordsRequestEntry, 0, len(payloads))
	for i := range payloads {
		data, err := json.Marshal(&payloads[i])
		if err != nil {
			return 0, fmt.Errorf("erro ao serializar item %d do lote: %w", i, err)
		}
		entries = append(entries, types.PutRecordsRequestEntry{
			PartitionKey: aws.String(payloads[i].TruckID),
			Data:         data,
		})
	}

	out, err := c.raw.PutRecords(ctx, &awsKinesis.PutRecordsInput{
		StreamName: aws.String(c.streamName),
		Records:    entries,
	})
	if err != nil {
		return 0, fmt.Errorf("erro no PutRecords do Kinesis: %w", err)
	}

	failed := int(aws.ToInt32(out.FailedRecordCount))
	return len(entries) - failed, nil
}

