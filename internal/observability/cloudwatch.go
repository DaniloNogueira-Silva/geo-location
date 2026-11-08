package observability

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/danil/geo-location/internal/config"
)

const NamespaceFleetTelemetry = "FleetTelemetry"

// CloudWatchClient publica métricas de engenharia e de negócio no Amazon CloudWatch (MiniStack).
type CloudWatchClient struct {
	raw       *cloudwatch.Client
	namespace string
}

// NewCloudWatchClient inicializa o cliente do Amazon CloudWatch apontando para o MiniStack/AWS.
func NewCloudWatchClient(ctx context.Context, cfg config.Config) (*CloudWatchClient, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKey, cfg.AWSSecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar config AWS para CloudWatch: %w", err)
	}

	raw := cloudwatch.NewFromConfig(awsCfg, func(o *cloudwatch.Options) {
		if cfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpoint)
		}
	})

	return &CloudWatchClient{
		raw:       raw,
		namespace: NamespaceFleetTelemetry,
	}, nil
}

// Raw retorna o cliente subjacente do AWS SDK v2.
func (c *CloudWatchClient) Raw() *cloudwatch.Client {
	return c.raw
}

// RecordGatewayIngestion publica métricas de latência e contagem de eventos recebidos na API Edge.
func (c *CloudWatchClient) RecordGatewayIngestion(ctx context.Context, count int, latencyMs float64) error {
	now := time.Now().UTC()
	_, err := c.raw.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(c.namespace),
		MetricData: []types.MetricDatum{
			{
				MetricName: aws.String("IngestedEvents"),
				Timestamp:  aws.Time(now),
				Value:      aws.Float64(float64(count)),
				Unit:       types.StandardUnitCount,
				Dimensions: []types.Dimension{
					{Name: aws.String("Component"), Value: aws.String("Gateway")},
				},
			},
			{
				MetricName: aws.String("GatewayLatencyMs"),
				Timestamp:  aws.Time(now),
				Value:      aws.Float64(latencyMs),
				Unit:       types.StandardUnitMilliseconds,
				Dimensions: []types.Dimension{
					{Name: aws.String("Component"), Value: aws.String("Gateway")},
				},
			},
		},
	})
	return err
}

// RecordLambdaBatch publica métricas do processamento em lote da AWS Lambda (Redis GEOADD + Postgres Batch Insert).
func (c *CloudWatchClient) RecordLambdaBatch(ctx context.Context, batchSize, redisUpdated, pgInserted int, durationMs float64) error {
	now := time.Now().UTC()
	_, err := c.raw.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(c.namespace),
		MetricData: []types.MetricDatum{
			{
				MetricName: aws.String("ProcessedBatchSize"),
				Timestamp:  aws.Time(now),
				Value:      aws.Float64(float64(batchSize)),
				Unit:       types.StandardUnitCount,
				Dimensions: []types.Dimension{
					{Name: aws.String("Component"), Value: aws.String("LambdaProcessor")},
				},
			},
			{
				MetricName: aws.String("RedisGeoUpdated"),
				Timestamp:  aws.Time(now),
				Value:      aws.Float64(float64(redisUpdated)),
				Unit:       types.StandardUnitCount,
				Dimensions: []types.Dimension{
					{Name: aws.String("Component"), Value: aws.String("LambdaProcessor")},
				},
			},
			{
				MetricName: aws.String("PostgresRowsInserted"),
				Timestamp:  aws.Time(now),
				Value:      aws.Float64(float64(pgInserted)),
				Unit:       types.StandardUnitCount,
				Dimensions: []types.Dimension{
					{Name: aws.String("Component"), Value: aws.String("LambdaProcessor")},
				},
			},
			{
				MetricName: aws.String("BatchProcessingLatencyMs"),
				Timestamp:  aws.Time(now),
				Value:      aws.Float64(durationMs),
				Unit:       types.StandardUnitMilliseconds,
				Dimensions: []types.Dimension{
					{Name: aws.String("Component"), Value: aws.String("LambdaProcessor")},
				},
			},
		},
	})
	return err
}

