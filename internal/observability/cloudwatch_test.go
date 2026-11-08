package observability

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/danil/geo-location/internal/config"
)

func TestCloudWatchPublishAndListMetricsInMiniStack(t *testing.T) {
	ctx := context.Background()
	cfg := config.Load()

	cw, err := NewCloudWatchClient(ctx, cfg)
	if err != nil {
		t.Fatalf("falha ao inicializar CloudWatchClient: %v", err)
	}

	if err := cw.RecordGatewayIngestion(ctx, 5, 2.35); err != nil {
		t.Fatalf("falha em RecordGatewayIngestion no MiniStack: %v", err)
	}

	if err := cw.RecordLambdaBatch(ctx, 25, 20, 25, 11.8); err != nil {
		t.Fatalf("falha em RecordLambdaBatch no MiniStack: %v", err)
	}

	out, err := cw.Raw().ListMetrics(ctx, &cloudwatch.ListMetricsInput{
		Namespace: aws.String(NamespaceFleetTelemetry),
	})
	if err != nil {
		t.Fatalf("falha ao consultar ListMetrics no MiniStack CloudWatch: %v", err)
	}

	foundMetrics := make(map[string]bool)
	for _, m := range out.Metrics {
		foundMetrics[aws.ToString(m.MetricName)] = true
	}

	expected := []string{
		"IngestedEvents",
		"GatewayLatencyMs",
		"ProcessedBatchSize",
		"RedisGeoUpdated",
		"PostgresRowsInserted",
		"BatchProcessingLatencyMs",
	}
	for _, name := range expected {
		if !foundMetrics[name] {
			t.Fatalf("métrica %q não encontrada no CloudWatch após publicação (encontradas: %v)", name, foundMetrics)
		}
	}
}

