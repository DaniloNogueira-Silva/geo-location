# Amazon Kinesis Data Stream para ingestão de telemetria de alta vazão
resource "aws_kinesis_stream" "telemetry_stream" {
  name             = var.kinesis_stream_name
  shard_count      = var.kinesis_shard_count
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds",
  ]

  tags = {
    Project     = var.project_name
    Environment = "local-ministack"
    Component   = "ingestion-streaming"
  }
}

