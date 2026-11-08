output "kinesis_stream_name" {
  description = "Nome do Stream do Kinesis provisionado"
  value       = aws_kinesis_stream.telemetry_stream.name
}

output "kinesis_stream_arn" {
  description = "ARN do Stream do Kinesis provisionado"
  value       = aws_kinesis_stream.telemetry_stream.arn
}

output "elasticache_cluster_id" {
  description = "ID do cluster ElastiCache Redis"
  value       = aws_elasticache_cluster.fleet_redis.cluster_id
}

output "rds_endpoint" {
  description = "Endpoint da instância RDS PostgreSQL"
  value       = aws_db_instance.fleet_postgres.endpoint
}

output "lambda_function_name" {
  description = "Nome da Lambda consumidora do Kinesis"
  value       = aws_lambda_function.telemetry_processor.function_name
}

