variable "aws_region" {
  description = "Região AWS emulada no MiniStack"
  type        = string
  default     = "us-east-1"
}

variable "ministack_endpoint" {
  description = "Endpoint local do MiniStack"
  type        = string
  default     = "http://localhost:4566"
}

variable "project_name" {
  description = "Prefixo dos recursos do projeto"
  type        = string
  default     = "fleet-telemetry"
}

variable "kinesis_stream_name" {
  description = "Nome do Stream do Amazon Kinesis para ingestão de telemetria"
  type        = string
  default     = "fleet-telemetry-stream"
}

variable "kinesis_shard_count" {
  description = "Quantidade de shards do Kinesis para paralelismo de ingestão"
  type        = number
  default     = 2
}

variable "redis_host" {
  description = "Host do Redis acessível pela Lambda/Workers"
  type        = string
  default     = "localhost:6379"
}

variable "postgres_dsn" {
  description = "Connection string do PostgreSQL (RDS)"
  type        = string
  default     = "postgres://fleet_admin:fleet_password@localhost:5432/fleet_telemetry?sslmode=disable"
}

variable "lambda_zip_path" {
  description = "Caminho para o pacote zip compilado da AWS Lambda em Go"
  type        = string
  default     = "../build/lambda_function.zip"
}

