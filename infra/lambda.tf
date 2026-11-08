# IAM Role para execução da AWS Lambda
resource "aws_iam_role" "lambda_exec_role" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Permissão para consumir o Kinesis e publicar logs no CloudWatch
resource "aws_iam_role_policy" "lambda_kinesis_policy" {
  name = "${var.project_name}-lambda-kinesis-policy"
  role = aws_iam_role.lambda_exec_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:ListShards",
          "kinesis:ListStreams"
        ]
        Resource = aws_kinesis_stream.telemetry_stream.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      }
    ]
  })
}

# Função AWS Lambda em Go (Runtime provided.al2023 -> binário 'bootstrap')
resource "aws_lambda_function" "telemetry_processor" {
  function_name    = "${var.project_name}-processor"
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["x86_64"]
  filename         = var.lambda_zip_path
  source_code_hash = fileexists(var.lambda_zip_path) ? filebase64sha256(var.lambda_zip_path) : null
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      REDIS_ADDR   = var.redis_host
      POSTGRES_DSN = var.postgres_dsn
      AWS_REGION   = var.aws_region
    }
  }

  tags = {
    Project     = var.project_name
    Environment = "local-ministack"
    Component   = "kinesis-consumer-worker"
  }
}

# Event Source Mapping acionando a Lambda em lotes a partir do Kinesis
resource "aws_lambda_event_source_mapping" "kinesis_to_lambda" {
  event_source_arn                   = aws_kinesis_stream.telemetry_stream.arn
  function_name                      = aws_lambda_function.telemetry_processor.arn
  starting_position                  = "LATEST"
  batch_size                         = 100
  maximum_batching_window_in_seconds = 2
  parallelization_factor             = 2
  maximum_retry_attempts             = 3
  bisect_batch_on_function_error     = true
}

