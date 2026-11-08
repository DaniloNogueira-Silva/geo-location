# Amazon ElastiCache (Redis) para estado imediato da frota e consultas GEOADD/GEOSEARCH
resource "aws_elasticache_cluster" "fleet_redis" {
  cluster_id           = "${var.project_name}-redis"
  engine               = "redis"
  node_type            = "cache.t3.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  engine_version       = "7.0"
  port                 = 6379

  tags = {
    Project     = var.project_name
    Environment = "local-ministack"
    Component   = "realtime-geospatial-cache"
  }
}

