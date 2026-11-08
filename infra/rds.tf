# Amazon RDS (PostgreSQL) para armazenamento histórico, auditoria e séries temporais
resource "aws_db_instance" "fleet_postgres" {
  identifier             = "${var.project_name}-rds"
  allocated_storage      = 20
  storage_type           = "gp3"
  engine                 = "postgres"
  engine_version         = "16.1"
  instance_class         = "db.t3.micro"
  db_name                = "fleet_telemetry"
  username               = "fleet_admin"
  password               = "fleet_password"
  port                   = 5432
  publicly_accessible    = true
  skip_final_snapshot    = true
  deletion_protection    = false

  tags = {
    Project     = var.project_name
    Environment = "local-ministack"
    Component   = "historical-storage"
  }
}

