# 🚚 Sistema de Telemetria e Rastreamento de Frotas em Tempo Real

Implementação completa da arquitetura de ingestão, streaming, armazenamento híbrido e observabilidade de telemetria de frotas em tempo real utilizando **Go**, **Amazon Kinesis**, **AWS Lambda**, **Amazon ElastiCache (Redis)**, **Amazon RDS (PostgreSQL)**, **Amazon CloudWatch**, **Grafana**, **MiniStack** e **Terraform**.

---

## 📂 Estrutura do Projeto

```text
Geo-Location/
├── cmd/
│   ├── gateway/
│   │   ├── main.go              # API Edge (POST /telemetry, /telemetry/batch, GET /trucks/nearby, /state, /history)
│   │   └── main_test.go         # Teste real ponta a ponta da API Gateway + Kinesis + Redis + Postgres
│   ├── lambda/
│   │   ├── main.go              # AWS Lambda Consumer (Lotes Kinesis -> Redis GEOADD + Postgres Batch Insert)
│   │   └── main_test.go         # Teste real do handler AWS Lambda com eventos atrasados e registros corrompidos
│   └── simulator/
│       ├── main.go              # Simulador de Carga (Goroutines + Buffer de queda de sinal 4G)
│       └── main_test.go         # Testes de cinemática, azimute e envio em lote pós-queda de 4G
├── internal/
│   ├── config/                  # Configurações centralizadas via variáveis de ambiente (+ testes)
│   ├── domain/                  # Structs de telemetria e regras de validação WGS84 (+ testes)
│   ├── kinesis/                 # Cliente AWS SDK v2 para Kinesis (+ testes reais no MiniStack)
│   ├── observability/           # Publicador de métricas no Amazon CloudWatch (+ testes reais no MiniStack)
│   └── storage/
│       ├── redis.go             # GEOADD atômico via Lua Script + GEOSEARCH por raio (+ testes reais no Redis)
│       └── postgres.go          # Batch Insert idempotente (pgx.Batch) e histórico (+ testes reais no Postgres)
├── observability/
│   └── grafana/                 # Provisionamento automático de Datasources e Dashboard com Mapa (Geomap)
├── infra/                       # Scripts Terraform apontando para o MiniStack (localhost:4566)
├── docs/
│   └── telemetry_schema.json    # JSON Schema (Draft 2020-12) do payload de telemetria
├── sql/
│   └── init.sql                 # DDL e índices de série temporal do PostgreSQL
├── scripts/
│   ├── build-lambda.ps1         # Compila binário Linux 'bootstrap' e empacota build/lambda_function.zip
│   └── terraform-apply.ps1      # Executa Terraform (local ou via Docker hashicorp/terraform) no MiniStack
└── docker-compose.yml           # MiniStack (4566), Redis (6379), PostgreSQL (5432) e Grafana (3000)
```

---

## 🚀 Como Executar Ponta a Ponta

### 1. Subir a Infraestrutura Local (Docker Compose)
```powershell
docker compose up -d
```
Serviços disponíveis:
* **MiniStack (AWS Local):** `http://localhost:4566`
* **Grafana (Dashboard & Mapa em Tempo Real):** `http://localhost:3000` (acesso direto sem senha ou `admin` / `admin`)
* **Redis (ElastiCache):** `localhost:6379`
* **PostgreSQL (RDS):** `localhost:5432`

### 2. Provisionar Recursos no MiniStack via Terraform
Mesmo que você não tenha o binário `terraform` instalado no Windows, o script abaixo compila o pacote da Lambda e executa o Terraform automaticamente via container Docker:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\terraform-apply.ps1
```

### 3. Iniciar o Gateway de Ingestão (API Edge)
```powershell
go run ./cmd/gateway
```

### 4. Iniciar o Consumidor AWS Lambda / Worker do Kinesis
```powershell
go run ./cmd/lambda
```

### 5. Iniciar o Simulador de Carga (Goroutines + Simulação de Queda 4G)
```powershell
# Simula 1000 caminhões a cada 2s com 5% de chance de entrar em zona sem sinal 4G e acumular lote
go run ./cmd/simulator -trucks=1000 -interval=2s -offline-prob=0.05
```

---

## 🧪 Executando a Suíte de Testes Reais (Unitários + Integração)

Com os containers do `docker compose` ativos, execute:
```powershell
go test -v ./...
```
O que os testes validam na prática:
* **Domínio & Limites Físicos (`internal/domain`)**: Limites WGS84 (`[-90, 90]`, `[-180, 180]`), rejeição de coordenada `(0,0)`, limites de combustível, velocidade e azimute.
* **Proteção contra Dados Atrasados no Redis (`internal/storage/redis_test.go`)**: Comprova que o script Lua no Redis rejeita pacotes com `timestamp` anterior ao estado atual (impedindo que um pacote 4G atrasado faça o caminhão "voltar no tempo" no mapa em tempo real) e valida a precisão espacial do `GEOSEARCH` em raios de 5 km e 25 km.
* **Ordenação e Idempotência no PostgreSQL (`internal/storage/postgres_test.go`)**: Envia lotes fora de ordem e duplicados (simulando retries da AWS Lambda) e comprova que `ON CONFLICT (event_id) DO NOTHING` evita duplicatas e que a leitura retorna a série temporal ordenada pelo `timestamp` gerado no caminhão.
* **Streaming no MiniStack Kinesis (`internal/kinesis/client_test.go`)**: Cria stream real no MiniStack, publica via `PutRecord` e `PutRecords` e lê diretamente do Shard via `GetShardIterator` + `GetRecords`.
* **Métricas no MiniStack CloudWatch (`internal/observability/cloudwatch_test.go`)**: Publica métricas reais de ingestão e processamento em lote e valida via `ListMetrics` no namespace `FleetTelemetry`.
* **Queda de Sinal 4G no Simulador (`cmd/simulator/main_test.go`)**: Comprova que durante a perda de sinal 4G o caminhão armazena os eventos em buffer preservando o `timestamp` original de cada coleta e descarrega tudo de uma vez em `POST /telemetry/batch` assim que o sinal retorna.
