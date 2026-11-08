# Script para subir toda a stack local (Docker Compose + Gateway + Lambda Worker + Simulador)
# Abre janelas dedicadas para o Gateway, Lambda Consumer e Simulador para você acompanhar os logs em tempo real.

$ErrorActionPreference = "Stop"
$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RootDir

Write-Host "1. Garantindo que MiniStack, Redis, PostgreSQL e Grafana estão rodando..." -ForegroundColor Cyan
docker compose up -d

Write-Host "2. Iniciando Gateway de Ingestão (http://localhost:8080)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$RootDir'; `$Host.UI.RawUI.WindowTitle = 'API Gateway (:8080)'; go run ./cmd/gateway"

Start-Sleep -Seconds 3

Write-Host "3. Iniciando Lambda / Worker Consumidor do Kinesis..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$RootDir'; `$Host.UI.RawUI.WindowTitle = 'AWS Lambda Consumer (Kinesis -> Redis + Postgres)'; go run ./cmd/lambda"

Start-Sleep -Seconds 2

Write-Host "4. Iniciando Simulador de Frota (200 caminhões a cada 2s com simulação de queda 4G)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$RootDir'; `$Host.UI.RawUI.WindowTitle = 'Fleet Simulator (Goroutines)'; go run ./cmd/simulator -trucks=200 -interval=2s -offline-prob=0.05"

Write-Host ""
Write-Host "✅ Tudo rodando!" -ForegroundColor Green
Write-Host "👉 Mapa e Dashboard no Grafana: http://localhost:3000" -ForegroundColor Yellow
Write-Host "👉 Healthcheck do Gateway:      http://localhost:8080/health" -ForegroundColor Yellow
Write-Host "👉 Caminhões próximos a SP:     http://localhost:8080/trucks/nearby?lat=-23.5505&lon=-46.6333&radius_km=80" -ForegroundColor Yellow

