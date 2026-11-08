# Script para compilar a AWS Lambda em Go (runtime provided.al2023 -> binário 'bootstrap')
# e gerar o arquivo build/lambda_function.zip consumido pelo Terraform (infra/lambda.tf)

$ErrorActionPreference = "Stop"

$BuildDir = Join-Path $PSScriptRoot "..\build"
if (-not (Test-Path $BuildDir)) {
    New-Item -ItemType Directory -Path $BuildDir | Out-Null
}

$BootstrapPath = Join-Path $BuildDir "bootstrap"
$ZipPath = Join-Path $BuildDir "lambda_function.zip"

Write-Host "Compilando binário Linux (x86_64) para AWS Lambda..." -ForegroundColor Cyan
$env:GOOS = "linux"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"

go build -tags lambda.norpc -ldflags="-s -w" -o $BootstrapPath ./cmd/lambda

Remove-Item Env:\GOOS
Remove-Item Env:\GOARCH
Remove-Item Env:\CGO_ENABLED

if (Test-Path $ZipPath) {
    Remove-Item $ZipPath -Force
}

Write-Host "Empacotando $ZipPath..." -ForegroundColor Cyan
Compress-Archive -Path $BootstrapPath -DestinationPath $ZipPath -Force

Write-Host "Pacote Lambda gerado com sucesso em: $ZipPath" -ForegroundColor Green

