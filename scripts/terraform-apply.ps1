# Script para provisionar a infraestrutura no MiniStack via Terraform
# Usa o binário local 'terraform' se instalado; caso contrário, executa via container Docker oficial (hashicorp/terraform).

$ErrorActionPreference = "Stop"

$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$InfraDir = Join-Path $RootDir "infra"

# 1. Garante que o pacote build/lambda_function.zip exista antes do Terraform Apply
& (Join-Path $PSScriptRoot "build-lambda.ps1")

if (Get-Command "terraform" -ErrorAction SilentlyContinue) {
    Write-Host "Executando Terraform local..." -ForegroundColor Cyan
    Push-Location $InfraDir
    try {
        terraform init -input=false
        terraform apply -auto-approve -input=false
    } finally {
        Pop-Location
    }
} else {
    Write-Host "Terraform CLI local não encontrado. Executando via container Docker (hashicorp/terraform)..." -ForegroundColor Cyan
    docker run --rm `
        -v "${RootDir}:/workspace" `
        -w /workspace/infra `
        hashicorp/terraform:1.9 init -input=false

    docker run --rm `
        -v "${RootDir}:/workspace" `
        -w /workspace/infra `
        -e TF_VAR_ministack_endpoint="http://host.docker.internal:4566" `
        hashicorp/terraform:1.9 apply -auto-approve -input=false
}

Write-Host "Infraestrutura provisionada com sucesso no MiniStack!" -ForegroundColor Green

