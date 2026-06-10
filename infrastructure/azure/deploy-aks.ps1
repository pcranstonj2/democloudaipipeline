param(
    [Parameter(Mandatory = $true)]
    [string]$ResourceGroup,

    [Parameter(Mandatory = $true)]
    [string]$Location,

    [Parameter(Mandatory = $true)]
    [string]$AksName,

    [Parameter(Mandatory = $true)]
    [string]$AcrName,

    [string]$Namespace = "transaction-pipeline",

    [switch]$SkipInfrastructure,
    [switch]$SkipImageBuild
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Require-Command {
    param([string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' is not installed or not in PATH."
    }
}

Require-Command -Name "az"
Require-Command -Name "kubectl"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$manifestPath = Join-Path $repoRoot "infrastructure\k8s\all-services.yaml"

if (-not (Test-Path $manifestPath)) {
    throw "Cannot find manifest at: $manifestPath"
}

if (-not $SkipInfrastructure) {
    Write-Host "Creating resource group '$ResourceGroup' in '$Location'..."
    az group create --name $ResourceGroup --location $Location | Out-Null

    Write-Host "Creating ACR '$AcrName'..."
    az acr create --resource-group $ResourceGroup --name $AcrName --sku Basic --admin-enabled false | Out-Null

    Write-Host "Creating AKS '$AksName' and attaching ACR..."
    az aks create --resource-group $ResourceGroup --name $AksName --node-count 3 --generate-ssh-keys --attach-acr $AcrName | Out-Null
}

Write-Host "Fetching AKS credentials for '$AksName'..."
az aks get-credentials --resource-group $ResourceGroup --name $AksName --overwrite-existing | Out-Null

$acrLoginServer = (az acr show --resource-group $ResourceGroup --name $AcrName --query loginServer -o tsv).Trim()
if ([string]::IsNullOrWhiteSpace($acrLoginServer)) {
    throw "Could not resolve ACR login server for '$AcrName'."
}

Write-Host "Using ACR login server: $acrLoginServer"

$services = @(
    @{ Name = "ingestion-service"; Dockerfile = "ingestion-service\Dockerfile"; Context = "ingestion-service" },
    @{ Name = "enrichment-service"; Dockerfile = "enrichment-service\Dockerfile"; Context = "enrichment-service" },
    @{ Name = "model-service"; Dockerfile = "model-service\Dockerfile"; Context = "model-service" },
    @{ Name = "scoring-orchestrator"; Dockerfile = "scoring-orchestrator\Dockerfile"; Context = "scoring-orchestrator" },
    @{ Name = "drift-monitor"; Dockerfile = "drift-monitor\Dockerfile"; Context = "drift-monitor" }
)

if (-not $SkipImageBuild) {
    foreach ($svc in $services) {
        $dockerfilePath = Join-Path $repoRoot $svc.Dockerfile
        $contextPath = Join-Path $repoRoot $svc.Context

        if (-not (Test-Path $dockerfilePath)) {
            throw "Missing Dockerfile: $dockerfilePath"
        }
        if (-not (Test-Path $contextPath)) {
            throw "Missing build context: $contextPath"
        }

        Write-Host "Building and pushing $($svc.Name):latest to ACR..."
        az acr build --registry $AcrName --image "$($svc.Name):latest" --file $dockerfilePath $contextPath | Out-Null
    }
}

$manifestContent = Get-Content -Raw -Path $manifestPath

foreach ($svc in $services) {
    $sourceImage = "image: $($svc.Name):latest"
    $targetImage = "image: $acrLoginServer/$($svc.Name):latest"
    $manifestContent = $manifestContent.Replace($sourceImage, $targetImage)
}

$tempManifest = Join-Path $env:TEMP ("all-services-aks-" + [guid]::NewGuid().ToString("N") + ".yaml")
Set-Content -Path $tempManifest -Value $manifestContent -Encoding UTF8

try {
    Write-Host "Applying manifest to AKS..."
    kubectl apply -f $tempManifest

    # Expose ingestion API publicly for quick testing.
    Write-Host "Patching ingestion-service to LoadBalancer type..."
    kubectl patch service ingestion-service -n $Namespace --type merge -p '{"spec":{"type":"LoadBalancer"}}'

    Write-Host "Deployment submitted. Fetch external endpoint with:"
    Write-Host "kubectl get service ingestion-service -n $Namespace"
}
finally {
    if (Test-Path $tempManifest) {
        Remove-Item -Path $tempManifest -Force
    }
}
