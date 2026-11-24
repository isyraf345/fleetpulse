# Reproduce the platform checks from a checkout using only synthetic fixtures.
# Prerequisites: Python 3.12 (py launcher), Node 24, .NET 10, Docker Desktop, Edge.
# Existing credentials and published data are preserved. No real-source downloads.
param([switch]$SkipInstall)
$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location -LiteralPath $projectRoot
$apiExecutable = Join-Path $projectRoot 'backend/FleetPulse.Api/bin/Debug/net10.0/FleetPulse.Api.exe'
if (Get-Process -Name 'FleetPulse.Api' -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $apiExecutable }) {
    throw 'Stop the running FleetPulse API before verification so Windows can rebuild its executable; restart with python -m serving.local api afterward.'
}
function Invoke-Checked {
    param([string]$Program, [string[]]$Arguments)
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Program failed with exit code $LASTEXITCODE" }
}
foreach ($program in @('py','node','npm','dotnet','docker')) {
    if (-not (Get-Command $program -ErrorAction SilentlyContinue)) { throw "Install prerequisite: $program" }
}
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) {
    Invoke-Checked 'py' @('-3.12','-m','venv','.venv')
}
$projectPython = Join-Path $projectRoot '.venv/Scripts/python.exe'
$env:DOTNET_CLI_HOME = Join-Path $projectRoot 'data/dotnet'
$env:NUGET_PACKAGES = Join-Path $projectRoot 'data/nuget-packages'
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
if (-not $SkipInstall) {
    Invoke-Checked $projectPython @('-m','pip','install','-r','requirements-serving-lock.txt')
    Invoke-Checked 'npm' @('--prefix','frontend','ci')
}
Invoke-Checked $projectPython @('-m','pip','check')
Invoke-Checked $projectPython @('-m','serving.local','init')
Invoke-Checked 'docker' @('compose','--env-file','.env','-f','infra/compose.yaml','up','-d','--wait','postgres')
Invoke-Checked $projectPython @('-m','serving.database')
Invoke-Checked 'npm' @('--prefix','frontend','run','build')
Invoke-Checked 'dotnet' @('restore','backend/FleetPulse.Api','--locked-mode')
Invoke-Checked 'dotnet' @('build','backend/FleetPulse.Api','--no-restore')
$previousIntegrationFlag = $env:FLEETPULSE_INTEGRATION
try {
    $env:FLEETPULSE_INTEGRATION = '1'
    Invoke-Checked $projectPython @('-m','unittest','discover','-s','tests','-v')
    Invoke-Checked $projectPython @('-m','ci.check_browser')
} finally {
    $env:FLEETPULSE_INTEGRATION = $previousIntegrationFlag
}
Write-Host 'Local verification complete. PostgreSQL remains running; temporary test databases and APIs were cleaned up.'
