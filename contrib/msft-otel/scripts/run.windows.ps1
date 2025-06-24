# This script is used to run the OpenTelemetry Collector with the generated config file.
# It assumes that the config file is located alongside this script.
# Usage: run.windows.ps1

$ErrorActionPreference = 'Stop'

# Get the directory of this script
$THIS_DIR = Split-Path -Parent $MyInvocation.MyCommand.Definition
$APP_NAME = "msft-git-otel.exe"

# Check if the base config file exists
$CONFIG_FILE = Join-Path $THIS_DIR 'config.yml'
if (-not (Test-Path $CONFIG_FILE)) {
    Write-Host "Config file not found: $CONFIG_FILE"
    exit 1
}

# Read Git config to get the socket path and Azure Monitor connection string
$TRACE2_PIPE = & git config --get trace2.eventTarget
if ([string]::IsNullOrWhiteSpace($TRACE2_PIPE)) {
    Write-Host "TRACE2 event target not set in Git config."
    exit 1
}

$AZMON_CONNSTRING = & git config --get otel.azuremonitor.connectionString
if ([string]::IsNullOrWhiteSpace($AZMON_CONNSTRING)) {
    Write-Host "Azure Monitor connection string not set in Git config."
    exit 1
}

# Set the environment variable for the collector
$env:TRACE2_PIPE = $TRACE2_PIPE
$env:AZURE_MONITOR_CONNECTION_STRING = $AZMON_CONNSTRING

# Run the custom collector with the specified config file
Write-Host "Running $APP_NAME custom collector with config: $CONFIG_FILE"
& "$THIS_DIR\$APP_NAME" --config "$CONFIG_FILE"
