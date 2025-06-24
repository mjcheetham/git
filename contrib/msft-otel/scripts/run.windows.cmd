@echo off
REM This script is used to run the OpenTelemetry Collector with the generated config file.
REM It assumes that the config file is located alongside this script.
REM Usage: run.cmd

SETLOCAL ENABLEDELAYEDEXPANSION

REM Get the directory of this script
SET "THIS_DIR=%~dp0"
SET "APP_NAME=msft-git-otel.exe"

REM Remove trailing backslash if present
IF "%THIS_DIR:~-1%"=="\" SET "THIS_DIR=%THIS_DIR:~0,-1%"

REM Check if the base config file exists
SET "CONFIG_FILE=%THIS_DIR%\config.yml"
IF NOT EXIST "%CONFIG_FILE%" (
    echo Config file not found: %CONFIG_FILE%
    EXIT /B 1
)

REM Read Git config to get the socket path and Azure Monitor connection string
FOR /F "usebackq delims=" %%A IN (`git config --get trace2.eventTarget`) ^
  DO SET "TRACE2_PIPE=%%A"
IF NOT DEFINED TRACE2_PIPE (
    echo TRACE2 event target not set in Git config.
    EXIT /B 1
)


FOR /F "usebackq delims=" %%A IN (`git config --get otel.azuremonitor.connectionString`) ^
  DO SET "AZMON_CONNSTRING=%%A"
IF NOT DEFINED AZURE_MONITOR_CONNECTION_STRING (
    echo Azure Monitor connection string not set in Git config.
    EXIT /B 1
)

REM Set the environment variables for the collector
SET "TRACE2_PIPE=%TRACE2_PIPE%"
SET "AZURE_MONITOR_CONNECTION_STRING=%AZMON_CONNSTRING%"

REM Run the custom collector with the specified config file
echo Running %APP_NAME% custom collector with config: %CONFIG_FILE%
"%APP_NAME%" --config "%CONFIG_FILE%"
