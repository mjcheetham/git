#!/bin/sh

# This script is used to run the OpenTelemetry Collector with the generated config file.
# It assumes that the config file is located alongside this script.
# Usage: ./run.sh

THIS_DIR=$(dirname "$0")
APP_NAME="msft-git-otel"

# Check if the base config file exists
CONFIG_FILE="${THIS_DIR}/config.yml"
if [ ! -f "${CONFIG_FILE}" ]; then
  echo "Config file not found: ${CONFIG_FILE}"
  exit 1
fi

# Read Git config to get the socket path and Azure Monitor connection string
TRACE2_SOCKET=$(git config --get trace2.eventTarget)
if [ -z "${AZURE_MONITOR_CONNECTION_STRING}" ]; then
  echo "TRACE2 event target not set in Git config."
  exit 1
fi

AZURE_MONITOR_CONNECTION_STRING=$(git config --get otel.azuremonitor.connectionString)
if [ -z "${AZURE_MONITOR_CONNECTION_STRING}" ]; then
  echo "Azure Monitor connection string set found in Git config."
  exit 1
fi

# Set the environment variables for the collector
export TRACE2_SOCKET
export AZURE_MONITOR_CONNECTION_STRING

# Run the custom collector with the specified config file
"${APP_NAME}" --config "${CONFIG_FILE}"
