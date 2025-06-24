#!/bin/sh

# The OpenTelemetry Collector Builder (OCB) is required to run this script to
# regenerate the collector. It is typically available via the 'builder' command,
# or can also be named 'ocb' depending on your setup.

# If OCB is not installed we should exit with an error message.
if command -v builder >/dev/null 2>&1; then
  OCB=$(command -v builder)
elif command -v ocb >/dev/null 2>&1; then
  OCB=$(command -v ocb)
else
  echo "OpenTelemetry Collector Builder is not available on the PATH."
  exit 1
fi

# The configuration file for the OpenTelemetry Collector Builder.
OCB_CONFIG_FILE="$(dirname "$0")/config.yml"

echo "Using OpenTelemetry Collector Builder: $OCB"
echo "Using configuration file: $OCB_CONFIG_FILE"

# Generate the OpenTelemetry Collector configuration using the OCB.
# The --skip-compilation flag is used to skip the compilation step, as we are
# only interested in generating the configuration.
$OCB --config=$OCB_CONFIG_FILE --skip-compilation
