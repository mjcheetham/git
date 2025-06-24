#!/bin/sh

ROOT_DIR="$(cd "$(dirname "$0")"; pwd)"
SRC_DIR="${ROOT_DIR}/src"
CONFIG_DIR="${ROOT_DIR}/config"
BIN_NAME="msft-git-otel"
BIN_EXT=""

POSIX_OSES=("macos" "linux")
case "$(uname -s)" in
  Linux*)
    OS=linux
    ;;
  Darwin*)
    OS=macos
    ;;
  MINGW*)
    OS=windows
    BIN_EXT=".exe"
    ;;
  *)
    echo "Unsupported OS";
    exit 1;;
esac

case "$(uname -m)" in
  x86_64)
    ARCH=x64
    ;;
  aarch64|arm64)
    ARCH=arm64
    ;;
  *)
    echo "Unsupported architecture";
    exit 1;;
esac

OUT_DIR="${ROOT_DIR}/out/${OS}-${ARCH}"
BIN_DIR="${OUT_DIR}/bin"
BIN_CONFIG_DIR="${BIN_DIR}/config"

# Ensure we have Go installed
if command -v go >/dev/null 2>&1; then
  GO="$(command -v go)"
else
  echo "Go is not installed. Please install Go to proceed."
  exit 1
fi

echo "Using Go: $GO"

# Ensure the custom collector has been generated
if [ ! -f "${SRC_DIR}/main.go" ]; then
  echo "Custom collector not found in ${SRC_DIR}. Please run the generate script first."
  exit 1
fi

#
# Build the custom collector
#
echo "Building custom collector..."
mkdir -p "${BIN_DIR}"
"${GO}" -C "${SRC_DIR}" build -o "${BIN_DIR}/${BIN_NAME}${BIN_EXT}" || {
  echo "Failed to build the custom collector."
  exit 1
}

#
# Copy scripts
#
echo "Copying scripts..."

# Copy the run script to the bin directory
case "$OS" in
  linux|macos)
    cp "${ROOT_DIR}/scripts/run.posix.sh" "${BIN_DIR}/run.sh"
    chmod +x "${BIN_DIR}/run.sh"
    ;;
  windows)
    cp "${ROOT_DIR}/scripts/run.windows.ps1" "${BIN_DIR}/run.ps1"
    ;;
esac


#
# Copy configuration files
#
echo "Copying configuration files..."

# Copy shared config files to the bin/config directory
mkdir -p "${BIN_CONFIG_DIR}"
cp ${CONFIG_DIR}/filter.yml "${BIN_CONFIG_DIR}/"
cp ${CONFIG_DIR}/pii.yml "${BIN_CONFIG_DIR}/"
cp -r ${CONFIG_DIR}/rulesets "${BIN_CONFIG_DIR}/"

# Copy the main config file based on the OS to the bin directory
case "$OS" in
  linux|macos)
    cp ${CONFIG_DIR}/config.posix.yml "${BIN_DIR}/config.yml"
    ;;
  windows)
    cp ${CONFIG_DIR}/config.windows.yml "${BIN_DIR}/config.yml"
    ;;
esac

#
# Create archive
#
echo "Creating archive..."

# Create archive of the output directory (use tar.gz for Linux/Mac, zip for Windows)
ARCHIVE_NAME="msft-otel-collector_${OS}-${ARCH}"
case "$OS" in
  linux|macos)
    tar -czf "${ARCHIVE_NAME}.tar.gz" -C "${BIN_DIR}" .
    ;;
  windows)
    win_BIN_DIR=$(cygpath -w "${BIN_DIR}")
    win_OUT_DIR=$(cygpath -w "${OUT_DIR}")
    powershell -NoProfile -Command \
      "\$ProgressPreference = 'SilentlyContinue'; \
      Compress-Archive -Force -Path '${win_BIN_DIR}\\*' \
      -DestinationPath '${win_OUT_DIR}\\${ARCHIVE_NAME}.zip'"
    ;;
esac
