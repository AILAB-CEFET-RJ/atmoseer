#!/bin/bash

set -e  # Exit on error

# Path to environment file
ENV_FILE="config/environment.yml"

# === Validate setup prerequisites ===

# Check if environment file exists
if [ ! -f "$ENV_FILE" ]; then
  echo "Error: File '$ENV_FILE' not found."
  exit 1
fi

# Check if conda is available
if ! command -v conda &> /dev/null; then
  echo "Error: Conda is not installed or not in PATH."
  exit 1
fi

# Extract environment name
ENV_NAME=$(grep '^name:' "$ENV_FILE" | cut -d ' ' -f2)

if [ -z "$ENV_NAME" ]; then
  echo "Error: Could not determine the environment name from $ENV_FILE."
  exit 1
fi

# === Remove existing conda environment ===

if conda info --envs | awk '{print $1}' | grep -Fxq "$ENV_NAME"; then
  echo "Removing existing environment: $ENV_NAME"
  conda env remove -n "$ENV_NAME" || {
    echo "Error: Failed to remove environment $ENV_NAME."
    exit 1
  }
fi

# === Create conda environment ===

echo "Creating new environment: $ENV_NAME"
conda env create -f "$ENV_FILE" || {
  echo "Error: Failed to create environment $ENV_NAME."
  exit 1
}

# === Create required data directories ===

echo "Creating data directories..."
mkdir -p ./data/ws \
         ./data/as \
         ./data/NWP/ERA5 \
         ./data/datasets \
         ./data/GPM \
         ./data/goes16

echo "Data directory setup complete."

# === Setup NASA Earthdata authentication ===

echo "Setting up NASA Earthdata credentials..."

if [[ -z "$EARTHDATA_USER" || -z "$EARTHDATA_PASS" ]]; then
  echo "ERROR: EARTHDATA_USER and EARTHDATA_PASS environment variables must be set."
  echo "Export them before running this script, e.g.:"
  echo "  export EARTHDATA_USER='your_username'"
  echo "  export EARTHDATA_PASS='your_password'"
  exit 1
fi

NETRC_PATH="$HOME/.netrc"
echo "machine urs.earthdata.nasa.gov login $EARTHDATA_USER password $EARTHDATA_PASS" > "$NETRC_PATH"
chmod 600 "$NETRC_PATH"
echo ".netrc created at $NETRC_PATH"

URS_COOKIES_PATH="$HOME/.urs_cookies"
touch "$URS_COOKIES_PATH"
chmod 600 "$URS_COOKIES_PATH"
echo ".urs_cookies created at $URS_COOKIES_PATH"

echo "NASA Earthdata setup complete."

# === Optional: Build docker image ===
# TODO: docker build -t atmoseer .

echo "Setup complete."
