#!/bin/bash

# Check if config file is provided
if [ -z "$1" ]; then
    echo "Usage: ./run_backfill.sh <config_path>"
    exit 1
fi

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Project root is one level up
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

CONFIG_PATH=$1

# If config path is relative, make it absolute or relative to CWD, 
# but we will run from PROJECT_ROOT, so we need to be careful.
# Simplest is to resolve it to absolute path.
if [[ "$CONFIG_PATH" != /* ]]; then
    CONFIG_PATH="$(pwd)/$CONFIG_PATH"
fi

echo "Starting Neutron Backfill with sleep prevention..."
echo "Config: $CONFIG_PATH"
echo "Process ID: $$"

# Use caffeinate to prevent sleep
# -i: Prevent idle sleep
# -s: Prevent system sleep
# -w <pid>: Wait for process with pid to exit (we wrap the python command)

# We run python directly inside caffeinate
# We run using uv to ensure environment consistency
cd "$PROJECT_ROOT"

if command -v caffeinate &> /dev/null; then
    echo "Running with sleep prevention (caffeinate)..."
    caffeinate -i -s uv run neutron-backfill "$CONFIG_PATH"
else
    echo "Running without sleep prevention..."
    uv run neutron-backfill "$CONFIG_PATH"
fi

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Backfill completed successfully."
else
    echo "Backfill failed with exit code $EXIT_CODE."
fi

exit $EXIT_CODE
