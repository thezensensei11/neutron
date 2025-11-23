#!/bin/bash

# Check if config file is provided
if [ -z "$1" ]; then
    echo "Usage: ./run_backfill.sh <config_path>"
    exit 1
fi

CONFIG_PATH=$1

echo "Starting Neutron Backfill with sleep prevention..."
echo "Config: $CONFIG_PATH"
echo "Process ID: $$"

# Use caffeinate to prevent sleep
# -i: Prevent idle sleep
# -s: Prevent system sleep
# -w <pid>: Wait for process with pid to exit (we wrap the python command)

# We run python directly inside caffeinate
PYTHONPATH=src caffeinate -i -s python3 -m neutron.core.downloader "$CONFIG_PATH"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Backfill completed successfully."
else
    echo "Backfill failed with exit code $EXIT_CODE."
fi

exit $EXIT_CODE
