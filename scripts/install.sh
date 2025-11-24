#!/bin/bash
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Project root is one level up
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Install uv if not installed
if ! command -v uv &> /dev/null; then
    if ! command -v curl &> /dev/null; then
        echo "Error: curl is required to install uv. Please install curl."
        exit 1
    fi
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    
    # Add cargo bin to path for current session if just installed
    if [ -f "$HOME/.cargo/env" ]; then
        source "$HOME/.cargo/env"
    fi
fi

echo "Syncing dependencies with uv..."
cd "$PROJECT_ROOT"
uv sync

# Ensure project structure exists
echo "Creating default directories..."
mkdir -p configs states logs data

# We run using uv to ensure environment consistency
# Note: "$CONFIG_PATH" is expected to be defined or passed as an argument.
# The original instruction had a syntax error in this line, which has been corrected.
# If this line is intended to replace the final echo, please clarify.
# For now, it's added after uv sync.
# caffeinate -i -s uv run neutron-backfill "$CONFIG_PATH"

echo "Installation complete. You can now run backfills using ./scripts/run_backfill.sh"
