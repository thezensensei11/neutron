# Neutron Setup & Usage Instructions

Neutron is designed to be a portable, robust data infrastructure package. It uses `uv` for dependency management, ensuring consistent execution across different environments.

## 1. Setup

To set up Neutron on a new machine (e.g., a fresh server or a colleague's laptop):

1.  **Clone the Repository**:
    ```bash
    git clone <repository_url>
    cd neutron
    ```

2.  **Install Dependencies**:
    Neutron uses `uv` for fast and reliable dependency management.
    ```bash
    # Install uv if you haven't already
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Sync dependencies
    uv sync
    ```

3.  **Start QuestDB**:
    If you plan to ingest Tick or Generic data, you need a running QuestDB instance.
    ```bash
    docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
    ```

## 2. Usage

### Running Backfills
The primary way to run Neutron is via the `run_backfill.sh` script. This script handles environment activation and prevents system sleep on macOS (`caffeinate`).

```bash
./scripts/run_backfill.sh configs/config_tick.json
```

Alternatively, you can run directly with `uv`:
```bash
uv run neutron-backfill configs/config_tick.json
```

### Data Quality Checks
To verify the integrity of your downloaded data and view rich analytics (Volume, Buy/Sell Ratios, Gaps):

```bash
uv run python scripts/data_quality_check.py
```

### Logs
- `logs/ohlcvdata.log`: Detailed logs for OHLCV backfills.
- `logs/tickdata.log`: Detailed logs for Tick/Generic data backfills.

## 3. Configuration
Neutron is configured via JSON files in the `configs/` directory.
- `configs/config_ohlcv.json`: Example for OHLCV data.
- `configs/config_tick.json`: Example for Tick/Generic data.

See `README.md` for detailed configuration options.

## 4. Troubleshooting
- **Permission Denied**: Ensure scripts are executable: `chmod +x scripts/*.sh`
- **QuestDB Connection Failed**: Ensure Docker container is running and ports (9009, 8812) are exposed.
- **Missing Data**: Check `data_state.json` or `tick_data_state.json` to see what Neutron thinks it has already downloaded.
