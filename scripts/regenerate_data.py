import logging
import json
from pathlib import Path
from neutron.services.aggregator import OHLCVAggregatorService
from neutron.services.synthetic import SyntheticOHLCVService
from neutron.core.processor import DataProcessor

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # 1. Create Config
    config = {
        "storage": {
            "ohlcv_path": "data/ohlcv",
            "aggregated_path": "data/aggregated"
        },
        "max_workers": 4,
        "processor_tasks": [
            {
                "type": "resample_ohlcv",
                "params": {
                    "assets": ["BTC"],
                    "timeframes": ["5m", "15m", "30m", "1h", "2h", "4h", "1d"],
                    "rewrite": True
                }
            }
        ]
    }
    
    # Save temp config
    with open("configs/config_regenerate.json", "w") as f:
        json.dump(config, f, indent=2)

    # 2. Run Aggregator (Spot & Swap)
    logger.info("Running Aggregator...")
    agg_service = OHLCVAggregatorService(config)
    agg_service.run({'rewrite': True})

    # 3. Run Synthetic Creation
    logger.info("Running Synthetic Service...")
    syn_service = SyntheticOHLCVService(config)
    syn_service.run({'rewrite': True})

    # 4. Run Resampler (via Processor)
    logger.info("Running Resampler...")
    processor = DataProcessor("configs/config_regenerate.json")
    processor.run()

    logger.info("Regeneration Complete.")

if __name__ == "__main__":
    main()
