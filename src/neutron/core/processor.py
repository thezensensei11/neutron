import logging
import json
from pathlib import Path
from typing import Dict, List, Any

from ..services.resampler import ResamplerService
from .config import ConfigLoader

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Orchestrator for data processing tasks (e.g., Resampling, Feature Engineering).
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config(config_path)
        
        # Initialize Services
        self.resampler_service = ResamplerService(self.config)

    def _load_config(self, path: str) -> Dict[str, Any]:
        """
        Loads configuration from JSON file.
        """
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {path}: {e}")
            return {}

    def run(self) -> Dict[str, Any]:
        """
        Executes all tasks defined in the 'processor_tasks' section of the config.
        Returns a dictionary of results from each task.
        """
        tasks = self.config.get('processor_tasks', [])
        results = {}
        
        if not tasks:
            logger.warning("No 'processor_tasks' found in configuration.")
            return results

        logger.info(f"Starting DataProcessor with {len(tasks)} tasks...")

        for task in tasks:
            task_type = task.get('type')
            params = task.get('params', {})
            
            try:
                if task_type == 'resample_ohlcv':
                    task_results = self._run_resample_ohlcv(params)
                    results[task_type] = task_results
                else:
                    logger.warning(f"Unknown task type: {task_type}")
            except Exception as e:
                logger.error(f"Task {task_type} failed: {e}")
                import traceback
                traceback.print_exc()

        logger.info("DataProcessor finished.")
        return results

    def _run_resample_ohlcv(self, params: Dict[str, Any]) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Runs the Resampler Service.
        """
        logger.info(">>> Task: Resample OHLCV")
        return self.resampler_service.run(params)

if __name__ == "__main__":
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) < 2:
        print("Usage: python -m neutron.core.processor <config_path>")
        sys.exit(1)
        
    config_file = sys.argv[1]
    processor = DataProcessor(config_file)
    processor.run()
