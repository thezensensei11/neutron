import json
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class TaskConfig:
    type: str
    params: Dict[str, Any]
    exchanges: Dict[str, Dict[str, Dict[str, List[str]]]] = None # exchange -> instrument_type -> params (e.g. symbols)

@dataclass
class StorageConfig:
    type: str = "database" # "database" or "parquet"
    path: Optional[str] = None # for parquet
    database_url: Optional[str] = None # for database

@dataclass
class NeutronConfig:
    storage: StorageConfig
    tasks: List[TaskConfig]
    data_state_path: str = "data_state.json"
    tick_data_state_path: str = "tick_data_state.json"
    exchange_state_path: str = "exchange_state.json"
    max_workers: int = 1

class ConfigLoader:
    @staticmethod
    def load(config_path: str) -> NeutronConfig:
        """Load configuration from a JSON file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            data = json.load(f)

        tasks = []
        for task_data in data.get('tasks', []):
            tasks.append(TaskConfig(
                type=task_data.get('type'),
                params=task_data.get('params', {}),
                exchanges=task_data.get('exchanges', {})
            ))

        storage_data = data.get('storage', {})
        # Backwards compatibility: check root 'database' key if storage not present
        if not storage_data and 'database' in data:
            storage_data = {
                'type': 'database',
                'database_url': data['database'].get('url')
            }
            
        storage_config = StorageConfig(
            type=storage_data.get('type', 'database'),
            path=storage_data.get('path'),
            database_url=storage_data.get('database_url')
        )

        return NeutronConfig(
            storage=storage_config,
            tasks=tasks,
            data_state_path=data.get('data_state_path', 'data_state.json'),
            tick_data_state_path=data.get('tick_data_state_path', 'tick_data_state.json'),
            exchange_state_path=data.get('exchange_state_path', 'exchange_state.json'),
            max_workers=data.get('max_workers', 1)
        )
