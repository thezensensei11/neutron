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
    ohlcv_path: str = "data/ohlcv" # Path for Parquet storage (OHLCV)
    questdb_host: str = "localhost"
    questdb_ilp_port: int = 9009
    questdb_pg_port: int = 8812
    questdb_username: str = "admin"
    questdb_password: str = "quest"
    questdb_database: str = "qdb"

@dataclass
class NeutronConfig:
    storage: StorageConfig
    tasks: List[TaskConfig]
    data_state_path: str = "states/ohlcv_data_state.json"
    tick_data_state_path: str = "states/tick_data_state.json"
    exchange_state_path: str = "states/exchange_state.json"
    max_workers: int = 1

class ConfigLoader:
    @staticmethod
    def load(config_path: str) -> NeutronConfig:
        """Load configuration from a JSON file."""
        if not os.path.exists(config_path):
            # Try looking in configs/ directory
            alt_path = os.path.join("configs", config_path)
            if os.path.exists(alt_path):
                config_path = alt_path
            else:
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
            
        storage_config = StorageConfig(
            ohlcv_path=storage_data.get('ohlcv_path', 'data/ohlcv'),
            questdb_host=storage_data.get('questdb_host', 'localhost'),
            questdb_ilp_port=storage_data.get('questdb_ilp_port', 9009),
            questdb_pg_port=storage_data.get('questdb_pg_port', 8812),
            questdb_username=storage_data.get('questdb_username', 'admin'),
            questdb_password=storage_data.get('questdb_password', 'quest'),
            questdb_database=storage_data.get('questdb_database', 'qdb')
        )

        return NeutronConfig(
            storage=storage_config,
            tasks=tasks,
            data_state_path=data.get('data_state_path', 'states/ohlcv_data_state.json'),
            tick_data_state_path=data.get('tick_data_state_path', 'states/tick_data_state.json'),
            exchange_state_path=data.get('exchange_state_path', 'states/exchange_state.json'),
            max_workers=data.get('max_workers', 1)
        )
