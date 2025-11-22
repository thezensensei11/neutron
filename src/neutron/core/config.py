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
class NeutronConfig:
    database_url: Optional[str]
    tasks: List[TaskConfig]

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

        return NeutronConfig(
            database_url=data.get('database', {}).get('url'),
            tasks=tasks
        )
