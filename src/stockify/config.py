# load and validates config
from pathlib import Path
import os
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "config.yaml"

def load_config():
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f)

def get_logs_path() -> Path:
    config = load_config()
    return PROJECT_ROOT / config["paths"]["logs"]

def get_ticker_list_path() -> Path:
    config = load_config()
    path = PROJECT_ROOT / config["paths"]["ticker_list"]
    return path

# def get_raw_data_path() -> Path:
#     config = load_config()
#     path = PROJECT_ROOT / config["paths"]["raw_data"]
#     return path

def get_raw_data_path() -> Path:
    blob = Path.cwd() / 'DataStorage'
    blob.mkdir(parents=True, exist_ok=True)
    return blob
