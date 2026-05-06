"""
S.TO HTTPX Scraper Configuration
Load credentials from .env file, set paths, and scraping options.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Credentials (store in .env file)
EMAIL = os.getenv("STO_EMAIL", "")
PASSWORD = os.getenv("STO_PASSWORD", "")

# Data storage
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# Logs directory
LOG_FILE = os.path.join(LOGS_DIR, "s_to_backup.log")

# Scraping configuration
NUM_WORKERS = int(os.getenv("STO_MAX_WORKERS", "10"))

# Timeouts
HTTP_REQUEST_TIMEOUT = 20.0

print(f"✓ Config loaded (DATA_DIR: {DATA_DIR})")
