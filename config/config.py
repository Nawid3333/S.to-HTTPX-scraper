import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Credentials (store in .env file)
EMAIL = os.getenv("STO_EMAIL", "")
PASSWORD = os.getenv("STO_PASSWORD", "")

# Data storage
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# Logs directory
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOGS_DIR, "s_to_backup.log")

# Scraping configuration
NUM_WORKERS = 10  # Number of parallel httpx sessions (optimal based on benchmarking)
