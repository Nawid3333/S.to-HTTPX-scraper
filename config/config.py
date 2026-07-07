"""
S.TO HTTPX Scraper Configuration
Load credentials from .env file, set paths, and scraping options.
"""

import os
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Credentials (store in .env file)
EMAIL = os.getenv("STO_EMAIL", "")
PASSWORD = os.getenv("STO_PASSWORD", "")


def _validate_and_normalize_url(url: str) -> str:
    """Validate and normalize a URL, raising ValueError for invalid URLs."""
    if not url:
        raise ValueError("URL cannot be empty")

    # Ensure URL has a scheme
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    # Parse and validate
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError(f"Invalid URL: {url}")
        return url.rstrip("/")
    except Exception as e:
        raise ValueError(f"Invalid URL '{url}': {e}") from e


# Site configuration (edit here, not in .env)
try:
    SITE_URL = _validate_and_normalize_url("https://s.to")
except ValueError:
    SITE_URL = "https://s.to"

STO_FALLBACK_SITE_URL = "https://serienstream.to"
try:
    STO_FALLBACK_SITE_URL = _validate_and_normalize_url(
        STO_FALLBACK_SITE_URL) if STO_FALLBACK_SITE_URL else None
except ValueError:
    STO_FALLBACK_SITE_URL = "https://serienstream.to"

_STO_FALLBACK_SITE_URLS = []
# Optional: add additional fallback domains as validated strings below
# _STO_FALLBACK_SITE_URLS.append(_validate_and_normalize_url("https://example.com"))

# Built-in fallback domains
_BUILTIN_FALLBACK_URLS = [
    "http://186.2.175.5/",
]
for builtin_url in _BUILTIN_FALLBACK_URLS:
    try:
        normalized = _validate_and_normalize_url(builtin_url)
        if normalized not in _STO_FALLBACK_SITE_URLS:
            _STO_FALLBACK_SITE_URLS.append(normalized)
    except ValueError:
        print(
            f"⚠ Warning: Invalid built-in fallback URL skipped: {builtin_url}")

# Ensure primary fallback URL is in the list
if STO_FALLBACK_SITE_URL and STO_FALLBACK_SITE_URL not in _STO_FALLBACK_SITE_URLS:
    _STO_FALLBACK_SITE_URLS.insert(0, STO_FALLBACK_SITE_URL)

# Build SITE_URLS list with validation
SITE_URLS = [SITE_URL]
for url in _STO_FALLBACK_SITE_URLS:
    if url and url != SITE_URL:
        SITE_URLS.append(url)

# Extract valid hosts for URL validation
VALID_SERIES_HOSTS = []
for url in SITE_URLS:
    try:
        host = urlparse(url).netloc
        if host:
            VALID_SERIES_HOSTS.append(host)
    except Exception:
        pass

DEFAULT_FALLBACK_SITE_URL = _STO_FALLBACK_SITE_URLS[0] if _STO_FALLBACK_SITE_URLS else None

# Data storage
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# Logs directory
LOG_FILE = os.path.join(LOGS_DIR, "s_to_backup.log")

# Scraping configuration (edit here, not in .env)
NUM_WORKERS = 10  # Number of parallel httpx sessions

# Timeouts (edit here, not in .env)
HTTP_REQUEST_TIMEOUT = 20.0

# Default batch file for single/batch URL import
# Edit DEFAULT_BATCH_FILE_PATH below to change the default batch file
DEFAULT_BATCH_FILE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "series_urls.txt")
DEFAULT_BATCH_FILE = os.path.abspath(DEFAULT_BATCH_FILE_PATH)

print(f"✓ Config loaded (DATA_DIR: {os.path.abspath(DATA_DIR)})")
