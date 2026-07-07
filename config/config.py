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


# Site configuration
try:
    SITE_URL = _validate_and_normalize_url(
        os.getenv("STO_SITE_URL", "https://s.to"))
except ValueError:
    SITE_URL = "https://s.to"

STO_FALLBACK_SITE_URL = os.getenv(
    "STO_FALLBACK_SITE_URL", "https://serienstream.to")
try:
    STO_FALLBACK_SITE_URL = _validate_and_normalize_url(
        STO_FALLBACK_SITE_URL) if STO_FALLBACK_SITE_URL else None
except ValueError:
    STO_FALLBACK_SITE_URL = "https://serienstream.to"

_STO_FALLBACK_SITE_URLS = []
fallback_urls_raw = os.getenv("STO_FALLBACK_SITE_URLS", "")
if fallback_urls_raw:
    for url in fallback_urls_raw.split(","):
        url = url.strip()
        if url:
            try:
                _STO_FALLBACK_SITE_URLS.append(
                    _validate_and_normalize_url(url))
            except ValueError:
                print(f"⚠ Warning: Invalid fallback URL skipped: {url}")

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

# Scraping configuration
try:
    NUM_WORKERS = int(os.getenv("STO_MAX_WORKERS", "10"))
    if NUM_WORKERS < 1:
        NUM_WORKERS = 10
except ValueError:
    NUM_WORKERS = 10

# Timeouts
try:
    HTTP_REQUEST_TIMEOUT = float(os.getenv("STO_REQUEST_TIMEOUT", "20.0"))
    if HTTP_REQUEST_TIMEOUT <= 0:
        HTTP_REQUEST_TIMEOUT = 20.0
except ValueError:
    HTTP_REQUEST_TIMEOUT = 20.0

print(f"✓ Config loaded (DATA_DIR: {DATA_DIR})")
