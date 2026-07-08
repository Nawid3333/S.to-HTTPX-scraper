# S.TO Series Scraper & Index Manager (httpx)

Scrapes watched TV series from **s.to** and maintains a local JSON index.
Uses **httpx** (no browser needed) with a multi-session architecture for fast, parallel scraping.

## Features

- **Multi-session parallel scraping** — 10 concurrent httpx sessions by default (configurable in `config/config.py`)
- **Host probing** — checks all configured hosts before scraping, compares site series count with the local index, and writes a `mismatch_report.json` when differences or duplicate slugs are detected
- **Duplicate slug detection** — finds duplicate slugs in the index and offers to delete them before continuing
- **Smart per-series ETA estimation** — each series stores its own `avg_scrape_seconds` (exponential moving average for ETA prediction) and `scrape_duration_seconds` (actual duration of the most recent scrape) in the index. ETA is predicted by summing those per-series averages for the remaining work, then blended with the live session rate (historical 85%→45% as progress increases). Because the database is stable, per-series history is the best predictor.
- **Checkpoint & resume** — automatically saves progress every 10 series; resume after interruptions (Ctrl+C safe)
- **Subscription & watchlist tracking** — scrape series from your s.to account subscriptions/watchlist and track status per series
- **New series detection** — detects newly added series on your account and lists them before scraping
- **Vanished series detection** — alerts when series disappear from your account
- **Bilingual episode titles** — stores both German and English titles per episode
- **Alternative titles** — extracts alternative titles from series pages
- **Series descriptions** — persists description text for each series
- **Ignored seasons** — automatically skips placeholder seasons (e.g. episode 0 only) via `.ignored_seasons.json`; three-way handling (silent filter, new detection with notification, stale detection)
- **Two-phase scraping** — when ignored-season series are present, scrapes them first and prompts if changes are detected before continuing
- **Ignored series** — skip specific series via `.ignored_series.json`
- **Completed series alerts** — warns about fully watched series not subscribed, and ongoing series not on watchlist
- **Batch URL import** — import series from a text file (comments supported)
- **Failed series retry** — automatically tracks failures for later bulk retry
- **Pause/resume** — create a `.pause_scraping` file to gracefully pause workers
- **Report generation** — full or filtered (subscribed/watchlist) statistics with export
- **Data integrity checks** — detects episode count drops, season removals, watched-status corruption, and title changes before merging; offers to delete & rescrape critical series
- **Atomic file writes** — all JSON writes use temp file + replace to prevent corruption
- **File locking** — prevents concurrent access corruption
- **Disk space check** — warns before scraping if free space is below 100 MB
- **Rotating log files** — 10 MB per file, 5 backups

## Requirements

- Python 3.8+
- Dependencies: `httpx`, `beautifulsoup4`, `python-dotenv`

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file inside the `config/` directory (see `config/.env.example`):

```
STO_EMAIL=your@email.com
STO_PASSWORD=yourpassword
```

`.env` is used **only for credentials**. All other settings (site URLs, fallback domains, workers, timeout, batch file paths) live in `config/config.py`.

The default batch file is `series_urls.txt` next to `main.py`. To change it, edit `DEFAULT_BATCH_FILE_PATH` in `config/config.py`.

Site URL and fallback domains are also defined in `config/config.py`:

```python
SITE_URL = "https://s.to"
STO_FALLBACK_SITE_URL = "https://serienstream.to"
```

Scraping parallelism and request timeout can be adjusted in `config/config.py`:

```python
NUM_WORKERS = 10            # Number of parallel httpx sessions
HTTP_REQUEST_TIMEOUT = 20.0  # Seconds per request
```

## Usage

```bash
python main.py
```

### Menu Options

| #   | Option                          | Description                                                                |
| --- | ------------------------------- | -------------------------------------------------------------------------- |
| 1   | **Scrape all series**           | Full scrape of all watched series. Choose single-session or multi-session. |
| 2   | **Scrape only NEW series**      | Scrapes only series not yet in the index (faster).                         |
| 3   | **Scrape unwatched series**     | Skips fully watched series; focuses on ongoing/partial.                    |
| 4   | **Generate report**             | Full or filtered report (subscribed / watchlist / both). Saves to JSON.    |
| 5   | **Single link / batch add**     | Add a single series by URL, or batch-import from a text file.              |
| 6   | **Scrape subscribed/watchlist** | Fetch series from your s.to subscription or watchlist pages.               |
| 7   | **Retry failed scrapes**        | Bulk retry all series that failed in previous runs.                        |
| 8   | **Pause scraping**              | Creates `.pause_scraping` flag file for graceful worker pause.             |
| 9   | **Exit**                        | Clean exit.                                                                |

### Scraping Modes (Option 1)

1. **Single session** — one httpx client, sequential (most reliable)
2. **Multi-session** — parallel workers (default, faster)

### Batch File Format (Option 5)

One URL per line. Lines starting with `#` are treated as comments:

```
# Action series
https://s.to/serie/Breaking-Bad
https://s.to/serie/Better-Call-Saul
```

### Reports (Option 4)

Reports include:

- Total series, completed, ongoing, not started counts
- Total/watched/unwatched episode counts
- Average completion percentage and distribution
- Subscription and watchlist counts
- Most/least completed series lists
- Ignored episode-0 season count

Filter options:

- Full report (all series)
- Subscribed only
- Watchlist only
- Both subscribed and watchlist

After report generation, you can export ongoing series URLs back to the default batch file (`series_urls.txt` by default).

## Episode 0 / Ignored Seasons

Some s.to series have "episode 0" entries that are placeholders with no watch links. These cause series to appear incomplete.

The file `data/.ignored_seasons.json` lists known seasons with this issue:

```json
[
  { "slug": "unicorn-warriors-eternal", "season": "1" },
  { "slug": "goofy-und-max", "season": "1" }
]
```

Only the `slug` and `season` fields are used for matching; `url`/`link` are optional and informational. Slugs are normalized to lowercase internally, so `Spider-Noir` and `spider-noir` match the same series.

**Three-way behavior during scraping:**

| Scenario                                            | Behavior                                                                      |
| --------------------------------------------------- | ----------------------------------------------------------------------------- |
| Season is in ignore list                            | Episode 0 silently filtered; season marked `ignored_episode_0: true` in index |
| New episode 0 detected (not in list)                | Warning printed; added to `failed_links` for review                           |
| Season in ignore list but episode 0 is gone (stale) | Notification before rest of scrape; prompt to continue                        |

When ignored-season series are found in a scrape, they are processed first (two-phase). If any new or stale entries are detected, the scraper prompts before continuing with remaining series.

## Project Structure

```
├── main.py                     # Entry point & interactive menu
├── requirements.txt
├── series_urls.txt             # Optional batch URL file
├── config/
│   ├── config.py               # Settings (credentials, site URLs, workers, timeout, paths)
│   └── .env                    # Credentials (not committed)
├── data/
│   ├── series_index.json       # Main series database
│   ├── series_index.json.bak*  # 3 backup generations (auto-managed)
│   ├── series_report.json      # Generated report
│   ├── mismatch_report.json    # Created when index/site differences are detected
│   ├── .ignored_seasons.json   # Episode 0 ignore list
│   ├── .ignored_series.json    # Series to skip during scraping
│   ├── .scrape_checkpoint.json # Resume checkpoint (auto-managed)
│   ├── .failed_series.json     # Failed series list (auto-managed)
│   └── .pause_scraping         # Pause flag file (auto-managed)
├── src/
│   ├── scraper.py              # SToScraper — httpx scraping engine
│   └── index_manager.py        # IndexManager — merge, stats, reports
└── logs/
    └── s_to_backup.log         # Rotating log file
```

## Author

Nawid Salehie

## License

Private project — not licensed for redistribution.
