"""
S.TO Series Scraper — powered by httpx (no browser needed).
"""

import asyncio
import json
import logging
import os
import re
import threading
import time
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from config.config import (
    EMAIL, PASSWORD, DATA_DIR, SERIES_INDEX_FILE, NUM_WORKERS,
)

logger = logging.getLogger(__name__)

# ── Constants ───────────────────────────────────────────────────────────────
SITE_URL = "https://s.to"
LOGIN_URL = f"{SITE_URL}/login"
SERIES_LIST_URL = f"{SITE_URL}/serien"
ACCOUNT_SUBSCRIBED_URL = f"{SITE_URL}/account/subscribed"
ACCOUNT_WATCHLIST_URL = f"{SITE_URL}/account/watchlist"
CHECKPOINT_EVERY = 10
REQUEST_TIMEOUT = 20.0
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"

_SERIE_PATH_RE = re.compile(r'(/serie/[^/]+)')
_SERIE_SLUG_RE = re.compile(r'^/serie/([^/?#]+)/?$')
_UTILITY_PAGES = {'alle serien', 'andere serien', 'beliebte serien',
                  'neue serien', 'empfehlung', 'meistgesehen', 'serien'}


# ── HTML helpers ────────────────────────────────────────────────────────────

def _parse_episodes(html: str) -> list[dict]:
    """Parse episode rows from a season page.

    Uses s.to-specific selectors:
      - Table: .episode-table tbody tr.episode-row
      - Number: th.episode-number-cell
      - Title (DE): .episode-title-ger
      - Title (EN): .episode-title-eng
      - Watched: 'seen' class on the row
    Falls back to generic selectors if primary ones fail.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Try s.to-specific selectors first
    rows = soup.select(".episode-table tbody tr.episode-row")
    if not rows:
        rows = soup.select("tr.episode-row")
    if not rows:
        rows = soup.select(".episode-row")
    if not rows:
        # Final fallback: generic table rows (like bs.to)
        table = soup.select_one("table.episodes")
        if table:
            rows = table.select("tr")

    if not rows:
        return []

    episodes = []
    for idx, row in enumerate(rows, start=1):
        # Extract episode number
        num_cell = row.select_one("th.episode-number-cell")
        ep_num = num_cell.get_text(strip=True) if num_cell else ""
        if not ep_num:
            # Fallback: data attribute or first <td>
            ep_num = row.get("data-episode-season-id", "")
        if not ep_num:
            cols = row.find_all("td")
            if cols:
                ep_num = cols[0].get_text(strip=True)
        if not ep_num:
            ep_num = str(idx)

        try:
            ep_num_int = int(ep_num)
        except ValueError:
            ep_num_int = idx

        # Extract titles (German and English)
        ger_cell = row.select_one(".episode-title-ger")
        eng_cell = row.select_one(".episode-title-eng")
        title_ger = ger_cell.get_text(strip=True) if ger_cell else ""
        title_eng = eng_cell.get_text(strip=True) if eng_cell else ""

        # Fallback: generic title from <strong> in second column (bs.to style)
        title = ""
        if not title_ger and not title_eng:
            cols = row.find_all("td")
            if len(cols) >= 2:
                title_tag = cols[1].find("strong")
                title = title_tag.get_text(strip=True) if title_tag else cols[1].get_text(strip=True)

        # Check if watched — s.to uses 'seen' class, bs.to uses 'watched' class
        row_classes = row.get("class") or []
        watched = "seen" in row_classes or "watched" in row_classes

        ep = {"number": ep_num_int, "watched": watched}
        if title_ger:
            ep["title_ger"] = title_ger
        if title_eng:
            ep["title_eng"] = title_eng
        if title and not title_ger and not title_eng:
            ep["title"] = title
        episodes.append(ep)
    return episodes


def _extract_season_links(html: str, series_slug: str) -> list[tuple[str, str]]:
    """Extract season numbers and URLs from the #season-nav element.

    Uses data-season-pill attributes first, then falls back to href patterns.
    Handles season 0 (Filme/OVAs/Specials) correctly.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Primary: data-season-pill attributes from #season-nav
    seasons = []
    seen = set()
    for link in soup.select("#season-nav a[data-season-pill]"):
        season_num = link.get("data-season-pill", "")
        if season_num is not None and season_num != "" and season_num not in seen:
            seen.add(season_num)
            url = f"{SITE_URL}/serie/{series_slug}/staffel-{season_num}"
            seasons.append((season_num, url))

    if seasons:
        return seasons

    # Fallback: href pattern /serie/{slug}/staffel-{num}
    staffel_pattern = re.compile(
        rf'/serie/{re.escape(series_slug)}/staffel-(\d+)', re.IGNORECASE
    )
    for a_tag in soup.find_all("a", href=True):
        m = staffel_pattern.search(a_tag["href"])
        if m and m.group(1) not in seen:
            seen.add(m.group(1))
            url = f"{SITE_URL}/serie/{series_slug}/staffel-{m.group(1)}"
            seasons.append((m.group(1), url))

    if seasons:
        return seasons

    # bs.to-style fallback: #seasons a
    links = []
    for a in soup.select("#seasons a"):
        label = a.get_text(strip=True)
        href = a.get("href", "")
        if not label or not href:
            continue
        href = href.split("?")[0].split("#")[0]
        if href.startswith("http"):
            url = href
        elif href.startswith("serie/"):
            url = f"{SITE_URL}/{href}"
        else:
            url = f"{SITE_URL}/serie/{series_slug}/{href}" if not href.startswith("/") else f"{SITE_URL}{href}"
        key = (label, url)
        if key not in seen:
            seen.add(key)
            links.append((label, url))
    return links


def _extract_title(html: str) -> str | None:
    """Extract series title from the page.

    Tries h1.fw-bold first (s.to), then h2 (bs.to fallback).
    Strips trailing 'Staffel N' suffixes.
    """
    soup = BeautifulSoup(html, "html.parser")
    # s.to: h1.fw-bold
    h1 = soup.select_one("h1.fw-bold")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text
    # bs.to fallback: h2
    h2 = soup.find("h2")
    if h2:
        text = h2.get_text(strip=True)
        text = re.sub(r'\s*Staffel\s*\d+.*$', '', text)
        return text or None
    return None


def _detect_subscription_status(html: str) -> tuple[bool | None, bool | None]:
    """Detect subscription and watchlist status from a series page.

    Looks for .js-action-btn buttons with data-type 'favorite' (subscribed)
    and 'watchlater' (watchlist). Active state is indicated by
    'btn-glass-primary' class or data-active="1".

    Returns (subscribed, watchlist) — None if button not found.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Verify logged-in state
    if not soup.select_one("form[action='/logout']"):
        return (None, None)

    # Prefer desktop-only container to avoid duplicate mobile buttons
    buttons = soup.select(".d-none.d-md-flex .js-action-btn") or soup.select(".js-action-btn")
    if not buttons:
        return (None, None)

    subscribed = None
    watchlist = None
    for button in buttons:
        data_type = button.get("data-type", "")
        is_active = "btn-glass-primary" in (button.get("class") or []) or \
                    button.get("data-active") == "1"
        if data_type == "favorite":
            subscribed = bool(is_active)
        elif data_type == "watchlater":
            watchlist = bool(is_active)

    return (subscribed, watchlist)


# ── Exception ───────────────────────────────────────────────────────────────

class ScrapingPaused(Exception):
    pass


# ── SToScraper (httpx) ─────────────────────────────────────────────────────

class SToScraper:
    """S.TO series scraper powered by httpx (no browser needed)."""

    def __init__(self):
        self.series_data: list[dict] = []
        self.all_discovered_series: list[dict] | None = None
        self.completed_links: set[str] = set()
        self.failed_links: list[dict] = []

        self.checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(DATA_DIR, '.failed_series.json')
        self.ignore_file = os.path.join(DATA_DIR, '.ignored_series.json')
        self.ignored_seasons_file = os.path.join(DATA_DIR, '.ignored_seasons.json')
        self.pause_file = os.path.join(DATA_DIR, '.pause_scraping')

        self._checkpoint_mode: str | None = None
        self._use_parallel: bool = True
        self._lock = threading.Lock()
        self._last_pause_check = 0.0
        self._pause_cached = False
        self.paused = False
        self._ignored_seasons_cache: set[tuple[str, str]] | None = None
        self._stale_ignored_warnings: list[dict] = []

    # ── Static / class methods ──────────────────────────────────────────────

    @staticmethod
    def get_checkpoint_mode(data_dir):
        cp_file = os.path.join(data_dir, '.scrape_checkpoint.json')
        try:
            if os.path.exists(cp_file):
                with open(cp_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data.get('mode')
        except Exception:
            pass
        return None

    # ── Checkpoint management ───────────────────────────────────────────────

    def save_checkpoint(self, include_data=False):
        with self._lock:
            payload = {
                'completed_links': list(self.completed_links),
                'mode': self._checkpoint_mode,
                'timestamp': time.time(),
            }
            if include_data:
                payload['series_data'] = self.series_data
            tmp = self.checkpoint_file + '.tmp'
            try:
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(payload, f, ensure_ascii=False)
                os.replace(tmp, self.checkpoint_file)
            except Exception as e:
                logger.error(f"Failed to save checkpoint: {e}")

    def load_checkpoint(self) -> bool:
        with self._lock:
            try:
                if not os.path.exists(self.checkpoint_file):
                    return False
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.completed_links = set(data.get('completed_links', []))
                    self._checkpoint_mode = data.get('mode')
                    saved_data = data.get('series_data')
                    if saved_data:
                        self.series_data = saved_data
                elif isinstance(data, list):
                    self.completed_links = set(data)
                return bool(self.completed_links)
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
                return False

    def clear_checkpoint(self):
        with self._lock:
            try:
                if os.path.exists(self.checkpoint_file):
                    os.remove(self.checkpoint_file)
            except OSError:
                pass

    # ── Failed series management ────────────────────────────────────────────

    def save_failed_series(self):
        with self._lock:
            existing = []
            try:
                if os.path.exists(self.failed_file):
                    with open(self.failed_file, 'r', encoding='utf-8') as f:
                        existing = json.load(f)
            except Exception:
                pass
            seen = {e.get('url') for e in existing if isinstance(e, dict)}
            for f in self.failed_links:
                if isinstance(f, dict) and f.get('url') not in seen:
                    existing.append(f)
                    seen.add(f.get('url'))
            tmp = self.failed_file + '.tmp'
            try:
                with open(tmp, 'w', encoding='utf-8') as f_out:
                    json.dump(existing, f_out, indent=2, ensure_ascii=False)
                os.replace(tmp, self.failed_file)
            except Exception as e:
                logger.error(f"Failed to save failed series: {e}")

    def load_failed_series(self) -> list:
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    with open(self.failed_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:
                pass
            return []

    def clear_failed_series(self):
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    os.remove(self.failed_file)
            except OSError:
                pass

    # ── Ignore list management ──────────────────────────────────────────────

    def load_ignored_series(self) -> list[dict]:
        try:
            if os.path.exists(self.ignore_file):
                with open(self.ignore_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def save_ignored_series(self, ignored: list[dict]):
        tmp = self.ignore_file + '.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(ignored, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.ignore_file)
        except Exception as e:
            logger.error(f"Failed to save ignored series: {e}")

    def get_ignored_slugs(self) -> set[str]:
        return {self.get_series_slug_from_url(s.get('url', '')) for s in self.load_ignored_series()} - {'unknown'}

    # ── Ignored seasons management ──────────────────────────────────────────

    def load_ignored_seasons(self) -> list[dict]:
        try:
            if os.path.exists(self.ignored_seasons_file):
                with open(self.ignored_seasons_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def get_ignored_seasons_set(self) -> set[tuple[str, str]]:
        """Return set of (slug, season) tuples that have episode 0 ignored."""
        return {(e.get('slug', ''), str(e.get('season', ''))) for e in self.load_ignored_seasons()} - {('', '')}

    def _get_ignored_seasons(self) -> set[tuple[str, str]]:
        """Cached version — loads from file once per run."""
        if self._ignored_seasons_cache is None:
            self._ignored_seasons_cache = self.get_ignored_seasons_set()
        return self._ignored_seasons_cache

    # ── URL helpers ─────────────────────────────────────────────────────────

    def get_series_slug_from_url(self, url):
        try:
            path = urlparse(url).path if url.startswith('http') else url
            parts = path.split('/')
            if 'serie' in parts:
                idx = parts.index('serie')
                if idx + 1 < len(parts) and parts[idx + 1]:
                    return parts[idx + 1]
            return 'unknown'
        except Exception:
            return 'unknown'

    def normalize_to_series_url(self, url):
        if not url:
            return url
        url = url.split('?')[0].split('#')[0]
        m = _SERIE_PATH_RE.search(url)
        if m:
            return f"{SITE_URL}{m.group(1)}"
        return url

    # ── Pause detection ─────────────────────────────────────────────────────

    def _check_pause(self):
        now = time.time()
        if now - self._last_pause_check < 5:
            return self._pause_cached
        self._last_pause_check = now
        self._pause_cached = os.path.exists(self.pause_file)
        return self._pause_cached

    def _clear_pause_file(self):
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
        except OSError:
            pass

    # ── Index helpers (for new_only mode) ───────────────────────────────────

    def load_existing_slugs(self) -> set[str]:
        existing = set()
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                items = data if isinstance(data, list) else list(data.values())
                for item in items:
                    url = item.get('url', '') or item.get('link', '')
                    if url:
                        existing.add(self.get_series_slug_from_url(url))
        except Exception:
            pass
        existing.discard('unknown')
        return existing

    # ── Async internals ─────────────────────────────────────────────────────

    async def _create_logged_in_client(self) -> httpx.AsyncClient:
        client = httpx.AsyncClient(
            headers={"User-Agent": UA},
            timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=10.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
        )
        # Get login page to retrieve CSRF token
        resp = await client.get(LOGIN_URL)
        soup = BeautifulSoup(resp.text, "html.parser")

        # s.to uses a hidden _token field (Laravel CSRF) or security_token
        token = ""
        for name in ("_token", "security_token"):
            token_input = soup.find("input", {"name": name})
            if token_input and token_input.get("value"):
                token = token_input["value"]
                break

        # s.to login uses email + password
        login_data = {
            "email": EMAIL,
            "password": PASSWORD,
        }
        if token:
            login_data["_token"] = token

        login_resp = await client.post(LOGIN_URL, data=login_data, follow_redirects=True)

        if "logout" not in login_resp.text.lower():
            await client.aclose()
            raise RuntimeError("Login failed — check credentials")

        return client

    async def _get_all_series(self, client: httpx.AsyncClient) -> list[dict]:
        """Fetch the full series catalogue from s.to/serien."""
        resp = await client.get(SERIES_LIST_URL)
        soup = BeautifulSoup(resp.text, "html.parser")
        series, seen_slugs = [], set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = _SERIE_SLUG_RE.match(href)
            if not m:
                # Also try with leading domain stripped
                if href.startswith("serie/"):
                    parts = href.split("/")
                    slug = parts[1] if len(parts) > 1 and parts[1] else None
                else:
                    continue
            else:
                slug = m.group(1)
            if not slug:
                continue
            title = a.get_text(strip=True)
            if not title or title.lower().strip() in _UTILITY_PAGES:
                continue
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            series.append({
                "title": title,
                "link": f"/serie/{slug}",
                "url": f"{SITE_URL}/serie/{slug}",
            })
        return series

    async def _get_account_series(self, client: httpx.AsyncClient,
                                   source: str = 'both') -> list[dict]:
        """Fetch subscribed/watchlist series from account pages.

        Args:
            source: 'subscribed', 'watchlist', or 'both'

        Returns list of series dicts with title, link, url keys.
        """
        pages = []
        if source in ('subscribed', 'both'):
            pages.append((ACCOUNT_SUBSCRIBED_URL, 'Subscriptions'))
        if source in ('watchlist', 'both'):
            pages.append((ACCOUNT_WATCHLIST_URL, 'Watchlist'))

        seen_slugs = set()
        series_list = []
        count_before = 0

        for base_url, label in pages:
            count_before = len(series_list)
            page_num = 1
            while True:
                url = base_url if page_num == 1 else f"{base_url}?page={page_num}"
                try:
                    resp = await client.get(url, follow_redirects=True)
                except httpx.HTTPError as e:
                    logger.warning(f"Could not fetch {url}: {e}")
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                page_found = 0
                for link in soup.find_all("a", href=True):
                    href = link.get("href", "")
                    m = _SERIE_SLUG_RE.match(href)
                    if not m:
                        continue
                    slug = m.group(1)
                    if slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)
                    title = link.get_text(strip=True) or slug
                    series_list.append({
                        "title": title,
                        "link": f"/serie/{slug}",
                        "url": f"{SITE_URL}/serie/{slug}",
                    })
                    page_found += 1

                # Check for pagination
                has_next = False
                pagination = soup.find("ul", class_="pagination")
                if pagination:
                    next_link = pagination.find("a", attrs={"rel": "next"})
                    if next_link:
                        has_next = True

                if has_next:
                    page_num += 1
                else:
                    break

            count_after = len(series_list)
            print(f"  ✓ {label}: {count_after - count_before} series found")

        return series_list

    async def _scrape_one_series(self, client: httpx.AsyncClient, info: dict) -> dict:
        """Scrape a single series: all seasons, episodes, subscription status."""
        url = info["url"]
        slug = self.get_series_slug_from_url(url)

        try:
            resp = await client.get(url, follow_redirects=True)
        except httpx.HTTPError as e:
            return self._error_result(info, str(e))

        html = resp.text
        title = _extract_title(html) or info.get("title", slug)
        if title.lower().strip() in _UTILITY_PAGES:
            return self._error_result(info, "utility page")

        # Detect subscription/watchlist status from the main series page
        subscribed, watchlist = _detect_subscription_status(html)

        season_links = _extract_season_links(html, slug)
        if not season_links:
            # Default to season 1 if no nav found
            season_links = [("1", url)]

        seasons_data = []
        total_watched, total_eps = 0, 0
        ignored_seasons = self._get_ignored_seasons()
        has_episode_zero = False
        stale_ignored = []

        for label, season_url in season_links:
            try:
                sr = await client.get(season_url, follow_redirects=True)
            except httpx.HTTPError:
                continue
            episodes = _parse_episodes(sr.text)

            ep0_exists = any(ep["number"] == 0 for ep in episodes)
            is_ignored = (slug, label) in ignored_seasons

            if ep0_exists and is_ignored:
                # Already in ignored list — silently filter out episode 0
                episodes = [ep for ep in episodes if ep["number"] != 0]
            elif ep0_exists and not is_ignored:
                # New episode 0 — flag for warning + failed_links
                has_episode_zero = True
            elif not ep0_exists and is_ignored:
                # Stale: episode 0 no longer exists but still in ignored list
                stale_ignored.append({"slug": slug, "season": label})

            watched_count = sum(1 for ep in episodes if ep["watched"])
            total_count = len(episodes)
            season_entry = {
                "season": label,
                "url": season_url,
                "episodes": episodes,
                "watched_episodes": watched_count,
                "total_episodes": total_count,
            }
            if ep0_exists and is_ignored:
                season_entry["ignored_episode_0"] = True
            seasons_data.append(season_entry)
            total_watched += watched_count
            total_eps += total_count

        result = {
            "title": title,
            "link": info["link"],
            "url": info["url"],
            "total_seasons": len(seasons_data),
            "total_episodes": total_eps,
            "watched_episodes": total_watched,
            "unwatched_episodes": max(0, total_eps - total_watched),
            "subscribed": subscribed,
            "watchlist": watchlist,
            "seasons": seasons_data,
        }
        if has_episode_zero:
            result["_has_episode_zero"] = True
        if stale_ignored:
            result["_stale_ignored_seasons"] = stale_ignored
        return result

    @staticmethod
    def _error_result(info: dict, reason: str) -> dict:
        return {
            "title": f"[ERROR: {reason}]",
            "link": info.get("link", ""),
            "url": info.get("url", ""),
            "total_seasons": 0,
            "total_episodes": 0,
            "watched_episodes": 0,
            "unwatched_episodes": 0,
            "subscribed": None,
            "watchlist": None,
            "seasons": [],
        }

    # ── Worker ──────────────────────────────────────────────────────────────

    async def _worker(self, worker_id: int, queue: asyncio.Queue,
                      results: list, progress: dict, total: int):
        try:
            client = await self._create_logged_in_client()
        except RuntimeError:
            logger.warning(f"Worker {worker_id} login failed, retrying...")
            await asyncio.sleep(1)
            try:
                client = await self._create_logged_in_client()
            except RuntimeError:
                logger.error(f"Worker {worker_id} login failed permanently")
                return

        try:
            while True:
                if self._check_pause():
                    raise ScrapingPaused("Pause file detected")

                try:
                    info = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                result = await self._scrape_one_series(client, info)

                if result["title"].startswith("[ERROR"):
                    self.failed_links.append({
                        "url": info["url"],
                        "title": info.get("title", ""),
                        "link": info.get("link", ""),
                        "reason": "scrape_error",
                    })
                elif result.get("total_episodes", 0) == 0:
                    results.append(result)
                    self.failed_links.append({
                        "url": info["url"],
                        "title": result.get("title", info.get("title", "")),
                        "link": info.get("link", ""),
                        "reason": "zero_episodes",
                    })
                else:
                    results.append(result)
                    if result.get("_has_episode_zero"):
                        self.failed_links.append({
                            "url": info["url"],
                            "title": result.get("title", info.get("title", "")),
                            "link": info.get("link", ""),
                            "reason": "episode_0_placeholder",
                        })
                    if result.get("_stale_ignored_seasons"):
                        with self._lock:
                            for entry in result["_stale_ignored_seasons"]:
                                self._stale_ignored_warnings.append({
                                    "title": result.get("title", ""),
                                    "slug": entry["slug"],
                                    "season": entry["season"],
                                })

                link = info.get("link", "")
                if link:
                    self.completed_links.add(link)

                progress["done"] += 1
                done = progress["done"]

                # Progress bar + ETA
                elapsed = time.perf_counter() - progress["start"]
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                eta_mins = f"{eta / 60:.1f}"
                pct = int((done / total) * 100)
                bar_len = 30
                filled = int(bar_len * done / total)
                bar = '█' * filled + '░' * (bar_len - filled)

                season_labels = [s.get('season', '?') for s in result.get('seasons', [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""

                # Subscription status indicators
                sub_parts = []
                if result.get("subscribed") is not None:
                    sub_parts.append(f"Sub:{'✓' if result['subscribed'] else '✗'}")
                if result.get("watchlist") is not None:
                    sub_parts.append(f"WL:{'✓' if result['watchlist'] else '✗'}")
                sub_info = f" ({' '.join(sub_parts)})" if sub_parts else ""

                ep0_warn = " ⚠ episode 0 detected" if result.get("_has_episode_zero") else ""

                if result["title"].startswith("[ERROR"):
                    print(f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m | ⚠ {info.get('title', '?')}: Failed")
                elif result["total_episodes"] == 0:
                    print(f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m | ⚠ {result['title']}{season_info}: No episodes{sub_info}")
                else:
                    print(f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m | ✓ {result['title']}{season_info}: {result['watched_episodes']}/{result['total_episodes']} watched{sub_info}{ep0_warn}")

                if done % CHECKPOINT_EVERY == 0:
                    self.series_data = list(results)
                    self.save_checkpoint(include_data=True)
        finally:
            await client.aclose()

    # ── Async scrape orchestrators ──────────────────────────────────────────

    def _filter_completed(self, series_list: list[dict]) -> list[dict] | None:
        if not self.completed_links:
            return series_list
        before = len(series_list)
        filtered = [s for s in series_list if s.get('link') not in self.completed_links]
        if before != len(filtered):
            print(f"  Skipping {before - len(filtered)} already-completed series")
        if not filtered:
            print("✓ All series already scraped (from checkpoint)")
            return None
        return filtered

    async def _scrape_list(self, series_list: list[dict], num_workers: int | None = None):
        """Scrape a list of series using multi-session workers."""
        filtered = self._filter_completed(series_list)
        if filtered is None:
            return

        queue: asyncio.Queue = asyncio.Queue()
        for s in filtered:
            queue.put_nowait(s)

        results: list[dict] = list(self.series_data)  # keep checkpoint data
        n = min(num_workers or NUM_WORKERS, len(filtered))
        progress = {"done": 0, "start": time.perf_counter()}

        print(f"→ Scraping {len(filtered)} series with {n} session(s)...")

        tasks = [
            self._worker(i, queue, results, progress, len(filtered))
            for i in range(n)
        ]
        await asyncio.gather(*tasks)

        self.series_data = results

    def _ignored_seasons_continue(self) -> bool:
        """After scraping ignored-season series, check for changes and prompt.

        Returns True to continue scraping, False to stop.
        """
        has_stale = bool(self._stale_ignored_warnings)
        has_new_ep0 = any(
            f.get("reason") == "episode_0_placeholder" for f in self.failed_links
        )

        if not has_stale and not has_new_ep0:
            print("✓ Ignored seasons: all OK")
            return True

        if has_stale:
            print(f"\n⚠ Episode 0 no longer exists for these ignored seasons — consider removing from .ignored_seasons.json:")
            for w in self._stale_ignored_warnings:
                print(f"  • {w['title']} (season {w['season']}, slug: {w['slug']})")

        if has_new_ep0:
            new_ep0 = [f for f in self.failed_links if f.get("reason") == "episode_0_placeholder"]
            print(f"\n⚠ New episode 0 detected in {len(new_ep0)} series (added to .failed_series.json):")
            for f in new_ep0:
                print(f"  • {f.get('title', f.get('url', '?'))}")

        answer = input("\nContinue scraping remaining series? (y/n): ").strip().lower()
        if answer != 'y':
            print("✗ Scraping stopped. Saving progress...")
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            return False
        return True

    async def _async_run(self, single_url=None, url_list=None,
                         new_only=False, retry_failed=False,
                         account_source=None):
        """Async core of run()."""
        # Use a temp client for discovery, then close it
        tmp = await self._create_logged_in_client()
        try:
            print("✓ Logged in to s.to")
            await self._async_run_inner(tmp, single_url=single_url,
                                        url_list=url_list, new_only=new_only,
                                        retry_failed=retry_failed,
                                        account_source=account_source)
        finally:
            if not tmp.is_closed:
                await tmp.aclose()

    async def _async_run_inner(self, tmp, single_url=None, url_list=None,
                               new_only=False, retry_failed=False,
                               account_source=None):

        if single_url:
            self._checkpoint_mode = 'single'
            main_url = self.normalize_to_series_url(single_url)
            m = _SERIE_PATH_RE.search(main_url)
            link = m.group(1) if m else main_url
            info = {"title": main_url.split("/")[-1], "link": link, "url": main_url}
            print(f"→ Scraping single series: {main_url}")
            result = await self._scrape_one_series(tmp, info)
            await tmp.aclose()
            if result["title"].startswith("[ERROR"):
                self.failed_links.append(info)
            self.series_data = [result]
            return

        if url_list:
            self._checkpoint_mode = 'batch'
            series_list = []
            for u in url_list:
                main_url = self.normalize_to_series_url(u)
                m = _SERIE_PATH_RE.search(main_url)
                link = m.group(1) if m else main_url
                series_list.append({"title": main_url.split("/")[-1], "link": link, "url": main_url})
            await tmp.aclose()
            n = NUM_WORKERS if self._use_parallel and len(series_list) > 1 else 1
            await self._scrape_list(series_list, num_workers=n)
            print(f"  Successfully scraped: {len(self.series_data)}/{len(url_list)} series")
            return

        if retry_failed:
            self._checkpoint_mode = 'retry'
            failed_list = self.load_failed_series()
            await tmp.aclose()
            if not failed_list:
                print("✓ No failed series found")
                return
            print(f"✓ Found {len(failed_list)} failed series — retrying in sequential mode")
            await self._scrape_list(failed_list, num_workers=1)
            return

        if account_source:
            self._checkpoint_mode = account_source
            print(f"→ Fetching {account_source} series from account pages...")
            account_series = await self._get_account_series(tmp, source=account_source)
            await tmp.aclose()
            if not account_series:
                print("✓ No series found on account pages")
                return

            # New series detection
            existing_slugs = self.load_existing_slugs()
            new_titles = [s["title"] for s in account_series
                          if self.get_series_slug_from_url(s.get('link', '')) not in existing_slugs]
            if new_titles:
                print(f"\nℹ {len(new_titles)} new series detected:")
                for t in new_titles:
                    print(f"  + {t}")
                print()

            # Two-phase scraping: ignored-season series first
            ignored_slugs_set = {slug for slug, _ in self._get_ignored_seasons()}
            ignored_batch = [s for s in account_series
                            if self.get_series_slug_from_url(s.get('link', '')) in ignored_slugs_set]
            rest_batch = [s for s in account_series
                         if self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs_set]

            if ignored_batch:
                print(f"→ Phase 1: Scraping {len(ignored_batch)} series with ignored seasons...")
                await self._scrape_list(ignored_batch, num_workers=1)
                if not self._ignored_seasons_continue():
                    return

            print(f"→ Found {len(rest_batch)} remaining series — scraping...")
            n = NUM_WORKERS if self._use_parallel else 1
            await self._scrape_list(rest_batch, num_workers=n)
            return

        if new_only:
            self._checkpoint_mode = 'new_only'
            print("→ Fetching series list...")
            all_series = await self._get_all_series(tmp)
            await tmp.aclose()
            self.all_discovered_series = all_series
            existing_slugs = self.load_existing_slugs()
            ignored_slugs = self.get_ignored_slugs()
            new_list = [s for s in all_series
                        if self.get_series_slug_from_url(s.get('link', '')) not in existing_slugs
                        and self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs]
            print(f"→ New series to scrape: {len(new_list)} (out of {len(all_series)})")
            if not new_list:
                print("✓ No new series detected — nothing to scrape")
                return
            if len(new_list) <= 50:
                for s in new_list:
                    print(f"  + {s['title']}")
            await self._scrape_list(new_list, num_workers=1)
            return

        # Default: scrape all
        self._checkpoint_mode = 'all_series'
        print("→ Fetching series list...")
        all_series = await self._get_all_series(tmp)
        await tmp.aclose()
        self.all_discovered_series = all_series
        ignored_slugs = self.get_ignored_slugs()
        print(f"✓ Found {len(all_series)} series")

        # New series detection
        existing_slugs = self.load_existing_slugs()
        new_titles = [s["title"] for s in all_series
                      if self.get_series_slug_from_url(s.get('link', '')) not in existing_slugs
                      and self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs]
        if new_titles:
            print(f"\nℹ {len(new_titles)} new series detected:")
            for t in new_titles:
                print(f"  + {t}")
            print()

        if ignored_slugs:
            all_series = [s for s in all_series
                          if self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs]
            skipped = len(self.all_discovered_series) - len(all_series)
            if skipped:
                print(f"  Skipping {skipped} ignored series")

        # Two-phase scraping: ignored-season series first
        ignored_slugs_set = {slug for slug, _ in self._get_ignored_seasons()}
        ignored_batch = [s for s in all_series
                        if self.get_series_slug_from_url(s.get('link', '')) in ignored_slugs_set]
        rest_batch = [s for s in all_series
                     if self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs_set]

        if ignored_batch:
            print(f"→ Phase 1: Scraping {len(ignored_batch)} series with ignored seasons...")
            await self._scrape_list(ignored_batch, num_workers=1)
            if not self._ignored_seasons_continue():
                return

        n = NUM_WORKERS if self._use_parallel else 1
        await self._scrape_list(rest_batch, num_workers=n)
        print(f"\n✓ Successfully scraped {len(self.series_data)} series")

    # ── Public API ───────────────────────────────────────────────────────────

    def run(self, single_url=None, url_list=None, new_only=False,
            resume_only=False, retry_failed=False, parallel=None,
            account_source=None):
        """Main entry point: login, scrape, save checkpoint."""
        if parallel is not None:
            self._use_parallel = parallel
            print(f"→ Using {'multi-session' if parallel else 'single-session'} mode")
        else:
            self._use_parallel = True

        # Clear any stale pause file from a previous run
        self._clear_pause_file()

        try:
            if resume_only:
                if self.load_checkpoint():
                    print(f"→ Resuming from checkpoint ({len(self.completed_links)} series already done)")
                else:
                    print("⚠ No checkpoint found. Starting fresh...")

            asyncio.run(self._async_run(
                single_url=single_url,
                url_list=url_list,
                new_only=new_only,
                retry_failed=retry_failed,
                account_source=account_source,
            ))

            # Alert for empty series (0 episodes)
            empty = [s for s in self.series_data if s.get('total_episodes', 0) == 0]
            if empty:
                print(f"\n⚠ {len(empty)} series with 0 episodes:")
                for s in empty:
                    print(f"  • {s['title']} → {s['url']}")

            self.save_checkpoint(include_data=True)
            if not self.failed_links:
                self.clear_failed_series()
            else:
                self.save_failed_series()

        except ScrapingPaused:
            self.paused = True
            self._clear_pause_file()
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
        except BaseException:
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            raise


