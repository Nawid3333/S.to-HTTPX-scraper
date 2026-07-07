#!/usr/bin/env python3
# pylint: disable=broad-exception-caught,too-many-branches
"""
S.TO Series Scraper & Index Manager  (httpx)

Scrapes watched TV series from s.to and maintains a local JSON index.
Uses httpx (no browser needed) with multi-session architecture.
Supports checkpoint resume, batch URL import, subscription/watchlist tracking,
and interactive change confirmation.
"""

import asyncio
import copy
import json
import logging
import logging.handlers
import os
import re
import shutil
import sys
import time
from collections import Counter
from datetime import datetime
from urllib.parse import urlparse

# Ensure project root is on sys.path so imports work from any working directory
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config.config import (  # noqa: E402  # pylint: disable=import-error,no-name-in-module,wrong-import-position
    EMAIL, PASSWORD, DATA_DIR, SERIES_INDEX_FILE, LOG_FILE, SITE_URL, SITE_URLS,
    DEFAULT_BATCH_FILE,
)
from src.scraper import SToScraper  # noqa: E402  # pylint: disable=wrong-import-position
from src.index_manager import (  # noqa: E402  # pylint: disable=wrong-import-position
    IndexManager, confirm_and_save_changes, show_vanished_series,
    _extract_slug_from_field, get_episode_counts,
    remove_series_from_index,
)


def _extract_slug(entry):
    """Extract series slug from an index entry using link (primary) or url (fallback)."""
    slug = _extract_slug_from_field(entry.get('link', ''))
    if slug:
        return slug
    slug = _extract_slug_from_field(entry.get('url', ''))
    if slug:
        title = entry.get('title', '?')
        print(f"  ⚠ Used URL fallback for slug extraction: {title}")
        logger.warning("Used URL fallback for slug extraction: %s", title)
        return slug
    return None


# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

ACTIVE_SITE_URL = None

_SERIE_URL_RE = re.compile(r'/serie/[^/]+')

_MODE_LABELS = {
    'all_series': 'Scrape all series (option 1)',
    'new_only': 'Scrape NEW series only (option 2)',
    'unwatched': 'Scrape unwatched series (option 3)',
    'batch': 'Batch add (option 5)',
    'subscribed': 'Subscribed series (option 6)',
    'watchlist': 'Watchlist series (option 6)',
    'both': 'Subscribed+Watchlist series (option 6)',
    'retry': 'Retry failed (option 7)',
}


def print_header():
    print("" + "=" * 60)
    print("  S.TO SERIES SCRAPER & INDEX MANAGER  (httpx)")
    print("=" * 60)


def print_completed_series_alerts(index_manager=None):
    """Alert user about series that need attention:
    1. Fully watched but not subscribed
    2. Ongoing (started but incomplete) but not on watchlist
    """
    try:
        if index_manager is None:
            index_manager = IndexManager(SERIES_INDEX_FILE)

        if not index_manager.series_index:
            return

        completed_not_sub = []
        ongoing_no_wl = []

        for s in index_manager.series_index.values():
            total, watched = get_episode_counts(s)
            subscribed = s.get('subscribed', False)
            watchlist = s.get('watchlist', False)

            if total > 0 and watched == total and not subscribed:
                completed_not_sub.append(s)
            elif total > 0 and 0 < watched < total and not watchlist:
                ongoing_no_wl.append(s)

        if completed_not_sub:
            completed_not_sub.sort(key=lambda s: s.get('title', ''))
            print("\n" + "⚠"*35)
            print(f"⚠ {len(completed_not_sub)} COMPLETED SERIES — NOT SUBSCRIBED:")
            print("─" * 70)
            for s in completed_not_sub:
                print(f"  • {s.get('title')}")
            print("─" * 70)
            print("  Consider subscribing or leaving as-is.")
            print("⚠" * 35)

            rescrape = input(
                "\nRescrape these series to update Sub/WL status? (y/n): ").strip().lower()
            if rescrape == 'y':
                urls = [s.get('url')
                        for s in completed_not_sub if s.get('url')]
                if not urls:
                    print("✗ No URLs found for these series")
                else:
                    print(f"\n→ Rescraping {len(urls)} completed series...")
                    _run_scrape_and_save(
                        run_kwargs={"url_list": urls, "parallel": False},
                        description=f"Rescrape completed series ({len(urls)})",
                        success_msg=f"Rescrape completed! {len(urls)} series updated.",
                        no_data_msg="No data scraped",
                    )

        if ongoing_no_wl:
            ongoing_no_wl.sort(key=lambda s: s.get('title', ''))
            print("\n" + "⚠"*35)
            print(f"⚠ {len(ongoing_no_wl)} ONGOING SERIES — NOT ON WATCHLIST:")
            print("─" * 70)
            for s in ongoing_no_wl:
                print(f"  • {s.get('title')}")
            print("─" * 70)
            print("  Consider adding them to your watchlist.")
            print("⚠" * 35)

    except Exception as e:
        logger.error("Error printing series alerts: %s", e)


def check_disk_space(min_mb=100):
    """Check if enough disk space is available."""
    try:
        stat = shutil.disk_usage(DATA_DIR)
        available_mb = stat.free / (1024 * 1024)
        if available_mb < min_mb:
            print("\n✗ WARNING: Low disk space!")
            print(
                f"  Available: {available_mb:.1f} MB (minimum needed: {min_mb} MB)")
            print("  Please free up disk space before scraping.\n")
            return False
        return True
    except Exception as e:
        logger.warning("Could not check disk space: %s", e)
        return True


def validate_credentials():
    if not (EMAIL and PASSWORD):
        print("\n✗ ERROR: Credentials not configured!")
        print("\nPlease follow these steps:")
        print("1. Copy '.env.example' to '.env' inside the config/ folder")
        print("2. Add your s.to email and password to the .env file")
        print("3. Save the file and try again\n")
        return False
    return True


def show_menu():  # pylint: disable=too-many-branches
    print("\nOptions:")
    print("  1. Scrape all series")
    print("  2. Scrape only NEW series")
    print("  3. Scrape unwatched series")
    print("  4. Generate report")
    print("  5. Single link / batch add")
    print("  6. Scrape subscribed/watchlist series")
    print("  7. Retry failed scrapes")
    print("  8. Pause scraping")
    print("  9. Exit\n")


def _check_checkpoint(expected_mode):
    """Check for an existing checkpoint and prompt the user to resume or discard."""
    saved_mode = SToScraper.get_checkpoint_mode(DATA_DIR)
    if saved_mode is None:
        return {'ok': True, 'resume': False}

    saved_label = _MODE_LABELS.get(saved_mode, saved_mode)
    expected_label = _MODE_LABELS.get(expected_mode, expected_mode)
    checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')

    if saved_mode == expected_mode:
        print(f"\n⚠ Checkpoint found from a previous \"{saved_label}\" run!\n")
        choice = input("Resume from checkpoint? (y/n): ").strip().lower()
        if choice == 'y':
            return {'ok': True, 'resume': True}
        discard = input(
            "Discard old checkpoint and start fresh? (y/n): ").strip().lower()
        if discard == 'y':
            try:
                os.remove(checkpoint_file)
            except OSError:
                pass
            return {'ok': True, 'resume': False}
        return {'ok': False, 'resume': False}

    print(f"\n⚠ A checkpoint exists from a different mode: \"{saved_label}\"")
    print(f"   You are about to run: \"{expected_label}\"\n")
    discard = input(
        "Discard the old checkpoint and continue? (y/n): ").strip().lower()
    if discard == 'y':
        try:
            os.remove(checkpoint_file)
        except OSError:
            pass
        return {'ok': True, 'resume': False}
    return {'ok': False, 'resume': False}


def _host_label(site_url):
    """Return the hostname part of a URL for display."""
    return urlparse(site_url).netloc


def _format_host_rows(hosts):
    """Return a list of table-formatted host status lines.

    hosts is a list of (label, status, count, idx_match) tuples.
    """
    if not hosts:
        return []

    host_w = max(len(label) for label, *_ in hosts)
    lines = [
        f"  {'Host':<{host_w}}  Status    Series      Index",
        f"  {'-' * host_w}  ------    ------      -----",
    ]
    for label, status, count, idx_match in hosts:
        status_txt = "OK" if status else "FAILED"
        count_txt = f"{count:,}" if count is not None else "-"
        match_txt = "match" if idx_match is True else (
            "mismatch" if idx_match is False else "-")
        lines.append(
            f"  {label:<{host_w}}  {status_txt:<8}  {count_txt:<10}  {match_txt}"
        )
    return lines


def _probe_hosts(scraper, site_urls):
    """Return probe results; on failure, print the error and return an empty list."""
    try:
        return asyncio.run(scraper.probe_sites(site_urls))
    except Exception as exc:
        print(f"  ✗ Probe failed: {exc}")
        logger.exception("Host probe failed")
        return []


def _fetch_site_slugs_for_host(scraper, site_url):
    """Fetch site slugs for a reachable host; return None on error."""
    try:
        return asyncio.run(scraper.get_series_slugs_for_site(site_url))
    except Exception as exc:
        logger.warning(
            "Could not fetch series slugs for %s: %s", site_url, exc)
        return None


def _collect_index_slugs(idx_mgr):
    """Collect slugs from the local index, including entries without a slug."""
    index_slugs_list = []
    index_entries_without_slug = []
    for title, s in idx_mgr.series_index.items():
        slug = _extract_slug(s)
        if slug:
            index_slugs_list.append(slug)
        else:
            index_entries_without_slug.append({
                'title': s.get('title', title),
                'link': s.get('link', ''),
                'url': s.get('url', ''),
            })

    index_duplicates = {
        slug: n for slug, n in Counter(index_slugs_list).items() if n > 1
    }
    return set(index_slugs_list), index_duplicates, index_entries_without_slug


def _save_mismatch_report(report_path, count, idx_count, index_slugs, site_slugs,
                          only_in_index, only_on_site, index_duplicates,
                          index_entries_without_slug):
    """Write a mismatch report JSON and log the result."""
    report = {
        'generated': datetime.now().isoformat(),
        'site_count': count,
        'index_count': idx_count,
        'index_unique_slugs': len(index_slugs),
        'site_unique_slugs': len(site_slugs),
        'index_entries_without_slug_count': len(index_entries_without_slug),
        'only_in_index': sorted(only_in_index),
        'only_on_site': sorted(only_on_site),
        'index_duplicates': index_duplicates,
        'index_entries_without_slug': index_entries_without_slug,
    }
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"    📄 Mismatch report saved: {report_path}")
    logger.info(
        "Mismatch report saved: %d only-in-index, %d only-on-site, %d index duplicates",
        len(only_in_index), len(only_on_site), len(index_duplicates))


def _remove_duplicate_index_entries(idx_mgr, index_duplicates):
    """Prompt to remove duplicate index entries and save if confirmed."""
    dup_extra = sum(index_duplicates.values()) - len(index_duplicates)
    print(
        f"    ⚠ Found {len(index_duplicates)} duplicate slug(s) in index (extra count: {dup_extra})")
    print("    Duplicate slugs:")
    for dup_slug in sorted(index_duplicates):
        print(f"      - {dup_slug}")

    choice = input("\nDelete duplicate index entries? (y/n): ").strip().lower()
    if choice != 'y':
        return

    removed_titles = []
    for title, series in list(idx_mgr.series_index.items()):
        slug = _extract_slug(series)
        if slug in index_duplicates:
            removed_titles.append(title)
            del idx_mgr.series_index[title]
    idx_mgr.save_index()
    print(
        f"    🗑 Removed {len(removed_titles)} duplicate entries. Run 'Fetch new series' to rescrape them.")
    logger.info(
        "Removed %d duplicate index entries: %s",
        len(removed_titles), sorted(index_duplicates))


def _cross_check_index(scraper, site_url, count, idx_mgr=None):
    """Compare site slugs against the local index and report mismatches.

    idx_mgr can be passed in to avoid reloading the index repeatedly.
    """
    if idx_mgr is None:
        idx_mgr = IndexManager(SERIES_INDEX_FILE)
    idx_count = len(idx_mgr.series_index)
    if idx_count == 0:
        return None

    match = count == idx_count
    diff = count - idx_count
    if match:
        return True

    site_slugs = _fetch_site_slugs_for_host(scraper, site_url)
    if site_slugs is None:
        return None

    index_slugs, index_duplicates, index_entries_without_slug = _collect_index_slugs(
        idx_mgr)
    only_in_index = index_slugs - site_slugs
    only_on_site = site_slugs - index_slugs
    has_real_mismatch = bool(only_in_index or index_duplicates)

    report_path = os.path.join(DATA_DIR, 'mismatch_report.json')
    if has_real_mismatch:
        _save_mismatch_report(
            report_path, count, idx_count, index_slugs, site_slugs,
            only_in_index, only_on_site, index_duplicates,
            index_entries_without_slug)
        print(f"    → Mismatch report created: {report_path}")
    else:
        if os.path.exists(report_path):
            os.remove(report_path)
        logger.info(
            "Index count %d is below site count %d; no mismatch report generated",
            idx_count, count)

    if index_duplicates:
        _remove_duplicate_index_entries(idx_mgr, index_duplicates)

    return False


def _probe_sites_before_scrape(scraper, idx_mgr=None):
    """Probe configured hosts, show OK/FAILED, and auto-select the first working one.

    This function is always called from a synchronous context (the main menu loop),
    so asyncio.run() is the correct way to execute async coroutines here.
    """
    global ACTIVE_SITE_URL  # pylint: disable=global-statement
    site_urls = SITE_URLS or [SITE_URL]
    if not site_urls:
        return SITE_URL

    # Load the index once and reuse it for every host cross-check.
    if idx_mgr is None:
        idx_mgr = IndexManager(SERIES_INDEX_FILE)

    print("\n→ Checking host availability...\n")
    results = _probe_hosts(scraper, site_urls)

    ok_hosts = []
    host_counts = {}
    table_rows = []

    for entry in results:
        site_url = entry['site_url']
        label = _host_label(site_url)
        ok = bool(entry.get("ok"))
        count = None
        idx_match = None

        if ok:
            ok_hosts.append(site_url)
            site_slugs = _fetch_site_slugs_for_host(scraper, site_url)
            count = len(site_slugs) if site_slugs is not None else None
            host_counts[site_url] = count
            if count is not None:
                idx_match = _cross_check_index(
                    scraper, site_url, count, idx_mgr=idx_mgr)

        table_rows.append((label, ok, count, idx_match))

    for line in _format_host_rows(table_rows):
        print(line)

    scraper.site_url = ok_hosts[0] if ok_hosts else SITE_URL
    suffix = "" if ok_hosts else " (default)"
    print(f"\n→ Active host: {scraper.site_url}{suffix}")

    if len(ok_hosts) >= 2:
        counts = [
            host_counts.get(host) for host in ok_hosts
            if host_counts.get(host) is not None
        ]
        if len(counts) == len(ok_hosts) and counts:
            match = all(count == counts[0] for count in counts[1:])
            print(f"→ Cross-host counts: match = {match}")
        else:
            print("→ Cross-host counts: match = False")

    ACTIVE_SITE_URL = scraper.site_url
    return scraper.site_url


def _run_scrape_and_save(run_kwargs, description, success_msg, no_data_msg,
                         pre_save_hook=None, vanished_scope=None):
    """Common pattern: create scraper, run, confirm & save, handle errors.

    Args:
        pre_save_hook: Optional callable(scraper, pre_index) called after scraping
                       but before confirm_and_save. Can modify scraper.series_data.
        vanished_scope: Override scope for show_vanished_series (default: auto-detect).
    """
    pre_index = IndexManager(SERIES_INDEX_FILE) if pre_save_hook else None
    t_start = time.perf_counter()
    try:
        scraper = SToScraper()
        if ACTIVE_SITE_URL:
            scraper.site_url = ACTIVE_SITE_URL
        else:
            _probe_sites_before_scrape(scraper)
        scraper.run(**run_kwargs)

        if scraper.series_data:
            if pre_save_hook:
                pre_save_hook(scraper, pre_index)

            index_manager = IndexManager(SERIES_INDEX_FILE)

            if scraper.all_discovered_series is not None:
                all_slugs = {_extract_slug(
                    s) for s in scraper.all_discovered_series} - {None}
                scope = vanished_scope or (
                    'new_only' if run_kwargs.get('new_only') else 'all')
                show_vanished_series(
                    index_manager.series_index, all_slugs, scope,
                    index_file=SERIES_INDEX_FILE,
                    new_data=scraper.series_data,
                )
                # Always reload: show_vanished_series may have deleted entries from disk
                index_manager.load_index()

            result = confirm_and_save_changes(
                scraper.series_data, description, index_manager)
            if isinstance(result, dict) and result.get('rescrape'):
                # User already confirmed deletion in the integrity dialog — proceed directly
                n = len(result['urls'])
                print(
                    f"\n→ Deleting {n} critical series from index before rescraping...")
                remove_series_from_index(SERIES_INDEX_FILE, result['titles'])
                print(f"\n→ Rescraping {n} critical series...\n")
                _run_scrape_and_save(
                    run_kwargs={'url_list': result['urls'], 'parallel': False},
                    description=f"Rescrape critical series ({n})",
                    success_msg=f"Critical series rescraping completed! {n} series updated.",
                    no_data_msg="No data scraped for critical series",
                )
            elif result:
                print(f"\n✓ {success_msg}")
                print_completed_series_alerts(index_manager)
                logger.info(success_msg)
                # Final cross-check: scraped count vs index count (full scrapes only)
                if scraper.all_discovered_series is not None:
                    scraped_count = len(scraper.series_data)
                    idx_count = len(index_manager.series_index)
                    if scraped_count == idx_count:
                        print(f"  Index count: {idx_count}  →  match = True")
                    else:
                        diff = scraped_count - idx_count
                        sign = '+' if diff > 0 else ''
                        print(
                            f"  Index count: {idx_count}  →  match = False ({sign}{diff} difference)")
                        print(
                            "  → Run a full scrape (option 1) to detect vanished/renamed series.")
        else:
            if run_kwargs.get('retry_failed') and scraper.failed_links:
                n = len(scraper.failed_links)
                print(f"\n✗ All {n} retried series failed again:")
                for entry in scraper.failed_links:
                    title = entry.get('title') or entry.get('url', '?')
                    reason = entry.get('reason', 'unknown error')
                    print(f"  • {title}  →  {reason}")
                print("\n→ Failed list preserved. Use option 7 to retry again.")
                logger.warning("All %d retried series failed again", n)
            else:
                print(f"\n⚠ {no_data_msg}")
                logger.warning(no_data_msg)

        # Only clear checkpoint if scraping completed (not paused)
        if not scraper.paused:
            scraper.clear_checkpoint()
        else:
            print("\n⚠ Scraping was paused — checkpoint preserved for resume.")

        if scraper.failed_links:
            print(
                f"\n⚠ {len(scraper.failed_links)} series failed during scraping.")
            print("→ Use option 7 (Retry failed series) to rescrape these later.")

        t_elapsed = time.perf_counter() - t_start
        print(f"\n⏱ Scrape duration: {t_elapsed / 60:.1f}m ({t_elapsed:.1f}s)")

        return scraper
    except (KeyboardInterrupt, SystemExit):
        print("\n⚠ Scraping interrupted by Ctrl+C")
        if 'scraper' in locals() and scraper.series_data:
            index_manager = IndexManager(SERIES_INDEX_FILE)
            result = confirm_and_save_changes(
                scraper.series_data, description, index_manager)
            if isinstance(result, dict) and result.get('rescrape'):
                remove_series_from_index(SERIES_INDEX_FILE, result['titles'])
                for url, title in zip(result['urls'], result['titles']):
                    scraper.failed_links.append({
                        'url': url, 'title': title,
                        'link': '', 'reason': 'integrity_check_failed',
                    })
                scraper.save_failed_series()
                print(
                    f"\n✓ {len(result['urls'])} critical series removed from index and added to retry list.")
                print("→ Use option 7 (Retry failed series) to rescrape these.")
                logger.info(
                    "Critical series removed from index and added to retry list after Ctrl+C")
            elif result:
                print(
                    f"\n✓ Partial data saved ({len(scraper.series_data)} series)")
                logger.info("%s interrupted — partial data saved", description)
        if 'scraper' in locals() and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed.")
            print("→ Use option 7 (Retry failed series) to rescrape these later.")
        return scraper if 'scraper' in locals() else None
    except OSError as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error("Network error in %s: %s", description, e)
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}")
        logger.error("Unexpected error in %s: %s", description, e)
    return None


def scrape_all_series():
    print("\n→ Starting S.TO scraper (httpx)...\n")

    chk = _check_checkpoint('all_series')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    print("\nScraping mode:")
    print("  1. Single session (slower, but most reliable)")
    print("  2. Multi-session (faster, parallel sessions)")
    print("  0. Back\n")
    mode_choice = input("Choose mode (0-2) [default: 2]: ").strip() or '2'

    if mode_choice == '0':
        return
    use_parallel = mode_choice != '1'

    _run_scrape_and_save(
        run_kwargs={'resume_only': resume, 'parallel': use_parallel},
        description="All series scrape",
        success_msg="Scraping completed and saved!",
        no_data_msg="No series data scraped",
    )


def scrape_new_series():
    print("\n→ Starting S.TO scraper — NEW series only (httpx)...\n")

    chk = _check_checkpoint('new_only')
    if not chk['ok']:
        print("✗ Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs={'new_only': True, 'resume_only': chk['resume']},
        description="New series data",
        success_msg="New series scraping completed successfully!",
        no_data_msg="No new series found",
    )


def scrape_unwatched():
    """Scrape only unwatched/ongoing/unstarted series from the existing index."""
    print("\n→ Scrape unwatched series (skipping fully watched)...\n")

    index_manager = IndexManager(SERIES_INDEX_FILE)
    if not index_manager.series_index:
        print("✗ No series in index. Run a full scrape first (option 1).")
        return

    unwatched_urls = []
    skipped = 0
    for series in index_manager.series_index.values():
        total, watched = get_episode_counts(series)
        if total > 0 and watched >= total:
            skipped += 1
        else:
            url = series.get('url')
            if url:
                unwatched_urls.append(url)

    if not unwatched_urls:
        print("✓ All series are fully watched! Nothing to scrape.")
        return

    print(
        f"  Found {len(unwatched_urls)} unwatched/ongoing series (skipping {skipped} fully watched)\n")

    chk = _check_checkpoint('unwatched')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    print("\nScraping mode:")
    print("  1. Single session (slower, but most reliable)")
    print("  2. Multi-session (faster, parallel sessions)")
    print("  0. Back\n")
    mode_choice = input("Choose mode (0-2) [default: 2]: ").strip() or '2'

    if mode_choice == '0':
        return
    if mode_choice not in ['1', '2']:
        print("⚠ Invalid choice, using default (parallel)")
        use_parallel = True
    else:
        use_parallel = mode_choice == '2'

    _run_scrape_and_save(
        run_kwargs={
            'url_list': unwatched_urls,
            'resume_only': resume,
            'parallel': use_parallel,
            'checkpoint_mode': 'unwatched',
        },
        description=f"Unwatched series scrape ({len(unwatched_urls)} series)",
        success_msg=f"Unwatched series scraping completed! ({len(unwatched_urls)} series)",
        no_data_msg="No data scraped",
    )


def single_or_batch_add():
    default_file = DEFAULT_BATCH_FILE
    print("\n→ Add single link / batch from file")
    print("  • Paste URL → scrapes single series")
    print("  • Enter filename → uses that file for batch")
    print(f"  • Press Enter → uses default ({default_file})")
    print("  • Type 0   → back to main menu\n")

    user_input = input(f"Enter [default: {default_file}]: ").strip()

    if user_input == '0':
        return
    if not user_input:
        user_input = default_file

    if user_input.startswith(('http://', 'https://')):
        add_single_series(user_input)
    else:
        if not os.path.exists(user_input):
            print(f"✗ File not found: {user_input}")
            return
        batch_add_from_file(user_input)


def add_single_series(url):
    print(f"\n→ Scraping single series: {url}\n")
    parsed = urlparse(url)
    if not _SERIE_URL_RE.search(parsed.path):
        print("✗ Invalid s.to series URL format")
        return

    _run_scrape_and_save(
        run_kwargs={'single_url': url, 'parallel': False},
        description="Single series",
        success_msg="Series added/updated successfully!",
        no_data_msg="No data scraped for this series",
    )


def batch_add_from_file(file_path):
    try:
        urls = []
        skipped = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                url = line.strip()
                if not url or url.startswith('#'):
                    continue
                parsed = urlparse(url)
                if parsed.scheme and parsed.scheme not in ('http', 'https'):
                    skipped.append((line_num, url))
                    continue
                if not _SERIE_URL_RE.search(parsed.path):
                    skipped.append((line_num, url))
                    continue
                urls.append(url)
        if skipped:
            print(f"⚠ Skipped {len(skipped)} invalid URL(s):")
            for line_num, bad_url in skipped[:5]:
                print(f"  Line {line_num}: {bad_url[:80]}")
            if len(skipped) > 5:
                print(f"  ... and {len(skipped) - 5} more")
    except Exception as e:
        print(f"✗ Failed to read file: {str(e)}")
        logger.error("Failed to read file %s: %s", file_path, e)
        return

    if not urls:
        print("✗ No valid URLs found in file")
        return

    print(f"✓ Found {len(urls)} valid URL(s) in file\n")
    print("URLs to process:")
    for url in urls[:5]:
        print(f"  • {url}")
    if len(urls) > 5:
        print(f"  ... and {len(urls) - 5} more")

    confirm = input("\nProceed with batch add? (y/n): ").strip().lower()
    if confirm != 'y':
        print("✗ Cancelled")
        return

    chk = _check_checkpoint('batch')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    print(f"\n→ Starting batch scraper for {len(urls)} series...\n")

    run_kwargs = {'url_list': urls, 'resume_only': resume, 'parallel': True}

    _run_scrape_and_save(
        run_kwargs=run_kwargs,
        description=f"Batch add ({len(urls)} series)",
        success_msg=f"Batch add completed! {len(urls)} series processed.",
        no_data_msg="No data scraped",
    )


def _show_ongoing_and_export(report, index_manager):
    """Show ongoing series and offer to export their URLs to series_urls.txt"""
    ongoing_count = report['categories']['ongoing']['count']
    if ongoing_count == 0:
        return

    print(f"\n{'=' * 70}")
    print(f"ONGOING SERIES ({ongoing_count}):")
    ongoing_titles = report['categories']['ongoing']['titles']
    for title in ongoing_titles[:10]:
        print(f"  - {title}")
    if ongoing_count > 10:
        print(f"  ... and {ongoing_count - 10} more\n")

    export = input(
        f"\nExport {ongoing_count} ongoing series URLs to series_urls.txt? (y/n): ").strip().lower()
    if export == 'y':
        try:
            urls = []
            for title in ongoing_titles:
                series_data = index_manager.series_index.get(title, {})
                url = series_data.get('url') or series_data.get('link')
                if url:
                    if not url.startswith('http'):
                        url = f"{SITE_URL}{url}"
                    urls.append(url)

            if urls:
                urls_file = DEFAULT_BATCH_FILE
                with open(urls_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(urls) + '\n')
                print(f"\n✓ Exported {len(urls)} URLs to {urls_file}")
                print("  → Use option 5 (Batch add from file) to rescrape these series")
                logger.info("Exported %d URLs to %s", len(urls), urls_file)
            else:
                print("\n⚠ Could not extract URLs from ongoing series")
        except Exception as e:
            print(f"\n✗ Failed to export URLs: {str(e)}")
            logger.error("Failed to export URLs: %s", e)


def _print_report_summary(report, report_file, filter_name=None):
    """Print enhanced report summary to console."""
    stats = report['metadata']['statistics']
    ongoing_count = report['categories']['ongoing']['count']
    not_started_count = report['categories']['not_started']['count']
    not_started_sub_wl_count = report['categories']['not_started_subscribed_watchlist']['count']
    waiting_count = report['categories']['waiting_for_new_episodes']['count']

    header = f"REPORT SUMMARY ({filter_name.upper().replace('_', ' ')}):" if filter_name else "REPORT SUMMARY:"
    print("\n" + "-"*70)
    print(header)
    print("-"*70)
    print(f"  Total series:        {stats['total_series']}")
    print(
        f"  Completed (100%):    {stats.get('completed_count', stats['watched'])}")
    print(
        f"  Ongoing (started):   {stats.get('ongoing_count', ongoing_count)}")
    if waiting_count > 0:
        print(f"  Waiting for new eps: {waiting_count}")
    print(
        f"  Not started (0%):    {stats.get('not_started_count', not_started_count)}")
    if not_started_sub_wl_count > 0:
        print(f"  Not started (Sub/WL):{not_started_sub_wl_count}")
    print(f"  Total episodes:      {stats['total_episodes']}")
    print(f"  Watched episodes:    {stats['watched_episodes']}")
    print(f"  Unwatched episodes:  {stats.get('unwatched_episodes', 0)}")
    print(
        f"  Avg episodes/series: {stats.get('average_episodes_per_series', 0)}")
    print(f"  Average completion:  {stats['average_completion']:.1f}%")
    print(f"  Subscribed:          {stats.get('subscribed_count', 0)}")
    print(f"  Watchlist:           {stats.get('watchlist_count', 0)}")
    print(
        f"  Both (Sub+WL):       {stats.get('both_subscribed_and_watchlist', 0)}")

    dist = stats.get('completion_distribution', {})
    if dist:
        parts = [f"{k}: {v}" for k, v in dist.items()]
        print("\n  Completion Distribution:")
        print(f"    {'  |  '.join(parts)}")

    most = stats.get('most_completed_series', [])
    if most:
        print(f"\n  Most Completed Ongoing (top {len(most)}):")
        for i, s in enumerate(most, 1):
            print(
                f"    {i}. {s['title']} — {s['completion']:.1f}% ({s['progress']})")

    least = stats.get('least_completed_series', [])
    if least:
        print(f"  Least Completed Ongoing (bottom {len(least)}):")
        for i, s in enumerate(least, 1):
            print(
                f"    {i}. {s['title']} — {s['completion']:.1f}% ({s['progress']})")

    print(f"\n  Saved to:            {report_file}")
    print("-"*70 + "\n")


def generate_report():
    """Generate series report with optional filtering by subscription status"""
    print("\n→ Generate report")
    print("  1. Full report (all series)")
    print("  2. Subscription/watchlist filtered report")
    print("  0. Back\n")

    choice = input("Choose report type (0-2): ").strip()

    if choice == '0':
        return

    try:
        index_manager = IndexManager(SERIES_INDEX_FILE)

        if choice == '1':
            print("\n→ Generating full report...")
            report = index_manager.get_full_report()
            report_file = os.path.join(DATA_DIR, 'series_report.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            _print_report_summary(report, report_file)
            logger.info("Full report generated")
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)

        elif choice == '2':
            print("\n→ Subscription/watchlist report")
            print("  1. Only subscribed")
            print("  2. Only watchlist")
            print("  3. Both")
            print("  0. Back\n")

            sub_choice = input("Choose filter (0-3): ").strip()

            if sub_choice == '0':
                return

            if sub_choice == '1':
                print("\n→ Generating report for subscribed series...")
                report = index_manager.get_full_report(
                    filter_subscribed=True, filter_watchlist=False)
                filter_name = "subscribed_only"
            elif sub_choice == '2':
                print("\n→ Generating report for watchlist series...")
                report = index_manager.get_full_report(
                    filter_subscribed=False, filter_watchlist=True)
                filter_name = "watchlist_only"
            elif sub_choice == '3':
                print("\n→ Generating report for subscribed AND watchlist...")
                report = index_manager.get_full_report(
                    filter_subscribed=True, filter_watchlist=True)
                filter_name = "both_subscribed_watchlist"
            else:
                print("⚠ Invalid choice")
                return

            report_file = os.path.join(
                DATA_DIR, f'series_report_{filter_name}.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            _print_report_summary(report, report_file, filter_name)
            logger.info("Filtered report generated: %s", filter_name)
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)

        else:
            print("⚠ Invalid choice")

    except Exception as e:
        print(f"\n✗ Error generating report: {str(e)}")
        logger.error("Error generating report: %s", e)


def _inject_disappeared_series(scraper, pre_index, source):
    """Inject stubs for series no longer on account pages so merge can prompt."""
    discovered_slugs = {_extract_slug(s) for s in (
        scraper.all_discovered_series or [])} - {None}
    failed_slugs = {_extract_slug(
        fl) for fl in scraper.failed_links if isinstance(fl, dict)} - {None}
    scraped_titles = {s.get('title')
                      for s in scraper.series_data if s.get('title')}

    for field, sources in [('watchlist', ('watchlist', 'both')), ('subscribed', ('subscribed', 'both'))]:
        if source not in sources:
            continue
        injected = []
        for title, entry in pre_index.series_index.items():
            if not entry.get(field, False):
                continue
            slug = _extract_slug(entry)
            if not slug or slug in discovered_slugs or slug in failed_slugs:
                continue
            if title in scraped_titles:
                # Already scraped — just flip the flag
                for item in scraper.series_data:
                    if item.get('title') == title:
                        item[field] = False
                        break
            else:
                stub = copy.deepcopy(entry)
                stub[field] = False
                scraper.series_data.append(stub)
                scraped_titles.add(title)
            injected.append(title)
        if injected:
            print(
                f"\n  ⚠ {len(injected)} series no longer {field} (will prompt for confirmation):")
            for name in injected:
                print(f"    • {name}")


def scrape_subscribed_watchlist():
    """Scrape subscribed/watchlist series with disappeared-series detection."""
    print("\n→ Scrape subscribed/watchlist series")
    print("  1. Only subscribed")
    print("  2. Only watchlist")
    print("  3. Both")
    print("  0. Back\n")

    sub_choice = input("Choose source (0-3) [default: 3]: ").strip() or '3'
    if sub_choice == '0':
        return
    source = {'1': 'subscribed', '2': 'watchlist'}.get(sub_choice, 'both')

    chk = _check_checkpoint(source)
    if not chk['ok']:
        print("✗ Cancelled")
        return

    def _hook(scraper, pre_index):
        _inject_disappeared_series(scraper, pre_index, source)

    _run_scrape_and_save(
        run_kwargs={'account_source': source, 'resume_only': chk['resume']},
        description="Account series",
        success_msg="Account series scraping completed!",
        no_data_msg="No series found on your account pages",
        pre_save_hook=_hook,
        vanished_scope=source,
    )


def retry_failed_series():
    """Retry previously failed series in sequential mode"""
    print("\n→ Retry failed series from last run\n")

    chk = _check_checkpoint('retry')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    temp_scraper = SToScraper()
    failed_list = temp_scraper.load_failed_series()
    if not failed_list:
        print("✓ No failed series found. Nothing to retry.")
        return
    print(f"✓ Found {len(failed_list)} failed series from last run")
    print("\n→ Starting retry in sequential mode (for reliability)...")

    _run_scrape_and_save(
        run_kwargs={'retry_failed': True,
                    'parallel': False, 'resume_only': resume},
        description="Retry data",
        success_msg="Retry completed successfully!",
        no_data_msg="No data from retry",
    )


def pause_scraping():
    """Create a pause file to signal workers to pause scraping"""
    pause_file = os.path.join(DATA_DIR, '.pause_scraping')
    try:
        with open(pause_file, 'w', encoding='utf-8') as f:
            f.write('PAUSE')
        print(f"\n✓ Pause file created: {pause_file}")
        print("Workers will pause at next checkpoint.\n")
        logger.info("Pause file created: %s", pause_file)
    except Exception as e:
        print(f"\n✗ Failed to create pause file: {str(e)}")
        logger.error("Failed to create pause file: %s", e)


def main():
    """Main application loop"""
    idx_mgr = IndexManager(SERIES_INDEX_FILE)
    index_count = len(idx_mgr.series_index)
    print(
        f"✓ Index loaded ({os.path.abspath(SERIES_INDEX_FILE)}) ({index_count:,} entries)\n")

    print_header()

    if not validate_credentials():
        sys.exit(1)

    print(f"\u2713 Credentials found for user: {EMAIL}\n")

    if not check_disk_space():
        response = input("Continue anyway? (y/n): ").strip().lower()
        if response != 'y':
            sys.exit(1)

    scraper = SToScraper()
    _probe_sites_before_scrape(scraper, idx_mgr=idx_mgr)

    while True:
        show_menu()
        choice = input("Enter your choice (1-9): ").strip()

        if not choice.isdigit() or not 1 <= int(choice) <= 9:
            print("✗ Invalid choice. Please enter a number between 1 and 9.")
            continue

        if choice in ['1', '2', '3', '5', '6', '7']:
            if not check_disk_space():
                print("⚠ Aborting due to low disk space.")
                continue

        if choice == '1':
            scrape_all_series()
        elif choice == '2':
            scrape_new_series()
        elif choice == '3':
            scrape_unwatched()
        elif choice == '4':
            generate_report()
        elif choice == '5':
            single_or_batch_add()
        elif choice == '6':
            scrape_subscribed_watchlist()
        elif choice == '7':
            retry_failed_series()
        elif choice == '8':
            pause_scraping()
        elif choice == '9':
            print("\n✓ Goodbye!\n")
            break


if __name__ == "__main__":
    main()
