#!/usr/bin/env python3
# pylint: disable=broad-exception-caught,too-many-branches
"""
S.TO Series Scraper & Index Manager  (httpx)

Scrapes watched TV series from s.to and maintains a local JSON index.
Uses httpx (no browser needed) with multi-session architecture.
Supports checkpoint resume, batch URL import, subscription/watchlist tracking,
and interactive change confirmation.
"""

import json
import logging
import logging.handlers
import os
import re
import shutil
import sys
from urllib.parse import urlparse

# Ensure project root is on sys.path so imports work from any working directory
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config.config import (  # noqa: E402  # pylint: disable=import-error,no-name-in-module,wrong-import-position
    EMAIL, PASSWORD, DATA_DIR, LOG_FILE,
)
from src.scraper import SToScraper  # noqa: E402  # pylint: disable=wrong-import-position
from src.index_manager import (  # noqa: E402  # pylint: disable=wrong-import-position
    IndexManager, confirm_and_save_changes, show_vanished_series,
    get_episode_counts,
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

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
    print("\n" + "="*60)
    print("  S.TO SERIES SCRAPER & INDEX MANAGER  (httpx)")
    print("="*60 + "\n")


def print_completed_series_alerts(index_manager=None):
    """Alert user about series that need attention:
    1. Fully watched but not subscribed
    2. Ongoing (started but incomplete) but not on watchlist
    """
    try:
        if index_manager is None:
            index_manager = IndexManager()

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
            print(f"\n⚠ {len(completed_not_sub)} COMPLETED SERIES — NOT SUBSCRIBED:")
            print("─" * 70)
            for s in completed_not_sub:
                print(f"  • {s.get('title')}")
            print("─" * 70)
            print("  Consider subscribing or leaving as-is.")

            rescrape = input("\nRescrape these series to update Sub/WL status? (y/n): ").strip().lower()
            if rescrape == 'y':
                urls = [s.get('url') for s in completed_not_sub if s.get('url')]
                if urls:
                    print(f"\n→ Rescraping {len(urls)} completed series...\n")
                    _run_scrape_and_save(
                        run_kwargs={'url_list': urls, 'parallel': False},
                        description=f"Rescrape completed series ({len(urls)})",
                        success_msg=f"Rescrape completed! {len(urls)} series updated.",
                        no_data_msg="No data scraped",
                    )

        if ongoing_no_wl:
            ongoing_no_wl.sort(key=lambda s: s.get('title', ''))
            print(f"\n⚠ {len(ongoing_no_wl)} ONGOING SERIES — NOT ON WATCHLIST:")
            print("─" * 70)
            for s in ongoing_no_wl:
                print(f"  • {s.get('title')}")
            print("─" * 70)
            print("  Consider adding them to your watchlist.")

    except Exception as e:
        logger.error(f"Error printing series alerts: {e}")


def check_disk_space(min_mb=100):
    """Check if enough disk space is available."""
    try:
        stat = shutil.disk_usage(DATA_DIR)
        available_mb = stat.free / (1024 * 1024)
        if available_mb < min_mb:
            print("\n✗ WARNING: Low disk space!")
            print(f"  Available: {available_mb:.1f} MB (minimum needed: {min_mb} MB)")
            return False
        return True
    except Exception as e:
        logger.warning(f"Could not check disk space: {e}")
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
        discard = input("Discard old checkpoint and start fresh? (y/n): ").strip().lower()
        if discard == 'y':
            try:
                os.remove(checkpoint_file)
            except OSError:
                pass
            return {'ok': True, 'resume': False}
        return {'ok': False, 'resume': False}

    print(f"\n⚠ A checkpoint exists from a different mode: \"{saved_label}\"")
    print(f"   You are about to run: \"{expected_label}\"\n")
    discard = input("Discard the old checkpoint and continue? (y/n): ").strip().lower()
    if discard == 'y':
        try:
            os.remove(checkpoint_file)
        except OSError:
            pass
        return {'ok': True, 'resume': False}
    return {'ok': False, 'resume': False}


def _run_scrape_and_save(run_kwargs, description, success_msg, no_data_msg):
    """Create scraper, run, confirm & save. Returns the scraper or None on error."""
    try:
        scraper = SToScraper()
        scraper.run(**run_kwargs)

        if scraper.series_data:
            if scraper.all_discovered_series is not None:
                all_slugs = set()
                for s in scraper.all_discovered_series:
                    slug = scraper.get_series_slug_from_url(s.get('link', ''))
                    if slug and slug != 'unknown':
                        all_slugs.add(slug)
                scope = 'new_only' if run_kwargs.get('new_only') else 'all'
                index_manager = IndexManager()
                show_vanished_series(index_manager.series_index, all_slugs, scope)

            if confirm_and_save_changes(scraper.series_data, description):
                print(f"\n✓ {success_msg}")
                print_completed_series_alerts()
                logger.info(success_msg)
        else:
            print(f"\n⚠ {no_data_msg}")
            logger.warning(no_data_msg)

        # Only clear checkpoint if scraping completed (not paused)
        if not scraper.paused:
            scraper.clear_checkpoint()
        else:
            print("\n⚠ Scraping was paused — checkpoint preserved for resume.")

        if scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed during scraping.")
            print("→ Use option 7 (Retry failed series) to rescrape these later.")

        return scraper
    except (KeyboardInterrupt, SystemExit):
        print("\n⚠ Scraping interrupted by Ctrl+C")
        if 'scraper' in locals() and scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, description):
                print(f"\n✓ Partial data saved ({len(scraper.series_data)} series)")
                logger.info(f"{description} interrupted — partial data saved")
        if 'scraper' in locals() and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed.")
            print("→ Use option 7 (Retry failed series) to rescrape these later.")
        return scraper if 'scraper' in locals() else None
    except OSError as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in {description}: {e}")
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}")
        logger.error(f"Unexpected error in {description}: {e}")
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

    index_manager = IndexManager()
    if not index_manager.series_index:
        print("✗ No series in index. Run a full scrape first (option 1).")
        return

    unwatched_urls = []
    skipped = 0
    for series in index_manager.series_index.values():
        total, watched = get_episode_counts(series)
        if 0 < total <= watched:
            skipped += 1
        else:
            url = series.get('url')
            if url:
                unwatched_urls.append(url)

    if not unwatched_urls:
        print("✓ All series are fully watched! Nothing to scrape.")
        return

    print(f"  Found {len(unwatched_urls)} unwatched/ongoing series (skipping {skipped} fully watched)\n")

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
    use_parallel = mode_choice != '1'

    _run_scrape_and_save(
        run_kwargs={'url_list': unwatched_urls, 'resume_only': resume, 'parallel': use_parallel},
        description=f"Unwatched series scrape ({len(unwatched_urls)} series)",
        success_msg=f"Unwatched series scraping completed! ({len(unwatched_urls)} series)",
        no_data_msg="No data scraped",
    )


def single_or_batch_add():
    default_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
    print("\n→ Add single link / batch from file")
    print("  • Paste URL → scrapes single series")
    print("  • Enter filename → uses that file for batch")
    print("  • Press Enter → uses default (series_urls.txt)")
    print("  • Type 0   → back to main menu\n")

    user_input = input("Enter [default: series_urls.txt]: ").strip()

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
        logger.error(f"Failed to read file {file_path}: {e}")
        return

    if not urls:
        print("✗ No valid URLs found in file")
        return

    print(f"✓ Found {len(urls)} valid URL(s) in file\n")
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

    print(f"\n→ Starting batch scraper for {len(urls)} series...\n")

    _run_scrape_and_save(
        run_kwargs={'url_list': urls, 'resume_only': chk['resume'], 'parallel': True},
        description=f"Batch add ({len(urls)} series)",
        success_msg=f"Batch add completed! {len(urls)} series processed.",
        no_data_msg="No data scraped",
    )


def _show_ongoing_and_export(report, index_manager):
    ongoing_count = report['categories']['ongoing']['count']
    if ongoing_count == 0:
        return

    print(f"\nONGOING SERIES ({ongoing_count}):")
    ongoing_titles = report['categories']['ongoing']['titles']
    for title in ongoing_titles[:10]:
        print(f"  - {title}")
    if ongoing_count > 10:
        print(f"  ... and {ongoing_count - 10} more\n")

    export = input(f"\nExport {ongoing_count} ongoing series URLs to series_urls.txt? (y/n): ").strip().lower()
    if export == 'y':
        try:
            urls = []
            for title in ongoing_titles:
                series_data = index_manager.series_index.get(title, {})
                url = series_data.get('url') or series_data.get('link')
                if url:
                    if not url.startswith('http'):
                        url = f"https://s.to{url}"
                    urls.append(url)
            if urls:
                urls_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
                with open(urls_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(urls) + '\n')
                print(f"\n✓ Exported {len(urls)} URLs to series_urls.txt")
                logger.info(f"Exported {len(urls)} URLs to series_urls.txt")
            else:
                print("\n⚠ Could not extract URLs from ongoing series")
        except Exception as e:
            print(f"\n✗ Failed to export URLs: {str(e)}")
            logger.error(f"Failed to export URLs: {e}")


def _print_report_summary(report, report_file, filter_name=None):
    stats = report['metadata']['statistics']
    ongoing_count = report['categories']['ongoing']['count']
    not_started_count = report['categories']['not_started']['count']
    waiting_count = report['categories']['waiting_for_new_episodes']['count']

    header = f"REPORT SUMMARY ({filter_name.upper().replace('_', ' ')}):" if filter_name else "REPORT SUMMARY:"
    print("\n" + "-"*70)
    print(header)
    print("-"*70)
    print(f"  Total series:        {stats['total_series']}")
    print(f"  Completed (100%):    {stats.get('completed_count', stats['watched'])}")
    print(f"  Ongoing (started):   {stats.get('ongoing_count', ongoing_count)}")
    if waiting_count > 0:
        print(f"  Waiting for new eps: {waiting_count}")
    print(f"  Not started (0%):    {stats.get('not_started_count', not_started_count)}")
    print(f"  Total episodes:      {stats['total_episodes']}")
    print(f"  Watched episodes:    {stats['watched_episodes']}")
    print(f"  Unwatched episodes:  {stats.get('unwatched_episodes', 0)}")
    print(f"  Avg episodes/series: {stats.get('average_episodes_per_series', 0)}")
    print(f"  Average completion:  {stats['average_completion']:.1f}%")
    print(f"  Subscribed:          {stats.get('subscribed_count', 0)}")
    print(f"  Watchlist:           {stats.get('watchlist_count', 0)}")
    print(f"  Both (Sub+WL):       {stats.get('both_subscribed_and_watchlist', 0)}")

    dist = stats.get('completion_distribution', {})
    if dist:
        parts = [f"{k}: {v}" for k, v in dist.items()]
        print("\n  Completion Distribution:")
        print(f"    {'  |  '.join(parts)}")

    most = stats.get('most_completed_series', [])
    if most:
        print(f"\n  Most Completed (top {len(most)}):")
        for i, s in enumerate(most, 1):
            print(f"    {i}. {s['title']} — {s['completion']:.1f}% ({s['progress']})")

    least = stats.get('least_completed_series', [])
    if least:
        print(f"  Least Completed (bottom {len(least)}):")
        for i, s in enumerate(least, 1):
            print(f"    {i}. {s['title']} — {s['completion']:.1f}% ({s['progress']})")

    print(f"\n  Saved to:            {report_file}")
    print("-"*70 + "\n")


def generate_report():
    print("\n→ Generate report")
    print("  1. Full report (all series)")
    print("  2. Subscription/watchlist filtered report")
    print("  0. Back\n")

    choice = input("Choose report type (0-2): ").strip()
    if choice == '0':
        return

    try:
        index_manager = IndexManager()

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
                report = index_manager.get_full_report(filter_subscribed=True)
                filter_name = "subscribed_only"
            elif sub_choice == '2':
                report = index_manager.get_full_report(filter_watchlist=True)
                filter_name = "watchlist_only"
            elif sub_choice == '3':
                report = index_manager.get_full_report(filter_subscribed=True, filter_watchlist=True)
                filter_name = "both_subscribed_watchlist"
            else:
                print("⚠ Invalid choice")
                return

            report_file = os.path.join(DATA_DIR, f'series_report_{filter_name}.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            _print_report_summary(report, report_file, filter_name)
            logger.info(f"Filtered report generated: {filter_name}")
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)

        else:
            print("⚠ Invalid choice")
    except Exception as e:
        print(f"\n✗ Error generating report: {str(e)}")
        logger.error(f"Error generating report: {e}")


def scrape_subscribed_watchlist():
    print("\n→ Scrape subscribed/watchlist series")
    print("  1. Only subscribed")
    print("  2. Only watchlist")
    print("  3. Both")
    print("  0. Back\n")

    sub_choice = input("Choose source (0-3) [default: 3]: ").strip() or '3'
    if sub_choice == '0':
        return

    if sub_choice == '1':
        source = 'subscribed'
    elif sub_choice == '2':
        source = 'watchlist'
    elif sub_choice == '3':
        source = 'both'
    else:
        print("⚠ Invalid choice, using default (both)")
        source = 'both'

    chk = _check_checkpoint(source)
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    _run_scrape_and_save(
        run_kwargs={'account_source': source, 'resume_only': resume, 'parallel': True},
        description=f"Account series ({source})",
        success_msg=f"Account series scraping completed! ({source})",
        no_data_msg="No series found on your account pages",
    )


def retry_failed_series():
    print("\n→ Retry failed series from last run\n")

    temp_scraper = SToScraper()
    failed_list = temp_scraper.load_failed_series()
    if not failed_list:
        print("✓ No failed series found. Nothing to retry.")
        return
    print(f"✓ Found {len(failed_list)} failed series from last run")
    print("→ Starting retry in sequential mode (for reliability)...")

    chk = _check_checkpoint('retry')
    if not chk['ok']:
        print("✗ Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs={'retry_failed': True, 'parallel': False, 'resume_only': chk['resume']},
        description="Retry data",
        success_msg="Retry completed successfully!",
        no_data_msg="No data from retry",
    )


def pause_scraping():
    pause_file = os.path.join(DATA_DIR, '.pause_scraping')
    try:
        with open(pause_file, 'w', encoding='utf-8') as f:
            f.write('PAUSE')
        print(f"\n✓ Pause file created: {pause_file}")
        print("Workers will pause at next checkpoint.\n")
        logger.info(f"Pause file created: {pause_file}")
    except Exception as e:
        print(f"\n✗ Failed to create pause file: {str(e)}")
        logger.error(f"Failed to create pause file: {e}")


def main():
    print_header()

    if not validate_credentials():
        sys.exit(1)

    if not check_disk_space():
        response = input("Continue anyway? (y/n): ").strip().lower()
        if response != 'y':
            sys.exit(1)

    print(f"✓ Credentials found for: {EMAIL}\n")

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
