"""
S.TO Index Manager — manages the persistent series index.

Handles data merging, change detection, subscription/watchlist tracking,
and analytics. Ported from BS.to HTTPX index_manager with s.to extensions.
"""

import json
import logging
import os
import re
import shutil
import tempfile
from collections import defaultdict
from datetime import datetime

from config.config import SERIES_INDEX_FILE, DATA_DIR

logger = logging.getLogger(__name__)


def _create_file_backup(filepath):
    """Create a backup of a file (up to 3 generations kept)."""
    if not os.path.exists(filepath):
        return
    try:
        backup_dir = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        
        # Remove oldest backup if 3 already exist
        for i in range(3, 10):
            old_backup = os.path.join(backup_dir, f"{filename}.bak{i}")
            if os.path.exists(old_backup):
                try:
                    os.remove(old_backup)
                except OSError:
                    pass
        
        # Shift existing backups: .bak2 -> .bak3, .bak1 -> .bak2, original -> .bak1
        for i in range(2, 0, -1):
            src = os.path.join(backup_dir, f"{filename}.bak{i}")
            dst = os.path.join(backup_dir, f"{filename}.bak{i+1}")
            if os.path.exists(src):
                try:
                    shutil.move(src, dst)
                except OSError:
                    pass
        
        # Create new backup
        backup_path = os.path.join(backup_dir, f"{filename}.bak1")
        shutil.copy2(filepath, backup_path)
        logger.debug(f"Created backup: {backup_path}")
    except Exception as e:
        logger.warning(f"Could not create backup of {filepath}: {e}")


def _atomic_write_json(filepath, data):
    """Write JSON to file atomically via temp file + os.replace."""
    dirpath = os.path.dirname(filepath)
    os.makedirs(dirpath, exist_ok=True)
    
    if os.path.exists(filepath):
        _create_file_backup(filepath)
    
    fd, tmp_path = tempfile.mkstemp(dir=dirpath, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, filepath)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise

_SEASON_NUMBER_RE = re.compile(r'(staffel|season|s)\s*(\d+)', re.IGNORECASE)


def _validate_series_entry(series, title=''):
    """Validate that a series entry has the required structure."""
    if not isinstance(series, dict):
        logger.warning(f"Skipping invalid series entry (not dict): {title}")
        return False
    if not series.get('url'):
        logger.warning(f"Skipping series '{title}' - missing 'url' field")
        return False
    seasons = series.get('seasons')
    if seasons is not None and not isinstance(seasons, list):
        logger.warning(f"Skipping series '{title}' - 'seasons' must be list, got {type(seasons)}")
        return False
    for season in (seasons or []):
        if not isinstance(season, dict):
            continue
        episodes = season.get('episodes')
        if episodes is not None and not isinstance(episodes, list):
            logger.warning(f"Series '{title}' season '{season.get('season', '?')}' has invalid episodes type")
            season['episodes'] = []
    return True


def _find_series(new_data, title):
    """Look up a series by title in either a dict or list."""
    if isinstance(new_data, dict):
        return new_data.get(title)
    if isinstance(new_data, list):
        return next((s for s in new_data if s.get('title') == title), None)
    return None


def _get_season_stats(series, season_label):
    """Get (total_episodes, watched_episodes) for a specific season."""
    if not series:
        return 0, 0
    for s in series.get('seasons', []):
        if s.get('season') == season_label:
            eps = s.get('episodes', [])
            return len(eps), sum(1 for ep in eps if ep.get('watched', False))
    return 0, 0


def get_episode_counts(series):
    """Get (total_episodes, watched_episodes) across all seasons of a series."""
    total = 0
    watched = 0
    for season in series.get('seasons', []):
        eps = season.get('episodes', [])
        if season.get('ignored_episode_0'):
            eps = [ep for ep in eps if ep.get('number') != 0]
        total += len(eps)
        watched += sum(1 for ep in eps if ep.get('watched', False))
    return total, watched


def _order_series_entry(series):
    """Return a stable series dict with series-level metadata before seasons."""
    ordered = {
        'title': series.get('title', ''),
        'link': series.get('link', ''),
        'url': series.get('url', ''),
        'total_seasons': series.get('total_seasons', len(series.get('seasons', []))),
        'total_episodes': series.get('total_episodes', 0),
        'watched_episodes': series.get('watched_episodes', 0),
        'unwatched_episodes': series.get(
            'unwatched_episodes',
            series.get('total_episodes', 0) - series.get('watched_episodes', 0)
        ),
        'subscribed': series.get('subscribed'),
        'watchlist': series.get('watchlist'),
        'seasons': series.get('seasons', []),
    }
    if 'added_date' in series:
        ordered['added_date'] = series['added_date']
    if 'last_updated' in series:
        ordered['last_updated'] = series['last_updated']
    return ordered


def paginate_list(items, formatter, page_size=50):
    """Print items with pagination; Enter = next page, q = skip."""
    if not items:
        return
    total = len(items)
    idx = 0
    while idx < total:
        end = min(idx + page_size, total)
        for item in items[idx:end]:
            print(formatter(item))
        idx = end
        if idx < total:
            choice = input(f"  ({idx}/{total}) Enter = more, q = skip: ").strip().lower()
            if choice == 'q':
                print(f"  ... skipped {total - idx} remaining")
                break


def format_season_ep(season_label, ep_num):
    """Format season/episode for display (e.g. S1E5, [Specials] Ep 3)."""
    match = _SEASON_NUMBER_RE.search(str(season_label))
    if match:
        return f"S{match.group(2)}E{ep_num}"
    if str(season_label).strip().isdigit():
        return f"S{season_label}E{ep_num}"
    return f"[{season_label}] Ep {ep_num}"


def group_episodes_by_season(episode_list, new_data, prefix='[+]'):
    """Group (title, season, ep_number) tuples by season and format for display."""
    grouped = defaultdict(list)
    for item in episode_list:
        title, season, ep_num = item[0], item[1], item[2]
        grouped[(title, season)].append(ep_num)
    
    if isinstance(new_data, list):
        new_data_dict = {s.get('title'): s for s in new_data}
    elif isinstance(new_data, dict):
        new_data_dict = new_data
    else:
        new_data_dict = {}
    
    result = []
    for (title, season), ep_nums in sorted(grouped.items()):
        series = new_data_dict.get(title, {})
        total_in_season, watched_in_season = _get_season_stats(series, season)
        if total_in_season > 0:
            result.append(f"  {prefix} {title} [{season}]: {watched_in_season}/{total_in_season} episodes")
        else:
            for ep_num in sorted(ep_nums):
                result.append(f"  {prefix} {title} {format_season_ep(season, ep_num)}")
    
    return result


def _extract_slug(entry):
    """Extract series slug from an index entry's link or url field."""
    for field in ('link', 'url'):
        value = entry.get(field, '') if isinstance(entry, dict) else ''
        if not value or not isinstance(value, str):
            continue
        idx = value.find('/serie/')
        if idx == -1:
            continue
        slug = value[idx + len('/serie/'):].strip('/').split('/')[0]
        if slug:
            return slug
    return None


def show_vanished_series(old_data, all_discovered_slugs, scrape_scope):
    """Show informational notification about previously indexed series not found."""
    if scrape_scope not in ('all', 'new_only'):
        return []

    vanished = []
    corrupt_entries = []

    for title, entry in old_data.items():
        slug = _extract_slug(entry)
        if slug is None:
            corrupt_entries.append(title)
            continue
        if slug not in all_discovered_slugs:
            vanished.append(title)

    if corrupt_entries:
        print(f"\n⚠ {len(corrupt_entries)} index entry(s) have corrupt/missing URL data:")
        for t in corrupt_entries[:10]:
            print(f"  • {t}")
        if len(corrupt_entries) > 10:
            print(f"  ... and {len(corrupt_entries) - 10} more")

    if vanished:
        print(f"\n{'─'*70}")
        print(f"  [INFO] {len(vanished)} previously indexed series NOT found in current scrape:")
        print(f"{'─'*70}")
        for title in vanished[:20]:
            print(f"  • {title}  (not found on s.to)")
        if len(vanished) > 20:
            print(f"  ... and {len(vanished) - 20} more")
        print(f"{'─'*70}")
        print("  These series are preserved unchanged in the index.")

    return vanished


def _detect_housekeeping_changes(old_data, new_dict):
    """Predict ep0 removals and ignored flag changes that the merge will apply.

    Must be called BEFORE _merge_series_data because the merge mutates
    old_data season objects in place (shared references).

    Returns dict with 'added' and 'removed' lists of (title, [seasons]).
    'added'  = seasons where ep0 will be removed and ignored flag set.
    'removed' = seasons where the ignored flag will be cleared.
    """
    if isinstance(old_data, list):
        old_map = {s.get('title'): s for s in old_data if s and s.get('title')}
    else:
        old_map = dict(old_data) if old_data else {}

    added = {}   # title -> [season_labels]
    removed = {} # title -> [season_labels]
    for title, new_entry in new_dict.items():
        o_entry = old_map.get(title)
        if not o_entry:
            continue
        old_seasons = {s.get('season'): s for s in o_entry.get('seasons', [])}
        for new_season in new_entry.get('seasons', []):
            label = new_season.get('season')
            o_season = old_seasons.get(label)
            if not o_season:
                continue
            old_has_ep0 = any(ep.get('number') == 0 for ep in o_season.get('episodes', []))
            old_flag = o_season.get('ignored_episode_0', False)
            new_flag = new_season.get('ignored_episode_0', False)
            if (old_has_ep0 and new_flag) or (not old_flag and new_flag):
                added.setdefault(title, []).append(str(label))
            elif old_flag and not new_flag:
                removed.setdefault(title, []).append(str(label))
    return {'added': added, 'removed': removed}


def detect_changes(old_data, new_data):
    """Detect changes between old and new data. Returns dict of change lists.

    Tracks subscription and watchlist status changes in addition to
    episode changes.
    """
    changes = {
        "new_series": [],
        "new_episodes": [],
        "newly_watched": [],
        "newly_unwatched": [],
        "subscription_changes": [],
        "watchlist_changes": [],
    }
    
    if not old_data:
        old_data = []
    if not new_data:
        new_data = []
    
    old_titles = set(old_data.keys()) if isinstance(old_data, dict) else {s.get('title') for s in (old_data or []) if s and s.get('title')}
    new_titles = set(new_data.keys()) if isinstance(new_data, dict) else {s.get('title') for s in (new_data or []) if s and s.get('title')}
    
    if isinstance(old_data, list):
        old_data = {s.get('title'): s for s in (old_data or []) if s and s.get('title')}
    if isinstance(new_data, list):
        new_data = {s.get('title'): s for s in (new_data or []) if s and s.get('title')}
    
    # New series
    for title in new_titles - old_titles:
        if title:
            changes["new_series"].append(title)
    
    # Episode and subscription changes for existing series
    for title in old_titles & new_titles:
        try:
            old_series = old_data.get(title, {})
            new_series = new_data.get(title, {})
            
            if not old_series or not isinstance(old_series, dict):
                continue
            if not new_series or not isinstance(new_series, dict):
                continue
            
            # Subscription status changes
            old_sub = old_series.get('subscribed')
            new_sub = new_series.get('subscribed')
            if old_sub is not None and new_sub is not None and old_sub != new_sub:
                changes["subscription_changes"].append((title, old_sub, new_sub))
            
            old_wl = old_series.get('watchlist')
            new_wl = new_series.get('watchlist')
            if old_wl is not None and new_wl is not None and old_wl != new_wl:
                changes["watchlist_changes"].append((title, old_wl, new_wl))
            
            # Build map of (season, ep_number) -> watched status for old data
            old_eps = {}
            for season in old_series.get('seasons', []):
                if not season or not isinstance(season, dict):
                    continue
                s_label = season.get('season', '')
                for ep in season.get('episodes', []):
                    if not ep or not isinstance(ep, dict):
                        continue
                    ep_num = ep.get('number')
                    if ep_num is not None:
                        old_eps[(s_label, str(ep_num))] = bool(ep.get('watched', False))
            
            # Check new episodes and watch status changes
            for season in new_series.get('seasons', []):
                if not season or not isinstance(season, dict):
                    continue
                s_label = season.get('season', '')
                for ep in season.get('episodes', []):
                    if not ep or not isinstance(ep, dict):
                        continue
                    ep_num = ep.get('number')
                    if ep_num is None:
                        continue
                    ep_key = (s_label, str(ep_num))
                    new_watched = bool(ep.get('watched', False))
                    
                    if ep_key not in old_eps:
                        changes["new_episodes"].append((title, s_label, ep_num))
                    elif old_eps[ep_key] != new_watched:
                        if not old_eps[ep_key] and new_watched:
                            changes["newly_watched"].append((title, s_label, ep_num))
                        elif old_eps[ep_key] and not new_watched:
                            changes["newly_unwatched"].append((title, s_label, ep_num))
        except Exception as e:
            logger.debug(f"Error detecting changes for '{title}': {e}")
            continue
    
    return changes


def show_changes(changes, include_unwatched=True, include_watched=True, new_data=None):
    """Print formatted change summary with pagination."""
    total = 0
    for k, v in changes.items():
        if k == 'newly_unwatched' and not include_unwatched:
            continue
        if k == 'newly_watched' and not include_watched:
            continue
        total += len(v)
    if total == 0:
        return 0

    print("\n" + "="*70)
    print("  CHANGES DETECTED")
    print("="*70)

    if changes["new_series"]:
        print(f"\n[NEW SERIES] ({len(changes['new_series'])})")
        def format_new_series(title):
            if not new_data:
                return f"  + {title}"
            series = _find_series(new_data, title)
            if not series:
                return f"  + {title}"
            watched = series.get('watched_episodes', 0)
            total_ep = series.get('total_episodes', 0)
            sub = series.get('subscribed')
            wl = series.get('watchlist')
            sub_info = ""
            if sub is not None or wl is not None:
                parts = []
                if sub is not None:
                    parts.append(f"Sub:{'✓' if sub else '✗'}")
                if wl is not None:
                    parts.append(f"WL:{'✓' if wl else '✗'}")
                sub_info = f" ({' '.join(parts)})"
            return f"  + {title}: {watched}/{total_ep} watched{sub_info}"
        paginate_list(changes["new_series"], format_new_series)

    if changes["new_episodes"]:
        if new_data:
            grouped_lines = group_episodes_by_season([(x[0], x[1], x[2]) for x in changes["new_episodes"]], new_data)
            print(f"\n[NEW EPISODES] ({len(changes['new_episodes'])})")
            paginate_list(grouped_lines, lambda line: line)
        else:
            print(f"\n[NEW EPISODES] ({len(changes['new_episodes'])})")
            paginate_list(changes["new_episodes"], lambda x: f"  + {x[0]} [{x[1]}] Ep {x[2]}")

    if changes["newly_watched"] and include_watched:
        print(f"\n[NEWLY WATCHED] ({len(changes['newly_watched'])} episodes)")
        watched_lines = group_episodes_by_season(changes["newly_watched"], new_data)
        paginate_list(watched_lines, lambda line: line)

    if changes.get("newly_unwatched") and include_unwatched:
        print(f"\n[SITE REPORTS UNWATCHED] ({len(changes['newly_unwatched'])} episodes)")
        unwatched_lines = group_episodes_by_season(changes["newly_unwatched"], new_data, prefix='[!]')
        paginate_list(unwatched_lines, lambda line: line)

    if changes.get("subscription_changes"):
        print(f"\n[SUBSCRIPTION CHANGES] ({len(changes['subscription_changes'])})")
        for title, old_val, new_val in changes["subscription_changes"]:
            old_str = "subscribed" if old_val else "not subscribed"
            new_str = "subscribed" if new_val else "not subscribed"
            print(f"  ~ {title}: {old_str} → {new_str}")

    if changes.get("watchlist_changes"):
        print(f"\n[WATCHLIST CHANGES] ({len(changes['watchlist_changes'])})")
        for title, old_val, new_val in changes["watchlist_changes"]:
            old_str = "on watchlist" if old_val else "not on watchlist"
            new_str = "on watchlist" if new_val else "not on watchlist"
            print(f"  ~ {title}: {old_str} → {new_str}")

    print("\n" + "="*70)
    return total


def _load_existing_index():
    """Load the current series index from disk."""
    if not os.path.exists(SERIES_INDEX_FILE):
        logger.info(f"No existing index found at {SERIES_INDEX_FILE}")
        return []
    try:
        with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, (list, dict)):
            print("⚠ Index file is not a valid list or dict, ignoring.")
            return []
        logger.info(f"Loaded index from {SERIES_INDEX_FILE} ({len(data)} entries)")
        return data
    except json.JSONDecodeError as e:
        print(f"[ERROR] Index file corrupted: {e}")
        return []
    except OSError as e:
        print(f"[ERROR] Cannot read index file: {e}")
        return []
    except Exception as e:
        print(f"⚠ Error loading index: {str(e)}")
        return []


def _prompt_watch_status_changes(changes, new_dict):
    """Prompt user to confirm watched/unwatched flips."""
    allow_watched = False
    allow_unwatched = False

    if changes["newly_watched"]:
        print(f"\n[OK] {len(changes['newly_watched'])} episode(s) would change from UNWATCHED to WATCHED")
        print("-"*70)
        grouped = defaultdict(list)
        for x in changes["newly_watched"]:
            grouped[(x[0], x[1])].append(x[2])
        for (title, season), ep_nums in grouped.items():
            series = new_dict.get(title)
            total_in_season, watched_in_season = _get_season_stats(series, season)
            if total_in_season > 0:
                print(f"  [+] {title} [{season}]: {watched_in_season}/{total_in_season} episodes")
            else:
                print(f"  [+] {title} [{season}]: {len(ep_nums)} episode(s)")
        print("-"*70)
        if input("\nAllow these episodes to be marked as WATCHED? (y/n): ").strip().lower() == 'y':
            allow_watched = True

    if changes["newly_unwatched"]:
        print(f"\n[WARN] {len(changes['newly_unwatched'])} episode(s) would change from WATCHED to UNWATCHED")
        print("-"*70)
        grouped = defaultdict(list)
        for x in changes["newly_unwatched"]:
            grouped[(x[0], x[1])].append(x[2])
        for (title, season), ep_nums in grouped.items():
            series = new_dict.get(title)
            total_in_season, watched_in_season = _get_season_stats(series, season)
            if total_in_season > 0:
                print(f"  [!] {title} [{season}]: {watched_in_season}/{total_in_season} episodes")
            else:
                print(f"  [!] {title} [{season}]: {len(ep_nums)} episode(s)")
        print("-"*70)
        if input("\nAllow these episodes to be marked as UNWATCHED? (y/n): ").strip().lower() == 'y':
            allow_unwatched = True

    return allow_watched, allow_unwatched


def _prompt_subscription_changes(changes):
    """Prompt user to confirm subscription/watchlist changes."""
    allow_sub = False
    allow_wl = False

    if changes.get("subscription_changes"):
        print(f"\n[SUBSCRIPTION] {len(changes['subscription_changes'])} series subscription status changed")
        print("-"*70)
        for title, old_val, new_val in changes["subscription_changes"]:
            old_str = "subscribed" if old_val else "not subscribed"
            new_str = "subscribed" if new_val else "not subscribed"
            print(f"  ~ {title}: {old_str} → {new_str}")
        print("-"*70)
        if input("\nAllow subscription status changes? (y/n): ").strip().lower() == 'y':
            allow_sub = True

    if changes.get("watchlist_changes"):
        print(f"\n[WATCHLIST] {len(changes['watchlist_changes'])} series watchlist status changed")
        print("-"*70)
        for title, old_val, new_val in changes["watchlist_changes"]:
            old_str = "on watchlist" if old_val else "not on watchlist"
            new_str = "on watchlist" if new_val else "not on watchlist"
            print(f"  ~ {title}: {old_str} → {new_str}")
        print("-"*70)
        if input("\nAllow watchlist status changes? (y/n): ").strip().lower() == 'y':
            allow_wl = True

    return allow_sub, allow_wl


def _merge_series_data(old_data, new_dict, allow_watched, allow_unwatched,
                       allow_sub=False, allow_wl=False):
    """Merge new scraped data into the existing index.

    Preserves all existing series and only applies watched/unwatched
    flips and subscription changes when the corresponding flag is True.
    """
    if isinstance(old_data, list):
        merged = {s.get('title'): s for s in old_data}
    else:
        merged = dict(old_data)

    for title, new_entry in new_dict.items():
        if title not in merged:
            new_entry['added_date'] = datetime.now().isoformat()
            merged[title] = _order_series_entry(new_entry)
            continue

        old_entry = merged[title]
        old_seasons = {s.get('season'): s for s in old_entry.get('seasons', [])}

        for new_season in new_entry.get('seasons', []):
            season_label = new_season.get('season')
            if season_label in old_seasons:
                old_eps = {str(ep.get('number')): ep for ep in old_seasons[season_label].get('episodes', [])}
                for new_ep in new_season.get('episodes', []):
                    ep_num = str(new_ep.get('number'))
                    if ep_num in old_eps:
                        old_watched = old_eps[ep_num].get('watched', False)
                        new_watched = new_ep.get('watched', False)
                        if allow_watched and (not old_watched and new_watched):
                            new_ep['watched'] = True
                        elif allow_unwatched and (old_watched and not new_watched):
                            new_ep['watched'] = False
                        else:
                            new_ep['watched'] = old_watched
                    old_eps[ep_num] = new_ep
                # Remove leftover episode 0 if this season is in the ignore list
                if new_season.get('ignored_episode_0'):
                    old_eps.pop('0', None)
                old_seasons[season_label]['episodes'] = sorted(
                    old_eps.values(), key=lambda e: e.get('number', 0)
                )
                # Sync ignored_episode_0: add or remove based on new data
                if new_season.get('ignored_episode_0'):
                    old_seasons[season_label]['ignored_episode_0'] = True
                else:
                    old_seasons[season_label].pop('ignored_episode_0', None)
            else:
                old_seasons[season_label] = new_season

        old_entry['seasons'] = list(old_seasons.values())
        old_entry['total_seasons'] = len(old_entry['seasons'])
        total_eps, watched_eps = get_episode_counts(old_entry)
        old_entry['watched_episodes'] = watched_eps
        old_entry['total_episodes'] = total_eps
        old_entry['unwatched_episodes'] = old_entry['total_episodes'] - old_entry['watched_episodes']
        old_entry['url'] = new_entry.get('url', old_entry.get('url'))
        old_entry['last_updated'] = datetime.now().isoformat()

        # Update subscription/watchlist status if allowed and new value is not None
        new_sub = new_entry.get('subscribed')
        new_wl = new_entry.get('watchlist')
        if new_sub is not None:
            if allow_sub or old_entry.get('subscribed') is None:
                old_entry['subscribed'] = new_sub
        if new_wl is not None:
            if allow_wl or old_entry.get('watchlist') is None:
                old_entry['watchlist'] = new_wl

        merged[title] = _order_series_entry(old_entry)

    return merged


def confirm_and_save_changes(new_data, description="data"):
    """Show changes, prompt for confirmation, merge, and save."""
    old_data = _load_existing_index()

    if isinstance(new_data, list):
        new_dict = {s.get('title'): s for s in new_data}
    else:
        new_dict = dict(new_data)

    changes = detect_changes(old_data, new_dict)
    logger.info(f"Detected changes: { {k: len(v) for k,v in changes.items()} }")

    allow_watched, allow_unwatched = _prompt_watch_status_changes(changes, new_dict)
    allow_sub, allow_wl = _prompt_subscription_changes(changes)

    if not allow_watched:
        changes["newly_watched"] = []
    if not allow_unwatched:
        changes["newly_unwatched"] = []
    if not allow_sub:
        changes["subscription_changes"] = []
    if not allow_wl:
        changes["watchlist_changes"] = []

    # Detect housekeeping changes BEFORE merge (merge mutates old_data in place)
    housekeeping = _detect_housekeeping_changes(old_data, new_dict)

    merged = _merge_series_data(old_data, new_dict, allow_watched, allow_unwatched,
                                allow_sub, allow_wl)

    main_changes = sum(len(v) for k, v in changes.items() if k != 'newly_unwatched')
    if allow_unwatched:
        main_changes += len(changes['newly_unwatched'])

    if main_changes == 0:
        has_housekeeping = housekeeping['added'] or housekeeping['removed']
        if has_housekeeping:
            total = sum(len(v) for v in housekeeping['added'].values()) + \
                    sum(len(v) for v in housekeeping['removed'].values())
            print(f"\n⚠ {total} ignored episode 0 change(s):")
            print(f"{'─'*70}")
            if housekeeping['added']:
                print("  Remove ep0 & mark ignored:")
                for title in sorted(housekeeping['added']):
                    seasons = ', '.join(housekeeping['added'][title])
                    print(f"    • {title}  [S{seasons}]")
            if housekeeping['removed']:
                print("  Unmark ignored (ep0 no longer present):")
                for title in sorted(housekeeping['removed']):
                    seasons = ', '.join(housekeeping['removed'][title])
                    print(f"    • {title}  [S{seasons}]")
            print(f"{'─'*70}")
            if input("Apply these changes? (y/n): ").strip().lower() != 'y':
                print("✗ Changes discarded.")
                return False
            try:
                series_list = [_order_series_entry(series) for series in merged.values()]
                _atomic_write_json(SERIES_INDEX_FILE, series_list)
                print(f"✓ Saved {len(series_list)} series to index")
            except Exception as e:
                print(f"✗ Failed to save: {str(e)}")
                return False
        else:
            print(f"\n✓ {description} already up to date.")
        return True

    show_changes(changes, include_unwatched=allow_unwatched, include_watched=allow_watched, new_data=new_dict)

    if input(f"\nSave these changes? (y/n): ").strip().lower() != 'y':
        print("✗ Changes discarded. Nothing saved.")
        return False

    try:
        series_list = [_order_series_entry(series) for series in merged.values()]
        _atomic_write_json(SERIES_INDEX_FILE, series_list)
        print(f"✓ Saved {len(series_list)} series to index")
        return True
    except Exception as e:
        print(f"✗ Failed to save: {str(e)}")
        return False


class IndexManager:
    def __init__(self):
        self.series_index = {}
        self.ensure_data_dir()
        self.load_index()

    def ensure_data_dir(self):
        os.makedirs(DATA_DIR, exist_ok=True)

    def load_index(self):
        """Load series index from JSON with corruption detection."""
        self.series_index = {}
        if not os.path.exists(SERIES_INDEX_FILE):
            logger.info(f"No existing index found at {SERIES_INDEX_FILE}")
            return
        try:
            with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                self.series_index = {item.get("title"): item for item in data if item.get("title")}
            elif isinstance(data, dict):
                first_item = next(iter(data.values()), None)
                if first_item and isinstance(first_item, dict) and first_item.get('title'):
                    self.series_index = data
                else:
                    self.series_index = {item.get("title"): item for item in data.values() if isinstance(item, dict) and item.get("title")}
            else:
                self.series_index = {}
            
            validated = {}
            for title, series in self.series_index.items():
                if _validate_series_entry(series, title):
                    validated[title] = series
            self.series_index = validated
            
            print(f"[OK] Loaded {len(self.series_index)} series from index")
            logger.info(f"Loaded {len(self.series_index)} series from {SERIES_INDEX_FILE}")
        except json.JSONDecodeError as e:
            print(f"[ERROR] Index file corrupted: {e}")
            self.series_index = {}
        except OSError as e:
            print(f"[ERROR] Cannot read index file: {e}")
            self.series_index = {}
        except Exception as e:
            print(f"[WARN] Error loading index: {str(e)}")
            self.series_index = {}

    def get_statistics(self):
        """Return detailed analytics about the series index."""
        series_with_progress = self.get_series_with_progress()
        total = len(series_with_progress)
        
        if total == 0:
            return {
                "total_series": 0,
                "watched": 0,
                "unwatched": 0,
                "watched_percentage": 0.0,
                "empty_series": 0
            }
        
        watched = sum(1 for s in series_with_progress if not s['is_incomplete'])
        unwatched = total - watched
        empty_count = len([s for s in self.series_index.values() if s.get('empty', False)])
        
        completion_percentages = [s['completion'] for s in series_with_progress]
        avg_completion = round(sum(completion_percentages) / total, 2)
        
        total_episodes = sum(s['total_episodes'] for s in series_with_progress)
        watched_episodes = sum(s['watched_episodes'] for s in series_with_progress)
        avg_episodes_per_series = round(total_episodes / total, 1) if total > 0 else 0
        
        completion_ranges = {
            "0-25%": sum(1 for p in completion_percentages if 0 <= p < 25),
            "25-50%": sum(1 for p in completion_percentages if 25 <= p < 50),
            "50-75%": sum(1 for p in completion_percentages if 50 <= p < 75),
            "75-99%": sum(1 for p in completion_percentages if 75 <= p < 100),
            "100%": sum(1 for p in completion_percentages if p == 100)
        }
        
        ongoing_only = [s for s in series_with_progress if 0 < s['completion'] < 100]
        sorted_ongoing = sorted(ongoing_only, key=lambda x: x['completion'], reverse=True)
        most_completed = sorted_ongoing[:5]
        least_completed = sorted_ongoing[-5:] if sorted_ongoing else []

        completed_count = watched
        ongoing_count = len(ongoing_only)
        not_started_count = sum(1 for s in series_with_progress if s['watched_episodes'] == 0)

        # Ignored episode 0 count
        ignored_ep0_count = sum(
            1 for s in self.series_index.values()
            for season in s.get('seasons', [])
            if season.get('ignored_episode_0')
        )

        # Subscription/watchlist counts
        subscribed_count = sum(1 for s in self.series_index.values() if s.get('subscribed'))
        watchlist_count = sum(1 for s in self.series_index.values() if s.get('watchlist'))
        both_count = sum(1 for s in self.series_index.values() if s.get('subscribed') and s.get('watchlist'))
        
        return {
            "total_series": total,
            "watched": watched,
            "unwatched": unwatched,
            "watched_percentage": round((watched / total * 100), 2),
            "empty_series": empty_count,
            "completed_count": completed_count,
            "ongoing_count": ongoing_count,
            "not_started_count": not_started_count,
            "average_completion": avg_completion,
            "total_episodes": total_episodes,
            "watched_episodes": watched_episodes,
            "unwatched_episodes": total_episodes - watched_episodes,
            "average_episodes_per_series": avg_episodes_per_series,
            "completion_distribution": completion_ranges,
            "subscribed_count": subscribed_count,
            "watchlist_count": watchlist_count,
            "both_subscribed_and_watchlist": both_count,
            "ignored_episode_0_seasons": ignored_ep0_count,
            "most_completed_series": [
                {"title": s['title'], "completion": s['completion'], "progress": f"{s['watched_episodes']}/{s['total_episodes']}"}
                for s in most_completed
            ],
            "least_completed_series": [
                {"title": s['title'], "completion": s['completion'], "progress": f"{s['watched_episodes']}/{s['total_episodes']}"}
                for s in least_completed
            ]
        }
        
    def get_full_report(self, filter_subscribed=None, filter_watchlist=None):
        """Generate a comprehensive report with categories and insights.

        Args:
            filter_subscribed: If True, only include subscribed series.
                               If False, only non-subscribed. None = no filter.
            filter_watchlist:  If True, only include watchlist series.
                               If False, only non-watchlist. None = no filter.
        """
        series_progress = self.get_series_with_progress()
        stats = self.get_statistics()

        # Apply subscription/watchlist filters
        if filter_subscribed is not None:
            series_progress = [s for s in series_progress if s['subscribed'] == filter_subscribed]
        if filter_watchlist is not None:
            series_progress = [s for s in series_progress if s['watchlist'] == filter_watchlist]

        watched_series = [s for s in series_progress if not s['is_incomplete']]
        ongoing_series = [s for s in series_progress if s['is_incomplete'] and s['watched_episodes'] > 0]
        not_started_series = [s for s in series_progress if s['is_incomplete'] and s['watched_episodes'] == 0]
        
        ongoing_sorted = sorted(ongoing_series, key=lambda x: x['completion'], reverse=True)
        ongoing_titles = [s['title'] for s in ongoing_sorted]
        not_started_titles = sorted([s['title'] for s in not_started_series])
        
        episode_ranges = {
            "short_series": [s['title'] for s in series_progress if s['total_episodes'] <= 5],
            "medium_series": [s['title'] for s in series_progress if 6 <= s['total_episodes'] <= 25],
            "long_series": [s['title'] for s in series_progress if s['total_episodes'] > 25]
        }
        
        completion_insights = {
            "high_completion_threshold": 80,
            "near_completion": [s['title'] for s in ongoing_sorted if 80 <= s['completion'] < 100][:10],
            "stalled_series": [s['title'] for s in ongoing_sorted if s['completion'] < 25][:10]
        }
        
        report = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "total_series_in_index": len(self.series_index),
                "active_series": len(series_progress),
                "statistics": stats
            },
            "categories": {
                "watched": {
                    "count": len(watched_series),
                    "titles": sorted([s['title'] for s in watched_series])
                },
                "ongoing": {
                    "count": len(ongoing_series),
                    "titles": ongoing_titles,
                    "details": [{"title": s['title'], "completion": s['completion'], 
                               "progress": f"{s['watched_episodes']}/{s['total_episodes']}"}
                              for s in ongoing_sorted[:20]]
                },
                "not_started": {
                    "count": len(not_started_series),
                    "titles": not_started_titles
                }
            },
            "insights": {
                "completion_distribution": stats.get("completion_distribution", {}),
                "episode_ranges": episode_ranges,
                "near_completion": completion_insights["near_completion"],
                "stalled_series": completion_insights["stalled_series"],
                "most_completed": stats.get("most_completed_series", [])[:10],
                "least_completed": stats.get("least_completed_series", [])[:10]
            },
            "raw_data": {
                "all_series": self.series_index,
                "series_progress": series_progress
            }
        }
        return report
        
    def get_series_with_progress(self, sort_by='completion', reverse=False):
        """Return series list with episode progress and completion percentages."""
        series_list = []
        for s in self.series_index.values():
            total_eps = 0
            watched_eps = 0
            for season in s.get('seasons', []):
                eps = season.get('episodes', [])
                if season.get('ignored_episode_0'):
                    eps = [ep for ep in eps if ep.get('number') != 0]
                total_eps += len(eps)
                watched_eps += sum(1 for ep in eps if ep.get('watched', False))
            is_incomplete = (total_eps == 0) or (watched_eps < total_eps)
            completion = round((watched_eps / total_eps) * 100, 2) if total_eps > 0 else 0.0
            series_list.append({
                'title': s.get('title', ''),
                'watched_episodes': watched_eps,
                'total_episodes': total_eps,
                'is_incomplete': is_incomplete,
                'completion': completion,
                'empty': s.get('empty', False),
                'subscribed': s.get('subscribed', False),
                'watchlist': s.get('watchlist', False),
            })
        if sort_by:
            series_list.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)
        return series_list
