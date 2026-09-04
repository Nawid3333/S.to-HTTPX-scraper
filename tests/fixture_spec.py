"""Which parser outputs the golden fixtures pin, for this site.

Each project defines the same two names so `capture_fixtures.py` and
`test_golden_parse.py` stay identical across the three scrapers; only this
adapter differs.
"""

from src.scraper import (  # noqa: E402
    _check_error_page,
    _detect_subscription_status,
    _extract_description_alt_title,
    _extract_season_links,
    _extract_title,
    _is_logged_in,
    _parse_episodes,
    make_doc,
)

SCRAPER_CLASS_NAME = "SToScraper"
SLUG_RE = r"/serie/([^/?#]+)"
SERIES_PATH = "/serie/{slug}"
CATALOGUE_PATH = "/serien"


def parse_all(html: str, slug: str, base_url: str) -> dict:
    """Run every parser this scraper applies to a page, as a plain dict.

    Plain data only: the recorded golden file predates the move off
    BeautifulSoup and was left untouched across it, so these tests re-parse
    every captured page with the lxml helpers and compare against what the
    soup ones produced.
    """
    doc = make_doc(html)
    subscribed, watchlist = _detect_subscription_status(doc)
    title = _extract_title(doc)
    return {
        "is_logged_in": _is_logged_in(doc),
        "error_page": _check_error_page(doc),
        "title": title,
        "alt_titles": _extract_description_alt_title(doc, title or ""),
        "subscribed": subscribed,
        "watchlist": watchlist,
        "season_links": [list(x) for x in _extract_season_links(doc, slug, base_url)],
        "episodes": _parse_episodes(html),
    }
