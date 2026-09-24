"""Which parser outputs the golden fixtures pin, for this site.

Each project defines the same names so `capture_fixtures.py`,
`test_golden_parse.py` and `site_check.py` stay identical across the three
scrapers; only this adapter differs.
"""

from src.scraper import (  # noqa: E402
    LOGIN_PATH,
    _build_full_url,
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

# ── Live site check (tests/site_check.py) ──────────────────────────────────
# The login form fields _login_client posts. The CSRF token is optional
# there (sent only when the page has one), so it is not required here.
LOGIN_FIELDS = ("email", "password")
# Read by config.config; set as repository secrets for the monthly workflow.
CREDENTIAL_VARS = ("STO_EMAIL", "STO_PASSWORD")
# Long-running series that should outlive any one check. A slug that has
# gone is skipped, and series linked from the home page are tried after these.
PROBE_SLUGS = ("die-simpsons", "the-walking-dead", "breaking-bad")
# The index held 10,896 series in September 2026. Far below that, the
# catalogue parse is losing series rather than the site shrinking.
MIN_CATALOGUE = 5000
# Series pages carry subscribe and watchlist buttons (_detect_subscription_status).
HAS_ACCOUNT_BUTTONS = True


def login_url(site_url: str) -> str:
    return _build_full_url(site_url, LOGIN_PATH)


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
