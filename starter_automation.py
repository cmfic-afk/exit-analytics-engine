"""
Retirement Maxing — Low-touch AI content engine
================================================

A single-file automation that runs the daily pipeline:
  1. Fetch new items from a list of RSS feeds
  2. Score each item for newsworthiness with Claude Haiku (cheap)
  3. If 5+ items score above threshold, draft today's newsletter with Sonnet
  4. Self-grade the draft; either save to Beehiiv (manual approve) or skip
  5. Twice a week, generate N programmatic SEO pages and commit them to the site repo

Designed to run as a GitHub Actions cron (free) or on any cheap VPS.

Setup checklist:
  1. pip install anthropic feedparser requests python-dotenv
  2. Fill in the CONFIG block below.
  3. Set environment variables: ANTHROPIC_API_KEY, BEEHIIV_API_KEY, BEEHIIV_PUBLICATION_ID
  4. Run locally: `python starter_automation.py --mode daily`
  5. Once it works, add the workflow file (see end of this file) and push.

Author: Claude (built for MLK)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import feedparser  # type: ignore
import requests
from anthropic import Anthropic

# ---------------------------------------------------------------------------
# CONFIG — fill these in. This is the only block you ever edit.
# ---------------------------------------------------------------------------

CONFIG = {
    # Your niche
    "niche": "Personal financial independence and early retirement (FIRE)",
    "audience": (
        "high-earning professionals and dual-income households pursuing financial "
        "independence and early retirement — readers who already know the basics "
        "(index funds, savings rates, tax-advantaged accounts) and want data-driven "
        "analysis, not motivational platitudes. HENRYs, FatFIRE/ChubbyFIRE candidates, "
        "and self-directed investors building a path out of W-2 work."
    ),
    "newsletter_name": "Exit Analytics",
    "newsletter_voice": (
        "Numbers-first. Skip the 'live below your means' sermons — assume readers "
        "have the basics covered. Each issue surfaces specific data, after-tax math, "
        "or strategies that move someone closer to financial independence. "
        "Direct, confident, slightly dry. One-line opinion per item, max. "
        "Never use 'in today's economy', 'experts say', 'it depends on your goals', "
        "or 'consult a financial advisor' boilerplate. "
        "Open with the most surprising figure or insight, not a recap."
    ),
    "brand_voice": (
        "Analytical and specific. Real numbers, real after-tax math, real fund tickers "
        "and expense ratios. Cite specific tax code sections (e.g. 'Section 72(t) SEPP', "
        "'Rule of 55'), real Treasury yields, real ETF expense ratios. "
        "Write like a CFA who hates fluff explaining to a peer. "
        "No motivational filler, no hedging, no boilerplate disclaimers (except where "
        "genuinely required for compliance)."
    ),

    # RSS feeds — curated for primary-data and high-signal commentary.
    # Each feed will hit the scorer; bad days will surface warnings in the logs and
    # the engine continues. Verify any URL flagged as failing in the logs.
    "rss_feeds": [
        # ============ Primary government / regulatory sources ============
        # These are the news that actually moves FIRE math. Rate decisions, tax
        # law changes, contribution limits, official inflation prints.
        "https://www.federalreserve.gov/feeds/press_all.xml",         # Fed press releases
        "https://www.bls.gov/feed/news_release.rss",                  # BLS (CPI, jobs reports)
        "https://www.irs.gov/pub/irs-utl/irsnewsfeed.xml",            # IRS news (verify URL)

        # ============ Data-driven analysis blogs (the high-signal core) ============
        "https://ofdollarsanddata.com/feed/",        # Nick Maggiulli — original quantitative analysis
        "https://awealthofcommonsense.com/feed/",    # Ben Carlson — daily, data-grounded
        "https://earlyretirementnow.com/feed/",      # Karsten — heavy math on withdrawal rates
        "https://www.madfientist.com/feed/",         # Mad Fientist — infrequent but high quality
        "https://www.physicianonfire.com/feed/",     # High-earner focus
        "https://www.whitecoatinvestor.com/feed/",   # WCI — extremely tactical, high-earner

        # ============ Tax & policy analysis ============
        "https://taxfoundation.org/feed/",           # Tax policy think tank

        # ============ Bogleheads forum (Investing Theory only — high-signal subset) ============
        "https://www.bogleheads.org/forum/feed.php?f=10",  # Investing Theory subforum

        # ============ Reddit (high-signal subset only) ============
        # Dropped: r/Fire, r/leanFIRE, r/fatFIRE, r/ChubbyFIRE, r/Bogleheads
        # (anecdotal noise; r/financialindependence and r/HENRYfinance carry the signal)
        "https://www.reddit.com/r/financialindependence/.rss",
        "https://www.reddit.com/r/HENRYfinance/.rss",
    ],

    # Affiliate products — empty until you're approved into the programs below.
    # The engine still runs and produces newsletters without affiliates; it just won't
    # insert affiliate links. Add entries as your applications come back approved.
    #
    # Programs to apply for (priority order — most pay $50-200+ per signup):
    #   Brokerages / dashboards:  M1 Finance, Empower, Wealthfront, Betterment, SoFi
    #   Real estate platforms:    Fundrise, Arrived, RealtyMogul
    #   Planning tools:           ProjectionLab, Boldin (formerly NewRetirement)
    #   Tax software:             FreeTaxUSA, TurboTax (via affiliate networks)
    # Most listed on Impact (impact.com) or PartnerStack (partnerstack.com).
    "affiliate_products": [
        # Example shape — uncomment + replace placeholders after each application is approved:
        # {"id": "m1_finance",     "name": "M1 Finance",     "category": "investing platform",   "url": "https://m1.finance/REPLACE_ME"},
        # {"id": "fundrise",       "name": "Fundrise",       "category": "real estate",          "url": "https://fundrise.com/r/REPLACE_ME"},
        # {"id": "projectionlab",  "name": "ProjectionLab",  "category": "planning tool",        "url": "https://projectionlab.com/?ref=REPLACE_ME"},
        # {"id": "empower",        "name": "Empower",        "category": "financial dashboard",  "url": "https://empower.com/REPLACE_ME"},
    ],

    # Behaviour knobs — start permissive, tighten as the engine matures
    "score_threshold": 55,           # items below this are dropped (raise to 65-70 once mature)
    "min_items_to_send": 3,          # fewer than this and we skip the day (raise to 5 once mature)
    "publish_grade_threshold": 60,   # newsletter draft must self-grade >= this to send
    "pages_per_seo_run": 3,          # how many SEO pages to generate per run
    "models": {
        "cheap": "claude-haiku-4-5-20251001",
        "premium": "claude-sonnet-4-6",
    },

    # Paths
    "state_file": ".automation_state.json",
    "seo_data_file": "seo_pages.json",
    "seo_output_dir": "src/content/pages",  # where Astro will look for the generated pages
}

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("retirement-maxing")

_CLAUDE_CLIENT = None  # lazy-init so market_data mode works without ANTHROPIC_API_KEY
BEEHIIV_KEY = os.environ.get("BEEHIIV_API_KEY", "")
BEEHIIV_PUB = os.environ.get("BEEHIIV_PUBLICATION_ID", "")
FRED_KEY = os.environ.get("FRED_API_KEY", "")  # Optional; market-data fields gracefully degrade if missing


def _claude() -> Anthropic:
    global _CLAUDE_CLIENT
    if _CLAUDE_CLIENT is None:
        _CLAUDE_CLIENT = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
    return _CLAUDE_CLIENT


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class NewsItem:
    title: str
    url: str
    summary: str
    source: str
    published: str
    score: int = 0
    one_line_take: str = ""
    affiliate_url: str | None = None


@dataclass
class MarketData:
    """A week's snapshot of the numbers that drive the FIRE math.

    Note: ETF price data was intentionally removed. Free sources (Stooq, Yahoo) either
    block cloud IPs or only provide price (no yield/distribution), which would make us
    a basic data-aggregator rather than an analytical publication. If a specific Move
    template ever needs live ETF yields, add a paid source (Polygon/Alpha Vantage) then.
    """
    # Treasury yields
    treasury_3mo: float | None = None
    treasury_2y: float | None = None
    treasury_10y: float | None = None
    treasury_30y: float | None = None
    treasury_10y_prior_week: float | None = None
    # Macro
    fed_funds: float | None = None
    cpi_yoy: float | None = None       # core CPI YoY %
    mortgage_30y: float | None = None
    # Meta
    as_of: str = ""
    sources: list = field(default_factory=list)

    @property
    def treasury_10y_change(self) -> float | None:
        if self.treasury_10y is not None and self.treasury_10y_prior_week is not None:
            return round(self.treasury_10y - self.treasury_10y_prior_week, 2)
        return None


@dataclass
class State:
    seen_urls: set[str] = field(default_factory=set)
    last_seo_run: str = ""
    last_newsletter_send: str = ""

    @classmethod
    def load(cls, path: str) -> "State":
        if Path(path).exists():
            data = json.loads(Path(path).read_text())
            return cls(
                seen_urls=set(data.get("seen_urls", [])),
                last_seo_run=data.get("last_seo_run", ""),
                last_newsletter_send=data.get("last_newsletter_send", ""),
            )
        return cls()

    def save(self, path: str) -> None:
        Path(path).write_text(json.dumps({
            "seen_urls": sorted(self.seen_urls),
            "last_seo_run": self.last_seo_run,
            "last_newsletter_send": self.last_newsletter_send,
        }, indent=2))


# ---------------------------------------------------------------------------
# Claude helpers
# ---------------------------------------------------------------------------

def claude_json(model: str, prompt: str, max_tokens: int = 4096) -> dict:
    """Call Claude and parse a JSON response. Retries on transient errors."""
    for attempt in range(3):
        try:
            resp = _claude().messages.create(
                model=model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip()
            # Strip code fences if Claude wrapped its JSON
            if text.startswith("```"):
                text = text.split("```", 2)[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            return json.loads(text)
        except (json.JSONDecodeError, requests.RequestException) as e:
            log.warning("claude_json attempt %d failed: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
    raise RuntimeError("claude_json failed after 3 attempts")


# ---------------------------------------------------------------------------
# Market data fetchers (Treasury, Fed, CPI, ETFs)
# ---------------------------------------------------------------------------
#
# All sources are free; one (FRED) requires a free API key (30-second signup
# at https://fred.stlouisfed.org/docs/api/api_key.html). If FRED_API_KEY is
# unset, the macro fields gracefully degrade to None and the briefing still
# produces; it just omits those numbers.
#
# Sources:
#   - Treasury yields: FRED (DGS3MO, DGS2, DGS10, DGS30)
#   - Fed funds, CPI, mortgage: FRED (DFF, CPILFESL, MORTGAGE30US)
# ---------------------------------------------------------------------------


def fetch_treasury_yields(market: MarketData) -> None:
    """Fetch Treasury constant-maturity yields via FRED. Requires FRED_API_KEY.

    The fiscaldata.treasury.gov endpoint was unstable (path changes); FRED is the
    source the Federal Reserve itself uses and stays consistent.
    """
    if not FRED_KEY:
        log.info("FRED_API_KEY not set — skipping Treasury yield fetch.")
        return
    series_to_attr = {
        "DGS3MO": "treasury_3mo",
        "DGS2": "treasury_2y",
        "DGS10": "treasury_10y",
        "DGS30": "treasury_30y",
    }
    for series_id, attr in series_to_attr.items():
        val = fetch_fred_series(series_id)
        if val is not None:
            setattr(market, attr, val)
    # Prior-week 10Y for week-over-week change calc
    prior = fetch_fred_series_prior("DGS10", days_back=7)
    if prior is not None:
        market.treasury_10y_prior_week = prior
    if market.treasury_10y is not None:
        market.as_of = datetime.now(timezone.utc).date().isoformat()
        market.sources.append("US Treasury yields via Federal Reserve Economic Data (FRED)")


def fetch_fred_series_prior(series_id: str, days_back: int = 7) -> float | None:
    """Find the observation closest to N days before today for a daily series."""
    if not FRED_KEY:
        return None
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
        "&sort_order=desc&limit=20"
    )
    try:
        from datetime import timedelta
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        obs = r.json().get("observations", [])
        if not obs:
            return None
        target = datetime.now(timezone.utc).date() - timedelta(days=days_back)
        best, best_diff = None, 999
        for o in obs:
            try:
                d = datetime.strptime(o["date"], "%Y-%m-%d").date()
                diff = abs((d - target).days)
                if diff < best_diff and o.get("value") not in (".", "", None):
                    best, best_diff = float(o["value"]), diff
            except (ValueError, KeyError):
                continue
        return best
    except Exception as e:
        log.warning("FRED prior series fetch failed for %s: %s", series_id, e)
        return None


def fetch_fred_series(series_id: str, units: str = "lin") -> float | None:
    """Pull the latest observation from a FRED series. Returns None if no key or on error."""
    if not FRED_KEY:
        return None
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
        f"&sort_order=desc&limit=1&units={units}"
    )
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        obs = r.json().get("observations", [])
        if obs and obs[0].get("value") not in (".", "", None):
            return float(obs[0]["value"])
    except Exception as e:
        log.warning("FRED fetch failed for %s: %s", series_id, e)
    return None


def fetch_macro_data(market: MarketData) -> None:
    """Federal Funds Rate, core CPI YoY, 30-year mortgage average — all via FRED."""
    if not FRED_KEY:
        log.info("FRED_API_KEY not set — skipping macro fetch (Fed funds, CPI, mortgage will be blank).")
        return
    market.fed_funds = fetch_fred_series("DFF")
    market.cpi_yoy = fetch_fred_series("CPILFESL", units="pc1")  # Core CPI, 12-month % change
    market.mortgage_30y = fetch_fred_series("MORTGAGE30US")
    if any([market.fed_funds, market.cpi_yoy, market.mortgage_30y]):
        market.sources.append("Federal Reserve Economic Data, St. Louis Fed (FRED)")


def fetch_market_data() -> MarketData:
    """One-stop fetch for the week's market snapshot."""
    market = MarketData()
    fetch_treasury_yields(market)
    fetch_macro_data(market)
    return market


def _to_float(x) -> float | None:
    try:
        if x in (None, "", "N/A"):
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def format_market_data_md(market: MarketData) -> str:
    """Render the MarketData as a markdown table for the 'Numbers This Week' section."""
    lines = ["| Metric | Current | Note |", "|---|---|---|"]

    def row(metric: str, value: float | None, fmt: str, note: str = "") -> None:
        if value is None:
            lines.append(f"| {metric} | n/a | {note} |")
        else:
            lines.append(f"| {metric} | {fmt.format(value)} | {note} |")

    # Treasury yields
    change = market.treasury_10y_change
    change_str = f"{change:+.2f} WoW" if change is not None else ""
    row("10-year Treasury yield", market.treasury_10y, "{:.2f}%", change_str)
    row("2-year Treasury yield", market.treasury_2y, "{:.2f}%")
    row("3-month Treasury yield", market.treasury_3mo, "{:.2f}%")
    # Macro
    row("Federal Funds Rate", market.fed_funds, "{:.2f}%")
    row("Core CPI (year-over-year)", market.cpi_yoy, "{:.1f}%")
    row("30-year mortgage average", market.mortgage_30y, "{:.2f}%")
    if market.as_of:
        lines.append("")
        lines.append(f"*Data as of {market.as_of}. Sources: {'; '.join(market.sources)}.*")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 1. Fetch RSS
# ---------------------------------------------------------------------------

def fetch_new_items(feeds: list[str], seen_urls: set[str]) -> list[NewsItem]:
    items: list[NewsItem] = []
    for feed_url in feeds:
        try:
            parsed = feedparser.parse(feed_url)
            for entry in parsed.entries[:25]:  # cap per feed
                url = entry.get("link", "")
                if not url or url in seen_urls:
                    continue
                items.append(NewsItem(
                    title=entry.get("title", "Untitled"),
                    url=url,
                    summary=entry.get("summary", "")[:1000],
                    source=parsed.feed.get("title", feed_url),
                    published=entry.get("published", ""),
                ))
        except Exception as e:
            log.warning("Failed to parse %s: %s", feed_url, e)
    log.info("Fetched %d new items across %d feeds", len(items), len(feeds))
    return items


# ---------------------------------------------------------------------------
# 2. Score with Haiku
# ---------------------------------------------------------------------------

SCORER_PROMPT = """You are the curation editor for {newsletter_name}, a newsletter for {audience} about {niche}.

Score the news item below on a 0-100 scale based on how relevant and valuable it is for our audience.

Scoring rubric:
- 90-100: Major, specific, actionable news for our audience
- 70-89: Useful tactical insight, interesting trend data, or a strong how-to
- 50-69: Adjacent / interesting but not directly actionable
- 30-49: Loosely related, mostly filler
- 0-29: Off-topic, opinion piece without substance, or content marketing fluff

Return only valid JSON in this exact format:
{{"score": <0-100>, "one_line_take": "<a single sentence summarizing why this matters, or 'skip' if score < 50>"}}

News item:
Title: {title}
URL: {url}
Summary: {summary}
"""


def score_items(items: list[NewsItem]) -> list[NewsItem]:
    for item in items:
        try:
            result = claude_json(
                CONFIG["models"]["cheap"],
                SCORER_PROMPT.format(
                    newsletter_name=CONFIG["newsletter_name"],
                    audience=CONFIG["audience"],
                    niche=CONFIG["niche"],
                    title=item.title,
                    url=item.url,
                    summary=item.summary[:600],
                ),
                max_tokens=200,
            )
            item.score = int(result.get("score", 0))
            item.one_line_take = result.get("one_line_take", "")
        except Exception as e:
            log.warning("Score failed for %s: %s", item.url, e)
            item.score = 0
    items.sort(key=lambda x: x.score, reverse=True)
    return items


# ---------------------------------------------------------------------------
# 3. Affiliate decision
# ---------------------------------------------------------------------------

def maybe_add_affiliate(item: NewsItem) -> None:
    """Cheap lookup: if any affiliate product name appears in the title or summary, use it."""
    text = (item.title + " " + item.summary).lower()
    for prod in CONFIG["affiliate_products"]:
        if prod["name"].lower() in text:
            item.affiliate_url = prod["url"]
            return


# ---------------------------------------------------------------------------
# 4. Draft newsletter
# ---------------------------------------------------------------------------

DRAFTER_PROMPT = """You are writing today's issue of {newsletter_name}, a newsletter for {audience} about {niche}.

Voice & style:
{voice}

Today's curated items (already filtered for quality, scored > {threshold}):
{items_json}

Write the issue with this structure:
1. Subject line — 6 words max, punchy, hints at the most valuable item. No clickbait.
2. Opening — 2-3 sentences. The single most important insight of the day. No "Hello readers!".
3. Top story — 80-150 words. The highest-scored item. Include the link.
4. Quick hits — 3-5 bullet points, one sentence each, covering the rest of the items. Each bullet includes a link.
5. One-line closer — a personal observation, question, or call to action.

If any item has an "affiliate_url" field, use it instead of the regular URL.

Return only valid JSON:
{{
  "subject_line": "...",
  "preview_text": "<10 words, shows in inbox under subject>",
  "body_markdown": "<the full newsletter body in markdown>",
  "self_grade": <0-100, your honest score for whether this issue is good enough to send>,
  "skip_reason": "<empty string, OR a reason to skip sending today if the items just aren't good enough>"
}}

If self_grade < {pub_threshold}, set skip_reason and don't waste subscribers' attention on a weak issue.
"""


def draft_newsletter(top_items: list[NewsItem]) -> dict | None:
    items_payload = [{
        "title": i.title,
        "url": i.url,
        "score": i.score,
        "one_line_take": i.one_line_take,
        "summary": i.summary[:400],
        "affiliate_url": i.affiliate_url,
    } for i in top_items]

    try:
        result = claude_json(
            CONFIG["models"]["premium"],
            DRAFTER_PROMPT.format(
                newsletter_name=CONFIG["newsletter_name"],
                audience=CONFIG["audience"],
                niche=CONFIG["niche"],
                voice=CONFIG["newsletter_voice"],
                threshold=CONFIG["score_threshold"],
                pub_threshold=CONFIG["publish_grade_threshold"],
                items_json=json.dumps(items_payload, indent=2),
            ),
            max_tokens=4000,
        )
        return result
    except Exception as e:
        log.error("Newsletter draft failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# 5. Beehiiv submission
# ---------------------------------------------------------------------------

def send_to_beehiiv(draft: dict, auto_send: bool = False) -> bool:
    """Save the AI-drafted issue as a markdown file in drafts/ for you to copy-paste into Beehiiv.

    Why not direct API post: Beehiiv gates the Posts API behind their Enterprise plan
    (custom-priced, $$$). Saving the draft to a file is the cleanest workaround at lower tiers.

    Workflow:
      1. This script writes drafts/YYYY-MM-DD.md on every run that produces an issue.
      2. The GitHub Actions auto-commit step pushes it to the repo.
      3. With the GitHub mobile app installed and repo notifications on, you get a phone
         push when a new draft lands. Tap, copy the markdown, paste into Beehiiv → Posts
         → New post → paste body → set subject from the heading → Send. Total: ~90 seconds.

    The `auto_send` arg is kept for future use if/when Beehiiv changes their pricing.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = Path("drafts")
    out_dir.mkdir(exist_ok=True)

    # If a draft for today already exists, suffix with -v2, -v3, etc.
    draft_file = out_dir / f"{timestamp}.md"
    iteration = 1
    while draft_file.exists():
        iteration += 1
        draft_file = out_dir / f"{timestamp}-v{iteration}.md"

    content = (
        f"# {draft['subject_line']}\n\n"
        f"**Preview text:** {draft['preview_text']}\n\n"
        f"**Self-grade:** {draft.get('self_grade', 'n/a')} / 100\n\n"
        "---\n\n"
        f"{draft['body_markdown']}\n"
    )
    draft_file.write_text(content)
    log.info("Draft saved to %s — paste into Beehiiv to send", draft_file)

    # Also print to the Actions log so you can read it directly in GitHub if you prefer
    print("\n=== NEWSLETTER DRAFT ===")
    print(f"File: {draft_file}")
    print(f"Subject: {draft['subject_line']}")
    print(f"Preview: {draft['preview_text']}")
    print("=== END ===\n")
    return True


# ---------------------------------------------------------------------------
# 6. SEO page generation
# ---------------------------------------------------------------------------

PAGE_PROMPT = """You are writing a programmatic page for {niche}. Your audience is {audience}. The site's brand voice is:

{brand_voice}

Page data:
{page_data_json}

Write a comprehensive page covering this query. Requirements:
- 1500-2500 words.
- H1 contains the primary keyword exactly once.
- Open with a 100-word TL;DR that directly answers the query — no preamble.
- Include 3-5 H2 sections. Each H2 is a sub-question the reader has.
- For comparison pages, include a markdown comparison table with at least 5 attributes per option.
- Use specific numbers, real pricing, real feature names. If you don't know a real fact, write "[VERIFY: <thing>]" rather than guessing.
- Affiliate disclosure at the top.
- End with a FAQ section: 3-5 questions, 50-word answers each.

Forbidden:
- Filler phrases ("in today's fast-paced world", "as we all know")
- Fabricated statistics, study citations, or quotes
- Hedging without value

Return only valid JSON:
{{
  "slug": "<url-slug, lowercase-hyphenated>",
  "title_tag": "<55-60 chars, includes primary keyword>",
  "meta_description": "<150-155 chars>",
  "body_markdown": "<full page in markdown>",
  "verify_flags": ["<each [VERIFY: ...] item>"],
  "self_grade": <0-100>
}}
"""

GRADER_PROMPT = """You are a strict SEO and content quality reviewer. Grade the page below.

Page:
{page_markdown}

Score each criterion 0-10. Return only valid JSON:
{{
  "scores": {{"uniqueness": <0-10>, "factual_density": <0-10>, "answer_quality": <0-10>, "structure": <0-10>, "risk": <0-10>}},
  "total_score": <0-50>,
  "verdict": "<PUBLISH | REVISE | KILL>",
  "verdict_reason": "<one sentence>",
  "specific_fixes": ["<fix1>", "<fix2>"]
}}

PUBLISH if total >= 35 AND risk >= 8.
REVISE if total 25-34 or risk 5-7.
KILL if total < 25 or risk < 5.
"""


def generate_seo_pages(n: int) -> int:
    """Generate up to `n` pages from the queue in seo_pages.json."""
    data_path = Path(CONFIG["seo_data_file"])
    if not data_path.exists():
        log.warning("No %s found — skipping SEO generation.", CONFIG["seo_data_file"])
        return 0
    queue = json.loads(data_path.read_text())
    pending = [p for p in queue if not p.get("generated", False)]
    if not pending:
        log.info("No pending pages in SEO queue.")
        return 0

    out_dir = Path(CONFIG["seo_output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    generated = 0
    for page_data in pending[:n]:
        try:
            draft = claude_json(
                CONFIG["models"]["premium"],
                PAGE_PROMPT.format(
                    niche=CONFIG["niche"],
                    audience=CONFIG["audience"],
                    brand_voice=CONFIG["brand_voice"],
                    page_data_json=json.dumps(page_data, indent=2),
                ),
                max_tokens=6000,
            )
            grade = claude_json(
                CONFIG["models"]["premium"],
                GRADER_PROMPT.format(page_markdown=draft["body_markdown"]),
                max_tokens=1000,
            )
            if grade.get("verdict") != "PUBLISH":
                log.warning("Page %s graded %s — skipping. Reason: %s",
                            page_data.get("primary_keyword"), grade.get("verdict"), grade.get("verdict_reason"))
                continue

            # Write the page as Astro markdown with frontmatter
            slug = draft["slug"]
            md_path = out_dir / f"{slug}.md"
            frontmatter = (
                "---\n"
                f"title: \"{draft['title_tag']}\"\n"
                f"description: \"{draft['meta_description']}\"\n"
                f"slug: \"{slug}\"\n"
                f"published: {datetime.now(timezone.utc).isoformat()}\n"
                f"verify_flags: {json.dumps(draft.get('verify_flags', []))}\n"
                "---\n\n"
            )
            md_path.write_text(frontmatter + draft["body_markdown"])
            page_data["generated"] = True
            page_data["slug"] = slug
            generated += 1
            log.info("Generated page: %s", slug)
        except Exception as e:
            log.error("Page generation failed for %s: %s", page_data.get("primary_keyword"), e)

    data_path.write_text(json.dumps(queue, indent=2))
    return generated


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def run_daily(state: State, auto_send: bool = False) -> None:
    """The daily newsletter pipeline."""
    if not CONFIG["rss_feeds"]:
        log.error("No RSS feeds configured. Add some in CONFIG['rss_feeds'].")
        return

    items = fetch_new_items(CONFIG["rss_feeds"], state.seen_urls)
    state.seen_urls.update(i.url for i in items)
    if not items:
        log.info("No new items today.")
        return

    items = score_items(items)
    top = [i for i in items if i.score >= CONFIG["score_threshold"]]
    log.info("%d items scored above %d", len(top), CONFIG["score_threshold"])

    if len(top) < CONFIG["min_items_to_send"]:
        log.info("Not enough quality items (%d < %d). Skipping today.", len(top), CONFIG["min_items_to_send"])
        return

    for item in top:
        maybe_add_affiliate(item)

    draft = draft_newsletter(top[:8])
    if not draft:
        return
    if draft.get("skip_reason"):
        log.info("Drafter chose to skip: %s", draft["skip_reason"])
        return
    if draft.get("self_grade", 0) < CONFIG["publish_grade_threshold"]:
        log.info("Self-grade %d below threshold %d. Skipping.",
                 draft.get("self_grade"), CONFIG["publish_grade_threshold"])
        return

    if send_to_beehiiv(draft, auto_send=auto_send):
        state.last_newsletter_send = datetime.now(timezone.utc).isoformat()


def run_seo(state: State) -> None:
    """Twice-a-week SEO page generation."""
    n = generate_seo_pages(CONFIG["pages_per_seo_run"])
    if n:
        state.last_seo_run = datetime.now(timezone.utc).isoformat()
    log.info("Generated %d SEO page(s).", n)


def run_market_data() -> None:
    """Standalone fetch + print of the week's market snapshot. Useful for verifying
    data sources work before wiring them into the weekly briefing."""
    log.info("Fetching market data...")
    market = fetch_market_data()
    md = format_market_data_md(market)
    print("\n" + "=" * 60)
    print("MARKET DATA SNAPSHOT")
    print("=" * 60)
    print(md)
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["daily", "seo", "both", "market_data"],
        default="daily",
    )
    parser.add_argument("--auto-send", action="store_true",
                        help="Auto-send the newsletter instead of leaving it as a draft.")
    args = parser.parse_args()

    # market_data mode doesn't need Anthropic
    if args.mode == "market_data":
        run_market_data()
        return 0

    if not os.environ.get("ANTHROPIC_API_KEY"):
        log.error("ANTHROPIC_API_KEY not set.")
        return 1

    state = State.load(CONFIG["state_file"])
    try:
        if args.mode in ("daily", "both"):
            run_daily(state, auto_send=args.auto_send)
        if args.mode in ("seo", "both"):
            run_seo(state)
    finally:
        state.save(CONFIG["state_file"])
    return 0


if __name__ == "__main__":
    sys.exit(main())


# ---------------------------------------------------------------------------
# GitHub Actions workflow (save as .github/workflows/daily.yml)
# ---------------------------------------------------------------------------
#
# name: Daily Content Engine
# on:
#   schedule:
#     - cron: '0 13 * * *'        # daily 1pm UTC = 9am ET
#     - cron: '0 14 * * 1,4'      # SEO pages Mon + Thu
#   workflow_dispatch:
#
# jobs:
#   run:
#     runs-on: ubuntu-latest
#     steps:
#       - uses: actions/checkout@v4
#       - uses: actions/setup-python@v5
#         with: { python-version: '3.11' }
#       - run: pip install anthropic feedparser requests
#       - name: Run daily pipeline
#         if: github.event.schedule == '0 13 * * *' || github.event_name == 'workflow_dispatch'
#         env:
#           ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
#           BEEHIIV_API_KEY: ${{ secrets.BEEHIIV_API_KEY }}
#           BEEHIIV_PUBLICATION_ID: ${{ secrets.BEEHIIV_PUBLICATION_ID }}
#         run: python starter_automation.py --mode daily
#       - name: Run SEO generation
#         if: github.event.schedule == '0 14 * * 1,4'
#         env:
#           ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
#         run: python starter_automation.py --mode seo
#       - name: Commit generated pages
#         run: |
#           git config user.name "content-bot"
#           git config user.email "bot@users.noreply.github.com"
#           git add -A
#           git diff --quiet && git diff --staged --quiet || git commit -m "auto: daily content engine run"
#           git push
