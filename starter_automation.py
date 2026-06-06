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
    move_history: list = field(default_factory=list)  # IDs of Move templates used, in order

    @classmethod
    def load(cls, path: str) -> "State":
        if Path(path).exists():
            data = json.loads(Path(path).read_text())
            return cls(
                seen_urls=set(data.get("seen_urls", [])),
                last_seo_run=data.get("last_seo_run", ""),
                last_newsletter_send=data.get("last_newsletter_send", ""),
                move_history=data.get("move_history", []),
            )
        return cls()

    def save(self, path: str) -> None:
        Path(path).write_text(json.dumps({
            "seen_urls": sorted(self.seen_urls),
            "last_seo_run": self.last_seo_run,
            "last_newsletter_send": self.last_newsletter_send,
            "move_history": self.move_history,
        }, indent=2))


# ---------------------------------------------------------------------------
# Editorial constants — Move templates, disclaimer, acronym guide
# ---------------------------------------------------------------------------
# These define what each weekly issue looks like.
# Move templates rotate weekly so we cover ~15 distinct analytical angles
# over a quarter. Each template asks Claude to do specific math with this
# week's actual market data; that's the value the issue delivers.
# ---------------------------------------------------------------------------

MOVE_TEMPLATES = [
    {
        "id": "roth_conversion_timing",
        "title_hint": "When the Roth conversion math beats tax-deferral",
        "core_question": "Given current 10-year Treasury yield and equity expected returns, at what marginal tax bracket does a Roth conversion become net-positive compared to leaving funds in a Traditional Individual Retirement Account (IRA)?",
        "data_needs": ["treasury_10y"],
        "instructions": "Use the terminal after-tax value framing (the standard quantitative approach): compare (a) Roth terminal value = (principal − conversion_tax) × (1 + r)^n, fully tax-free, vs (b) Traditional terminal value × (1 − tax_at_RMD). Run three bracket scenarios (24%, 32%, 35%) with consistent assumptions: $200K principal, 18-year horizon to Required Minimum Distribution (RMD), 7% nominal return, conversion tax paid from outside the IRA (the only structurally honest comparison). For each bracket, show the side-by-side terminal values and the dollar gap. Then add a Tax Cuts and Jobs Act (TCJA) sunset sensitivity: re-run assuming RMD-era brackets revert (35% → 39.6%, 32% → 35%) and show how the conclusion shifts. Conclude with the bracket where conversion's terminal value clearly beats deferral. The current 10-year Treasury matters mainly as the discount rate when sensitivity-testing the 'pay conversion tax from outside funds' assumption."
    },
    {
        "id": "after_tax_yield_ranking",
        "title_hint": "The after-tax yield ranking that just shifted",
        "core_question": "For a 32% federal marginal bracket investor, what's the after-tax yield ranking of: current 10-year Treasury, top High-Yield Savings Account (HYSA), SCHD ETF dividends, and JEPQ ETF income?",
        "data_needs": ["treasury_10y", "fed_funds"],
        "instructions": "Calculate after-tax yields for: (1) 10-year Treasury at current rate (federal taxable, state-exempt; assume average state tax 5%), (2) top HYSA approximately 4.1% Annual Percentage Yield (fully taxable federal + state), (3) Schwab US Dividend Equity ETF (SCHD) trailing yield approximately 3.5% (qualified dividends taxed at 15% federal), (4) JPMorgan Nasdaq Equity Premium Income ETF (JEPQ) trailing yield approximately 9% but mostly ordinary income (taxed at marginal rate). Show the math; rank by after-tax yield. Note which spread has widened or narrowed materially in recent months."
    },
    {
        "id": "mortgage_payoff_vs_invest",
        "title_hint": "Mortgage payoff vs invest at the current rate spread",
        "core_question": "At the current 30-year mortgage rate, does aggressively paying down beat investing the same dollars in a 60/40 portfolio?",
        "data_needs": ["mortgage_30y", "treasury_10y"],
        "instructions": "Compare: extra principal payment guaranteed return = current mortgage rate (after-tax for most readers since the standard deduction increase under Tax Cuts and Jobs Act (TCJA) eliminated itemizing for most). Versus: 60/40 portfolio long-term expected real return approximately 4-5%. Calculate the nominal return the portfolio must earn to beat the mortgage payoff after-tax. Discuss when payoff wins (high marginal cost, low risk tolerance, near-retirement) vs when investing wins (long horizon, high savings rate already, mortgage rate locked in below market). Use concrete numbers: $50K extra principal vs $50K to a brokerage at the current rate spread, 10-year horizon."
    },
    {
        "id": "sequence_returns_stress_test",
        "title_hint": "Sequence-of-returns risk at age 55 in 2026",
        "core_question": "How does a 60/40 portfolio with $1.5M balance fare under a sharp first-year drawdown applied at current valuations?",
        "data_needs": ["cpi_yoy"],
        "instructions": "Model a 4%-rule withdrawal portfolio with $1.5M starting balance, age 55, planning 35-year retirement. Apply: (1) -22% first-year return scenario, (2) -8% first-year scenario, (3) baseline +7% nominal scenario. Use current core Consumer Price Index (CPI) year-over-year for the spending adjustment. Project terminal balance at age 90 under each. The point: sequence-of-returns risk in the first 5 years matters 10-20x more than later returns. Quantify the cost of staying 100% equity in this window vs adding a bond tent or holding 2-3 years of cash. Be specific about the dollar gap."
    },
    {
        "id": "mega_backdoor_math",
        "title_hint": "Mega backdoor Roth: how much you can actually stuff in",
        "core_question": "Given current Internal Revenue Code Section 415(c) limits, what's the maximum mega backdoor Roth contribution for a high earner with a generous 401(k) plan?",
        "data_needs": [],
        "instructions": "Walk through: 2026 employee 401(k) deferral limit, 2026 Section 415(c) overall limit (currently $69,000 for under 50), the gap available for after-tax contributions assuming maxed employer match. Explain the conversion mechanic to Roth Individual Retirement Account (IRA) via in-plan rollover or in-service distribution. Calculate the lifetime impact: a $30K/year mega backdoor contribution for 10 years compounding at 7% real vs the same money in a taxable account at the same return. Net the tax drag on taxable. Quantify the tax-free growth differential at age 65 if started at age 35."
    },
    {
        "id": "hsa_triple_tax",
        "title_hint": "The Health Savings Account triple-tax breakeven",
        "core_question": "At what age does using Health Savings Account (HSA) dollars for medical expenses immediately vs investing them and paying out-of-pocket break even?",
        "data_needs": [],
        "instructions": "Model: $4,300 single HSA contribution invested in 80/20 portfolio at 7% expected return. Versus: pay $4,300 of current medical expenses out-of-pocket with taxable dollars (also at 7% if invested). Calculate the value differential at ages 65, 70, and 80. Note: after age 65 HSA distributions for non-medical purposes are taxed at ordinary income (like Traditional IRA), but qualified medical reimbursements stay tax-free indefinitely. The full 'triple-tax' (deductible going in, tax-free growth, tax-free out) only realizes if you save medical receipts and reimburse decades later. Quantify the receipt-hoarding strategy's actual dollar value."
    },
    {
        "id": "geographic_arbitrage_tax",
        "title_hint": "The state-income-tax move math at high incomes",
        "core_question": "For a $500K household income, what's the lifetime tax differential between California or New York vs Texas, Florida, or Washington (zero state income tax)?",
        "data_needs": [],
        "instructions": "Calculate: California marginal state tax at $500K Adjusted Gross Income (AGI) is roughly 9.3%; New York is similar. Texas, Florida, Washington, Nevada, Wyoming, South Dakota, Tennessee, and New Hampshire (on wage income): 0% state income tax. Annual savings on $500K AGI roughly $35-45K. Compounded 10 years invested at 7% real: roughly $500K terminal differential. But net out estimated cost-of-living differential including housing. Conclude with a break-even: if you capture even 50% of the tax savings net of higher housing cost, the move is positive expected value. Mention the often-overlooked downside: lower property tax states often have higher sales tax or property tax burden."
    },
    {
        "id": "ibond_vs_treasury_spread",
        "title_hint": "I-Bonds vs short-Treasury: when the spread argues for which",
        "core_question": "At the current I-Bond composite rate vs current short-Treasury yields, which is the right cash sleeve?",
        "data_needs": ["treasury_3mo", "cpi_yoy"],
        "instructions": "Compare: Series I Savings Bonds (composite rate = fixed component + inflation component, $10K/year per-individual purchase cap, 1-year lockup, 5-year hold for full interest, ~3-month interest forfeiture for early withdrawal). Versus: 3-month Treasury (current yield, fully liquid in secondary market, federally taxable but state-tax-exempt). When does I-Bond win: high inflation, long hold, want tax deferral until redemption. When Treasury wins: liquidity matters, inflation moderates, short horizon. Compute the current break-even hold period using actual rates."
    },
    {
        "id": "529_to_roth_secure",
        "title_hint": "The 529-to-Roth conversion window under SECURE 2.0",
        "core_question": "How should families with overfunded 529 accounts use the new $35K lifetime 529-to-Roth conversion benefit?",
        "data_needs": [],
        "instructions": "Walk through SECURE 2.0 Act mechanics: 529 plan must be open ≥15 years; conversions limited to annual Roth IRA contribution limit; lifetime $35K cap; beneficiary must be the Roth account holder. Strategy: overfund 529 deliberately if confident not all education dollars will be needed, to use as quasi-additional Roth space. Quantify the value: $35K converted at age 25 invested at 7% real to age 65 = approximately $525K tax-free. The catch: the annual ceiling means it takes ~6 years of contributions to use the full $35K. Discuss the planning trade-offs."
    },
    {
        "id": "savings_rate_years_to_fi",
        "title_hint": "What savings rate hits Financial Independence in 12 years",
        "core_question": "What after-tax savings rate gets a household from zero to Financial Independence in 12 years using current real yields?",
        "data_needs": ["treasury_10y", "cpi_yoy"],
        "instructions": "Use Mr. Money Mustache's 'Shockingly Simple Math' formula: assuming 5% real return on savings and 4% withdrawal rate, years to Financial Independence (FI) map roughly to: 50% save rate = 17 years; 60% = 12.5 years; 70% = 8.5 years; 80% = 5.5 years. Provide the math. Compare with the current 10-year Treasury real yield (10Y nominal minus current core CPI year-over-year). If real yields are positive (which they are at current rates), the standard formula is conservative; recompute the 12-year target savings rate using actual real-yield assumptions instead of the standard 5%. Show the corrected savings rate. Acknowledge the assumption of constant savings rate and zero windfalls."
    },
    {
        "id": "rule_of_55_vs_sepp",
        "title_hint": "Rule of 55 vs Section 72(t) Substantially Equal Periodic Payments",
        "core_question": "For someone retiring at 55 with the bulk of savings in a 401(k), is the Rule of 55 better than Section 72(t) Substantially Equal Periodic Payments (SEPP)?",
        "data_needs": [],
        "instructions": "Walk through: Rule of 55 — penalty-free withdrawals from your most recent employer's 401(k) starting the year you turn 55 (age 50 for qualified public safety officers). Versus Section 72(t) Substantially Equal Periodic Payments (SEPP) — substantially equal periodic payments from any IRA or 401(k), three Internal Revenue Service (IRS)-approved calculation methods, must continue for 5 years or until age 59½ (whichever is later). Rule of 55 wins if: you can leave money in the 401(k) and haven't rolled over after separating. SEPP wins if: you've already rolled to IRA, or want lower monthly payments. Calculate the after-tax monthly income from a $1M balance under each strategy at current tax brackets."
    },
    {
        "id": "asset_location_math",
        "title_hint": "Asset location math at $1M, $3M, and $5M portfolios",
        "core_question": "Where should bonds, Real Estate Investment Trusts (REITs), and high-yield equity Exchange-Traded Funds (ETFs) live across taxable, Roth, and Traditional accounts?",
        "data_needs": [],
        "instructions": "Standard guidance: bonds and Real Estate Investment Trusts (REITs) belong in tax-advantaged accounts (Roth and Traditional); broad-market index funds belong in taxable (qualified dividends + step-up at death). Calculate the tax-drag savings of correct asset location vs random location for portfolios of $1M, $3M, and $5M at a 32% federal bracket: roughly $1,500/year savings at $1M; $4,500 at $3M; $8,000 at $5M. Conclude with the threshold (~$2M) where this becomes worth the operational complexity. Note that asset location matters less if your taxable account is already a small fraction of total assets."
    },
    {
        "id": "donor_advised_fund_bunching",
        "title_hint": "Donor-Advised Fund bunching at the current charity bracket",
        "core_question": "For a high earner who normally gives 3-5% annually, does Donor-Advised Fund (DAF) bunching beat year-over-year giving under post-Tax Cuts and Jobs Act itemization rules?",
        "data_needs": [],
        "instructions": "Math: the 2026 standard deduction is approximately $30K married-filing-jointly. To benefit from itemizing, total deductions must exceed that threshold. Strategy: bunch 3-5 years of charitable giving into a Donor-Advised Fund (DAF) in one year, itemize big that year, take standard deduction in off years. Calculate: bunching $50K into a DAF in year 1 + standard deduction ($30K × 4) in years 2-5 = $170K total deductions over 5 years vs $10K/yr × 5 + standard $0 (each year below itemization threshold) = $50K total deductions. Net federal tax savings at 32% bracket: approximately $38K over the 5-year cycle. Discuss highly-appreciated stock as the optimal funding vehicle (avoids capital gains)."
    },
    {
        "id": "fed_funds_hysa_spread",
        "title_hint": "The Fed funds vs High-Yield Savings spread",
        "core_question": "At the current Federal Funds Rate vs top High-Yield Savings Account (HYSA) Annual Percentage Yield, how big is the bank's margin and is moving cash worth it?",
        "data_needs": ["fed_funds"],
        "instructions": "Compare: current Federal Funds Rate vs the top HYSA APY (approximate; readers should verify with their bank or a rate-comparison site). When the spread is wide (Fed funds > top HYSA by >75 basis points), the bank is profiting from sticky deposits. When the spread compresses, banks have priced fairly. Calculate annual interest cost of leaving cash at a 0.5% APY large-bank account vs moving to a top-tier HYSA at 4.1% APY: on $50K of cash that's approximately $1,800/year of forgone interest. Mention the friction cost of moving cash (a 30-minute setup) and the Federal Deposit Insurance Corporation (FDIC) $250K coverage cap considerations for very large balances."
    },
    {
        "id": "bond_tent_at_current_10y",
        "title_hint": "Bond tent strategy at the current 10-year Treasury",
        "core_question": "For someone 5 years from retirement, what bond allocation glidepath makes sense at the current 10-year Treasury yield?",
        "data_needs": ["treasury_10y", "cpi_yoy"],
        "instructions": "Walk through the bond tent concept (Wade Pfau): increase bond allocation in the 5-10 years pre-retirement to mitigate sequence-of-returns risk, then DECREASE bond allocation during the first 5-10 years of retirement (rising equity glidepath). Math: at current 10-year Treasury yield, real (inflation-adjusted) bond return is roughly the nominal yield minus current core CPI. Equities offer approximately 5-6% real expected long-term. Calculate the optimal pre-retirement bond allocation. Note: works best when bonds are bought as individual Treasury notes or a Treasury ladder, not bond funds (avoids duration risk in rising-rate regimes)."
    },
]


# ---------------------------------------------------------------------------
# Acronym guide — passed into the drafter prompt so Claude spells them out
# on first use. Add to this list as the niche evolves.
# ---------------------------------------------------------------------------

ACRONYM_GUIDE = """First use of any acronym in this issue MUST spell out the term followed by the acronym in parentheses. Subsequent uses can use the abbreviation alone. Examples:
- FIRE → Financial Independence, Retire Early (FIRE)
- HENRY → high earner not rich yet (HENRY)
- HYSA → High-Yield Savings Account (HYSA)
- APY → Annual Percentage Yield (APY)
- FDIC → Federal Deposit Insurance Corporation (FDIC)
- CPI → Consumer Price Index (CPI)
- ETF → Exchange-Traded Fund (ETF)
- IRA → Individual Retirement Account (IRA)
- RMD → Required Minimum Distribution (RMD)
- SEPP → Substantially Equal Periodic Payments (SEPP)
- HSA → Health Savings Account (HSA)
- DAF → Donor-Advised Fund (DAF)
- CFP → Certified Financial Planner (CFP)
- CPA → Certified Public Accountant (CPA)
- REIT → Real Estate Investment Trust (REIT)
- AGI → Adjusted Gross Income (AGI)
- TCJA → Tax Cuts and Jobs Act (TCJA)
- FI → Financial Independence (FI)
- IRS → Internal Revenue Service (IRS)
- AUM → Assets Under Management (AUM)
- DRIP → Dividend Reinvestment Plan (DRIP)
- FOMC → Federal Open Market Committee (FOMC)
- SECURE → Setting Every Community Up for Retirement Enhancement Act (SECURE Act)
- TIPS → Treasury Inflation-Protected Securities (TIPS)
- SEP-IRA → Simplified Employee Pension Individual Retirement Account (SEP-IRA)
- SIMPLE → Savings Incentive Match Plan for Employees (SIMPLE IRA)
- QCD → Qualified Charitable Distribution (QCD)
- LTCG → Long-Term Capital Gains (LTCG)
- AMT → Alternative Minimum Tax (AMT)
- NIIT → Net Investment Income Tax (NIIT)
- WoW → Week-over-Week (WoW)
- YoY → Year-over-Year (YoY)
- YTD → Year-to-Date (YTD)
Treat ETF tickers (SCHD, JEPQ, VTI, VOO) as readable as-is, but spell out the fund name on first use, e.g. 'Schwab US Dividend Equity ETF (SCHD)'.
This rule is strict. Reread the body before returning JSON and confirm every acronym was spelled out at first appearance."""


# ---------------------------------------------------------------------------
# Disclaimer footer — appended verbatim to every issue. Linked Tier 2 page
# lives at exit-analytics.com/disclaimer (publish from disclaimer_page.md).
# ---------------------------------------------------------------------------

DISCLAIMER_FOOTER = """---

**Important disclosure.** Exit Analytics is a general-circulation publication providing impersonal commentary and analysis based on publicly available information. It is not investment, tax, or legal advice. The publisher is not a registered investment adviser, broker-dealer, or tax professional, and no fiduciary or advisory relationship is formed by reading or subscribing. Information reflects data and law as of publication; conditions, regulations, and individual circumstances vary and change. Consult a Certified Financial Planner (CFP), Certified Public Accountant (CPA), or attorney before acting on any strategy mentioned. All investments involve risk including possible loss of principal. Past performance does not guarantee future results. Forward-looking statements may not materialize. This issue may contain affiliate links; we receive compensation from certain providers we mention, which does not affect our analysis. You alone are responsible for your decisions. Full terms: [exit-analytics.com/disclaimer](https://exit-analytics.com/disclaimer)."""


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
# 4. Draft the weekly briefing (the Exit Analytics issue)
# ---------------------------------------------------------------------------
#
# The new architecture: each issue has four sections.
#   1. The Numbers This Week — market data table (auto-generated)
#   2. The Move — original analytical piece using one of MOVE_TEMPLATES, rotating
#   3. Three reads — the top-scored news items with one-line takeaways
#   4. Disclaimer footer (legal protection, appended verbatim)
# ---------------------------------------------------------------------------


def select_next_move(state: State, market: MarketData) -> dict | None:
    """Pick the next Move template, preferring ones not used in the last 10 issues.

    A template is eligible if all of its data_needs fields are populated in this
    week's market data. We rotate to avoid repeating any single angle too often.
    """
    recent = set(state.move_history[-10:])

    def template_eligible(tmpl: dict) -> bool:
        return all(getattr(market, fld, None) is not None for fld in tmpl.get("data_needs", []))

    eligible = [t for t in MOVE_TEMPLATES if template_eligible(t)]
    if not eligible:
        return None
    fresh = [t for t in eligible if t["id"] not in recent]
    pool = fresh if fresh else eligible
    pick = pool[0]
    state.move_history.append(pick["id"])
    return pick


WEEKLY_BRIEFING_PROMPT = """You are writing this week's issue of {newsletter_name}, a weekly Friday briefing for {audience}.

== VOICE ==
{voice}

== THIS WEEK'S MARKET DATA ==
{market_data_md}

== THE MOVE TEMPLATE FOR THIS WEEK ==
Title hint (you can refine; keep the core idea): {move_title_hint}
Core question to answer: {move_core_question}
Specific analysis instructions: {move_instructions}

This is the most important section of the issue. Do real math with the actual current data above. Show calculations. Cite specific numbers, real tax code sections, real fund expense ratios where relevant. **400-700 words** — analysis takes space; don't artificially cap it. NOT a summary of someone else's analysis — original calculation in our voice.

**Preferred analytical framing for retirement-account decisions:** compare **terminal after-tax values** directly rather than mixing opportunity-cost and present-value approaches. For Roth-vs-Traditional decisions specifically: Roth terminal = (principal − conversion_tax) × (1 + r)^n vs Traditional terminal = principal × (1 + r)^n × (1 − tax_at_RMD). This matches how Bogleheads, CFP candidates, and most quantitative writers frame these problems and avoids the confusion of comparing opportunity costs against present-valued tax liabilities.

== TOP SCORED NEWS ITEMS (for the 'Three Reads' section) ==
{items_json}

Pick the 3 most useful items. If fewer than 3 are above quality bar, use what you have or omit the section.

== ACRONYM GUIDE (STRICT) ==
{acronym_guide}

== ISSUE STRUCTURE ==
The body markdown must follow exactly this structure:

## The Numbers This Week
{market_data_md}

[Optional: 1-2 sentences of context — what's notable in the data]

## The Move
### [your title for this week, refined from the title hint]

[300-500 words of original analytical writing per the instructions above. Show math.]

## Three Reads
- [link](url) — one-line takeaway
- [link](url) — one-line takeaway
- [link](url) — one-line takeaway

## Closing

[One-line personal observation, question, or thought-provoking remark. Not a sign-off; an observation.]

{disclaimer}

== FORBIDDEN ==
- Filler phrases: "in today's economy", "experts say", "it depends on your goals", "as we all know", "navigating these uncertain times"
- Casual interjections that don't belong in a dry analytical publication: "But wait —", "Here's the thing", "Here's where it gets interesting", "Now here's the kicker", "Let me explain", "buckle up", "spoiler"
- Conversational hedges: "you might be wondering", "it's worth noting that", "interestingly enough"
- Hedging without value: "you should probably consider" without saying when or why
- Fabricated statistics, study citations, or quotes
- "AI assistant" or "as an AI" anywhere
- Generic financial-advice-blog tone
- Replacing the disclaimer footer text; copy it verbatim
- Em-dash bridging between two thoughts when a period would do the same work cleaner

== SUBJECT LINE ==
6 words maximum. Punchy. Hints at the most surprising figure in The Numbers or the punchline of The Move. Examples of good ones:
- "10Y at 4.42%, Roth math just shifted"
- "Why mortgage payoff stops being free"
- "The savings rate that buys 12 years"

== RETURN FORMAT ==
Return only valid JSON, no markdown fence:
{{
  "subject_line": "...",
  "preview_text": "<10 words max, what shows under the subject in the inbox>",
  "body_markdown": "<the full markdown body INCLUDING the disclaimer footer verbatim at the end>",
  "self_grade": <0-100, honest assessment of whether this issue is publishable>,
  "skip_reason": "<empty string, OR a reason to skip if the data was insufficient or the draft is weak>"
}}

If self_grade < {pub_threshold}, set skip_reason and we won't publish."""


def draft_weekly_briefing(market: MarketData, top_items: list, state: State) -> dict | None:
    """Build a single weekly issue from market data + a Move template + top news items."""
    move = select_next_move(state, market)
    if move is None:
        log.warning("No Move template eligible (market data missing). Skipping issue.")
        return None
    log.info("Selected Move template: %s", move["id"])

    items_payload = [{
        "title": i.title,
        "url": i.url,
        "score": i.score,
        "one_line_take": i.one_line_take,
        "summary": i.summary[:400],
        "affiliate_url": i.affiliate_url,
    } for i in top_items[:8]]  # cap; drafter picks final 3

    try:
        result = claude_json(
            CONFIG["models"]["premium"],
            WEEKLY_BRIEFING_PROMPT.format(
                newsletter_name=CONFIG["newsletter_name"],
                audience=CONFIG["audience"],
                voice=CONFIG["newsletter_voice"],
                market_data_md=format_market_data_md(market),
                move_title_hint=move["title_hint"],
                move_core_question=move["core_question"],
                move_instructions=move["instructions"],
                items_json=json.dumps(items_payload, indent=2),
                acronym_guide=ACRONYM_GUIDE,
                disclaimer=DISCLAIMER_FOOTER,
                pub_threshold=CONFIG["publish_grade_threshold"],
            ),
            max_tokens=8000,
        )
        # Safety: ensure disclaimer is in the body
        body = result.get("body_markdown", "")
        if "Important disclosure" not in body:
            result["body_markdown"] = body.rstrip() + "\n\n" + DISCLAIMER_FOOTER
            log.info("Appended disclaimer footer (drafter omitted it).")
        result["_move_id"] = move["id"]
        return result
    except Exception as e:
        log.error("Weekly briefing draft failed: %s", e)
        # Roll back the move_history entry since we didn't actually produce
        if state.move_history and state.move_history[-1] == move["id"]:
            state.move_history.pop()
        return None


# Backwards compatibility shim so existing callers don't break.
def draft_newsletter(top_items):
    """Deprecated stub. Use draft_weekly_briefing in the new architecture."""
    log.warning("draft_newsletter() is deprecated; use draft_weekly_briefing()")
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

def run_weekly_briefing(state: State, auto_send: bool = False) -> None:
    """Build this week's Exit Analytics issue: market data + a Move + three reads.

    Architecture (after the rebuild):
      1. Fetch this week's market snapshot (Treasury, Fed, CPI, mortgage).
      2. Fetch RSS items, score for quality, take the top few for the reading list.
         The reading list is a nice-to-have, NOT the issue's value driver.
      3. Pick the next Move template from the rotating pool.
      4. Draft the issue: subject, preview, body with all 4 sections + disclaimer.
      5. Save to drafts/ for the user to paste into Beehiiv.

    Even with 0 scored news items, an issue is produced — the Move and Numbers
    sections drive the value, not the curated links.
    """
    # 1. Market data
    log.info("Fetching market data...")
    market = fetch_market_data()
    if market.treasury_10y is None and market.fed_funds is None:
        log.error("Both Treasury and macro fetches failed. Skipping issue this week.")
        return

    # 2. News items (for the reading list only)
    top_items = []
    if CONFIG["rss_feeds"]:
        items = fetch_new_items(CONFIG["rss_feeds"], state.seen_urls)
        state.seen_urls.update(i.url for i in items)
        if items:
            items = score_items(items)
            top_items = sorted(items, key=lambda i: i.score, reverse=True)[:8]
            log.info("%d items scored; passing top %d to drafter", len(items), len(top_items))
            for item in top_items:
                maybe_add_affiliate(item)
        else:
            log.info("No new RSS items this run; issue will skip the reading list.")
    else:
        log.warning("No RSS feeds configured; issue will have no reading list.")

    # 3 + 4. Draft the issue
    draft = draft_weekly_briefing(market, top_items, state)
    if not draft:
        return
    if draft.get("skip_reason"):
        log.info("Drafter chose to skip: %s", draft["skip_reason"])
        return
    if draft.get("self_grade", 0) < CONFIG["publish_grade_threshold"]:
        log.info("Self-grade %d below threshold %d. Skipping.",
                 draft.get("self_grade"), CONFIG["publish_grade_threshold"])
        return

    # 5. Save
    if send_to_beehiiv(draft, auto_send=auto_send):
        state.last_newsletter_send = datetime.now(timezone.utc).isoformat()


# Keep the old name as an alias so existing workflow yaml still calls into the right place.
def run_daily(state: State, auto_send: bool = False) -> None:
    """Alias for run_weekly_briefing. The weekly cadence is enforced by the cron in the workflow yaml."""
    run_weekly_briefing(state, auto_send=auto_send)


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
