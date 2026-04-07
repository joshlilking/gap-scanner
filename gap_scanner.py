#!/usr/bin/env python3
"""Stock Gap Screener -- Scans for gap-down setups on daily charts.

Strategy:
- Detect stocks that gap down significantly (>5%) on earnings/news
- Entry: daily close above high of post-gap candle
- SL: below low of the gap candle
- TP: low of candle before the gap (pre-gap candle low)
- Min 2:1 R/R ratio
- Trailing stop: every 8% gain, move SL up 3%
"""

import asyncio
import json
import logging
import os
import re
import time
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from aiohttp import web
import aiohttp

try:
    import yfinance as yf
except ImportError:
    yf = None
    print("WARNING: yfinance not installed. Run: pip install yfinance")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
SETUPS_FILE = os.path.join(DATA_DIR, "gap_setups.json")

CALLMEBOT_PHONE = "+31611243774"
CALLMEBOT_APIKEY = "4391194"

SCAN_HOUR_UTC = 21          # 4 PM ET (during EDT)
CHART_DAYS = 90             # days of OHLCV for chart endpoint
SETTINGS_FILE = os.path.join(DATA_DIR, "scan_settings.json")

# Default scan settings (user-configurable via API)
DEFAULT_SETTINGS = {
    "min_gap_pct": 5.0,         # minimum gap-down %
    "max_gap_pct": 50.0,        # max gap-down % (filter out delistings)
    "min_price": 1.0,           # min stock price
    "max_price": 0,             # max stock price (0 = no limit)
    "min_market_cap": 0,        # min market cap in millions (0 = any)
    "max_market_cap": 0,        # max market cap in millions (0 = any)
    "min_volume": 0,            # min avg daily volume (0 = any)
    "lookback_days": 30,        # how far back to scan for gaps
    "trailing_gain_step": 8.0,  # every N% gain, raise SL
    "trailing_sl_raise": 3.0,   # raise SL by N% from entry
    "expire_days": 60,          # auto-expire watching setups after N days
}

_scan_settings: Dict[str, Any] = {}


def load_settings():
    global _scan_settings
    _ensure_dirs()
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as f:
            _scan_settings = {**DEFAULT_SETTINGS, **json.load(f)}
    else:
        _scan_settings = dict(DEFAULT_SETTINGS)
    log.info("Scan settings: %s", _scan_settings)


def save_settings():
    _ensure_dirs()
    with open(SETTINGS_FILE, "w") as f:
        json.dump(_scan_settings, f, indent=2)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gap_scanner")

# ---------------------------------------------------------------------------
# Ticker universe -- dynamically built from Wikipedia S&P500/Nasdaq/Russell
# + Finviz screener + hardcoded extras. Cached to disk.
# ---------------------------------------------------------------------------

TICKER_CACHE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "ticker_universe.json"
)
TICKER_CACHE_MAX_AGE = 7 * 86400  # refresh weekly

# Hardcoded extras that might not appear in index lists
_EXTRA_TICKERS = [
    "KLC", "LPCN", "MAXN", "CSIQ", "JKS", "DQ", "FSLR", "ENPH", "SEDG",
    "SMCI", "ARM", "MARA", "RIOT", "HUT", "CLSK", "CIFR", "IREN",
    "CVNA", "CAVA", "DUOL", "ONON", "BIRK", "CART", "TOST", "BROS",
    "SOFI", "HOOD", "AFRM", "UPST", "OPEN", "CLOV",
    "AMC", "GME", "BB", "NOK", "PLUG", "FCEL", "BLDP",
    "RIVN", "LCID", "NIO", "XPEV", "LI", "FSR",
    "DKNG", "PENN", "MGM", "WYNN", "LVS", "CZR",
    "BABA", "JD", "PDD", "BILI", "IQ", "TAL", "FUTU", "TIGR", "VNET", "GDS",
    "BIDU", "MNSO", "ZH", "YMM",
    "SNAP", "PINS", "MTCH", "BMBL", "RBLX",
    "ASTS", "RKLB", "LUNR", "JOBY", "LILM", "ACHR", "SOUN",
    "STEM", "CHPT", "EVGO", "BLNK", "RUN", "NOVA", "ARRY",
    "IMVT", "ACAD", "HALO", "PTCT", "RVMD", "PCVX", "KRYS", "CORT", "AXSM",
    "TGTX", "FATE", "KRTX", "ARDX", "VKTX", "SMMT", "DVAX", "NUVB", "XNCR",
    "CELH", "SAM", "BUD", "CHWY", "W", "ETSY",
    "DOCS", "OSCR", "GDRX", "HIMS", "TMDX",
    "CPRX", "ALKS", "JAZZ", "UTHR", "ITCI", "INVA", "SUPN",
    "CHEF", "PFGC", "USFD",
]


def _fetch_nasdaq_traded() -> List[str]:
    """Fetch ALL traded stocks from Nasdaq API (Nasdaq + NYSE + AMEX).
    This is the most comprehensive source — returns ~8000+ tickers."""
    import urllib.request
    tickers = []
    url = "https://api.nasdaq.com/api/screener/stocks?tableType=traded&limit=25000&offset=0"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        rows = data.get("data", {}).get("table", {}).get("rows", [])
        for row in rows:
            sym = row.get("symbol", "").strip()
            # Skip warrants, units, preferred shares, ETFs with weird suffixes
            if not sym or len(sym) > 5:
                continue
            # Skip tickers with special chars (warrants like ABCDW, units like ABCDU)
            if not re.match(r'^[A-Z]{1,5}$', sym):
                continue
            tickers.append(sym)
        log.info("Nasdaq API returned %d common stock tickers", len(tickers))
    except Exception as e:
        log.warning("Nasdaq API fetch failed: %s", e)

    return tickers


def _fetch_nasdaq_ftp() -> List[str]:
    """Fallback: fetch ticker list from Nasdaq FTP-style download."""
    import urllib.request
    tickers = []
    # nasdaqtraded.txt has ALL Nasdaq/NYSE/AMEX listed securities
    url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8", errors="replace")
        for line in text.strip().split("\n")[1:]:  # skip header
            parts = line.split("|")
            if len(parts) < 8:
                continue
            sym = parts[1].strip()
            # Column 7 = ETF flag (Y/N), skip ETFs
            is_etf = parts[5].strip().upper() == "Y" if len(parts) > 5 else False
            # Only common stocks with clean symbols
            if sym and 1 <= len(sym) <= 5 and re.match(r'^[A-Z]+$', sym) and not is_etf:
                tickers.append(sym)
        log.info("Nasdaq FTP returned %d tickers", len(tickers))
    except Exception as e:
        log.warning("Nasdaq FTP fetch failed: %s", e)
    return tickers


def build_ticker_universe() -> List[str]:
    """Build comprehensive US stock universe from Nasdaq's full database."""
    all_tickers = set(_EXTRA_TICKERS)

    # Primary: Nasdaq API — all traded stocks
    nasdaq = _fetch_nasdaq_traded()
    all_tickers.update(nasdaq)

    # Fallback: Nasdaq FTP if API returned too few
    if len(nasdaq) < 2000:
        log.info("Nasdaq API returned few results, trying FTP fallback...")
        ftp_tickers = _fetch_nasdaq_ftp()
        all_tickers.update(ftp_tickers)

    result = sorted(all_tickers)
    log.info("Total ticker universe: %d", len(result))
    return result


def load_ticker_universe() -> List[str]:
    """Load cached ticker universe or build fresh."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(TICKER_CACHE_FILE):
        try:
            age = time.time() - os.path.getmtime(TICKER_CACHE_FILE)
            if age < TICKER_CACHE_MAX_AGE:
                with open(TICKER_CACHE_FILE, "r") as f:
                    cached = json.load(f)
                if len(cached) > 100:
                    log.info("Loaded %d tickers from cache (age: %.0fh)", len(cached), age / 3600)
                    return cached
        except Exception:
            pass

    tickers = build_ticker_universe()
    if len(tickers) > 100:
        with open(TICKER_CACHE_FILE, "w") as f:
            json.dump(tickers, f)
        log.info("Saved %d tickers to cache", len(tickers))
    return tickers


WATCHLIST = load_ticker_universe()

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_setups: List[Dict[str, Any]] = []


def _ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(STATIC_DIR, exist_ok=True)


def load_setups():
    global _setups
    _ensure_dirs()
    if os.path.exists(SETUPS_FILE):
        with open(SETUPS_FILE, "r") as f:
            _setups = json.load(f)
        log.info("Loaded %d setups from disk", len(_setups))
    else:
        _setups = []


def save_setups():
    _ensure_dirs()
    with open(SETUPS_FILE, "w") as f:
        json.dump(_setups, f, indent=2, default=str)


def find_setup(symbol: str, status: Optional[str] = None) -> Optional[Dict]:
    for s in _setups:
        if s["symbol"] == symbol.upper():
            if status is None or s["status"] == status:
                return s
    return None


# ---------------------------------------------------------------------------
# CallMeBot WhatsApp alerts
# ---------------------------------------------------------------------------

async def send_whatsapp(msg: str):
    """Send a WhatsApp message via CallMeBot. Handle the dollar-sign issue."""
    safe_msg = re.sub(r'\$(\d)', '$\u200b\\1', msg)
    encoded = urllib.parse.quote_plus(safe_msg)
    url = (
        f"https://api.callmebot.com/whatsapp.php"
        f"?phone={CALLMEBOT_PHONE}"
        f"&text={encoded}"
        f"&apikey={CALLMEBOT_APIKEY}"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    log.info("WhatsApp sent: %s", msg[:80])
                else:
                    body = await resp.text()
                    log.warning("WhatsApp API returned %d: %s", resp.status, body[:200])
    except Exception as e:
        log.error("WhatsApp send failed: %s", e)


# ---------------------------------------------------------------------------
# yfinance helpers
# ---------------------------------------------------------------------------

async def fetch_finviz_gaps() -> List[str]:
    """Scrape Finviz for stocks that recently gapped down. Uses multiple filters."""
    tickers = []
    # ta_gap_d = gap down any%, ta_gap_d5 = gap down >5%, ta_gap_d3 = >3%
    urls = [
        "https://finviz.com/screener.ashx?v=111&f=ta_gap_d&ft=4",
        "https://finviz.com/screener.ashx?v=111&f=ta_gap_d5&ft=4",
        "https://finviz.com/screener.ashx?v=111&f=ta_gap_d3&ft=4",
    ]
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    for url in urls:
        try:
            async with aiohttp.ClientSession() as session:
                # Page through results (Finviz shows 20 per page)
                for r in range(1, 200, 20):
                    page_url = f"{url}&r={r}" if r > 1 else url
                    async with session.get(page_url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status != 200:
                            break
                        html = await resp.text()
                        matches = re.findall(r'class="screener-link-primary"[^>]*>([A-Z]{1,5})</a>', html)
                        if not matches:
                            break
                        tickers.extend(matches)
                        if len(matches) < 20:
                            break
                    await asyncio.sleep(0.3)
        except Exception as e:
            log.warning("Finviz scrape failed for %s: %s", url, e)
    tickers = list(set(tickers))
    log.info("Finviz gap screener found %d tickers: %s", len(tickers), tickers[:20])
    return tickers


async def fetch_barchart_gaps() -> List[str]:
    """Scrape Barchart for stocks with recent gap-downs."""
    tickers = []
    # Barchart gap down signals page
    urls = [
        "https://www.barchart.com/stocks/signals/gap-down",
        "https://www.barchart.com/stocks/signals/gap-down?page=2",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
    }
    for url in urls:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        log.warning("Barchart returned %d for %s", resp.status, url)
                        continue
                    html = await resp.text()
                    import re as _re
                    # Barchart uses data-ng-href or <a> tags with /stocks/quotes/SYMBOL
                    matches = _re.findall(r'/stocks/quotes/([A-Z]{1,5})(?:["\s&?/])', html)
                    tickers.extend(matches)
        except Exception as e:
            log.warning("Barchart scrape failed: %s", e)
        await asyncio.sleep(1)  # be polite

    tickers = list(set(tickers))
    log.info("Barchart gap screener found %d tickers: %s", len(tickers), tickers[:20])
    return tickers


def fetch_daily(symbol: str, period: str = "3mo") -> Optional[Any]:
    """Fetch daily OHLCV for a single ticker. Returns a pandas DataFrame or None."""
    if yf is None:
        return None
    try:
        tk = yf.Ticker(symbol)
        df = tk.history(period=period, auto_adjust=True)
        if df is None or df.empty:
            return None
        return df
    except Exception as e:
        log.debug("Failed to fetch %s: %s", symbol, e)
        return None


def fetch_batch(symbols: List[str], period: str = "1mo") -> Dict[str, Any]:
    """Fetch daily data for many symbols using yfinance batch download."""
    if yf is None:
        return {}
    results = {}
    # yfinance download can handle batches
    try:
        data = yf.download(symbols, period=period, group_by="ticker",
                           threads=True, progress=False)
        if data is None or data.empty:
            return {}
        for sym in symbols:
            try:
                if len(symbols) == 1:
                    df = data
                else:
                    df = data[sym].dropna(how="all")
                if df is not None and not df.empty and len(df) >= 2:
                    results[sym] = df
            except (KeyError, TypeError):
                continue
    except Exception as e:
        log.error("Batch download error: %s", e)
    return results


# ---------------------------------------------------------------------------
# Gap detection logic
# ---------------------------------------------------------------------------

def detect_gaps(df, symbol: str, settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Scan a daily OHLCV DataFrame for gap-down setups.

    Candle definitions:
    - pre_gap (i-1): candle BEFORE the gap. Its low = TP target (gap fill).
    - gap_candle (i): the gap-down candle (opens well below prev close). Its low = SL.
    - post_gap (i+1): candle AFTER the gap. Its high = trigger/entry level.

    Detection: gap_candle opens significantly below pre_gap close.
    Entry: future daily close above post_gap high.
    SL: gap_candle low.
    TP: pre_gap low (gap fill target).
    R/R is calculated but NOT used as a filter — all gaps shown.
    """
    setups = []
    if df is None or len(df) < 3:
        return setups

    min_gap = settings.get("min_gap_pct", 5.0)
    max_gap = settings.get("max_gap_pct", 50.0)
    min_price = settings.get("min_price", 1.0)
    max_price = settings.get("max_price", 0)
    min_vol = settings.get("min_volume", 0)

    rows = df.reset_index()
    cols = {c.lower(): c for c in rows.columns}
    date_col = cols.get("date", cols.get("datetime", None))
    if date_col is None:
        return setups

    high_col = cols.get("high", "High")
    low_col = cols.get("low", "Low")
    close_col = cols.get("close", "Close")
    open_col = cols.get("open", "Open")
    vol_col = cols.get("volume", "Volume")

    for i in range(1, len(rows)):
        pre_gap = rows.iloc[i - 1]
        gap_candle = rows.iloc[i]
        has_post = i + 1 < len(rows)
        post_gap = rows.iloc[i + 1] if has_post else None

        pre_close = float(pre_gap[close_col])
        pre_low = float(pre_gap[low_col])
        pre_high = float(pre_gap[high_col])
        pre_open = float(pre_gap[open_col])
        gap_open = float(gap_candle[open_col])
        gap_high = float(gap_candle[high_col])
        gap_low = float(gap_candle[low_col])
        gap_close = float(gap_candle[close_col])
        if has_post:
            post_open = float(post_gap[open_col])
            post_high = float(post_gap[high_col])
            post_low = float(post_gap[low_col])
            post_close = float(post_gap[close_col])
        else:
            post_open = post_high = post_low = post_close = 0.0

        # Price filter
        if gap_close < min_price:
            continue
        if max_price > 0 and gap_close > max_price:
            continue

        # Volume filter
        if min_vol > 0:
            try:
                vol = float(gap_candle[vol_col])
                if vol < min_vol:
                    continue
            except (KeyError, TypeError, ValueError):
                pass

        # Gap-down: gap candle opens significantly below previous close
        gap_pct = ((gap_open - pre_close) / pre_close) * 100.0
        abs_gap = abs(gap_pct)
        if gap_pct > -min_gap:
            continue  # not enough gap down
        if abs_gap > max_gap:
            continue  # too extreme (likely delisting/reverse split)

        # Setup levels:
        # "Post-gap candle" = the gap-down candle itself (first candle after the gap event)
        # Entry trigger = HIGH of the gap-down candle (close above this to enter)
        # SL = LOW of the gap-down candle
        # TP = LOW of the pre-gap candle (gap fill target)
        trigger_price = gap_high       # entry: close above gap candle high
        sl_price = gap_low             # SL: below gap candle low
        tp_price = pre_low             # TP: pre-gap candle low (gap fill)

        # Sanity: trigger must be above SL
        if trigger_price <= sl_price:
            continue

        # Gap filled intraday: if gap candle high >= pre close, the gap
        # was recovered within the same candle — not a real unfilled gap
        if gap_high >= pre_close:
            continue

        # TP must be above trigger (reward must be positive)
        # If gap candle high >= pre_gap low, there's no gap zone to fill
        if tp_price <= trigger_price:
            continue

        # R/R — calculate but do NOT filter
        risk = trigger_price - sl_price
        reward = tp_price - trigger_price
        if risk > 0 and reward > 0:
            rr = round(reward / risk, 2)
        else:
            rr = 0.0

        # Date strings
        def _date_str(val):
            if hasattr(val, "strftime"):
                return val.strftime("%Y-%m-%d")
            return str(val)[:10]

        gap_date_str = _date_str(gap_candle[date_col])
        post_date_str = _date_str(post_gap[date_col]) if has_post else ""
        pre_date_str = _date_str(pre_gap[date_col])

        setup = {
            "symbol": symbol,
            "gap_date": gap_date_str,
            "pre_gap_date": pre_date_str,
            "post_gap_date": post_date_str,
            "gap_pct": round(gap_pct, 2),
            "pre_gap_open": round(pre_open, 2),
            "pre_gap_high": round(pre_high, 2),
            "pre_gap_close": round(pre_close, 2),
            "pre_gap_low": round(pre_low, 2),
            "gap_candle_open": round(gap_open, 2),
            "gap_candle_high": round(gap_high, 2),
            "gap_candle_low": round(gap_low, 2),
            "gap_candle_close": round(gap_close, 2),
            "post_gap_open": round(post_open, 2),
            "post_gap_high": round(post_high, 2),
            "post_gap_low": round(post_low, 2),
            "post_gap_close": round(post_close, 2),
            "trigger_price": round(trigger_price, 2),
            "sl_price": round(sl_price, 2),
            "tp_price": round(tp_price, 2),
            "rr_ratio": rr,
            "status": "watching",
            "entry_price": None,
            "entry_date": None,
            "exit_price": None,
            "exit_date": None,
            "pnl_pct": None,
            "trailing_sl": None,
            "current_price": None,
            "notes": "",
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }
        setups.append(setup)

    return setups


# ---------------------------------------------------------------------------
# Scan & update logic
# ---------------------------------------------------------------------------

def _get_market_cap(symbol: str) -> Optional[float]:
    """Get market cap in millions. Returns None if unavailable."""
    if yf is None:
        return None
    try:
        tk = yf.Ticker(symbol)
        info = tk.info or {}
        cap = info.get("marketCap")
        if cap:
            return cap / 1_000_000  # convert to millions
    except Exception:
        pass
    return None


def _check_gap_unfilled(df, tp_price: float, gap_date_str: str) -> bool:
    """Check if a gap-down has been filled (price returned to TP level).
    Returns True if the gap is still UNFILLED (valid setup)."""
    if df is None or df.empty:
        return True
    rows = df.reset_index()
    cols = {c.lower(): c for c in rows.columns}
    date_col = cols.get("date", cols.get("datetime", None))
    high_col = cols.get("high", "High")
    if date_col is None:
        return True

    gap_dt = datetime.strptime(gap_date_str, "%Y-%m-%d").date()
    for _, row in rows.iterrows():
        row_date = row[date_col]
        if hasattr(row_date, "date"):
            row_date = row_date.date()
        elif hasattr(row_date, "strftime"):
            row_date = datetime.strptime(row_date.strftime("%Y-%m-%d"), "%Y-%m-%d").date()
        else:
            row_date = datetime.strptime(str(row_date)[:10], "%Y-%m-%d").date()

        if row_date <= gap_dt:
            continue
        # If any candle after the gap has high reaching the TP (pre-gap low), gap is filled
        try:
            h = float(row[high_col])
            if h >= tp_price:
                return False
        except (KeyError, TypeError):
            continue
    return True


async def run_scan():
    """Full scan: detect new gaps, check triggers on watching setups, update active trades."""
    settings = _scan_settings
    log.info("Starting gap scan across %d tickers with settings: %s", len(WATCHLIST), settings)
    if yf is None:
        log.error("yfinance not available -- cannot scan")
        return {"error": "yfinance not installed"}

    new_gaps = []
    triggered = []
    lookback_days = settings.get("lookback_days", 10)
    expire_days = settings.get("expire_days", 30)
    min_mcap = settings.get("min_market_cap", 0)
    max_mcap = settings.get("max_market_cap", 0)

    # Use period based on lookback days (min 3 months to catch older gaps)
    fetch_period = "3mo" if lookback_days <= 60 else "6mo"

    # Supplement watchlist with Finviz + Barchart gap-down stocks
    finviz_tickers = await fetch_finviz_gaps()
    barchart_tickers = await fetch_barchart_gaps()
    scan_list = sorted(set(WATCHLIST + finviz_tickers + barchart_tickers))
    log.info("Scanning %d tickers (%d watchlist, %d Finviz, %d Barchart)",
             len(scan_list), len(WATCHLIST), len(finviz_tickers), len(barchart_tickers))

    # Batch download for efficiency
    batch_size = 100
    all_data: Dict[str, Any] = {}
    total_batches = (len(scan_list) + batch_size - 1) // batch_size
    for batch_num, start in enumerate(range(0, len(scan_list), batch_size), 1):
        chunk = scan_list[start:start + batch_size]
        log.info("Downloading batch %d/%d (%d tickers)...", batch_num, total_batches, len(chunk))
        chunk_data = fetch_batch(chunk, period=fetch_period)
        all_data.update(chunk_data)
        await asyncio.sleep(0.3)

    log.info("Fetched data for %d / %d tickers", len(all_data), len(scan_list))

    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=lookback_days)

    # Remove watching setups that no longer match current settings
    min_gap = settings.get("min_gap_pct", 5.0)
    max_gap = settings.get("max_gap_pct", 50.0)
    min_price = settings.get("min_price", 1.0)
    max_price_setting = settings.get("max_price", 0)
    before_count = len(_setups)
    _setups[:] = [
        s for s in _setups
        if s["status"] != "watching"
        or (
            abs(s.get("gap_pct", 0)) >= min_gap
            and abs(s.get("gap_pct", 0)) <= max_gap
            and s.get("gap_candle_close", 0) >= min_price
            and (max_price_setting == 0 or s.get("gap_candle_close", 0) <= max_price_setting)
        )
    ]
    removed = before_count - len(_setups)
    if removed:
        log.info("Removed %d watching setups that don't match current settings", removed)

    # Existing gap_dates per symbol to avoid duplicates
    existing_keys = set()
    for s in _setups:
        existing_keys.add((s["symbol"], s["gap_date"]))

    # Market cap cache to avoid repeated lookups
    mcap_cache: Dict[str, Optional[float]] = {}

    # 1. Detect new gaps
    for sym, df in all_data.items():
        detected = detect_gaps(df, sym, settings)
        for setup in detected:
            key = (setup["symbol"], setup["gap_date"])
            gap_dt = datetime.strptime(setup["gap_date"], "%Y-%m-%d").date()
            if key in existing_keys:
                continue
            if gap_dt < cutoff:
                continue

            # Check gap is still unfilled (price hasn't reached TP level)
            if not _check_gap_unfilled(df, setup["tp_price"], setup["gap_date"]):
                continue  # gap already filled, skip

            # Market cap filter (only fetch if needed)
            if min_mcap > 0 or max_mcap > 0:
                if sym not in mcap_cache:
                    mcap_cache[sym] = _get_market_cap(sym)
                mcap = mcap_cache[sym]
                if mcap is not None:
                    if min_mcap > 0 and mcap < min_mcap:
                        continue
                    if max_mcap > 0 and mcap > max_mcap:
                        continue

            _setups.append(setup)
            new_gaps.append(setup)
            existing_keys.add(key)

    # 2. Check watching setups for trigger (close above gap candle high)
    for setup in _setups:
        if setup["status"] != "watching":
            continue
        sym = setup["symbol"]
        df = all_data.get(sym)
        if df is None or df.empty:
            continue
        last_row = df.iloc[-1]
        close_col = "Close" if "Close" in df.columns else "close"
        try:
            last_close = float(last_row[close_col])
        except (KeyError, TypeError):
            continue

        # Update current price
        setup["current_price"] = round(last_close, 2)

        if last_close > setup["trigger_price"]:
            if " | Trigger hit" not in (setup.get("notes") or ""):
                triggered.append(setup)
            setup["notes"] = (setup.get("notes") or "") + f" | Trigger hit at {last_close:.2f}"

        # Expire old setups
        gap_dt = datetime.strptime(setup["gap_date"], "%Y-%m-%d").date()
        if (today - gap_dt).days > expire_days:
            setup["status"] = "expired"

    # 3. Update active trades
    await update_active_trades(all_data)

    save_setups()

    # Send alerts
    if new_gaps:
        lines = [f"NEW GAP-DOWN SETUPS ({len(new_gaps)}):"]
        for g in new_gaps[:10]:
            lines.append(
                f"  {g['symbol']} gap {g['gap_pct']}% on {g['gap_date']} "
                f"| trigger {g['trigger_price']} | R/R {g['rr_ratio']}"
            )
        if len(new_gaps) > 10:
            lines.append(f"  ... and {len(new_gaps) - 10} more")
        await send_whatsapp("\n".join(lines))

    if triggered:
        lines = [f"TRIGGER HIT on {len(triggered)} setups:"]
        for t in triggered[:5]:
            lines.append(f"  {t['symbol']} closed above {t['trigger_price']}")
        await send_whatsapp("\n".join(lines))

    summary = {
        "scanned": len(all_data),
        "new_gaps": len(new_gaps),
        "triggers_hit": len(triggered),
        "total_setups": len(_setups),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    log.info("Scan complete: %s", summary)
    return summary


async def update_active_trades(all_data: Optional[Dict] = None):
    """Check active trades for SL/TP hits and update trailing stops."""
    active = [s for s in _setups if s["status"] == "active"]
    if not active:
        return

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for setup in active:
        sym = setup["symbol"]

        # Get current price
        current_price = None
        if all_data and sym in all_data:
            df = all_data[sym]
            close_col = "Close" if "Close" in df.columns else "close"
            try:
                current_price = float(df.iloc[-1][close_col])
            except (KeyError, TypeError, IndexError):
                pass

        if current_price is None:
            # Fetch individually
            df = fetch_daily(sym, period="5d")
            if df is not None and not df.empty:
                close_col = "Close" if "Close" in df.columns else "close"
                try:
                    current_price = float(df.iloc[-1][close_col])
                except (KeyError, TypeError):
                    continue
            else:
                continue

        entry = setup.get("entry_price")
        if entry is None or entry <= 0:
            continue

        sl = setup.get("trailing_sl") or setup["sl_price"]
        tp = setup["tp_price"]

        # Check TP hit
        if current_price >= tp:
            pnl = ((tp - entry) / entry) * 100.0
            setup["status"] = "completed"
            setup["exit_price"] = round(tp, 2)
            setup["exit_date"] = today_str
            setup["pnl_pct"] = round(pnl, 2)
            setup["notes"] = (setup.get("notes") or "") + " | TP hit"
            await send_whatsapp(
                f"TP HIT: {sym} exited at {tp:.2f} | PnL: {pnl:+.1f}%"
            )
            continue

        # Check SL hit
        if current_price <= sl:
            pnl = ((sl - entry) / entry) * 100.0
            setup["status"] = "completed"
            setup["exit_price"] = round(sl, 2)
            setup["exit_date"] = today_str
            setup["pnl_pct"] = round(pnl, 2)
            setup["notes"] = (setup.get("notes") or "") + " | SL hit"
            await send_whatsapp(
                f"SL HIT: {sym} exited at {sl:.2f} | PnL: {pnl:+.1f}%"
            )
            continue

        # Trailing stop: every N% gain from entry, move SL up M% from entry
        trail_step = _scan_settings.get("trailing_gain_step", 8.0)
        trail_raise = _scan_settings.get("trailing_sl_raise", 3.0)
        gain_pct = ((current_price - entry) / entry) * 100.0
        if gain_pct > 0:
            steps = int(gain_pct / trail_step)
            if steps > 0:
                new_sl = entry * (1.0 + (steps * trail_raise / 100.0))
                new_sl = round(new_sl, 2)
                old_sl = setup.get("trailing_sl") or setup["sl_price"]
                if new_sl > old_sl:
                    setup["trailing_sl"] = new_sl
                    log.info("%s trailing SL raised to %.2f (gain %.1f%%, step %d)",
                             sym, new_sl, gain_pct, steps)

    save_setups()


# ---------------------------------------------------------------------------
# Scheduled scan loop
# ---------------------------------------------------------------------------

async def scheduler(app: web.Application):
    """Run the gap scan daily at SCAN_HOUR_UTC (21:00 UTC = 4 PM ET)."""
    log.info("Scheduler started -- will scan daily at %02d:00 UTC", SCAN_HOUR_UTC)
    while True:
        now = datetime.now(timezone.utc)
        target = now.replace(hour=SCAN_HOUR_UTC, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        wait_secs = (target - now).total_seconds()
        log.info("Next scan in %.0f seconds (at %s)", wait_secs, target.isoformat())
        await asyncio.sleep(wait_secs)
        try:
            await run_scan()
        except Exception as e:
            log.error("Scheduled scan failed: %s", e, exc_info=True)


async def start_scheduler(app: web.Application):
    app["scheduler_task"] = asyncio.create_task(scheduler(app))


async def stop_scheduler(app: web.Application):
    task = app.get("scheduler_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# CORS middleware
# ---------------------------------------------------------------------------

@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        resp = web.Response(status=200)
    else:
        try:
            resp = await handler(request)
        except web.HTTPException as ex:
            resp = ex
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return resp


# ---------------------------------------------------------------------------
# API handlers
# ---------------------------------------------------------------------------

async def handle_refresh_prices(request: web.Request) -> web.Response:
    """GET /api/gaps/prices -- fetch latest prices for all watching/active setups."""
    if yf is None:
        return web.json_response({"error": "yfinance not installed"}, status=500)

    symbols = list(set(
        s["symbol"] for s in _setups if s["status"] in ("watching", "active")
    ))
    if not symbols:
        return web.json_response({})

    prices = {}
    try:
        data = yf.download(symbols, period="1d", group_by="ticker",
                           threads=True, progress=False)
        if data is not None and not data.empty:
            for sym in symbols:
                try:
                    if len(symbols) == 1:
                        df = data
                    else:
                        df = data[sym].dropna(how="all")
                    if df is not None and not df.empty:
                        close_col = "Close" if "Close" in df.columns else "close"
                        prices[sym] = round(float(df.iloc[-1][close_col]), 2)
                except (KeyError, TypeError, IndexError):
                    continue
    except Exception as e:
        log.error("Price refresh error: %s", e)

    # Update current_price on setups
    for s in _setups:
        if s["symbol"] in prices:
            s["current_price"] = prices[s["symbol"]]
    save_setups()

    return web.json_response(prices)


async def handle_get_settings(request: web.Request) -> web.Response:
    """GET /api/gaps/settings -- return current scan settings."""
    return web.json_response(_scan_settings)


async def handle_save_settings(request: web.Request) -> web.Response:
    """POST /api/gaps/settings -- save scan settings."""
    global _scan_settings
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    # Merge with defaults, only accept known keys
    for key in DEFAULT_SETTINGS:
        if key in body:
            val = body[key]
            # Type coerce
            if isinstance(DEFAULT_SETTINGS[key], float):
                val = float(val)
            elif isinstance(DEFAULT_SETTINGS[key], int):
                val = int(val)
            elif isinstance(DEFAULT_SETTINGS[key], str):
                val = str(val)
            _scan_settings[key] = val
    save_settings()
    return web.json_response(_scan_settings)


async def handle_download_csv(request: web.Request) -> web.Response:
    """GET /api/gaps/download -- download setups as CSV with settings header."""
    import io, csv as csvmod
    output = io.StringIO()

    # Write settings as comment header
    output.write("# Gap Screener Settings\n")
    for k, v in _scan_settings.items():
        output.write(f"# {k}: {v}\n")
    output.write("#\n")

    fields = [
        "symbol", "status", "gap_date", "pre_gap_date", "post_gap_date", "gap_pct",
        "current_price",
        "pre_gap_open", "pre_gap_high", "pre_gap_low", "pre_gap_close",
        "gap_candle_open", "gap_candle_high", "gap_candle_low", "gap_candle_close",
        "post_gap_open", "post_gap_high", "post_gap_low", "post_gap_close",
        "trigger_price", "sl_price", "tp_price", "rr_ratio",
        "entry_price", "entry_date", "exit_price", "exit_date", "pnl_pct",
        "trailing_sl", "notes", "detected_at",
    ]
    writer = csvmod.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for s in _setups:
        writer.writerow(s)
    csv_content = output.getvalue()
    return web.Response(
        text=csv_content,
        content_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=gap_setups.csv"},
    )


async def handle_delete_setup(request: web.Request) -> web.Response:
    """DELETE /api/gaps/{symbol} -- remove a setup."""
    symbol = request.match_info["symbol"].upper()
    gap_date = request.query.get("gap_date", "")
    global _setups
    before = len(_setups)
    if gap_date:
        _setups = [s for s in _setups if not (s["symbol"] == symbol and s["gap_date"] == gap_date)]
    else:
        _setups = [s for s in _setups if s["symbol"] != symbol]
    removed = before - len(_setups)
    save_setups()
    return web.json_response({"removed": removed, "remaining": len(_setups)})


async def handle_clear_setups(request: web.Request) -> web.Response:
    """POST /api/gaps/clear -- clear all setups (or by status)."""
    global _setups
    status = request.query.get("status")
    if status:
        _setups = [s for s in _setups if s["status"] != status]
    else:
        _setups = []
    save_setups()
    return web.json_response({"remaining": len(_setups)})


async def handle_get_gaps(request: web.Request) -> web.Response:
    """GET /api/gaps -- returns all setups. Supports ?status=, ?sort=, ?order= params."""
    status_filter = request.query.get("status")
    sort_by = request.query.get("sort", "gap_pct")  # default sort by gap %
    order = request.query.get("order", "asc")  # asc = biggest gap first (most negative)
    results = list(_setups)
    if status_filter:
        results = [s for s in results if s["status"] == status_filter]
    # Sort
    if sort_by in ("gap_pct", "rr_ratio", "trigger_price", "symbol", "gap_date"):
        reverse = (order == "desc")
        try:
            results.sort(key=lambda s: s.get(sort_by, 0) or 0, reverse=reverse)
        except TypeError:
            pass
    return web.json_response(results)


_scan_task: Optional[asyncio.Task] = None
_scan_result: Optional[Dict] = None
_scan_running = False


async def _run_scan_bg():
    global _scan_result, _scan_running
    _scan_running = True
    try:
        _scan_result = await run_scan()
    except Exception as e:
        log.error("Scan failed: %s", e)
        _scan_result = {"error": str(e)}
    finally:
        _scan_running = False


async def handle_scan(request: web.Request) -> web.Response:
    """GET /api/gaps/scan -- trigger scan. Returns immediately, runs in background."""
    global _scan_task, _scan_result
    if _scan_running:
        return web.json_response({"status": "running", "message": "Scan already in progress"})
    _scan_result = None
    _scan_task = asyncio.create_task(_run_scan_bg())
    return web.json_response({"status": "started", "tickers": len(WATCHLIST)})


async def handle_scan_status(request: web.Request) -> web.Response:
    """GET /api/gaps/scan/status -- check scan progress."""
    if _scan_running:
        return web.json_response({"status": "running", "setups_so_far": len(_setups)})
    if _scan_result is not None:
        return web.json_response({"status": "complete", "result": _scan_result})
    return web.json_response({"status": "idle"})


async def handle_chart(request: web.Request) -> web.Response:
    """GET /api/gaps/chart/{symbol} -- return daily OHLCV for charting."""
    symbol = request.match_info["symbol"].upper()
    df = fetch_daily(symbol, period=f"{CHART_DAYS}d")
    if df is None or df.empty:
        return web.json_response({"error": f"No data for {symbol}"}, status=404)

    rows = df.reset_index()
    date_col = "Date" if "Date" in rows.columns else "Datetime"
    ohlcv = []
    for _, row in rows.iterrows():
        try:
            dt_val = row[date_col]
            if hasattr(dt_val, "isoformat"):
                dt_str = dt_val.isoformat()
            else:
                dt_str = str(dt_val)
            ohlcv.append({
                "date": dt_str[:10],
                "open": round(float(row.get("Open", row.get("open", 0))), 2),
                "high": round(float(row.get("High", row.get("high", 0))), 2),
                "low": round(float(row.get("Low", row.get("low", 0))), 2),
                "close": round(float(row.get("Close", row.get("close", 0))), 2),
                "volume": int(row.get("Volume", row.get("volume", 0))),
            })
        except Exception:
            continue

    # Also find setup info if it exists
    setup = find_setup(symbol)

    return web.json_response({
        "symbol": symbol,
        "ohlcv": ohlcv,
        "setup": setup,
    })


async def handle_activate(request: web.Request) -> web.Response:
    """POST /api/gaps/{symbol}/activate -- manually activate a trade."""
    symbol = request.match_info["symbol"].upper()
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    setup = find_setup(symbol, status="watching")
    if not setup:
        return web.json_response(
            {"error": f"No watching setup found for {symbol}"}, status=404
        )

    entry_price = body.get("entry_price", setup["trigger_price"])
    setup["status"] = "active"
    setup["entry_price"] = round(float(entry_price), 2)
    setup["entry_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    setup["trailing_sl"] = setup["sl_price"]
    save_setups()

    await send_whatsapp(
        f"TRADE ACTIVATED: {symbol} entry at {setup['entry_price']:.2f} "
        f"| SL {setup['sl_price']:.2f} | TP {setup['tp_price']:.2f} "
        f"| R/R {setup['rr_ratio']}"
    )

    return web.json_response(setup)


async def handle_close(request: web.Request) -> web.Response:
    """POST /api/gaps/{symbol}/close -- manually close a trade."""
    symbol = request.match_info["symbol"].upper()
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    setup = find_setup(symbol, status="active")
    if not setup:
        return web.json_response(
            {"error": f"No active trade found for {symbol}"}, status=404
        )

    exit_price = body.get("exit_price")
    if exit_price is None:
        # Try to get current price
        df = fetch_daily(symbol, period="5d")
        if df is not None and not df.empty:
            close_col = "Close" if "Close" in df.columns else "close"
            exit_price = float(df.iloc[-1][close_col])
        else:
            return web.json_response(
                {"error": "Provide exit_price or ensure yfinance works"}, status=400
            )

    exit_price = round(float(exit_price), 2)
    entry = setup["entry_price"]
    pnl = ((exit_price - entry) / entry) * 100.0

    setup["status"] = "completed"
    setup["exit_price"] = exit_price
    setup["exit_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    setup["pnl_pct"] = round(pnl, 2)
    setup["notes"] = (setup.get("notes") or "") + f" | Manual close"
    save_setups()

    await send_whatsapp(
        f"TRADE CLOSED: {symbol} at {exit_price:.2f} | PnL: {pnl:+.1f}%"
    )

    return web.json_response(setup)


async def handle_active(request: web.Request) -> web.Response:
    """GET /api/gaps/active -- return only active trades."""
    active = [s for s in _setups if s["status"] == "active"]
    return web.json_response(active)


async def handle_stats(request: web.Request) -> web.Response:
    """GET /api/gaps/stats -- return portfolio stats."""
    active = [s for s in _setups if s["status"] == "active"]
    completed = [s for s in _setups if s["status"] == "completed"]
    watching = [s for s in _setups if s["status"] == "watching"]
    expired = [s for s in _setups if s["status"] == "expired"]

    winners = [s for s in completed if (s.get("pnl_pct") or 0) > 0]
    losers = [s for s in completed if (s.get("pnl_pct") or 0) <= 0]

    total_pnl = sum(s.get("pnl_pct") or 0 for s in completed)
    avg_win = (sum(s["pnl_pct"] for s in winners) / len(winners)) if winners else 0
    avg_loss = (sum(s["pnl_pct"] for s in losers) / len(losers)) if losers else 0
    win_rate = (len(winners) / len(completed) * 100) if completed else 0

    # Active trade unrealized P/L
    active_pnl = []
    for s in active:
        entry = s.get("entry_price")
        if entry and entry > 0:
            df = fetch_daily(s["symbol"], period="5d")
            if df is not None and not df.empty:
                close_col = "Close" if "Close" in df.columns else "close"
                try:
                    curr = float(df.iloc[-1][close_col])
                    unrealized = ((curr - entry) / entry) * 100.0
                    active_pnl.append({
                        "symbol": s["symbol"],
                        "entry": entry,
                        "current": round(curr, 2),
                        "unrealized_pct": round(unrealized, 2),
                        "sl": s.get("trailing_sl") or s["sl_price"],
                        "tp": s["tp_price"],
                    })
                except Exception:
                    pass

    stats = {
        "total_setups": len(_setups),
        "watching": len(watching),
        "active": len(active),
        "completed": len(completed),
        "expired": len(expired),
        "winners": len(winners),
        "losers": len(losers),
        "win_rate_pct": round(win_rate, 1),
        "total_pnl_pct": round(total_pnl, 2),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "active_positions": active_pnl,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return web.json_response(stats)


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

def create_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware])

    app.router.add_get("/api/gaps", handle_get_gaps)
    app.router.add_get("/api/gaps/scan", handle_scan)
    app.router.add_get("/api/gaps/scan/status", handle_scan_status)
    app.router.add_get("/api/gaps/active", handle_active)
    app.router.add_get("/api/gaps/stats", handle_stats)
    app.router.add_get("/api/gaps/prices", handle_refresh_prices)
    app.router.add_get("/api/gaps/settings", handle_get_settings)
    app.router.add_post("/api/gaps/settings", handle_save_settings)
    app.router.add_get("/api/gaps/download", handle_download_csv)
    app.router.add_post("/api/gaps/clear", handle_clear_setups)
    app.router.add_get("/api/gaps/chart/{symbol}", handle_chart)
    app.router.add_post("/api/gaps/{symbol}/activate", handle_activate)
    app.router.add_post("/api/gaps/{symbol}/close", handle_close)
    app.router.add_delete("/api/gaps/{symbol}", handle_delete_setup)

    # Serve the main page
    async def handle_index(request):
        return web.FileResponse(os.path.join(STATIC_DIR, "gaps.html"))

    app.router.add_get("/", handle_index)

    # Serve static files if the directory exists
    if os.path.isdir(STATIC_DIR):
        app.router.add_static("/static/", STATIC_DIR, name="static")

    app.on_startup.append(start_scheduler)
    app.on_cleanup.append(stop_scheduler)

    return app


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    load_setups()
    load_settings()
    app = create_app()
    log.info("Starting Gap Scanner on http://0.0.0.0:5558")
    log.info("Watchlist: %d tickers | Setups loaded: %d", len(WATCHLIST), len(_setups))
    web.run_app(app, host="0.0.0.0", port=5558)
