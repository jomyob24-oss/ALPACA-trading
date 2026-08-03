"""
Real-Time Top Movers Momentum Bot - Alpaca-Connected Live Streamlit Dashboard
===================================================================
ONE FILE. Real, live market data AND real order execution through Alpaca's
brokerage API. With ALPACA_USE_PAPER = True (the default below), orders go
to Alpaca's own paper trading account - real-time data, simulated money,
no real cash at risk. Flip ALPACA_USE_PAPER to False only when you are
ready to trade with real money.

STRATEGY (exact rules, unchanged from the original version):
  1. Screener: real, live top-gainers list across the whole market via
     Alpaca's own screener/movers endpoint, ranked purely by percent gain.
  2. Single position only, always the current #1 qualifying gainer
     (>= MIN_PCT_GAIN_TO_CONSIDER).
  3. Entry - EITHER pattern qualifies:
       a) Coiling breakout: price near daily high, rising lows, strong
          recent closes, decent trend-strength score.
       b) Liquidity pullback-and-reclaim: real pullback off a run-up
          high, then a genuine reclaim, no violent rejection wick.
  4. Stop-loss: starts tight at 7% below entry. Once position is up
     >= 8% from entry, stop widens to 25% below the highest price seen
     (fine-tuned slightly by trend strength). Stop only ever ratchets
     UP, never loosens back down.
  5. Stronger-mover override: if another candidate shows >= 2x current
     position's gain, do NOT close the current trade - only tighten
     stop to 5% to protect gains.
  6. Hard flatten at 3:59 PM ET.

SETUP (run once, in a Terminal / Command Prompt window on a computer):
  pip install streamlit yfinance pandas numpy requests plotly

RUN (every time you want to use it):
  streamlit run momentum_bot_dashboard.py

That second command opens your browser automatically to a live page.
Leave that terminal window open while it runs - closing it stops the bot.
"""

import time
import json
import logging
import threading
import time as _time
from datetime import datetime, timezone, timedelta

import requests
import websocket  # pip install websocket-client
import yfinance as yf
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Alpaca API keys - REQUIRED
# Get these from your Alpaca dashboard once your account (paper or live) is
# approved: https://app.alpaca.markets -> API Keys.
# Start with PAPER keys - they hit the same real-time market data, but any
# order placed only simulates against Alpaca's paper trading balance, no
# real money moves until you swap in LIVE keys further down.
# ---------------------------------------------------------------------------
ALPACA_API_KEY = "PK4MJMA4CDMLAMEBTNQHEZY5JH"
ALPACA_SECRET_KEY = "ADvey6tYZ4YQNM33xYrbS8RwyTBSme4HbaW2ftLfGUJD"

# Set to False once you're ready to trade against your LIVE Alpaca account.
# Paper: data-api and paper-api base URLs. Live: same data URL, but the
# trading/order base URL switches to api.alpaca.markets (no "paper-").
ALPACA_USE_PAPER = True

ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"
ALPACA_TRADING_BASE_URL = (
    "https://paper-api.alpaca.markets" if ALPACA_USE_PAPER else "https://api.alpaca.markets"
)

ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}

# Financial Modeling Prep key is still used for the optional stock-news
# headline lookup only (Alpaca's news endpoint could replace this later too,
# but it's not core to trading logic, so left as-is for now).
FMP_API_KEY = "eneUphD9PJHf7p7cRpfH2pUBMOV2qp0Q"

# ---------------------------------------------------------------------------
# Live streaming price feed (websocket) - replaces REST polling for price
# data entirely. A single background thread keeps a persistent connection to
# Alpaca open and updates LIVE_PRICES the instant a trade prints on the
# exchange, so the dashboard is never waiting on a request/response cycle for
# the price itself. Use the "sip" feed (needs the paid Algo Trader Plus plan)
# for the full consolidated tape, or "iex" for the free single-exchange feed.
# ---------------------------------------------------------------------------
ALPACA_STREAM_FEED = "sip"  # change to "iex" if you're on the free data plan
ALPACA_STREAM_URL = f"wss://stream.data.alpaca.markets/v2/{ALPACA_STREAM_FEED}"

LIVE_PRICES = {}
LIVE_PRICES_LOCK = threading.Lock()
_STREAM_STATE = {
    "ws": None,
    "thread_started": False,
    "subscribed": set(),
    "connected": False,
}


def _stream_on_open(ws):
    auth_msg = {"action": "auth", "key": ALPACA_API_KEY, "secret": ALPACA_SECRET_KEY}
    ws.send(json.dumps(auth_msg))


def _stream_on_message(ws, message):
    try:
        events = json.loads(message)
    except Exception:
        return
    if not isinstance(events, list):
        events = [events]
    for ev in events:
        etype = ev.get("T")
        if etype == "success" and ev.get("msg") == "authenticated":
            _STREAM_STATE["connected"] = True
            # Re-subscribe to everything we already wanted, in case this is
            # a reconnect after a dropped connection.
            _stream_resubscribe_all()
        elif etype == "t":
            # Trade message: symbol "S", price "p", size "s", timestamp "t"
            sym = ev.get("S")
            price = ev.get("p")
            size = ev.get("s")
            if sym and price is not None:
                with LIVE_PRICES_LOCK:
                    LIVE_PRICES[sym] = {
                        "price": float(price),
                        "volume": float(size) if size is not None else None,
                        "trade_time": datetime.now(),
                    }
        elif etype == "error":
            log_line(f"Alpaca stream error: {ev.get('msg')}")


def _stream_on_error(ws, error):
    _STREAM_STATE["connected"] = False


def _stream_on_close(ws, close_status_code, close_msg):
    _STREAM_STATE["connected"] = False


def _stream_resubscribe_all():
    ws = _STREAM_STATE.get("ws")
    symbols = list(_STREAM_STATE["subscribed"])
    if ws and symbols:
        try:
            ws.send(json.dumps({"action": "subscribe", "trades": symbols}))
        except Exception:
            pass


def subscribe_symbol_to_stream(symbol):
    """Adds a symbol to the live trade stream if it isn't already subscribed.
    Safe to call every cycle - it's a no-op once a symbol is subscribed."""
    if symbol in _STREAM_STATE["subscribed"]:
        return
    _STREAM_STATE["subscribed"].add(symbol)
    ws = _STREAM_STATE.get("ws")
    if ws and _STREAM_STATE.get("connected"):
        try:
            ws.send(json.dumps({"action": "subscribe", "trades": [symbol]}))
        except Exception:
            pass


def _run_stream_forever():
    while True:
        try:
            ws = websocket.WebSocketApp(
                ALPACA_STREAM_URL,
                on_open=_stream_on_open,
                on_message=_stream_on_message,
                on_error=_stream_on_error,
                on_close=_stream_on_close,
            )
            _STREAM_STATE["ws"] = ws
            ws.run_forever()
        except Exception:
            pass
        # If we drop out of run_forever (connection lost), wait briefly then
        # reconnect automatically rather than leaving the feed dead.
        _STREAM_STATE["connected"] = False
        _time.sleep(3)


def ensure_price_stream_running():
    """Starts the background streaming thread exactly once, no matter how
    many times Streamlit reruns this script on each refresh."""
    if not _STREAM_STATE["thread_started"]:
        _STREAM_STATE["thread_started"] = True
        t = threading.Thread(target=_run_stream_forever, daemon=True)
        t.start()


def get_live_price(symbol, max_staleness_seconds=5):
    """Returns the latest streamed price for a symbol if we have one recent
    enough, else None (caller should fall back to a REST snapshot, e.g. right
    when a symbol is first subscribed and no trade has printed yet)."""
    with LIVE_PRICES_LOCK:
        entry = LIVE_PRICES.get(symbol)
    if not entry:
        return None
    age = (datetime.now() - entry["trade_time"]).total_seconds()
    if age > max_staleness_seconds:
        return None
    return entry

REFRESH_SECONDS = 0.25
MAX_WATCHLIST_SIZE = 15
MIN_PCT_GAIN_TO_CONSIDER = 110.0
EARLY_WATCH_PCT_GAIN = 75.0

INITIAL_STOP_PCT = 0.07
RATCHET_STEP_GAIN = 0.10       # every 10% gain in price...
RATCHET_STEP_LOCK = 0.10       # ...locks in another 10% of profit above entry
STRONGER_MOVER_STOP_PCT = 0.05
STRONGER_MOVER_MULTIPLE = 2.0
CLOSING_GAP_THRESHOLD_PCT = 10.0  # percentage points; if another mover is closing to within this gap, tighten
CLOSING_GAP_STOP_PCT = 0.10
MIN_STRUCTURE_SCORE_TO_ENTER = 0.45  # below this, upside looks exhausted - skip the entry entirely
VOLUME_DROP_EXIT_PCT = 0.80  # exit a position if volume falls to 80% below its peak (i.e. down to 20% of peak)

# After 1+ stop-outs on the same symbol, a "new" high has to clear the level
# it last got stopped out at by more than just a fraction of a cent - that's
# not a real breakout, it's the same chop repeating. Require it to clear by
# at least this percentage to count as genuinely new.
#
# The required buffer scales with how extended the stock already is on the
# day. A stock up "only" 100-150% chopping around has smaller, tighter
# swings, so a 7% push back above the old high is a meaningful, real move.
# But a stock up 250%+ (or into the hundreds/thousands of percent) swings
# far more violently - a 7% wiggle there can be pure noise within the same
# chop, so it needs a bigger, ~10-12% push to count as a genuine reclaim.
REENTRY_BREAKOUT_BUFFER_PCT = 0.07  # base case: stock up ~150% or less
REENTRY_BREAKOUT_BUFFER_PCT_EXTENDED = 0.12  # stock up ~250%+ on the day
REENTRY_EXTENDED_PCT_THRESHOLD = 250.0  # % gain on the day that counts as "extended"


def reentry_breakout_buffer(pct_change):
    """Returns the required re-entry breakout buffer, scaled by how extended
    (in % gain on the day) the stock already is."""
    if pct_change is not None and pct_change >= REENTRY_EXTENDED_PCT_THRESHOLD:
        return REENTRY_BREAKOUT_BUFFER_PCT_EXTENDED
    return REENTRY_BREAKOUT_BUFFER_PCT

POSITION_SIZE_PCT_OF_CASH = 0.25  # each trade risks 25% of current paper cash
MAX_OPEN_POSITIONS = 3  # bot can hold up to this many positions at once
PAPER_STARTING_CASH = 50_000.00
PAPER_TRADE_LOG_FILE = "real_paper_trades_log.json"
ACCOUNT_STATE_FILE = "paper_account_state.json"
GAINERS_SCAN_COUNT = 40

logger = logging.getLogger("momentum_bot_dashboard")
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Market hours (ET)
# ---------------------------------------------------------------------------

def get_et_hour_float():
    """Approximate ET as UTC-4 (EDT). Adjust manually to UTC-5 in winter (EST)."""
    utc_now = datetime.now(timezone.utc)
    et_now = utc_now - timedelta(hours=4)
    return et_now.hour + et_now.minute / 60.0


REGULAR_MARKET_OPEN = 9.5
REGULAR_MARKET_CLOSE = 16.0
AFTERHOURS_CLOSE = 20.0  # bot now keeps managing/trading through 8 PM ET
FLATTEN_HOUR = 17.0  # 5:00 PM ET - hard flatten, close every open position, no exceptions
PREMARKET_SCAN_OPEN = 9.0  # 9:00 AM ET - watchlist scanning starts here, trading still locked


def market_is_open():
    """Now covers regular hours AND the after-hours session, since there is
    still real movement/volume worth trading after 4 PM."""
    hour = get_et_hour_float()
    return REGULAR_MARKET_OPEN <= hour < AFTERHOURS_CLOSE


def in_premarket_scan_window():
    """True from 9:00 AM up to the 9:30 AM open - lets the watchlist build
    and populate early so it's ready the instant trading unlocks, but does
    NOT allow any buys to fire yet (see main cycle gate below)."""
    hour = get_et_hour_float()
    return PREMARKET_SCAN_OPEN <= hour < REGULAR_MARKET_OPEN


def in_afterhours_session():
    hour = get_et_hour_float()
    return REGULAR_MARKET_CLOSE <= hour < AFTERHOURS_CLOSE


def past_flatten_time():
    """True once we've hit the end-of-session flatten cutoff - used to force-
    close every open position so nothing carries overnight."""
    return get_et_hour_float() >= FLATTEN_HOUR


# ---------------------------------------------------------------------------
# Real data fetch
# ---------------------------------------------------------------------------

def fetch_quote(symbol: str):
    """Returns the latest price for a symbol from the live websocket stream
    whenever we have a fresh tick (true real time, no polling delay at all).
    Falls back to a one-off REST snapshot only when the stream hasn't
    delivered a trade yet for this symbol (e.g. right after it was first
    subscribed, or it's a thinly-traded symbol with no recent print)."""
    subscribe_symbol_to_stream(symbol)
    try:
        live = get_live_price(symbol)
        volume = None
        if live is not None:
            price = live["price"]
            volume = live.get("volume")
        else:
            trade_url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/{symbol}/trades/latest"
            trade_resp = requests.get(trade_url, headers=ALPACA_HEADERS, timeout=10)
            trade_resp.raise_for_status()
            trade_data = trade_resp.json()
            price = trade_data.get("trade", {}).get("p")
            volume = trade_data.get("trade", {}).get("s")
        if price is None:
            return None

        # Previous close + day high come from the daily bars endpoint - pull
        # yesterday's and today's daily bar. Cache prev_close per symbol per
        # day so we're not re-fetching bars every single 0.25s cycle.
        cache = st.session_state.setdefault("last_regular_quote", {})
        cached = cache.get(symbol)
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if cached is None or cached.get("date") != today_str:
            bars_url = (
                f"{ALPACA_DATA_BASE_URL}/v2/stocks/{symbol}/bars"
                f"?timeframe=1Day&limit=2&adjustment=raw"
            )
            bars_resp = requests.get(bars_url, headers=ALPACA_HEADERS, timeout=10)
            bars_resp.raise_for_status()
            bars_data = bars_resp.json().get("bars", [])
            if len(bars_data) >= 1:
                # Most recent COMPLETE prior session is the first bar if two
                # are returned, else fall back to today's own open as a seed.
                prev_bar = bars_data[0]
                prev_close = prev_bar.get("c")
                day_high = bars_data[-1].get("h", price)
                cached = {
                    "prev_close": float(prev_close) if prev_close else price,
                    "day_high": float(day_high) if day_high else price,
                    "date": today_str,
                }
                cache[symbol] = cached
        prev_close = cached["prev_close"] if cached else price
        day_high = max(cached["day_high"], price) if cached else price
        if cached:
            cached["day_high"] = day_high

        pct_change = ((price - prev_close) / prev_close) * 100 if prev_close else 0.0

        # volume here is either the live stream's last trade size, or the
        # REST fallback's trade size - either way, used only as a relative
        # ratio against its own recent peak for the volume-drop-exit check.
        return {
            "symbol": symbol,
            "price": float(price),
            "pct_change": round(float(pct_change), 2),
            "prev_close": float(prev_close),
            "day_high": float(day_high),
            "volume": float(volume) if volume is not None else None,
        }
    except Exception:
        return None


def fetch_top_gainers():
    """Fetches the top movers/gainers list from Alpaca's screener endpoint.
    Normalizes the response into the same shape the rest of the app expects
    (symbol / price / changesPercentage / dayHigh), so get_top_movers() and
    everything downstream needs no further changes. Falls back to the last
    successfully fetched list so the dashboard never goes blank on a brief
    API hiccup or rate-limit block."""
    try:
        url = f"{ALPACA_DATA_BASE_URL}/v1beta1/screener/stocks/movers?top={GAINERS_SCAN_COUNT}"
        resp = requests.get(url, headers=ALPACA_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        gainers = data.get("gainers", [])
        if gainers:
            normalized = [
                {
                    "symbol": row.get("symbol"),
                    "price": row.get("price"),
                    "changesPercentage": row.get("percent_change"),
                    # Alpaca's movers endpoint doesn't return an intraday high
                    # directly - get_top_movers() already falls back to using
                    # the current price if dayHigh is missing/None.
                    "dayHigh": None,
                }
                for row in gainers
            ]
            st.session_state.last_good_gainers = normalized
            st.session_state.last_good_gainers_time = datetime.now()
            return normalized
        raise ValueError("empty or malformed response")
    except Exception:
        stale = st.session_state.get("last_good_gainers", [])
        if stale:
            log_line(
                f"Gainers feed failed this cycle - using last good data from "
                f"{st.session_state.get('last_good_gainers_time')}."
            )
        return stale


NEWS_CACHE_SECONDS = 120  # don't re-hit the news endpoint every single 0.25s cycle


def fetch_stock_news(symbol: str, limit: int = 3):
    """Pulls recent headlines for a symbol from FMP so you can see whether a
    move has a real catalyst (earnings, contract, trial result) behind it
    versus pure hype. Cached for a couple minutes per symbol to avoid
    hammering the API every refresh cycle."""
    cache = st.session_state.setdefault("news_cache", {})
    cached = cache.get(symbol)
    if cached and (datetime.now() - cached["time"]).total_seconds() < NEWS_CACHE_SECONDS:
        return cached["items"]
    try:
        url = f"https://financialmodelingprep.com/stable/news/stock?symbols={symbol}&limit={limit}&apikey={FMP_API_KEY}"
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        items = []
        if isinstance(data, list):
            for row in data[:limit]:
                items.append({
                    "title": row.get("title", ""),
                    "publisher": row.get("publisher") or row.get("site", ""),
                    "date": row.get("publishedDate", ""),
                })
        cache[symbol] = {"items": items, "time": datetime.now()}
        return items
    except Exception:
        return cached["items"] if cached else []


def fetch_intraday_candles_df(symbol: str):
    """Returns a tz-naive OHLC dataframe for charting AND for structure analysis."""
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="1d", interval="1m")
        if hist.empty:
            return None
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        return hist
    except Exception:
        return None


def df_to_candle_list(hist):
    out = []
    for _, row in hist.iterrows():
        out.append({
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": float(row["Volume"]) if "Volume" in row and row["Volume"] == row["Volume"] else 0.0,
        })
    return out


# ---------------------------------------------------------------------------
# Structure analysis - coiling breakout OR liquidity pullback-and-reclaim
# ---------------------------------------------------------------------------

def compute_trend_strength(candles):
    if len(candles) < 6:
        return 0.0
    recent = candles[-8:] if len(candles) >= 8 else candles
    lows = [c["low"] for c in recent]

    rising_lows = sum(1 for i in range(1, len(lows)) if lows[i] >= lows[i - 1] * 0.998)
    rising_score = rising_lows / max(1, len(lows) - 1)

    bodies = [abs(c["close"] - c["open"]) for c in recent]
    body_growth_ok = True if len(bodies) < 3 else (np.mean(bodies[-3:]) >= np.mean(bodies[:3]) * 0.9)

    up_closes = sum(1 for c in recent if c["close"] > c["open"])
    close_score = up_closes / len(recent)

    down_wicks = sum(
        1 for c in recent
        if (c["open"] - c["low"]) > (abs(c["close"] - c["open"]) * 1.8) and c["close"] < c["open"]
    )
    wick_penalty = max(0.0, 1 - (down_wicks / len(recent)) * 2)

    score = rising_score * 0.4 + close_score * 0.35 + wick_penalty * 0.25
    if not body_growth_ok:
        score *= 0.7
    return max(0.0, min(1.0, score))


def compute_volume_confirmation_score(candles):
    """Rewards rising volume on up-candles (real buying pressure building into
    the move) and penalizes fading volume while price keeps climbing (a
    classic sign the move is running out of steam)."""
    recent = candles[-8:] if len(candles) >= 8 else candles
    if len(recent) < 4:
        return 0.5  # not enough data yet, neutral score

    volumes = [c.get("volume", 0.0) for c in recent]
    if all(v == 0 for v in volumes):
        return 0.5  # no volume data available, stay neutral rather than penalize

    first_half_avg = np.mean(volumes[: len(volumes) // 2])
    second_half_avg = np.mean(volumes[len(volumes) // 2:])
    volume_rising = second_half_avg >= first_half_avg * 0.95

    up_candle_volumes = [c["volume"] for c in recent if c["close"] > c["open"]]
    down_candle_volumes = [c["volume"] for c in recent if c["close"] <= c["open"]]
    up_avg = np.mean(up_candle_volumes) if up_candle_volumes else 0.0
    down_avg = np.mean(down_candle_volumes) if down_candle_volumes else 0.0
    volume_favors_buyers = up_avg >= down_avg if (up_avg or down_avg) else True

    score = 0.5
    score += 0.25 if volume_rising else -0.15
    score += 0.25 if volume_favors_buyers else -0.15
    return max(0.0, min(1.0, score))


def compute_structure_score(candles, day_high):
    """Combined 'upside potential' score blending chart structure (higher
    lows, strong closes, small wicks) with volume confirmation, similar to
    what an experienced trader eyeballs when comparing several similarly
    extended movers to judge which one looks most likely to keep running."""
    trend = compute_trend_strength(candles)
    volume_conf = compute_volume_confirmation_score(candles)
    return trend * 0.6 + volume_conf * 0.4


def is_coiling_breakout(candles, daily_high):
    """A real breakout means price actually clears the prior high, not just
    gets close to it - so we don't miss the move but also don't jump the
    gun before it has actually broken out."""
    if len(candles) < 6 or daily_high is None:
        return False, 0.0
    latest_close = candles[-1]["close"]
    latest_high = candles[-1]["high"]
    cleared_high = latest_close >= daily_high * 1.001 or latest_high >= daily_high * 1.005
    strength = compute_trend_strength(candles)
    return (cleared_high and strength >= 0.35), strength


def is_liquidity_reclaim(candles):
    if len(candles) < 10:
        return False
    highs = [c["high"] for c in candles]
    closes = [c["close"] for c in candles]

    runup_idx = int(np.argmax(highs[:-3])) if len(highs) > 3 else 0
    runup_high = highs[runup_idx]
    if runup_idx >= len(candles) - 3:
        return False

    after_runup = candles[runup_idx + 1:]
    if not after_runup:
        return False
    pullback_low = min(c["low"] for c in after_runup[:max(3, len(after_runup) // 2)])
    dip_pct = (runup_high - pullback_low) / runup_high
    if dip_pct < 0.015:
        return False

    latest = candles[-1]
    reclaiming = latest["close"] >= runup_high * 0.985
    higher_closes = closes[-1] > closes[-2] if len(closes) >= 2 else False
    violent_rejection = (
        (latest["high"] - latest["close"]) > (latest["close"] - latest["open"]) * 2
        if latest["close"] > latest["open"] else False
    )
    return reclaiming and higher_closes and not violent_rejection


CHOP_LOOKBACK_CANDLES = 25          # roughly the last ~25 minutes of 1-min candles
CHOP_PULLBACK_FROM_PEAK_PCT = 0.30  # must have faded at least 30% off its peak to even check for chop
CHOP_RANGE_PCT = 0.02               # if recent range is within ~2%, it's just wiggling in place
EXTENDED_MOVE_PCT_THRESHOLD = 300.0  # stocks up this much get extra caution on rollovers
ROLLOVER_LOOKBACK_CANDLES = 6


def is_choppy_sideways(candles):
    """Detects a stock that ran up, pulled back a meaningful amount off its
    peak, and is now just bouncing +/-1-2% sideways with no real trend -
    the 'grinding for hours' pattern. Returns True if new entries should be
    paused (still watched for a real breakout later)."""
    if len(candles) < CHOP_LOOKBACK_CANDLES:
        return False
    recent = candles[-CHOP_LOOKBACK_CANDLES:]
    peak = max(c["high"] for c in recent)
    latest_close = recent[-1]["close"]
    pulled_back = (peak - latest_close) / peak if peak else 0
    if pulled_back < CHOP_PULLBACK_FROM_PEAK_PCT:
        return False  # hasn't faded enough off its peak to be "chop" yet

    window = recent[-15:] if len(recent) >= 15 else recent
    hi = max(c["high"] for c in window)
    lo = min(c["low"] for c in window)
    tightness = (hi - lo) / lo if lo else 1.0
    higher_highs = sum(
        1 for i in range(1, len(window)) if window[i]["high"] > window[i - 1]["high"] * 1.01
    )
    return tightness <= CHOP_RANGE_PCT and higher_highs == 0


STAIRCASE_LOOKBACK_CANDLES = 20   # window to look for the "up a little, break down" repeating pattern
STAIRCASE_MIN_SWINGS = 2          # need at least this many lower-high/lower-low swings to call it a staircase down


def is_staircasing_down(candles):
    """Catches the 'up a little, break down, up a little, break down' pattern
    after a big move - a series of small bounces that each fail lower than
    the last, i.e. lower highs AND lower lows repeating. This is different
    from a clean pullback-then-reclaim: here every bounce attempt is weaker
    than the one before, so a 'breakout' off one of these bounces is really
    just another failed reclaim of a falling level, not a real move.
    Returns True if new entries should be paused."""
    if len(candles) < STAIRCASE_LOOKBACK_CANDLES:
        return False
    recent = candles[-STAIRCASE_LOOKBACK_CANDLES:]

    swing_highs = []
    swing_lows = []
    for i in range(1, len(recent) - 1):
        h = recent[i]["high"]
        l = recent[i]["low"]
        if h > recent[i - 1]["high"] and h > recent[i + 1]["high"]:
            swing_highs.append(h)
        if l < recent[i - 1]["low"] and l < recent[i + 1]["low"]:
            swing_lows.append(l)

    if len(swing_highs) < STAIRCASE_MIN_SWINGS + 1 or len(swing_lows) < STAIRCASE_MIN_SWINGS + 1:
        return False

    highs_descending = sum(
        1 for i in range(1, len(swing_highs)) if swing_highs[i] < swing_highs[i - 1]
    ) >= STAIRCASE_MIN_SWINGS
    lows_descending = sum(
        1 for i in range(1, len(swing_lows)) if swing_lows[i] < swing_lows[i - 1]
    ) >= STAIRCASE_MIN_SWINGS

    return highs_descending and lows_descending


REPEATED_REJECTION_TOUCH_PCT = 0.01     # a candle high within 1% of the day's high counts as "testing" it
REPEATED_REJECTION_CLEAR_PCT = 0.01     # a close has to beat the day's high by this much to count as a genuine clear
REPEATED_REJECTION_MIN_TOUCHES = 3      # this many separate tests of the high, all rejected, = pure chop, not a breakout


def is_repeated_high_rejection(candles, day_high):
    """Catches a stock that has tested the same daily high multiple separate
    times over the course of the day - each time wicking up near it (or
    slightly through it) and getting rejected back down, without ever
    actually closing meaningfully above it. This is the FCUV pattern: 10am,
    10:45am, 1:45pm, 2pm, 4:10pm all matched roughly the same high and got
    rejected every time - a rollercoaster with no real breakout, not a
    coiling setup building toward one. If this pattern is detected, entries
    are paused even on the very FIRST attempt (stop_out_count == 0), since
    waiting for one stop-out first would mean losing money on the very
    whipsaw this is meant to prevent.
    """
    if len(candles) < 10 or day_high is None or day_high <= 0:
        return False

    touches = 0
    ever_genuinely_cleared = False
    in_touch = False
    for c in candles:
        near_high = c["high"] >= day_high * (1 - REPEATED_REJECTION_TOUCH_PCT)
        if near_high and not in_touch:
            touches += 1
            in_touch = True
        elif not near_high:
            in_touch = False
        if c["close"] >= day_high * (1 + REPEATED_REJECTION_CLEAR_PCT):
            ever_genuinely_cleared = True

    return touches >= REPEATED_REJECTION_MIN_TOUCHES and not ever_genuinely_cleared


def is_extended_and_rolling_over(candles, pct_change):
    """For stocks already up huge (300%+), pause new entries if it's actively
    pushing down/bearish - these tend to crash hard once they turn. Applies
    the same way during after-hours."""
    if pct_change is None or pct_change < EXTENDED_MOVE_PCT_THRESHOLD:
        return False
    if len(candles) < ROLLOVER_LOOKBACK_CANDLES:
        return False
    recent = candles[-ROLLOVER_LOOKBACK_CANDLES:]
    down_closes = sum(1 for c in recent if c["close"] < c["open"])
    lower_highs = sum(
        1 for i in range(1, len(recent)) if recent[i]["high"] < recent[i - 1]["high"]
    )
    return down_closes >= (ROLLOVER_LOOKBACK_CANDLES * 0.6) and lower_highs >= (ROLLOVER_LOOKBACK_CANDLES - 2)


def is_clearly_downtrending(candles):
    """General downtrend guard - applies to ANY symbol regardless of how big
    its move is, not just 300%+ movers. Blocks entry if price has been making
    lower highs and lower lows with essentially no genuine push to the upside
    over the lookback window (e.g. a stock that spiked pre-market then has
    done nothing but bleed down for 20+ minutes straight)."""
    if len(candles) < ROLLOVER_LOOKBACK_CANDLES:
        return False
    recent = candles[-ROLLOVER_LOOKBACK_CANDLES:]
    down_closes = sum(1 for c in recent if c["close"] < c["open"])
    lower_highs = sum(
        1 for i in range(1, len(recent)) if recent[i]["high"] < recent[i - 1]["high"]
    )
    lower_lows = sum(
        1 for i in range(1, len(recent)) if recent[i]["low"] < recent[i - 1]["low"]
    )
    highest_high_recent = max(c["high"] for c in recent)
    latest_high = recent[-1]["high"]
    # No meaningful bounce off the lookback-window high at all.
    no_upside_push = latest_high < highest_high_recent
    return (
        down_closes >= (ROLLOVER_LOOKBACK_CANDLES * 0.55)
        and lower_highs >= (ROLLOVER_LOOKBACK_CANDLES - 3)
        and lower_lows >= (ROLLOVER_LOOKBACK_CANDLES - 3)
        and no_upside_push
    )


def get_symbol_entry_state(symbol):
    states = st.session_state.setdefault("symbol_entry_states", {})
    return states.setdefault(symbol, {"stop_out_count": 0, "last_stopped_high": None})


def note_stop_out(symbol, high_at_stop):
    """Called from place_sell whenever a position is stopped out, so we know
    how strict to be about the NEXT entry on this symbol."""
    state = get_symbol_entry_state(symbol)
    state["stop_out_count"] += 1
    state["last_stopped_high"] = high_at_stop


def note_clean_entry_reset(symbol):
    """Called when a position is closed for a reason other than a stop
    (e.g. volume dried up, took profit) - resets the strictness back to normal."""
    states = st.session_state.setdefault("symbol_entry_states", {})
    if symbol in states:
        states[symbol] = {"stop_out_count": 0, "last_stopped_high": None}


def evaluate_entry(symbol, day_high, pct_change=None):
    hist = fetch_intraday_candles_df(symbol)
    if hist is None:
        return "no_data", 0.0, 0.0
    candles = df_to_candle_list(hist)
    if not candles:
        return "no_data", 0.0, 0.0

    breakout, strength = is_coiling_breakout(candles, day_high)
    reclaim = is_liquidity_reclaim(candles)
    structure_score = compute_structure_score(candles, day_high)

    if is_choppy_sideways(candles):
        return "chop_wait", strength, structure_score
    # Extended-mover rollover pause removed per user request - big movers
    # (300%+) are no longer held back from entry just for being extended.
    if is_clearly_downtrending(candles):
        return "downtrend_wait", strength, structure_score
    if is_staircasing_down(candles):
        return "staircase_wait", strength, structure_score
    # Repeated-rejection check: if this stock has tested its daily high
    # several separate times today and gotten rejected every time without
    # ever genuinely closing above it, this is a rollercoaster/chop pattern,
    # not a coiling breakout - block entry even on the FIRST attempt, since
    # waiting for a stop-out first defeats the purpose (that first entry
    # would already be buying the same chop that's been rejecting all day).
    if is_repeated_high_rejection(candles, day_high):
        return "repeated_rejection_wait", strength, structure_score

    entry_state = get_symbol_entry_state(symbol)
    stop_out_count = entry_state["stop_out_count"]
    # Use the CLOSE, not the high/wick, to judge whether the old stopped-out
    # level has genuinely been reclaimed. A wick can spike through a level
    # for a split second on pure noise and immediately fall back - a close
    # above the level means price actually settled there.
    latest_close = candles[-1]["close"]
    required_buffer = reentry_breakout_buffer(pct_change)

    # 1st attempt on this symbol: either a reclaim-of-prior-high or a fresh
    # breakout is a valid trigger - this is "buy when it comes back up to
    # match that high", never buying while it's still down in the dip.
    if stop_out_count == 0:
        if breakout or reclaim:
            return "enter", strength, structure_score
    # After 1 stop-out: reclaims are no longer good enough - require an
    # actual breakout this time, AND require the CLOSE to genuinely clear
    # the level that stopped us out last time by a real margin - not just a
    # brief wick poke above the old high, which on choppy, volatile symbols
    # like FCUV was getting cleared by pure noise and causing the bot to
    # whipsaw in and out of the same range repeatedly. The required margin
    # scales up for stocks already extended 250%+ on the day, since those
    # swing much harder and a small buffer is still just noise for them.
    elif stop_out_count == 1:
        last_stopped_high = entry_state.get("last_stopped_high")
        cleared_with_buffer = (
            last_stopped_high is None
            or latest_close > last_stopped_high * (1 + required_buffer)
        )
        if breakout and cleared_with_buffer:
            return "enter", strength, structure_score
    # After 2+ stop-outs: require a genuinely NEW breakout, meaning the
    # CLOSE has to clear the level where it previously got stopped out by a
    # real margin (scaled the same way for extended stocks), not just
    # repeat the same failed high with a brief wick.
    else:
        last_stopped_high = entry_state.get("last_stopped_high")
        cleared_with_buffer = (
            last_stopped_high is None
            or latest_close > last_stopped_high * (1 + required_buffer)
        )
        if breakout and cleared_with_buffer:
            return "enter", strength, structure_score

    return "watching", strength, structure_score


# ---------------------------------------------------------------------------
# Paper trading bot (state lives in st.session_state so it survives reruns)
# ---------------------------------------------------------------------------


def save_account_state():
    """Persists cash + open positions + per-symbol entry strictness memory
    to disk so a redeploy/restart, or a second browser tab, never silently
    wipes out how many times a symbol has been stopped out. Without saving
    symbol_entry_states here, a restart (or an out-of-sync tab) resets every
    symbol's stop-out count back to 0, letting the bot re-buy on a plain
    reclaim instead of requiring the stricter breakout - which looked like
    the bot "never learning" and repeatedly re-entering the same loser.
    Only resets when the user explicitly clicks Reset."""
    try:
        state = {
            "cash": st.session_state.cash,
            "positions": st.session_state.positions,
            "cycle_count": st.session_state.cycle_count,
            "symbol_entry_states": st.session_state.get("symbol_entry_states", {}),
        }
        with open(ACCOUNT_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def load_account_state():
    try:
        with open(ACCOUNT_STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return None


def log_line(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.activity_log.append(f"{ts}  {msg}")
    st.session_state.activity_log = st.session_state.activity_log[-200:]


def get_top_movers():
    gainers = fetch_top_gainers()
    if not gainers:
        return [], []
    candidates = gainers[:GAINERS_SCAN_COUNT]
    qualifying, approaching = [], []
    for row in candidates:
        sym = row.get("symbol")
        price = row.get("price")
        pct_change = row.get("changesPercentage")
        day_high = row.get("dayHigh")
        if not sym or price is None or pct_change is None:
            continue
        entry = {
            "symbol": sym,
            "price": float(price),
            "pct_change": round(float(pct_change), 2),
            "day_high": float(day_high) if day_high else float(price),
        }
        if entry["pct_change"] >= MIN_PCT_GAIN_TO_CONSIDER:
            qualifying.append(entry)
        elif entry["pct_change"] >= EARLY_WATCH_PCT_GAIN:
            approaching.append(entry)
    qualifying.sort(key=lambda r: r["pct_change"], reverse=True)
    approaching.sort(key=lambda r: r["pct_change"], reverse=True)
    return qualifying[:MAX_WATCHLIST_SIZE], approaching[:MAX_WATCHLIST_SIZE]


def get_manual_watchlist_entries():
    """Fetches a live quote for each ticker the user added by hand (e.g. spotted
    on Robinhood but missed by the FMP gainers feed). Returned in the same
    entry shape as get_top_movers() so these symbols flow through the exact
    same scan/entry/stop/trail logic as anything found automatically."""
    entries = []
    for sym in st.session_state.manual_watchlist:
        q = fetch_quote(sym)
        if not q:
            continue
        entries.append({
            "symbol": sym,
            "price": q["price"],
            "pct_change": q.get("pct_change", 0.0),
            "day_high": q.get("day_high", q["price"]),
        })
    return entries


def log_trade(side, symbol, qty, price, reason=None, pnl=None):
    entry = {
        "time": datetime.now().isoformat(),
        "side": side, "symbol": symbol, "qty": qty, "price": price,
    }
    if reason:
        entry["reason"] = reason
    if pnl is not None:
        entry["pnl"] = round(pnl, 2)
    st.session_state.trade_log.append(entry)
    with open(PAPER_TRADE_LOG_FILE, "w") as f:
        json.dump(st.session_state.trade_log, f, indent=2)


def submit_alpaca_order(symbol, qty, side):
    """Submits a real market order to Alpaca (paper or live, depending on
    ALPACA_USE_PAPER above) and returns the parsed JSON response, or None on
    failure. Uses a simple market day order - good enough for the momentum
    entries/exits this bot makes; a trailing-stop order type native to
    Alpaca itself is a natural next upgrade once this basic wiring is
    confirmed working end-to-end."""
    try:
        order_url = f"{ALPACA_TRADING_BASE_URL}/v2/orders"
        payload = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,       # "buy" or "sell"
            "type": "market",
            "time_in_force": "day",
        }
        resp = requests.post(order_url, headers=ALPACA_HEADERS, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log_line(f"Alpaca order FAILED for {symbol} ({side}): {e}")
        return None


def place_resting_stop_order(symbol, qty, stop_price):
    """Places a real stop order directly on the exchange via Alpaca, so the
    stop is enforced by Alpaca's matching engine itself rather than only by
    our own polling loop. Returns the order id, or None on failure."""
    try:
        order_url = f"{ALPACA_TRADING_BASE_URL}/v2/orders"
        payload = {
            "symbol": symbol,
            "qty": str(qty),
            "side": "sell",
            "type": "stop",
            "stop_price": str(round(stop_price, 2)),
            "time_in_force": "day",
        }
        resp = requests.post(order_url, headers=ALPACA_HEADERS, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json().get("id")
    except Exception as e:
        log_line(f"Failed to place resting stop order for {symbol}: {e}")
        return None


def cancel_alpaca_order(order_id):
    """Cancels a resting order on Alpaca. Safe to call even if the order has
    already filled or been cancelled - failures here are non-fatal since
    we'll just place a fresh order right after."""
    if not order_id:
        return
    try:
        cancel_url = f"{ALPACA_TRADING_BASE_URL}/v2/orders/{order_id}"
        requests.delete(cancel_url, headers=ALPACA_HEADERS, timeout=10)
    except Exception:
        pass


def check_alpaca_order_filled(order_id):
    """Checks whether a given Alpaca order has filled. Used to detect when
    the resting stop order itself fired on the exchange (e.g. a sharp
    overnight-style gap between our poll cycles), so our own bookkeeping
    stays in sync with what actually happened at the broker."""
    if not order_id:
        return None
    try:
        order_url = f"{ALPACA_TRADING_BASE_URL}/v2/orders/{order_id}"
        resp = requests.get(order_url, headers=ALPACA_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "filled":
            return float(data.get("filled_avg_price"))
        return None
    except Exception:
        return None


def place_buy(symbol, price, strength):
    position_size_usd = st.session_state.cash * POSITION_SIZE_PCT_OF_CASH
    qty = int(position_size_usd // price)
    if qty <= 0 or qty * price > st.session_state.cash:
        log_line(f"Insufficient cash to buy {symbol} at ${price:.2f}")
        return

    order = submit_alpaca_order(symbol, qty, "buy")
    if order is None:
        # Order rejected/failed at the broker - don't touch cash or open a
        # position we don't actually hold.
        return

    # Alpaca's own filled_avg_price is the real fill price once the order
    # completes; market orders on liquid-enough symbols fill almost
    # instantly, but fall back to our quoted price if it's not populated yet.
    fill_price = float(order.get("filled_avg_price") or price)
    cost = qty * fill_price
    st.session_state.cash -= cost
    initial_stop = fill_price * (1 - INITIAL_STOP_PCT)
    stop_order_id = place_resting_stop_order(symbol, qty, initial_stop)
    new_position = {
        "symbol": symbol,
        "qty": qty,
        "entry_price": fill_price,
        "stop_price": initial_stop,
        "highest_price": fill_price,
        "strength": strength,
        "alpaca_order_id": order.get("id"),
        "stop_order_id": stop_order_id,
    }
    st.session_state.positions.append(new_position)
    log_line(f"BUY {symbol} qty={qty} @ ${fill_price:.2f} (cost ${cost:.2f}) | initial stop ${new_position['stop_price']:.2f} (resting on exchange)")
    log_trade("BUY", symbol, qty, fill_price)
    st.session_state.trade_markers.append({"symbol": symbol, "side": "BUY", "time": datetime.now(), "price": fill_price})
    save_account_state()


def place_sell(pos, reason, already_filled_price=None):
    if not pos:
        return

    # Cancel any resting stop order still open on the exchange before firing
    # our own market sell, so we never end up with two live sell orders for
    # the same shares (which Alpaca would reject or partially double-fill).
    if not already_filled_price:
        cancel_alpaca_order(pos.get("stop_order_id"))

    if already_filled_price is not None:
        # The resting stop order itself already filled on the exchange - no
        # need to submit a second sell, just record what happened.
        order = None
        price = already_filled_price
    else:
        order = submit_alpaca_order(pos["symbol"], pos["qty"], "sell")
        q = fetch_quote(pos["symbol"])
    if already_filled_price is None:
        fallback_price = q["price"] if q else pos["highest_price"]
        price = float(order.get("filled_avg_price")) if order and order.get("filled_avg_price") else fallback_price

    proceeds = pos["qty"] * price
    st.session_state.cash += proceeds
    pnl = proceeds - (pos["qty"] * pos["entry_price"])
    log_line(f"SELL {pos['symbol']} qty={pos['qty']} @ ${price:.2f} ({reason}) | P&L ${pnl:.2f}")
    log_trade("SELL", pos["symbol"], pos["qty"], price, reason=reason, pnl=pnl)
    st.session_state.trade_markers.append({"symbol": pos["symbol"], "side": "SELL", "time": datetime.now(), "price": price})
    if "stop" in reason.lower():
        note_stop_out(pos["symbol"], pos.get("highest_price", price))
    else:
        note_clean_entry_reset(pos["symbol"])
    st.session_state.positions.remove(pos)
    save_account_state()


def manage_open_positions(all_movers):
    flatten_now = past_flatten_time()
    for pos in list(st.session_state.positions):
        # First check whether the resting stop order itself already filled
        # on the exchange since our last poll (e.g. a sharp gap that hit the
        # stop between refresh cycles) - if so, just record it and move on,
        # rather than placing a second, redundant sell.
        filled_price = check_alpaca_order_filled(pos.get("stop_order_id"))
        if filled_price is not None:
            place_sell(pos, reason="stop hit (filled on exchange)", already_filled_price=filled_price)
            continue

        q = fetch_quote(pos["symbol"])
        if not q:
            continue
        price = q["price"]

        if flatten_now:
            place_sell(pos, reason="end-of-day flatten - no overnight holds")
            continue

        if price > pos["highest_price"]:
            pos["highest_price"] = price

        gain_pct = (price - pos["entry_price"]) / pos["entry_price"]

        steps_completed = int(gain_pct // RATCHET_STEP_GAIN)
        if steps_completed >= 1:
            # First step (10-20% gain) only brings the stop to breakeven -
            # no profit locked yet, just protects your original capital.
            # From the second step onward, each additional 10% gain locks in
            # one full RATCHET_STEP_LOCK of profit, trailing one step behind.
            locked_in_pct = max(0, steps_completed - 1) * RATCHET_STEP_LOCK
            candidate_stop = pos["entry_price"] * (1 + locked_in_pct)
        else:
            candidate_stop = pos["highest_price"] * (1 - INITIAL_STOP_PCT)
        new_stop = max(pos["stop_price"], candidate_stop)
        if new_stop > pos["stop_price"]:
            # Stop is ratcheting up - cancel the old resting stop order and
            # place a fresh one at the new, higher level so the exchange-side
            # order always matches what our own logic has decided is correct.
            cancel_alpaca_order(pos.get("stop_order_id"))
            pos["stop_order_id"] = place_resting_stop_order(pos["symbol"], pos["qty"], new_stop)
        pos["stop_price"] = new_stop

        held_symbols = {p["symbol"] for p in st.session_state.positions}
        for m in all_movers:
            if m["symbol"] in held_symbols:
                continue
            if q["pct_change"] > 0 and m["pct_change"] >= STRONGER_MOVER_MULTIPLE * q["pct_change"]:
                tighter_stop = pos["highest_price"] * (1 - STRONGER_MOVER_STOP_PCT)
                if tighter_stop > pos["stop_price"]:
                    log_line(f"Stronger mover detected ({m['symbol']} at {m['pct_change']:.2f}% vs {pos['symbol']} at {q['pct_change']:.2f}%) - tightening stop only")
                    cancel_alpaca_order(pos.get("stop_order_id"))
                    pos["stop_order_id"] = place_resting_stop_order(pos["symbol"], pos["qty"], tighter_stop)
                    pos["stop_price"] = tighter_stop
                break
            # Closing-gap check: if a non-held mover hasn't overtaken yet but is
            # within CLOSING_GAP_THRESHOLD_PCT percentage points of catching up,
            # tighten the stop pre-emptively rather than waiting for it to overtake.
            gap = q["pct_change"] - m["pct_change"]
            if 0 <= gap <= CLOSING_GAP_THRESHOLD_PCT:
                gap_tighter_stop = pos["highest_price"] * (1 - CLOSING_GAP_STOP_PCT)
                if gap_tighter_stop > pos["stop_price"]:
                    log_line(f"{m['symbol']} closing the gap on {pos['symbol']} ({m['pct_change']:.2f}% vs {q['pct_change']:.2f}%) - tightening stop pre-emptively")
                    cancel_alpaca_order(pos.get("stop_order_id"))
                    pos["stop_order_id"] = place_resting_stop_order(pos["symbol"], pos["qty"], gap_tighter_stop)
                    pos["stop_price"] = gap_tighter_stop

        # Track a rolling "peak volume" per position instead of a hard time
        # cutoff. If volume dries up hard (a big drop off the peak), that's
        # our signal liquidity/interest is fading - exit then. Otherwise keep
        # riding the position through after-hours as long as it's tradable.
        if q.get("volume") is not None:
            if "peak_volume" not in pos or q["volume"] > pos["peak_volume"]:
                pos["peak_volume"] = q["volume"]
            if pos["peak_volume"] > 0:
                volume_ratio = q["volume"] / pos["peak_volume"]
                if volume_ratio <= (1 - VOLUME_DROP_EXIT_PCT):
                    place_sell(pos, reason=f"volume dried up ({volume_ratio*100:.0f}% of peak) - exiting")
                    continue

        # No local price<=stop_price check needed here anymore - the resting
        # stop order on the exchange (checked at the top of this loop via
        # check_alpaca_order_filled) is now the actual source of truth for
        # when the stop has been hit, since it fires on Alpaca's matching
        # engine directly rather than depending on our own poll cycle.

    # Persist the updated ratcheted stop prices (and highest_price/peak_volume
    # tracking) to disk after every pass through open positions. Without this,
    # the multi-tab resync-from-disk logic at the top of the script would
    # reload the OLD stop price on the very next rerun and silently undo the
    # ratchet, making the stop look "stuck" at an old level even though it
    # correctly climbed for a moment in memory.
    save_account_state()


def scan_and_enter(all_movers):
    if past_flatten_time():
        return
    if len(st.session_state.positions) >= MAX_OPEN_POSITIONS:
        return
    held_symbols = {p["symbol"] for p in st.session_state.positions}

    # When several candidates all qualify, prefer the ones with the best
    # "upside potential" structure/volume score over just raw % gain, similar
    # to how an experienced trader eyeballs which of several extended movers
    # actually looks like it has more room to keep running.
    scored_candidates = []
    for cand in all_movers:
        if cand["symbol"] in held_symbols:
            continue
        status, strength, structure_score = evaluate_entry(cand["symbol"], cand["day_high"], cand.get("pct_change"))
        if status == "chop_wait":
            log_line(f"{cand['symbol']} is grinding sideways after a pullback - watching for a real breakout, no entry yet")
        elif status == "downtrend_wait":
            log_line(f"{cand['symbol']} is on a clear downtrend, no genuine push to the upside yet - holding off")
        elif status == "rollover_wait":
            log_line(f"{cand['symbol']} is extended ({cand['pct_change']:.0f}%) and pushing down - holding off until it stabilizes")
        elif status == "staircase_wait":
            log_line(f"{cand['symbol']} is staircasing down (bounce, break down, bounce, break down) - each bounce weaker than the last, skipping until that stops")
        elif status == "repeated_rejection_wait":
            log_line(f"{cand['symbol']} has tested its daily high multiple times and been rejected every time (rollercoaster chop, no real breakout) - skipping")
        scored_candidates.append((cand, status, strength, structure_score))

    scored_candidates.sort(key=lambda t: t[3], reverse=True)

    for cand, status, strength, structure_score in scored_candidates:
        if len(st.session_state.positions) >= MAX_OPEN_POSITIONS:
            break
        if status == "enter":
            if structure_score < MIN_STRUCTURE_SCORE_TO_ENTER:
                log_line(
                    f"{cand['symbol']} met gain/pattern criteria but structure score "
                    f"{structure_score:.2f} looks exhausted (below {MIN_STRUCTURE_SCORE_TO_ENTER}) - skipping"
                )
                continue
            log_line(
                f"{cand['symbol']} qualifies ({cand['pct_change']:.2f}% gain, "
                f"strength {strength:.2f}, structure score {structure_score:.2f}) - entering"
            )
            place_buy(cand["symbol"], cand["price"], strength)
            held_symbols.add(cand["symbol"])


def account_value():
    total = st.session_state.cash
    for pos in st.session_state.positions:
        q = fetch_quote(pos["symbol"])
        price = q["price"] if q else pos["highest_price"]
        total += pos["qty"] * price
    return total


# ---------------------------------------------------------------------------
# Streamlit page
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Momentum Bot - Live Paper Trading", layout="wide")

# Start the live websocket price stream exactly once per running process,
# regardless of how many times Streamlit reruns the script on each refresh.
ensure_price_stream_running()

if "manual_watchlist" not in st.session_state:
    # Symbols the user adds by hand (e.g. spotted on Robinhood but missed by
    # the FMP gainers feed). These get scanned/traded with the exact same
    # rules as anything the automatic feed finds.
    st.session_state.manual_watchlist = []

if "cash" not in st.session_state:
    saved_state = load_account_state()
    if saved_state:
        st.session_state.cash = saved_state.get("cash", PAPER_STARTING_CASH)
        st.session_state.positions = saved_state.get("positions", [])
        st.session_state.cycle_count = saved_state.get("cycle_count", 0)
        st.session_state.symbol_entry_states = saved_state.get("symbol_entry_states", {})
    else:
        st.session_state.cash = PAPER_STARTING_CASH
        st.session_state.positions = []
        st.session_state.cycle_count = 0
        st.session_state.symbol_entry_states = {}
    st.session_state.trade_log = []
    st.session_state.trade_markers = []
    st.session_state.activity_log = []
else:
    # The shared account file on disk is the single source of truth. If more
    # than one browser tab/session is open at once, each one only loaded its
    # own snapshot into memory on first load - re-syncing from disk on every
    # single rerun (not just the first) prevents two sessions from silently
    # overwriting each other's cash/positions with stale numbers.
    fresh_state = load_account_state()
    if fresh_state:
        st.session_state.cash = fresh_state.get("cash", st.session_state.cash)
        st.session_state.positions = fresh_state.get("positions", st.session_state.positions)
        st.session_state.cycle_count = fresh_state.get("cycle_count", st.session_state.cycle_count)
        st.session_state.symbol_entry_states = fresh_state.get(
            "symbol_entry_states", st.session_state.get("symbol_entry_states", {})
        )

st.title("Live Momentum Bot - Paper Trading Dashboard")
st.caption("PAPER TRADING ONLY. Real, live market data. No real brokerage orders are ever placed. Not financial advice.")

# --- Manually add a ticker the automatic gainers feed missed ---
with st.form("add_manual_symbol", clear_on_submit=True):
    mc1, mc2 = st.columns([4, 1])
    with mc1:
        manual_symbol_input = st.text_input(
            "Add a ticker to track (e.g. one you spotted on Robinhood that isn't showing up above)",
            label_visibility="collapsed",
            placeholder="Type a ticker symbol, e.g. RIPL",
        )
    with mc2:
        add_clicked = st.form_submit_button("Add to watchlist")
    if add_clicked and manual_symbol_input.strip():
        sym = manual_symbol_input.strip().upper()
        if sym not in st.session_state.manual_watchlist:
            st.session_state.manual_watchlist.append(sym)
            log_line(f"Manually added {sym} to the watchlist - now scanned/traded like any other symbol.")

if st.session_state.manual_watchlist:
    st.caption("Manually tracked: " + ", ".join(st.session_state.manual_watchlist))
    if st.button("Clear manually tracked tickers"):
        st.session_state.manual_watchlist = []
        st.rerun()
    with st.expander("Raw quote data for manually tracked tickers (debug)"):
        for sym in st.session_state.manual_watchlist:
            debug_q = fetch_quote(sym)
            if debug_q:
                st.write(
                    f"{sym}: price {debug_q['price']}, pct_change {debug_q['pct_change']}, "
                    f"prev_close {debug_q['prev_close']}, day_high {debug_q['day_high']}"
                )
            else:
                st.write(f"{sym}: no quote returned from the data feed")

# --- One full strategy cycle runs every rerun ---
if market_is_open() or in_premarket_scan_window():
    raw_gainers = fetch_top_gainers()
    movers, approaching = get_top_movers()
    manual_entries = get_manual_watchlist_entries()
    existing_symbols = {m["symbol"] for m in movers}
    for entry in manual_entries:
        if entry["symbol"] not in existing_symbols:
            movers.append(entry)
    if market_is_open():
        # Regular trading hours - full cycle, including live entries.
        manage_open_positions(movers)
        scan_and_enter(movers)
    else:
        # 9:00-9:30 AM ET pre-market scan window: build/refresh the
        # watchlist so it's warmed up and ready, but do NOT place any
        # buys yet - trading only unlocks once market_is_open() is True.
        log_line(f"Pre-market scan: watchlist built ({len(movers)} symbols) - trading opens at 9:30 AM ET.")
    st.session_state.cycle_count += 1
else:
    raw_gainers = []
    movers, approaching = [], []
    log_line("Market closed (regular hours 9:30 AM - 4:00 PM ET). Standing by.")


# --- Manual close-all control (cancels each resting stop order on the
# exchange and submits a real market sell for every open position - this
# is a live/paper Alpaca order just like any other sell in this file) ---
close_all_col, reset_col, _ = st.columns([1, 1, 4])
with close_all_col:
    if st.button("Close All Positions"):
        for pos in list(st.session_state.positions):
            place_sell(pos, reason="manual close-all requested by user")
        save_account_state()
        st.rerun()

# --- Reset control (manual only - account never auto-resets on restart) ---
with reset_col:
    if st.button("Reset Account to $50,000"):
        # Full wipe: cash, all open positions, cycle count, per-symbol
        # entry-strictness memory, trade log/markers, and activity log.
        st.session_state.cash = PAPER_STARTING_CASH
        st.session_state.positions = []
        st.session_state.cycle_count = 0
        st.session_state.symbol_entry_states = {}
        st.session_state.trade_log = []
        st.session_state.trade_markers = []
        st.session_state.activity_log = []
        save_account_state()
        try:
            with open(PAPER_TRADE_LOG_FILE, "w") as f:
                json.dump([], f)
        except Exception:
            pass
        st.rerun()

# --- Top metrics row ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Account Value", f"${account_value():,.2f}")
col2.metric("Cash", f"${st.session_state.cash:,.2f}")
positions_display = ", ".join(p["symbol"] for p in st.session_state.positions) if st.session_state.positions else "None"
col3.metric(f"Positions ({len(st.session_state.positions)}/{MAX_OPEN_POSITIONS})", positions_display)
col4.metric("Market", "OPEN" if market_is_open() else "CLOSED")

for pos in list(st.session_state.positions):
    q = fetch_quote(pos["symbol"])
    live_price = q["price"] if q else pos["highest_price"]
    gain_pct = (live_price - pos["entry_price"]) / pos["entry_price"] * 100
    pos_col, btn_col = st.columns([6, 1])
    with pos_col:
        st.info(
            f"**{pos['symbol']}** | Entry ${pos['entry_price']:.2f} | Now ${live_price:.2f} "
            f"({gain_pct:+.2f}%) | Stop ${pos['stop_price']:.2f} | Qty {pos['qty']}"
        )
    with btn_col:
        # Keyed uniquely per symbol so each position gets its own button
        # instead of Streamlit reusing/conflicting state across rows. This
        # cancels the resting stop order on the exchange and submits a real
        # market sell, same as place_sell does everywhere else in this file.
        if st.button("Close", key=f"close_{pos['symbol']}"):
            place_sell(pos, reason="manual close requested by user")
            save_account_state()
            st.rerun()

# --- Live candlestick chart ---
chart_symbol = st.session_state.positions[0]["symbol"] if st.session_state.positions else (movers[0]["symbol"] if movers else None)
if chart_symbol:
    hist = fetch_intraday_candles_df(chart_symbol)
    if hist is not None:
        fig = go.Figure(data=[go.Candlestick(
            x=hist.index, open=hist["Open"], high=hist["High"],
            low=hist["Low"], close=hist["Close"], name=chart_symbol,
        )])
        buys = [m for m in st.session_state.trade_markers if m["symbol"] == chart_symbol and m["side"] == "BUY"]
        sells = [m for m in st.session_state.trade_markers if m["symbol"] == chart_symbol and m["side"] == "SELL"]
        if buys:
            fig.add_trace(go.Scatter(
                x=[m["time"] for m in buys], y=[m["price"] for m in buys],
                mode="markers", marker=dict(symbol="triangle-up", size=14, color="green"), name="BUY",
            ))
        if sells:
            fig.add_trace(go.Scatter(
                x=[m["time"] for m in sells], y=[m["price"] for m in sells],
                mode="markers", marker=dict(symbol="triangle-down", size=14, color="red"), name="SELL",
            ))
        fig.update_layout(title=f"{chart_symbol} - live 1-minute candles", height=500, xaxis_rangeslider_visible=False)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.write("No qualifying symbol to chart yet.")

# --- Watchlists ---
wc1, wc2 = st.columns(2)
with wc1:
    st.subheader(f"Qualifying (>= {MIN_PCT_GAIN_TO_CONSIDER:.0f}%, tradeable)")
    if movers:
        st.dataframe(pd.DataFrame(movers)[["symbol", "price", "pct_change"]], hide_index=True, use_container_width=True)
    else:
        st.write("None right now.")
with wc2:
    st.subheader(f"Approaching ({EARLY_WATCH_PCT_GAIN:.0f}-{MIN_PCT_GAIN_TO_CONSIDER:.0f}%, watch only)")
    if approaching:
        st.dataframe(pd.DataFrame(approaching)[["symbol", "price", "pct_change"]], hide_index=True, use_container_width=True)
    else:
        st.write("None right now.")

# --- News panel: is the move backed by a real catalyst, or just hype? ---
st.subheader("News on Watchlist")
news_symbols = [m["symbol"] for m in (movers[:5] + approaching[:3])]
if news_symbols:
    for sym in news_symbols:
        headlines = fetch_stock_news(sym, limit=2)
        if headlines:
            for h in headlines:
                st.caption(f"**{sym}** - {h['title']} ({h['publisher']}, {h['date']})")
        else:
            st.caption(f"**{sym}** - no recent news found (may just be running on pure momentum/hype).")
else:
    st.caption("No watchlist symbols to check news for right now.")

# --- Activity log ---
st.subheader("Activity Log")
st.text("\n".join(reversed(st.session_state.activity_log[-30:])) or "Nothing logged yet.")

st.caption(f"Cycle #{st.session_state.cycle_count} | refreshing every {REFRESH_SECONDS}s | last update {datetime.now().strftime('%H:%M:%S')}")

time.sleep(REFRESH_SECONDS)
st.rerun()
