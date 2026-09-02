"""
AlphaCandle - Alpha Pattern Detection Engine.

Two-tier logic exactly as specified:

TIER 1 - PATTERN TIMEFRAME (default 3-minute candles for trend run + Alpha Candle):
  BUY setup (stock from Top Gainers list):
    - 3 or more consecutive GREEN candles.
    - Each candle must CLOSE ABOVE the previous candle's HIGH.
    - None of the 3+ candles may be a doji.
    - None of the 3+ candles may have volume that's collapsed below
      VOLUME_FLOOR_PCT_OF_AVG (40%) of the day's average volume so far -
      this allows natural volume tapering in a real trend while still
      filtering genuinely dead/illiquid candles.
    - The FIRST RED candle immediately after that run = Alpha Candle.
  SELL setup (stock from Top Losers list): exact mirror using red run /
    green Alpha Candle / previous candle's LOW.

  No maximum run length - a run of 3, 6, or 10 candles is equally valid.

TIER 2 - 1-MIN TIMEFRAME (precise breakout entry):
  Once an Alpha Candle exists, watch 1-min candles formed after it.
  BUY: a 1-min candle whose HIGH crosses above Alpha Candle HIGH AND whose
       CLOSE is also above Alpha Candle HIGH -> entry trigger.
  SELL: mirror using Alpha Candle LOW.
  This is checked continuously across the configured hold window in units of
  the pattern timeframe, with 1-minute bars driving the execution trigger.
  If no 1-min candle qualifies within that budget, the setup is abandoned so
  the scanner can move to a fresher setup rather than wait indefinitely.

Nothing here places orders - pure signal detection, handed to engine.py.
"""
import logging
from datetime import datetime, timedelta

import config

logger = logging.getLogger(__name__)

# --- Module-level constants (MUST stay at this level, not nested in a func) ---
PATTERN_TIMEFRAME_MINUTES = int(getattr(config, "PATTERN_TIMEFRAME", 3))
ENTRY_TIMEFRAME_MINUTES = int(getattr(config, "ENTRY_TIMEFRAME", 1))
DOJI_BODY_RATIO = getattr(config, "DOJI_BODY_RATIO", 0.18)
MIN_TREND_CANDLES = getattr(config, "MIN_TREND_CANDLES", 3)
VOLUME_FLOOR_PCT_OF_AVG = 0.40  # candle must have >= 40% of day's avg volume so far


def is_doji(candle) -> bool:
    rng = candle.high - candle.low
    if rng <= 0:
        return True
    body = abs(candle.close - candle.open)
    return (body / rng) < DOJI_BODY_RATIO


def is_green(candle) -> bool:
    return candle.close > candle.open


def is_red(candle) -> bool:
    return candle.close < candle.open


def candle_body_ratio(candle) -> float:
    candle_range = candle.high - candle.low
    if candle_range <= 0:
        return 0.0
    return abs(candle.close - candle.open) / candle_range


def find_trend_run_and_alpha(pattern_candles, is_bullish_setup):
    """
    Scans a day's worth of pattern candles (default 3-minute bars) in time order,
    looking for a qualifying trend run followed immediately by a counter-colour
    Alpha Candle. Returns None if no complete pattern exists yet in the data.
    """
    if pattern_candles is None or len(pattern_candles) < MIN_TREND_CANDLES + 1:
        return None

    trend_colour_green = is_bullish_setup
    trend_run = []
    alpha_candle = None
    alpha_idx = None

    for i in range(len(pattern_candles)):
        c = pattern_candles.iloc[i]

        # Loosened volume check: compare against day-so-far AVERAGE volume,
        # not an absolute ratcheting day-low. Allows natural volume tapering
        # in a genuine trend while still filtering dead/illiquid candles.
        if i > 0:
            prior_avg_vol = pattern_candles.iloc[:i]["volume"].mean()
            vol_ok = c.volume >= (prior_avg_vol * VOLUME_FLOOR_PCT_OF_AVG)
        else:
            vol_ok = True

        is_trend_colour = is_green(c) if trend_colour_green else is_red(c)
        is_counter_colour = is_red(c) if trend_colour_green else is_green(c)

        if alpha_candle is None:
            if is_trend_colour and not is_doji(c) and vol_ok:
                closes_beyond_prev = True
                if trend_run:
                    prev = trend_run[-1]
                    closes_beyond_prev = (c.close > prev.high) if trend_colour_green else (c.close < prev.low)
                if closes_beyond_prev:
                    trend_run.append(c)
                else:
                    trend_run = [c]
            elif is_trend_colour:
                # trend-coloured but doji or too-low volume -> breaks the run
                trend_run = []
            elif is_counter_colour and len(trend_run) >= MIN_TREND_CANDLES:
                alpha_candle = c
                alpha_idx = i
            else:
                trend_run = []
        else:
            break

    if alpha_candle is None:
        return None
    return {
        "alpha_candle": alpha_candle,
        "alpha_idx": alpha_idx,
        "trend_run": trend_run,
        "detected_at": datetime.now(config.TIME_ZONE),
    }


def check_1min_breakout(candles_1m_since_alpha, alpha_high, alpha_low, is_bullish_setup):
    """
    candles_1m_since_alpha: 1-min candles AFTER the Alpha Candle's pattern bar
    closed, oldest first.

    Returns the triggering 1-min candle (Series) once found, else None.
    """
    if candles_1m_since_alpha is None or candles_1m_since_alpha.empty:
        return None

    for _, candle in candles_1m_since_alpha.iterrows():
        if is_bullish_setup:
            wick_cross = float(candle["high"]) > float(alpha_high)
            close_confirm = float(candle["close"]) > float(alpha_high)
            if wick_cross and (not config.REQUIRE_ENTRY_CLOSE_BEYOND_TRIGGER or close_confirm):
                return candle
        else:
            wick_cross = float(candle["low"]) < float(alpha_low)
            close_confirm = float(candle["close"]) < float(alpha_low)
            if wick_cross and (not config.REQUIRE_ENTRY_CLOSE_BEYOND_TRIGGER or close_confirm):
                return candle
    return None


def hold_window_expired(alpha_detected_at) -> bool:
    hold_candles = int(getattr(config, "HOLD_CANDLES_PATTERN", 2))
    pattern_minutes = int(getattr(config, "PATTERN_TIMEFRAME", PATTERN_TIMEFRAME_MINUTES))
    budget = timedelta(minutes=hold_candles * pattern_minutes)
    return datetime.now(config.TIME_ZONE) > (alpha_detected_at + budget)


def initial_stop_loss(alpha_high, alpha_low, is_bullish_setup, entry_price=None) -> float:
    if entry_price is None:
        entry_price = alpha_high if is_bullish_setup else alpha_low
    entry_price = float(entry_price)
    buffer_points = max(
        entry_price * (getattr(config, "STOP_BUFFER_PCT", 0.10) / 100.0),
        getattr(config, "STOP_BUFFER_MIN_POINTS", 0.05),
    )
    return (float(alpha_low) - buffer_points) if is_bullish_setup else (float(alpha_high) + buffer_points)
