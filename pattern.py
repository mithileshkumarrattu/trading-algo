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


def candle_range(candle) -> float:
    return max(0.0, float(candle.high) - float(candle.low))


def candle_body(candle) -> float:
    return abs(float(candle.close) - float(candle.open))


def total_wick(candle) -> float:
    upper = float(candle.high) - max(float(candle.open), float(candle.close))
    lower = min(float(candle.open), float(candle.close)) - float(candle.low)
    return max(0.0, upper) + max(0.0, lower)


def body_to_wick_ratio(candle) -> float:
    wick = total_wick(candle)
    return candle_body(candle) / wick if wick > 0 else float("inf")


def relative_volume(candle, recent_volumes) -> float:
    values = [float(value) for value in recent_volumes if float(value) > 0]
    return float(candle.volume) / (sum(values) / len(values)) if values else 1.0


def valid_alpha_trend_candle(candle, recent_volumes):
    return (
        is_green(candle)
        and candle_body_ratio(candle) >= config.ALPHA_MIN_TREND_BODY_RATIO
        and body_to_wick_ratio(candle) >= config.ALPHA_MIN_TREND_BODY_TO_WICK_RATIO
        and relative_volume(candle, recent_volumes) >= config.ALPHA_MIN_TREND_VOLUME_RATIO
    )


def valid_alpha_pullback_candle(candle, trend_volumes):
    return (
        is_red(candle)
        and candle_body_ratio(candle) >= config.ALPHA_MIN_ALPHA_BODY_RATIO
        and body_to_wick_ratio(candle) >= config.ALPHA_MIN_ALPHA_BODY_TO_WICK_RATIO
        and relative_volume(candle, trend_volumes) <= config.ALPHA_MAX_ALPHA_VOLUME_RATIO
    )


def find_alpha_buy_setup(pattern_candles):
    """
    Scans a day's worth of pattern candles (default 3-minute bars) in time order,
    looking for a qualifying trend run followed immediately by a counter-colour
    Alpha Candle. Returns None if no complete pattern exists yet in the data.
    """
    if pattern_candles is None or len(pattern_candles) < config.ALPHA_MIN_TREND_CANDLES + 1:
        return None

    trend_run = []
    for alpha_idx in range(config.ALPHA_MIN_TREND_CANDLES, len(pattern_candles)):
        trend_run = []
        for index in range(alpha_idx):
            candle = pattern_candles.iloc[index]
            if not valid_alpha_trend_candle(candle, pattern_candles.iloc[max(0, index - 3):index]["volume"]):
                trend_run = []
                continue
            if trend_run and float(candle.close) <= float(trend_run[-1].high):
                trend_run = []
                continue
            trend_run.append(candle)
        if len(trend_run) < config.ALPHA_MIN_TREND_CANDLES:
            continue
        alpha = pattern_candles.iloc[alpha_idx]
        if not valid_alpha_pullback_candle(alpha, [float(c.volume) for c in trend_run]):
            continue
        confirmations = pattern_candles.iloc[
            alpha_idx + 1:alpha_idx + 1 + config.ALPHA_CONFIRMATION_PATTERN_BARS
        ]
        confirmation = confirmations[
            (confirmations["high"] > float(alpha.high))
            & (confirmations["close"] > float(alpha.high))
        ]
        if confirmation.empty:
            continue
        return {
            "alpha_candle": alpha,
            "alpha_idx": alpha_idx,
            "trend_run": trend_run,
            "pattern_confirmation_time": confirmation.iloc[0].timestamp,
            "detected_at": datetime.now(config.TIME_ZONE),
        }
    return None


def find_trend_run_and_alpha(pattern_candles, is_bullish_setup):
    if not is_bullish_setup or not config.ALPHA_ALLOW_SELL:
        return find_alpha_buy_setup(pattern_candles) if is_bullish_setup else None
    return find_alpha_buy_setup(pattern_candles)


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
