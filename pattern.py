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

import pandas as pd

import config

logger = logging.getLogger(__name__)

# --- Module-level constants (MUST stay at this level, not nested in a func) ---
PATTERN_TIMEFRAME_MINUTES = int(getattr(config, "PATTERN_TIMEFRAME", 3))
ENTRY_TIMEFRAME_MINUTES = int(getattr(config, "ENTRY_TIMEFRAME", 1))
DOJI_BODY_RATIO = getattr(config, "DOJI_BODY_RATIO", 0.18)
MIN_TREND_CANDLES = getattr(config, "MIN_TREND_CANDLES", 3)
VOLUME_FLOOR_PCT_OF_AVG = 0.40  # candle must have >= 40% of day's avg volume so far


def is_doji(candle) -> bool:
    rng = candle_range(candle)
    if rng <= 0:
        return True
    body = candle_body(candle)
    return (body / rng) < DOJI_BODY_RATIO


def is_green(candle) -> bool:
    return float(candle.close) > float(candle.open)


def is_red(candle) -> bool:
    return float(candle.close) < float(candle.open)


def candle_range(candle) -> float:
    return max(0.0, float(candle.high) - float(candle.low))


def candle_body(candle) -> float:
    return abs(float(candle.close) - float(candle.open))


def total_wick(candle) -> float:
    upper = float(candle.high) - max(float(candle.open), float(candle.close))
    lower = min(float(candle.open), float(candle.close)) - float(candle.low)
    return max(0.0, upper) + max(0.0, lower)


def body_ratio(candle) -> float:
    rng = candle_range(candle)
    return candle_body(candle) / rng if rng else 0.0


def candle_body_ratio(candle) -> float:
    return body_ratio(candle)


def body_to_wick_ratio(candle) -> float:
    wick = total_wick(candle)
    return candle_body(candle) / wick if wick > 0 else float("inf")


def close_position(candle) -> float:
    rng = candle_range(candle)
    return (float(candle.close) - float(candle.low)) / rng if rng else 0.5


def close_position_in_range(candle) -> float:
    return close_position(candle)


def bearish_close_position(candle) -> float:
    rng = candle_range(candle)
    return (float(candle.high) - float(candle.close)) / rng if rng else 0.5


def relative_volume(candle, reference_volumes) -> float:
    values = [float(v) for v in reference_volumes if float(v) > 0]
    if not values:
        return 1.0
    median_volume = float(pd.Series(values).median())
    return float(candle.volume) / median_volume if median_volume > 0 else 1.0


def distance_to_band_pct(candle, band_low, band_high, side="BUY") -> float:
    """
    For a BUY structure, measure how far the candle close is above the
    upper edge of the support band. Zero means it is in/under the band.
    For a SELL structure, measure how far the candle close is below the
    lower edge of the resistance band. Zero means it is in/above the band.
    """
    if side == "BUY":
        reference = max(float(band_high), 0.01)
        return max(0.0, (float(candle.close) - float(band_high)) / reference * 100.0)
    else:
        reference = max(float(band_low), 0.01)
        return max(0.0, (float(band_low) - float(candle.close)) / reference * 100.0)


def is_controlled_alpha_compression(
    candle,
    relative_volume_ratio,
    band_low,
    band_high,
    side="BUY",
) -> bool:
    br = body_ratio(candle)
    bwr = body_to_wick_ratio(candle)
    smma_distance = distance_to_band_pct(candle, band_low, band_high, side=side)

    return (
        getattr(config, "ALPHA_ALLOW_CONTROLLED_COMPRESSION", True)
        and getattr(config, "ALPHA_COMPRESSION_MIN_BODY_RATIO", 0.25) <= br
        < getattr(config, "ALPHA_MIN_TREND_BODY_RATIO", 0.45)
        and bwr >= getattr(config, "ALPHA_COMPRESSION_MIN_BODY_TO_WICK_RATIO", 1.0)
        and relative_volume_ratio >= getattr(config, "ALPHA_COMPRESSION_MIN_VOLUME_RATIO", 0.70)
        and smma_distance <= getattr(config, "ALPHA_COMPRESSION_MAX_DISTANCE_FROM_SMMA_PCT", 0.35)
    )


def valid_alpha_trend_candle(candle, recent_volumes, band_low=None, band_high=None, side="BUY"):
    is_buy = (side == "BUY")
    if is_buy and not is_green(candle):
        return False, "TREND_NOT_GREEN", {}
    if not is_buy and not is_red(candle):
        return False, "TREND_NOT_RED", {}

    br = body_ratio(candle)
    bwr = body_to_wick_ratio(candle)
    vr = relative_volume(candle, recent_volumes)

    if is_doji(candle):
        return False, "TREND_DOJI", {"body_ratio": round(br, 3)}

    # Hard safety rule: body to wick must not fall below hard threshold (0.75)
    hard_bwr = getattr(config, "ALPHA_MIN_TREND_BODY_TO_WICK_HARD", 0.75)
    if bwr < hard_bwr:
        return False, "TREND_BODY_TO_WICK", {
            "body_to_wick_ratio": round(bwr, 3),
            "required": hard_bwr,
        }

    warnings = []
    soft_br = getattr(config, "ALPHA_SOFT_TREND_BODY_RATIO", 0.25)
    soft_vr = getattr(config, "ALPHA_SOFT_TREND_VOLUME_RATIO", 0.60)

    if br < soft_br:
        warnings.append("LOW_TREND_BODY_RATIO")
    if bwr < 1.0:
        warnings.append("MODERATE_WICK_TREND")
    if vr < soft_vr:
        warnings.append("LOW_TREND_VOLUME")

    return True, "TREND_QUALIFIED", {
        "body_ratio": round(br, 3),
        "body_to_wick_ratio": round(bwr, 3),
        "volume_ratio": round(vr, 3),
        "warnings": warnings,
    }


def alpha_high_volume_pullback_is_valid(
    alpha,
    band_low: float,
    band_high: float,
    trend_reference_volume: list,
    side="BUY",
) -> tuple[bool, str]:
    volume_ratio = relative_volume(alpha, trend_reference_volume)
    reversal_limit = getattr(config, "ALPHA_REVERSAL_VOLUME_RATIO", 2.50)
    adverse_pos_max = getattr(config, "ALPHA_ADVERSE_CLOSE_POSITION_MAX", 0.30)
    max_through = getattr(config, "ALPHA_MAX_CLOSE_THROUGH_BAND_PCT", 0.25) / 100.0

    if side == "BUY":
        holds_band = float(alpha.close) >= band_low * (1 - max_through)
        close_pos = close_position(alpha)
    else:
        holds_band = float(alpha.close) <= band_high * (1 + max_through)
        close_pos = bearish_close_position(alpha)

    if volume_ratio > reversal_limit:
        if close_pos < adverse_pos_max or not holds_band:
            return False, "ALPHA_DISTRIBUTION_REVERSAL"
        return True, "HIGH_PULLBACK_VOLUME_SUPPORT_HOLD"

    if not holds_band:
        return False, "ALPHA_CLOSE_THROUGH_BAND"

    return True, "ALPHA_PULLBACK_NORMAL"


def valid_alpha_pullback_candle(candle, trend_volumes, band_low: float = None, band_high: float = None, side="BUY"):
    if side == "BUY" and not is_red(candle):
        return False, "ALPHA_NOT_RED", []
    if side == "SELL" and not is_green(candle):
        return False, "ALPHA_NOT_GREEN", []

    br = body_ratio(candle)
    bwr = body_to_wick_ratio(candle)
    vol_ratio = relative_volume(candle, trend_volumes)

    if is_doji(candle):
        return False, "ALPHA_DOJI", []

    hard_bwr = getattr(config, "ALPHA_MIN_ALPHA_BODY_TO_WICK_HARD", 0.75)
    if bwr < hard_bwr:
        return False, "ALPHA_WICK_DOMINANT", []

    warnings = []
    if br < getattr(config, "ALPHA_SOFT_ALPHA_BODY_RATIO", 0.20):
        warnings.append("LOW_ALPHA_BODY_RATIO")
    if bwr < 1.0:
        warnings.append("MODERATE_WICK_ALPHA")

    b_low = band_low if band_low is not None else float(candle.low)
    b_high = band_high if band_high is not None else float(candle.high)

    is_valid, reason = alpha_high_volume_pullback_is_valid(candle, b_low, b_high, trend_volumes, side=side)
    if not is_valid:
        return False, reason, []

    if reason == "HIGH_PULLBACK_VOLUME_SUPPORT_HOLD":
        warnings.append("HIGH_PULLBACK_VOLUME")

    return True, "ALPHA_PULLBACK_VALID", warnings


def calculate_alpha_quality_score(candle, trend_run, warnings, side="BUY"):
    score = 100
    br = body_ratio(candle)
    bwr = body_to_wick_ratio(candle)

    if br < 0.25:
        score -= 10
    if br < 0.20:
        score -= 10
    if bwr < 1.0:
        score -= 10
    if "HIGH_PULLBACK_VOLUME" in warnings:
        score -= 15
    if "LOW_TREND_VOLUME" in warnings:
        score -= 10
    if "LOW_TREND_BODY_RATIO" in warnings:
        score -= 10

    return max(30, min(100, score))


def entry_extension_pct(entry_price, trigger_level, side="BUY"):
    entry_p = float(entry_price)
    trig_p = float(trigger_level)
    if trig_p <= 0:
        return 0.0
    if side == "BUY":
        return max(0.0, (entry_p - trig_p) / trig_p * 100.0)
    return max(0.0, (trig_p - entry_p) / trig_p * 100.0)


def find_alpha_setup(pattern_candles, side="BUY"):
    """
    State-aware recent-candidate finder:
    Locates the most recent qualified Alpha setup in pattern_candles for side ('BUY' or 'SELL').
    Searches within the last ALPHA_SEARCH_LOOKBACK_PATTERN_BARS completed bars.
    """
    min_run = config.ALPHA_MIN_TREND_CANDLES
    if pattern_candles is None or len(pattern_candles) < min_run + 1:
        return None

    df = pattern_candles.copy().reset_index(drop=True)
    smma_len = getattr(config, "JP_SMMA_LENGTH", 10)
    import jp_pattern
    df["smma_high"] = jp_pattern.smma(df["high"], smma_len)
    df["smma_close"] = jp_pattern.smma(df["close"], smma_len)

    lookback_bars = getattr(config, "ALPHA_SEARCH_LOOKBACK_PATTERN_BARS", 6)
    start_search_idx = max(min_run, len(df) - lookback_bars)

    # Walk backward from the newest potential Alpha candle down to start_search_idx
    for alpha_idx in range(len(df) - 1, start_search_idx - 1, -1):
        alpha = df.iloc[alpha_idx]
        candidate_window = df.iloc[max(0, alpha_idx - min_run):alpha_idx]
        trend_volumes = [float(c.volume) for _, c in candidate_window.iterrows() if float(c.volume) > 0]

        b_low = min(float(alpha.smma_high), float(alpha.smma_close)) if pd.notna(alpha.smma_high) and pd.notna(alpha.smma_close) else float(alpha.low)
        b_high = max(float(alpha.smma_high), float(alpha.smma_close)) if pd.notna(alpha.smma_high) and pd.notna(alpha.smma_close) else float(alpha.high)

        is_pullback_valid, p_reason, pullback_warnings = valid_alpha_pullback_candle(alpha, trend_volumes, band_low=b_low, band_high=b_high, side=side)
        if not is_pullback_valid:
            continue

        reversed_run = []
        trend_warnings = set()
        for index in range(alpha_idx - 1, -1, -1):
            candle = df.iloc[index]
            recent_volumes = df.iloc[max(0, index - 3):index]["volume"]
            cand_band_low = min(float(candle.smma_high), float(candle.smma_close)) if pd.notna(candle.smma_high) and pd.notna(candle.smma_close) else b_low
            cand_band_high = max(float(candle.smma_high), float(candle.smma_close)) if pd.notna(candle.smma_high) and pd.notna(candle.smma_close) else b_high

            is_valid, reason, metrics = valid_alpha_trend_candle(candle, recent_volumes, band_low=cand_band_low, band_high=cand_band_high, side=side)
            if not is_valid:
                break

            if "warnings" in metrics:
                trend_warnings.update(metrics["warnings"])

            if side == "BUY":
                if reversed_run and float(reversed_run[-1].close) <= float(candle.high):
                    break
            else:
                if reversed_run and float(reversed_run[-1].close) >= float(candle.low):
                    break

            reversed_run.append(candle)

        trend_run = list(reversed(reversed_run))
        if len(trend_run) < min_run:
            continue

        all_warnings = list(sorted(set(pullback_warnings + list(trend_warnings))))
        quality_score = calculate_alpha_quality_score(alpha, trend_run, all_warnings, side=side)

        # Found valid contiguous trend run followed immediately by pullback Alpha!
        # Now check confirmation in pattern bars formed after Alpha (up to ALPHA_CONFIRMATION_PATTERN_BARS)
        deadline_bars = getattr(config, "ALPHA_CONFIRMATION_DEADLINE_BARS", getattr(config, "ALPHA_CONFIRMATION_PATTERN_BARS", 2))
        confirmations = df.iloc[
            alpha_idx + 1:alpha_idx + 1 + deadline_bars
        ]

        confirmation_candle = None
        conf_vol_ratio = 1.0
        pattern_confirmation_status = "WAITING_3M_CONFIRMATION"
        pattern_confirmation_time = None
        bars_seen = len(confirmations)

        if not confirmations.empty:
            for _, c_bar in confirmations.iterrows():
                if side == "BUY":
                    breaks = float(c_bar["high"]) > float(alpha.high)
                    closes = float(c_bar["close"]) > float(alpha.high) if config.ALPHA_REQUIRE_CLOSE_CONFIRMATION else True
                else:
                    breaks = float(c_bar["low"]) < float(alpha.low)
                    closes = float(c_bar["close"]) < float(alpha.low) if config.ALPHA_REQUIRE_CLOSE_CONFIRMATION else True

                if breaks and closes:
                    ref_vols = [float(alpha.volume)] + [float(c.volume) for c in trend_run[-3:] if float(c.volume) > 0]
                    vol_r = relative_volume(c_bar, ref_vols)
                    min_conf_vol = getattr(config, "ALPHA_CONFIRMATION_MIN_VOLUME_RATIO", 0.75)
                    if vol_r >= min_conf_vol:
                        confirmation_candle = c_bar
                        conf_vol_ratio = vol_r
                        pattern_confirmation_status = "CONFIRMED"
                        pattern_confirmation_time = c_bar.timestamp
                        break

        # If bars formed after alpha reach or exceed deadline and still not confirmed -> expired/skip
        bars_after_alpha = len(df) - 1 - alpha_idx
        if pattern_confirmation_status != "CONFIRMED" and bars_after_alpha >= deadline_bars:
            continue

        alpha_open_time = alpha.timestamp.to_pydatetime() if hasattr(alpha.timestamp, "to_pydatetime") else alpha.timestamp
        alpha_close_time = alpha_open_time + timedelta(minutes=config.ALPHA_TIMEFRAME)

        if pattern_confirmation_status == "CONFIRMED":
            stage = "AWAITING_1M_TRIGGER"
        elif bars_seen == 1:
            stage = "WAITING_SECOND_3M_CONFIRMATION"
        else:
            stage = "WAITING_3M_CONFIRMATION"

        # Determine candidate promotion eligibility
        is_candidate = (
            quality_score >= getattr(config, "ALPHA_MIN_CANDIDATE_QUALITY_SCORE", 70)
            and len(all_warnings) <= getattr(config, "ALPHA_MAX_CANDIDATE_WARNINGS", 1)
            and not any(w in getattr(config, "ALPHA_DISQUALIFYING_WARNINGS", ()) for w in all_warnings)
        )

        return {
            "strategy": "ALPHA",
            "side": side,
            "direction": side,
            "status": pattern_confirmation_status,
            "stage": stage,
            "is_candidate": is_candidate,
            "alpha_candle": alpha,
            "alpha_idx": alpha_idx,
            "alpha_open_time": alpha_open_time,
            "alpha_close_time": alpha_close_time,
            "pattern_open_time": alpha_open_time,
            "pattern_close_time": alpha_close_time,
            "alpha_high": float(alpha.high),
            "alpha_low": float(alpha.low),
            "pattern_high": float(alpha.high),
            "pattern_low": float(alpha.low),
            "trigger_price": float(alpha.high if side == "BUY" else alpha.low),
            "trend_run": trend_run,
            "pattern_confirmation_status": pattern_confirmation_status,
            "pattern_confirmation_time": pattern_confirmation_time,
            "confirmation_candle": confirmation_candle,
            "confirmation_bars_seen": bars_seen,
            "confirmation_bars_required": deadline_bars,
            "confirmation_volume_ratio": round(conf_vol_ratio, 3),
            "pullback_volume_ratio": round(relative_volume(alpha, trend_volumes), 3),
            "close_position_in_range": round(close_position(alpha) if side == "BUY" else bearish_close_position(alpha), 3),
            "band_low": b_low,
            "band_high": b_high,
            "quality_score": quality_score,
            "quality_warnings": all_warnings,
            "detected_at": datetime.now(config.TIME_ZONE),
        }

    return None


def evaluate_alpha_confirmation(pattern_candles: pd.DataFrame, watchlist_entry: dict) -> dict:
    """
    Evaluates completed pattern candles against an existing watchlist entry waiting for 3m confirmation.
    Returns dict with status: 'CONFIRMED', 'WAITING_3M_CONFIRMATION', 'WAITING_SECOND_3M_CONFIRMATION', or 'EXPIRED'.
    """
    if pattern_candles is None or pattern_candles.empty:
        return {"status": "WAITING_3M_CONFIRMATION", "bars_seen": 0}

    side = watchlist_entry.get("direction", "BUY")
    alpha_high = float(watchlist_entry.get("alpha_high", 0.0))
    alpha_low = float(watchlist_entry.get("alpha_low", 0.0))
    alpha_close_time_str = watchlist_entry.get("alpha_close_time")
    if not alpha_close_time_str:
        return {"status": "EXPIRED", "reason": "NO_ALPHA_CLOSE_TIME"}

    alpha_close_time = datetime.fromisoformat(alpha_close_time_str)
    if alpha_close_time.tzinfo is None:
        alpha_close_time = alpha_close_time.replace(tzinfo=config.TIME_ZONE)

    df = pattern_candles.copy().sort_values("timestamp").reset_index(drop=True)
    # Filter for candles that started at or after alpha_close_time
    confirmations = df[df["timestamp"] >= alpha_close_time].copy().reset_index(drop=True)
    deadline_bars = getattr(config, "ALPHA_CONFIRMATION_DEADLINE_BARS", 2)
    eligible_bars = confirmations.iloc[:deadline_bars]
    bars_seen = len(eligible_bars)

    for _, c_bar in eligible_bars.iterrows():
        if side == "BUY":
            breaks = float(c_bar["high"]) > alpha_high
            closes = float(c_bar["close"]) > alpha_high if config.ALPHA_REQUIRE_CLOSE_CONFIRMATION else True
        else:
            breaks = float(c_bar["low"]) < alpha_low
            closes = float(c_bar["close"]) < alpha_low if config.ALPHA_REQUIRE_CLOSE_CONFIRMATION else True

        if breaks and closes:
            signal_quality = watchlist_entry.get("signal_quality", {})
            ref_vol = float(watchlist_entry.get("alpha_volume", 0.0))
            vol_r = float(c_bar["volume"]) / ref_vol if ref_vol > 0 else 1.0
            min_conf_vol = getattr(config, "ALPHA_CONFIRMATION_MIN_VOLUME_RATIO", 1.20)
            if vol_r >= min_conf_vol:
                return {
                    "status": "CONFIRMED",
                    "confirmation_candle": c_bar,
                    "pattern_confirmation_time": c_bar["timestamp"].isoformat() if hasattr(c_bar["timestamp"], "isoformat") else str(c_bar["timestamp"]),
                    "confirmation_volume_ratio": round(vol_r, 3),
                    "bars_seen": bars_seen,
                }

    if bars_seen >= deadline_bars:
        return {"status": "EXPIRED", "reason": "NO_NEXT_TWO_3M_CONFIRMATION", "bars_seen": bars_seen}

    return {
        "status": "WAITING_SECOND_3M_CONFIRMATION" if bars_seen == 1 else "WAITING_3M_CONFIRMATION",
        "reason": "FIRST_CONFIRMATION_NOT_QUALIFIED" if bars_seen == 1 else "ALPHA_CANDIDATE_FORMED",
        "bars_seen": bars_seen,
        "bars_required": deadline_bars,
    }


def find_opening_momentum_setup(pattern_candles: pd.DataFrame, is_bullish_setup: bool = True, session_date=None) -> dict | None:
    """
    Opening Momentum Scanner:
    Validates completed 09:15-09:18 3m bar (and optional subsequent trend expansion bars up to 09:45).
    Requires strong body ratio, body-to-wick >= 1.0, and not a doji.
    Returns opening momentum candidate dict awaiting 1m breakout, or None.
    """
    if not getattr(config, "OPENING_MOMENTUM_ENABLED", True):
        return None

    if pattern_candles is None or pattern_candles.empty:
        return None

    df = pattern_candles.copy().sort_values("timestamp").reset_index(drop=True)
    if session_date is not None:
        df["_cdate"] = df["timestamp"].apply(lambda t: t.date() if hasattr(t, "date") else pd.to_datetime(t).date())
        df = df[df["_cdate"] == session_date].reset_index(drop=True)
        if df.empty:
            return None

    # First completed 3m bar is 09:15-09:18
    first_bar = df.iloc[0]
    if is_bullish_setup:
        if not is_green(first_bar):
            return None
    else:
        if not is_red(first_bar):
            return None

    if is_doji(first_bar):
        return None

    br = candle_body_ratio(first_bar)
    bwr = body_to_wick_ratio(first_bar)
    min_br = getattr(config, "OPENING_MOMENTUM_MIN_BODY_RATIO", getattr(config, "ALPHA_MIN_TREND_BODY_RATIO", 0.40))
    min_bwr = getattr(config, "OPENING_MOMENTUM_MIN_BODY_TO_WICK_RATIO", getattr(config, "ALPHA_MIN_TREND_BODY_TO_WICK_RATIO", 1.0))

    if br < min_br or bwr < min_bwr:
        return None

    # Determine direction and reference levels from opening candle(s)
    direction = "BUY" if is_bullish_setup else "SELL"
    c_open_time = first_bar["timestamp"].to_pydatetime() if hasattr(first_bar["timestamp"], "to_pydatetime") else first_bar["timestamp"]
    c_close_time = c_open_time + timedelta(minutes=int(getattr(config, "PATTERN_TIMEFRAME", 3)))

    high_level = float(first_bar["high"])
    low_level = float(first_bar["low"])
    trigger_price = high_level if is_bullish_setup else low_level
    stop_price = low_level if is_bullish_setup else high_level

    return {
        "strategy": "OPENING_MOMENTUM",
        "direction": direction,
        "is_bullish_setup": is_bullish_setup,
        "pattern_open_time": c_open_time,
        "pattern_close_time": c_close_time,
        "pattern_high": high_level,
        "pattern_low": low_level,
        "trigger_price": trigger_price,
        "stop_price": stop_price,
        "body_ratio": round(br, 3),
        "body_to_wick_ratio": round(bwr, 3),
        "detected_at": datetime.now(config.TIME_ZONE),
    }


def find_alpha_buy_setup(pattern_candles):
    return find_alpha_setup(pattern_candles, side="BUY")


def find_alpha_sell_setup(pattern_candles):
    return find_alpha_setup(pattern_candles, side="SELL")


def find_trend_run_and_alpha(pattern_candles, is_bullish_setup):
    if is_bullish_setup:
        return find_alpha_setup(pattern_candles, side="BUY") if config.ALPHA_ALLOW_BUY else None
    else:
        return find_alpha_setup(pattern_candles, side="SELL") if config.ALPHA_ALLOW_SELL else None


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
