"""
JP (Jackpot) Pattern - pattern-timeframe pullback continuation detector.

This module is intentionally independent from Alpha Candle logic.
It detects and reports JP candidates only; it does not place orders.
The market rule is still "pattern candle first, then event-driven execution".
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

import config


def smma(series: pd.Series, length: int) -> pd.Series:
    """Wilder-style Smoothed Moving Average."""
    values = pd.to_numeric(series, errors="coerce").astype(float)
    result = pd.Series(index=values.index, dtype=float)

    if len(values) < length:
        return result

    seed = values.iloc[:length].mean()
    result.iloc[length - 1] = seed

    for index in range(length, len(values)):
        previous = result.iloc[index - 1]
        result.iloc[index] = (((length - 1) * previous) + values.iloc[index]) / length

    return result


def jp_interacts_with_band(candle, band_low: float, band_high: float, side: str = "BUY") -> tuple[bool, str]:
    """
    Check if candle touches or is within price-scaled near-band tolerance.
    Tolerance = min(band_ref * 0.15%, 3.0 points).
    """
    band_ref = float(band_high) if side == "BUY" else float(band_low)
    pct_tol = getattr(config, "JP_BAND_NEAR_TOLERANCE_PCT", 0.15) / 100.0
    max_pts = getattr(config, "JP_BAND_NEAR_TOLERANCE_MAX_POINTS", 3.0)
    tolerance = min(band_ref * pct_tol, max_pts)

    candle_low = float(candle.low)
    candle_high = float(candle.high)

    # Exact touch
    if candle_low <= band_high and candle_high >= band_low:
        return True, "BAND_TOUCH"

    # Near band interaction
    if side == "BUY":
        if candle_low <= band_high + tolerance and candle_high >= band_low - tolerance:
            return True, "BAND_NEAR"
    else:
        if candle_high >= band_low - tolerance and candle_low <= band_high + tolerance:
            return True, "BAND_NEAR"

    return False, "NO_BAND_INTERACTION"


def _touches_band(candle, band_low: float, band_high: float) -> bool:
    candle_low = float(candle.low)
    candle_high = float(candle.high)
    return candle_low <= band_high and candle_high >= band_low


def touches_band(candle, band_low, band_high):
    return float(candle.low) <= float(band_high) and float(candle.high) >= float(band_low)


def close_above_band(candle, band_high):
    return float(candle.close) > float(band_high)


def close_below_band(candle, band_low):
    return float(candle.close) < float(band_low)


def candle_range(candle):
    return max(0.0, float(candle.high) - float(candle.low))


def body_to_wick_ratio(candle) -> float:
    body = abs(float(candle.close) - float(candle.open))
    upper = float(candle.high) - max(float(candle.open), float(candle.close))
    lower = min(float(candle.open), float(candle.close)) - float(candle.low)
    wick = max(0.0, upper) + max(0.0, lower)
    return body / wick if wick > 0 else float("inf")


def constructive_compression(
    context: pd.DataFrame,
    band_low: float,
    band_high: float,
    bullish: bool,
) -> bool:
    if len(context) < getattr(config, "JP_COMPRESSION_LOOKBACK_BARS", 3):
        return False

    closes = context["close"].astype(float)
    volumes = context["volume"].astype(float)
    ranges = (context["high"].astype(float) - context["low"].astype(float))

    if bullish:
        trend_side_closes = (closes >= band_low).sum()
        through_band = (closes < band_low).sum()
    else:
        trend_side_closes = (closes <= band_high).sum()
        through_band = (closes > band_high).sum()

    if trend_side_closes < getattr(config, "JP_COMPRESSION_MIN_CLOSES_ON_TREND_SIDE", 2):
        return False

    if through_band > getattr(config, "JP_COMPRESSION_MAX_CLOSES_THROUGH_BAND", 1):
        return False

    # Context must compress; it should not expand into random volatility.
    max_range_ratio = getattr(config, "JP_COMPRESSION_MAX_RANGE_RATIO", 1.00)
    med_range = ranges.median()
    if med_range > 0 and ranges.iloc[-1] > med_range * max_range_ratio:
        return False

    # Pullback/base volume should not exceed the preceding trend-volume baseline.
    max_vol_ratio = getattr(config, "JP_COMPRESSION_MAX_VOLUME_RATIO", 1.00)
    med_vol = volumes.median()
    if med_vol > 0 and volumes.mean() > med_vol * max_vol_ratio:
        return False

    return True


def _body_ratio(candle):
    rng = candle_range(candle)
    return abs(float(candle.close) - float(candle.open)) / rng if rng else 0.0


def _is_uptrend(df: pd.DataFrame, index: int) -> bool:
    if index < 2:
        return False
    high_smma = float(df.iloc[index]["jp_smma_high"])
    close_smma = float(df.iloc[index]["jp_smma_close"])
    prev_high_smma = float(df.iloc[index - 1]["jp_smma_high"])
    prev_close_smma = float(df.iloc[index - 1]["jp_smma_close"])
    return (
        high_smma > close_smma
        and high_smma > prev_high_smma
        and close_smma > prev_close_smma
        and float(df.iloc[index]["close"]) >= close_smma
    )


def _is_downtrend(df: pd.DataFrame, index: int) -> bool:
    if index < 2:
        return False
    high_smma = float(df.iloc[index]["jp_smma_high"])
    close_smma = float(df.iloc[index]["jp_smma_close"])
    prev_high_smma = float(df.iloc[index - 1]["jp_smma_high"])
    prev_close_smma = float(df.iloc[index - 1]["jp_smma_close"])
    return (
        high_smma < close_smma
        and high_smma < prev_high_smma
        and close_smma < prev_close_smma
        and float(df.iloc[index]["close"]) <= close_smma
    )


def find_jp_setup(pattern_candles: pd.DataFrame, is_bullish_setup: bool, session_date=None):
    """Return the most recent fresh JP pullback candidate, if any."""
    required = config.JP_SMMA_LENGTH + config.JP_CONTEXT_BARS + 2
    if pattern_candles is None or len(pattern_candles) < required:
        return None

    df = pattern_candles.copy().reset_index(drop=True)
    df["jp_smma_high"] = smma(df["high"], config.JP_SMMA_LENGTH)
    df["jp_smma_close"] = smma(df["close"], config.JP_SMMA_LENGTH)
    df = df.dropna(subset=["jp_smma_high", "jp_smma_close"]).reset_index(drop=True)
    if len(df) < 3:
        return None

    jp_index = len(df) - 2 if config.JP_REQUIRE_NEXT_PATTERN_CONFIRMATION else len(df) - 1
    candle = df.iloc[jp_index]

    # Warmup protection: Only detect fresh setups if the candle belongs to the current session
    if session_date is not None:
        candle_date = candle.timestamp.date() if hasattr(candle.timestamp, "date") else pd.to_datetime(candle.timestamp).date()
        if candle_date != session_date:
            return None
    band_low = min(float(candle.jp_smma_high), float(candle.jp_smma_close))
    band_high = max(float(candle.jp_smma_high), float(candle.jp_smma_close))
    prior = df.iloc[max(0, jp_index - config.JP_CONTEXT_BARS):jp_index]
    prior_touch_count = sum(
        touches_band(row, min(float(row.jp_smma_high), float(row.jp_smma_close)),
                     max(float(row.jp_smma_high), float(row.jp_smma_close)))
        for _, row in prior.iterrows()
    )

    warnings = []
    compression_valid = False
    max_touches = getattr(config, "JP_MAX_PRIOR_BAND_TOUCHES_SOFT", 3)
    if prior_touch_count > max_touches:
        if getattr(config, "JP_ALLOW_CONSTRUCTIVE_COMPRESSION", True) and constructive_compression(
            prior, band_low, band_high, bullish=is_bullish_setup
        ):
            compression_valid = True
            warnings.append("CONSTRUCTIVE_COMPRESSION")
        else:
            return None
    elif prior_touch_count >= 2:
        warnings.append("MODERATE_PRIOR_BAND_TOUCHES")

    if is_bullish_setup:
        trend_side_count = sum(close_above_band(row, max(float(row.jp_smma_high), float(row.jp_smma_close))) for _, row in prior.iterrows())
    else:
        trend_side_count = sum(close_below_band(row, min(float(row.jp_smma_high), float(row.jp_smma_close))) for _, row in prior.iterrows())
    if trend_side_count < getattr(config, "JP_MIN_PRIOR_TREND_SIDE_BARS_SOFT", 1):
        return None

    side = "BUY" if is_bullish_setup else "SELL"
    interacts, interaction_type = jp_interacts_with_band(candle, band_low, band_high, side=side)
    if not interacts:
        return None
    if interaction_type == "BAND_NEAR":
        warnings.append("BAND_NEAR")

    b_ratio = _body_ratio(candle)
    if b_ratio < getattr(config, "JP_HARD_DOJI_BODY_RATIO", 0.15):
        return None
    if b_ratio < getattr(config, "JP_MIN_BODY_RATIO", 0.20):
        warnings.append("LOW_JP_BODY_RATIO")

    # Hard safety check on wicks (0.75 hard floor)
    rng = candle_range(candle)
    body = abs(float(candle.close) - float(candle.open))
    upper = float(candle.high) - max(float(candle.open), float(candle.close))
    lower = min(float(candle.open), float(candle.close)) - float(candle.low)
    wick = max(0.0, upper) + max(0.0, lower)
    b_to_w = body / wick if wick > 0 else float("inf")
    hard_b_to_w = getattr(config, "JP_MIN_BODY_TO_WICK_HARD", 0.75)
    if b_to_w < hard_b_to_w:
        return None
    if b_to_w < 1.0:
        warnings.append("MODERATE_WICK_JP")

    reference_volume = float(prior["volume"].median()) if not prior.empty else 0.0
    volume_ratio = float(candle.volume) / reference_volume if reference_volume > 0 else 1.0
    min_vol_hard = getattr(config, "JP_MIN_VOLUME_RATIO_HARD", 0.60)
    reversal_vol_max = getattr(config, "JP_REVERSAL_VOLUME_RATIO", 3.00)

    if volume_ratio < min_vol_hard:
        return None
    if volume_ratio < 0.80:
        warnings.append("LOW_JP_VOLUME")
    if volume_ratio > reversal_vol_max:
        return None

    close = float(candle.close)
    max_through = getattr(config, "JP_MAX_CLOSE_THROUGH_BAND_PCT", 0.25) / 100.0
    if is_bullish_setup:
        if not _is_uptrend(df, jp_index) or close < band_low * (1 - max_through):
            return None
        direction = "BUY"
        trigger_price = float(candle.high)
        stop_price = float(candle.low)
    else:
        if not _is_downtrend(df, jp_index) or close > band_high * (1 + max_through):
            return None
        direction = "SELL"
        trigger_price = float(candle.low)
        stop_price = float(candle.high)

    candle_open_time = candle.timestamp.to_pydatetime()
    candle_close_time = candle_open_time + timedelta(minutes=config.JP_TIMEFRAME)
    confirmation = df.iloc[jp_index + 1]
    if config.JP_REQUIRE_NEXT_PATTERN_CONFIRMATION:
        if is_bullish_setup:
            confirmed = float(confirmation.high) > float(candle.high) and float(confirmation.close) > float(candle.high)
        else:
            confirmed = float(confirmation.low) < float(candle.low) and float(confirmation.close) < float(candle.low)
        if not confirmed:
            return None

        # Check confirmation volume ratio
        conf_vol = float(confirmation.volume)
        conf_ref = float(prior["volume"].median()) if not prior.empty and float(prior["volume"].median()) > 0 else float(candle.volume)
        conf_vol_ratio = conf_vol / conf_ref if conf_ref > 0 else 1.0
        min_conf_vol = getattr(config, "JP_CONFIRMATION_MIN_VOLUME_RATIO", 0.75)
        if conf_vol_ratio < min_conf_vol:
            return None
    else:
        conf_vol_ratio = 1.0

    # Calculate quality score (0-100)
    quality_score = 100
    if interaction_type == "BAND_NEAR":
        quality_score -= 10
    if "LOW_JP_VOLUME" in warnings:
        quality_score -= 10
    if "LOW_JP_BODY_RATIO" in warnings:
        quality_score -= 10
    if "MODERATE_WICK_JP" in warnings:
        quality_score -= 10
    if "MODERATE_PRIOR_BAND_TOUCHES" in warnings:
        quality_score -= 10
    quality_score = max(30, min(100, quality_score))

    return {
        "strategy": "JP",
        "direction": direction,
        "jp_candle": candle,
        "jp_index": jp_index,
        "jp_open_time": candle_open_time,
        "jp_close_time": candle_close_time,
        "pattern_open_time": candle_open_time,
        "pattern_close_time": candle_close_time,
        "pattern_high": float(candle.high),
        "pattern_low": float(candle.low),
        "smma_high": float(candle.jp_smma_high),
        "smma_close": float(candle.jp_smma_close),
        "band_low": band_low,
        "band_high": band_high,
        "trigger_price": trigger_price,
        "stop_price": stop_price,
        "structural_stop_price": stop_price,
        "body_ratio": round(_body_ratio(candle), 3),
        "volume_ratio": round(volume_ratio, 3),
        "prior_band_touch_count": prior_touch_count,
        "compression_valid": compression_valid,
        "confirmation_volume_ratio": round(conf_vol_ratio, 3),
        "trend_side_count": trend_side_count,
        "band_interaction": interaction_type,
        "quality_score": quality_score,
        "quality_warnings": warnings,
        "pattern_confirmation_time": confirmation.timestamp,
        "pattern_confirmation_status": "CONFIRMED",
        "detected_at": datetime.now(config.TIME_ZONE),
    }


def check_jp_confirmation(pattern_candles, jp_open_time, trigger_price, is_bullish_setup):
    """Check only the immediately following completed pattern candle."""
    if pattern_candles is None or pattern_candles.empty:
        return None
    expected_time = jp_open_time + timedelta(minutes=config.JP_TIMEFRAME)
    next_candle = pattern_candles[pattern_candles["timestamp"] == expected_time]
    if next_candle.empty:
        return None
    candle = next_candle.iloc[0]
    if is_bullish_setup:
        return candle if float(candle.high) > trigger_price and float(candle.close) > trigger_price else None
    return candle if float(candle.low) < trigger_price and float(candle.close) < trigger_price else None


def jp_stop_distance_pct(entry_price: float, stop_price: float) -> float:
    if entry_price <= 0:
        return 999.0
    return abs(entry_price - stop_price) / entry_price * 100
