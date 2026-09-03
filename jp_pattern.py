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


def _body_ratio(candle):
    candle_range = max(0.0, float(candle.high) - float(candle.low))
    return abs(float(candle.close) - float(candle.open)) / candle_range if candle_range else 0.0


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


def find_jp_setup(pattern_candles: pd.DataFrame, is_bullish_setup: bool):
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
    band_low = min(float(candle.jp_smma_high), float(candle.jp_smma_close))
    band_high = max(float(candle.jp_smma_high), float(candle.jp_smma_close))
    prior = df.iloc[max(0, jp_index - config.JP_CONTEXT_BARS):jp_index]
    prior_touch_count = sum(
        touches_band(row, min(float(row.jp_smma_high), float(row.jp_smma_close)),
                     max(float(row.jp_smma_high), float(row.jp_smma_close)))
        for _, row in prior.iterrows()
    )
    if prior_touch_count > config.JP_MAX_PRIOR_BAND_TOUCHES:
        return None
    if is_bullish_setup:
        trend_side_count = sum(close_above_band(row, max(float(row.jp_smma_high), float(row.jp_smma_close))) for _, row in prior.iterrows())
    else:
        trend_side_count = sum(close_below_band(row, min(float(row.jp_smma_high), float(row.jp_smma_close))) for _, row in prior.iterrows())
    if trend_side_count < config.JP_MIN_PRIOR_BARS_TREND_SIDE:
        return None
    if not touches_band(candle, band_low, band_high) or _body_ratio(candle) < config.JP_MIN_BODY_RATIO:
        return None
    reference_volume = float(prior["volume"].median()) if not prior.empty else 0.0
    volume_ratio = float(candle.volume) / reference_volume if reference_volume > 0 else 1.0
    if not config.JP_MIN_VOLUME_RATIO <= volume_ratio <= config.JP_MAX_VOLUME_RATIO:
        return None

    close = float(candle.close)
    max_through = config.JP_MAX_CLOSE_THROUGH_BAND_PCT / 100.0
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
        "body_ratio": _body_ratio(candle),
        "volume_ratio": volume_ratio,
        "prior_band_touch_count": prior_touch_count,
        "trend_side_count": trend_side_count,
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
