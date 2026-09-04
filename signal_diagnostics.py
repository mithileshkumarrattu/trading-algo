"""
Signal Diagnostics Engine for Alpha and JP strategies.
Provides structured diagnostic evaluation to identify exact rejection reasons
without modifying strategy thresholds or execution logic.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import pandas as pd

import config
import pattern
import jp_pattern
import state

logger = logging.getLogger(__name__)

# Alpha Rejection Reason Constants
ALPHA_REASONS = {
    "MARKET_REGIME_BLOCKED": "MARKET_REGIME_BLOCKED",
    "INSUFFICIENT_HISTORY": "INSUFFICIENT_HISTORY",
    "TREND_RUN_TOO_SHORT": "TREND_RUN_TOO_SHORT",
    "TREND_NOT_GREEN": "TREND_NOT_GREEN",
    "TREND_CLOSE_NOT_ABOVE_PREVIOUS_HIGH": "TREND_CLOSE_NOT_ABOVE_PREVIOUS_HIGH",
    "TREND_DOJI": "TREND_DOJI",
    "TREND_BODY_RATIO": "TREND_BODY_RATIO",
    "TREND_BODY_TO_WICK": "TREND_BODY_TO_WICK",
    "TREND_LOW_RELATIVE_VOLUME": "TREND_LOW_RELATIVE_VOLUME",
    "ALPHA_NOT_RED": "ALPHA_NOT_RED",
    "ALPHA_BODY_RATIO": "ALPHA_BODY_RATIO",
    "ALPHA_BODY_TO_WICK": "ALPHA_BODY_TO_WICK",
    "ALPHA_EXCESS_REVERSAL_VOLUME": "ALPHA_EXCESS_REVERSAL_VOLUME",
    "NO_NEXT_TWO_3M_CONFIRMATION": "NO_NEXT_TWO_3M_CONFIRMATION",
    "SETUP_EXPIRED": "SETUP_EXPIRED",
    "WAITING_FOR_1M_TRIGGER": "WAITING_FOR_1M_TRIGGER",
    "CANDLE_DATA_UNAVAILABLE": "CANDLE_DATA_UNAVAILABLE",
}

# JP Rejection Reason Constants
JP_REASONS = {
    "MARKET_REGIME_BLOCKED": "MARKET_REGIME_BLOCKED",
    "OPENING_CONDITION_FAILED": "OPENING_CONDITION_FAILED",
    "SMMA_TREND_INVALID": "SMMA_TREND_INVALID",
    "PRIOR_BAND_CHOP": "PRIOR_BAND_CHOP",
    "INSUFFICIENT_PRIOR_TREND_SIDE_BARS": "INSUFFICIENT_PRIOR_TREND_SIDE_BARS",
    "JP_NO_BAND_TOUCH": "JP_NO_BAND_TOUCH",
    "JP_CLOSE_THROUGH_BAND": "JP_CLOSE_THROUGH_BAND",
    "JP_BODY_RATIO": "JP_BODY_RATIO",
    "JP_VOLUME_TOO_LOW": "JP_VOLUME_TOO_LOW",
    "JP_VOLUME_TOO_HIGH": "JP_VOLUME_TOO_HIGH",
    "NEXT_3M_CONFIRMATION_FAILED": "NEXT_3M_CONFIRMATION_FAILED",
    "SETUP_EXPIRED": "SETUP_EXPIRED",
    "WAITING_FOR_1M_TRIGGER": "WAITING_FOR_1M_TRIGGER",
    "CANDLE_DATA_UNAVAILABLE": "CANDLE_DATA_UNAVAILABLE",
    "INSUFFICIENT_HISTORY": "INSUFFICIENT_HISTORY",
}

# In-memory deduplication tracker: (symbol, strategy, side) -> (last_pattern_time, last_reason, last_status)
_LAST_DIAGNOSTIC_STATE: Dict[Tuple[str, str, str], Tuple[str, str, str]] = {}


def diagnose_alpha_candidate(
    symbol: str,
    security_id: int,
    candidate_info: dict,
    today_pattern_candles: Optional[pd.DataFrame],
    regime: str,
    now: datetime,
) -> dict:
    """
    Evaluates Alpha setup rules step-by-step and returns structured diagnostic dict.
    """
    rank = candidate_info.get("rank")
    pct_change = candidate_info.get("pct_change", 0.0)
    volume = candidate_info.get("volume", 0.0)
    side = "BUY"
    strategy = "ALPHA"

    diag_base = {
        "symbol": symbol,
        "strategy": strategy,
        "side": side,
        "rank": rank,
        "pct_change": pct_change,
        "volume": volume,
        "evaluated_at": now.isoformat(),
        "pattern_time": None,
        "status": "REJECTED",
        "reason": "UNKNOWN",
        "metrics": {},
    }

    if config.ALPHA_REQUIRE_MARKET_BULLISH and regime == "BEARISH":
        diag_base["reason"] = "MARKET_REGIME_BLOCKED"
        diag_base["metrics"] = {"market_regime": regime}
        return diag_base

    if today_pattern_candles is None or today_pattern_candles.empty:
        diag_base["reason"] = "CANDLE_DATA_UNAVAILABLE"
        return diag_base

    df = today_pattern_candles.reset_index(drop=True)
    min_run = config.ALPHA_MIN_TREND_CANDLES
    if len(df) < min_run + 2:
        diag_base["reason"] = "INSUFFICIENT_HISTORY"
        diag_base["metrics"] = {"candles_count": len(df), "required": min_run + 2}
        return diag_base

    last_candle = df.iloc[-1]
    diag_base["pattern_time"] = str(last_candle.get("timestamp"))

    # Check the latest candle sequence.
    # We search backwards from the latest potential alpha candle down to min_run
    # To find why the most recent setup failed:
    alpha_idx = len(df) - 1
    # If the last candle is not red, maybe the setup happened at len(df)-2 (with 1 confirmation bar)
    candidate_alpha_indices = []
    if len(df) >= min_run + 1:
        candidate_alpha_indices.append(len(df) - 1)
    if len(df) >= min_run + 2:
        candidate_alpha_indices.append(len(df) - 2)
    if len(df) >= min_run + 3:
        candidate_alpha_indices.append(len(df) - 3)

    best_rejection: Optional[dict] = None

    for a_idx in candidate_alpha_indices:
        alpha = df.iloc[a_idx]
        p_time = str(alpha.get("timestamp"))

        # 1. Check if alpha is red
        if not pattern.is_red(alpha):
            rej = {
                "reason": "ALPHA_NOT_RED",
                "pattern_time": p_time,
                "metrics": {
                    "candle_open": float(alpha.open),
                    "candle_close": float(alpha.close),
                    "candle_type": "GREEN" if float(alpha.close) > float(alpha.open) else "DOJI",
                },
            }
            if best_rejection is None:
                best_rejection = rej
            continue

        # 2. Check alpha body ratio
        b_ratio = pattern.candle_body_ratio(alpha)
        if b_ratio < config.ALPHA_MIN_ALPHA_BODY_RATIO:
            rej = {
                "reason": "ALPHA_BODY_RATIO",
                "pattern_time": p_time,
                "metrics": {
                    "body_ratio": round(b_ratio, 3),
                    "required": config.ALPHA_MIN_ALPHA_BODY_RATIO,
                },
            }
            if best_rejection is None:
                best_rejection = rej
            continue

        # 3. Check alpha body to wick
        b_to_w = pattern.body_to_wick_ratio(alpha)
        if b_to_w < config.ALPHA_MIN_ALPHA_BODY_TO_WICK_RATIO:
            rej = {
                "reason": "ALPHA_BODY_TO_WICK",
                "pattern_time": p_time,
                "metrics": {
                    "body_to_wick_ratio": round(b_to_w, 3) if b_to_w != float("inf") else 999.0,
                    "required": config.ALPHA_MIN_ALPHA_BODY_TO_WICK_RATIO,
                },
            }
            if best_rejection is None:
                best_rejection = rej
            continue

        # 4. Check alpha volume
        cand_win = df.iloc[max(0, a_idx - min_run):a_idx]
        t_vols = [float(c.volume) for _, c in cand_win.iterrows() if float(c.volume) > 0]
        rel_vol = pattern.relative_volume(alpha, t_vols)
        if rel_vol > config.ALPHA_MAX_ALPHA_VOLUME_RATIO:
            rej = {
                "reason": "ALPHA_EXCESS_REVERSAL_VOLUME",
                "pattern_time": p_time,
                "metrics": {
                    "relative_volume": round(rel_vol, 3),
                    "max_allowed": config.ALPHA_MAX_ALPHA_VOLUME_RATIO,
                },
            }
            if best_rejection is None:
                best_rejection = rej
            continue

        # 5. Check preceding trend run
        trend_broken_reason = None
        trend_broken_metrics = {}
        reversed_run = []
        for idx in range(a_idx - 1, -1, -1):
            c = df.iloc[idx]
            rec_vols = df.iloc[max(0, idx - 3):idx]["volume"]

            if not pattern.is_green(c):
                trend_broken_reason = "TREND_NOT_GREEN"
                trend_broken_metrics = {"candle_time": str(c.timestamp), "open": float(c.open), "close": float(c.close)}
                break
            if pattern.is_doji(c):
                trend_broken_reason = "TREND_DOJI"
                trend_broken_metrics = {"candle_time": str(c.timestamp), "body_ratio": round(pattern.candle_body_ratio(c), 3)}
                break
            tb_ratio = pattern.candle_body_ratio(c)
            if tb_ratio < config.ALPHA_MIN_TREND_BODY_RATIO:
                trend_broken_reason = "TREND_BODY_RATIO"
                trend_broken_metrics = {"candle_time": str(c.timestamp), "body_ratio": round(tb_ratio, 3), "required": config.ALPHA_MIN_TREND_BODY_RATIO}
                break
            tb_to_w = pattern.body_to_wick_ratio(c)
            if tb_to_w < config.ALPHA_MIN_TREND_BODY_TO_WICK_RATIO:
                trend_broken_reason = "TREND_BODY_TO_WICK"
                trend_broken_metrics = {"candle_time": str(c.timestamp), "body_to_wick_ratio": round(tb_to_w, 3), "required": config.ALPHA_MIN_TREND_BODY_TO_WICK_RATIO}
                break
            t_rel_vol = pattern.relative_volume(c, rec_vols)
            if t_rel_vol < config.ALPHA_MIN_TREND_VOLUME_RATIO:
                trend_broken_reason = "TREND_LOW_RELATIVE_VOLUME"
                trend_broken_metrics = {"candle_time": str(c.timestamp), "relative_volume": round(t_rel_vol, 3), "required": config.ALPHA_MIN_TREND_VOLUME_RATIO}
                break
            if reversed_run and float(reversed_run[-1].close) <= float(c.high):
                trend_broken_reason = "TREND_CLOSE_NOT_ABOVE_PREVIOUS_HIGH"
                trend_broken_metrics = {
                    "later_close": float(reversed_run[-1].close),
                    "prior_high": float(c.high),
                }
                break
            reversed_run.append(c)

        if len(reversed_run) < min_run:
            rej = {
                "reason": trend_broken_reason or "TREND_RUN_TOO_SHORT",
                "pattern_time": p_time,
                "metrics": {
                    "valid_green_count": len(reversed_run),
                    "required": min_run,
                    **trend_broken_metrics,
                },
            }
            if best_rejection is None or rej["reason"] != "TREND_RUN_TOO_SHORT":
                best_rejection = rej
            continue

        # 6. Check confirmation
        confirmations = df.iloc[a_idx + 1:a_idx + 1 + config.ALPHA_CONFIRMATION_PATTERN_BARS]
        confirmation = confirmations[
            (confirmations["high"] > float(alpha.high))
            & (confirmations["close"] > float(alpha.high))
        ]
        if confirmation.empty:
            rej = {
                "reason": "NO_NEXT_TWO_3M_CONFIRMATION",
                "pattern_time": p_time,
                "metrics": {
                    "alpha_high": float(alpha.high),
                    "bars_checked": len(confirmations),
                    "max_bars": config.ALPHA_CONFIRMATION_PATTERN_BARS,
                },
            }
            best_rejection = rej
            continue

        # If we got here, Alpha 3m pattern is fully formed & confirmed!
        # Check if it is currently waiting for 1m trigger or expired
        alpha_open_time = alpha.timestamp.to_pydatetime() if hasattr(alpha.timestamp, "to_pydatetime") else alpha.timestamp
        alpha_close_time = alpha_open_time + timedelta(minutes=config.ALPHA_TIMEFRAME)
        alpha_age_minutes = (now - alpha_close_time).total_seconds() / 60.0
        alpha_key = f"{security_id}_{side}_{alpha_open_time.isoformat()}"

        if alpha_age_minutes > config.ALPHA_MAX_ENTRY_MINUTES or state.is_expired_alpha(alpha_key):
            diag_base["status"] = "REJECTED"
            diag_base["reason"] = "SETUP_EXPIRED"
            diag_base["pattern_time"] = p_time
            diag_base["metrics"] = {"age_minutes": round(alpha_age_minutes, 1), "max_allowed": config.ALPHA_MAX_ENTRY_MINUTES}
            return diag_base

        diag_base["status"] = "WAITING"
        diag_base["reason"] = "WAITING_FOR_1M_TRIGGER"
        diag_base["pattern_time"] = p_time
        diag_base["metrics"] = {
            "alpha_high": float(alpha.high),
            "alpha_low": float(alpha.low),
            "trend_length": len(reversed_run),
            "confirmation_time": str(confirmation.iloc[0].timestamp),
        }
        return diag_base

    if best_rejection:
        diag_base["reason"] = best_rejection["reason"]
        diag_base["pattern_time"] = best_rejection["pattern_time"]
        diag_base["metrics"] = best_rejection["metrics"]
    else:
        diag_base["reason"] = "TREND_NOT_GREEN"
        diag_base["pattern_time"] = str(last_candle.get("timestamp"))
        diag_base["metrics"] = {"note": "No valid green trend run found"}

    return diag_base


def diagnose_jp_candidate(
    symbol: str,
    security_id: int,
    candidate_info: dict,
    today_pattern_candles: Optional[pd.DataFrame],
    regime: str,
    now: datetime,
) -> dict:
    """
    Evaluates JP setup rules step-by-step and returns structured diagnostic dict.
    """
    rank = candidate_info.get("rank")
    pct_change = candidate_info.get("pct_change", 0.0)
    volume = candidate_info.get("volume", 0.0)
    is_bullish_setup = candidate_info.get("is_bullish_setup", True)
    side = "BUY" if is_bullish_setup else "SELL"
    strategy = "JP"

    diag_base = {
        "symbol": symbol,
        "strategy": strategy,
        "side": side,
        "rank": rank,
        "pct_change": pct_change,
        "volume": volume,
        "evaluated_at": now.isoformat(),
        "pattern_time": None,
        "status": "REJECTED",
        "reason": "UNKNOWN",
        "metrics": {},
    }

    if config.JP_REQUIRE_MARKET_REGIME:
        if (is_bullish_setup and regime != "BULLISH") or (not is_bullish_setup and regime != "BEARISH"):
            diag_base["reason"] = "MARKET_REGIME_BLOCKED"
            diag_base["metrics"] = {"market_regime": regime, "required": "BULLISH" if is_bullish_setup else "BEARISH"}
            return diag_base

    if today_pattern_candles is None or today_pattern_candles.empty:
        diag_base["reason"] = "CANDLE_DATA_UNAVAILABLE"
        return diag_base

    required = config.JP_SMMA_LENGTH + config.JP_CONTEXT_BARS + 2
    if len(today_pattern_candles) < required:
        diag_base["reason"] = "INSUFFICIENT_HISTORY"
        diag_base["metrics"] = {"candles_count": len(today_pattern_candles), "required": required}
        return diag_base

    df = today_pattern_candles.copy().reset_index(drop=True)
    df["jp_smma_high"] = jp_pattern.smma(df["high"], config.JP_SMMA_LENGTH)
    df["jp_smma_close"] = jp_pattern.smma(df["close"], config.JP_SMMA_LENGTH)
    df = df.dropna(subset=["jp_smma_high", "jp_smma_close"]).reset_index(drop=True)

    if len(df) < 3:
        diag_base["reason"] = "INSUFFICIENT_HISTORY"
        diag_base["metrics"] = {"valid_smma_bars": len(df), "required": 3}
        return diag_base

    jp_index = len(df) - 2 if config.JP_REQUIRE_NEXT_PATTERN_CONFIRMATION else len(df) - 1
    candle = df.iloc[jp_index]
    p_time = str(candle.get("timestamp"))
    diag_base["pattern_time"] = p_time

    band_low = min(float(candle.jp_smma_high), float(candle.jp_smma_close))
    band_high = max(float(candle.jp_smma_high), float(candle.jp_smma_close))

    # Prior band touch
    prior = df.iloc[max(0, jp_index - config.JP_CONTEXT_BARS):jp_index]
    prior_touch_count = sum(
        jp_pattern.touches_band(row, min(float(row.jp_smma_high), float(row.jp_smma_close)),
                                max(float(row.jp_smma_high), float(row.jp_smma_close)))
        for _, row in prior.iterrows()
    )
    if prior_touch_count > config.JP_MAX_PRIOR_BAND_TOUCHES:
        diag_base["reason"] = "PRIOR_BAND_CHOP"
        diag_base["metrics"] = {"prior_touches": prior_touch_count, "max_allowed": config.JP_MAX_PRIOR_BAND_TOUCHES}
        return diag_base

    # Trend side count
    if is_bullish_setup:
        trend_side_count = sum(jp_pattern.close_above_band(row, max(float(row.jp_smma_high), float(row.jp_smma_close))) for _, row in prior.iterrows())
    else:
        trend_side_count = sum(jp_pattern.close_below_band(row, min(float(row.jp_smma_high), float(row.jp_smma_close))) for _, row in prior.iterrows())

    if trend_side_count < config.JP_MIN_PRIOR_BARS_TREND_SIDE:
        diag_base["reason"] = "INSUFFICIENT_PRIOR_TREND_SIDE_BARS"
        diag_base["metrics"] = {"trend_side_bars": trend_side_count, "required": config.JP_MIN_PRIOR_BARS_TREND_SIDE}
        return diag_base

    # Band touch
    if not jp_pattern.touches_band(candle, band_low, band_high):
        diag_base["reason"] = "JP_NO_BAND_TOUCH"
        diag_base["metrics"] = {
            "high": float(candle.high),
            "low": float(candle.low),
            "band_low": round(band_low, 2),
            "band_high": round(band_high, 2),
        }
        return diag_base

    # Body ratio
    b_ratio = jp_pattern._body_ratio(candle)
    if b_ratio < config.JP_MIN_BODY_RATIO:
        diag_base["reason"] = "JP_BODY_RATIO"
        diag_base["metrics"] = {"body_ratio": round(b_ratio, 3), "required": config.JP_MIN_BODY_RATIO}
        return diag_base

    # Volume ratio
    ref_vol = float(prior["volume"].median()) if not prior.empty else 0.0
    vol_ratio = float(candle.volume) / ref_vol if ref_vol > 0 else 1.0
    if vol_ratio < config.JP_MIN_VOLUME_RATIO:
        diag_base["reason"] = "JP_VOLUME_TOO_LOW"
        diag_base["metrics"] = {"volume_ratio": round(vol_ratio, 3), "min_required": config.JP_MIN_VOLUME_RATIO}
        return diag_base
    if vol_ratio > config.JP_MAX_VOLUME_RATIO:
        diag_base["reason"] = "JP_VOLUME_TOO_HIGH"
        diag_base["metrics"] = {"volume_ratio": round(vol_ratio, 3), "max_allowed": config.JP_MAX_VOLUME_RATIO}
        return diag_base

    # Trend & close through band
    close = float(candle.close)
    max_through = config.JP_MAX_CLOSE_THROUGH_BAND_PCT / 100.0
    if is_bullish_setup:
        if not jp_pattern._is_uptrend(df, jp_index):
            diag_base["reason"] = "SMMA_TREND_INVALID"
            diag_base["metrics"] = {"trend": "NOT_UPTREND", "high_smma": float(candle.jp_smma_high), "close_smma": float(candle.jp_smma_close)}
            return diag_base
        if close < band_low * (1 - max_through):
            diag_base["reason"] = "JP_CLOSE_THROUGH_BAND"
            diag_base["metrics"] = {"close": close, "band_low_limit": round(band_low * (1 - max_through), 2)}
            return diag_base
    else:
        if not jp_pattern._is_downtrend(df, jp_index):
            diag_base["reason"] = "SMMA_TREND_INVALID"
            diag_base["metrics"] = {"trend": "NOT_DOWNTREND", "high_smma": float(candle.jp_smma_high), "close_smma": float(candle.jp_smma_close)}
            return diag_base
        if close > band_high * (1 + max_through):
            diag_base["reason"] = "JP_CLOSE_THROUGH_BAND"
            diag_base["metrics"] = {"close": close, "band_high_limit": round(band_high * (1 + max_through), 2)}
            return diag_base

    # Confirmation bar
    if config.JP_REQUIRE_NEXT_PATTERN_CONFIRMATION:
        confirmation = df.iloc[jp_index + 1]
        if is_bullish_setup:
            confirmed = float(confirmation.high) > float(candle.high) and float(confirmation.close) > float(candle.high)
        else:
            confirmed = float(confirmation.low) < float(candle.low) and float(confirmation.close) < float(candle.low)
        if not confirmed:
            diag_base["reason"] = "NEXT_3M_CONFIRMATION_FAILED"
            diag_base["metrics"] = {
                "candle_trigger": float(candle.high) if is_bullish_setup else float(candle.low),
                "conf_high": float(confirmation.high),
                "conf_close": float(confirmation.close),
                "conf_low": float(confirmation.low),
            }
            return diag_base

    # Pattern detected! Check expiry or waiting
    c_open_time = candle.timestamp.to_pydatetime() if hasattr(candle.timestamp, "to_pydatetime") else candle.timestamp
    c_close_time = c_open_time + timedelta(minutes=config.JP_TIMEFRAME)
    age_min = (now - c_close_time).total_seconds() / 60.0

    if c_close_time > now or age_min > config.JP_MAX_ENTRY_MINUTES:
        diag_base["status"] = "REJECTED"
        diag_base["reason"] = "SETUP_EXPIRED"
        diag_base["metrics"] = {"age_minutes": round(age_min, 1), "max_allowed": config.JP_MAX_ENTRY_MINUTES}
        return diag_base

    diag_base["status"] = "WAITING"
    diag_base["reason"] = "WAITING_FOR_1M_TRIGGER"
    diag_base["metrics"] = {
        "trigger_price": float(candle.high) if is_bullish_setup else float(candle.low),
        "stop_price": float(candle.low) if is_bullish_setup else float(candle.high),
        "band_low": round(band_low, 2),
        "band_high": round(band_high, 2),
    }
    return diag_base


def record_diagnostic(diag: dict) -> None:
    """
    Persists diagnostic into state.py and logs on reason/status transitions.
    """
    global _LAST_DIAGNOSTIC_STATE
    symbol = diag.get("symbol")
    strategy = diag.get("strategy")
    side = diag.get("side")
    status = diag.get("status")
    reason = diag.get("reason")
    pattern_time = str(diag.get("pattern_time"))

    key = (symbol, strategy, side)
    prev = _LAST_DIAGNOSTIC_STATE.get(key)
    curr = (pattern_time, reason, status)

    if prev != curr:
        _LAST_DIAGNOSTIC_STATE[key] = curr
        # Log transition concisely
        metrics_str = ", ".join(f"{k}={v}" for k, v in (diag.get("metrics") or {}).items())
        logger.info(
            "Diagnostic [%s %s %s Rank %s]: status=%s, reason=%s, pattern_time=%s, metrics={%s}",
            symbol,
            strategy,
            side,
            diag.get("rank"),
            status,
            reason,
            pattern_time,
            metrics_str,
        )
        if status == "REJECTED":
            state.add_log(f"{symbol} ({strategy} {side}): {reason} [{metrics_str}]")

    state.add_signal_diagnostic(diag)
