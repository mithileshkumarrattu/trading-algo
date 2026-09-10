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
    "COUNTERTREND_MOVE_TOO_SMALL": "COUNTERTREND_MOVE_TOO_SMALL",
    "NEUTRAL_REGIME_MOVE_TOO_SMALL": "NEUTRAL_REGIME_MOVE_TOO_SMALL",
    "MOVE_TOO_SMALL": "MOVE_TOO_SMALL",
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
    "ALPHA_CONFIRMATION_LOW_VOLUME": "ALPHA_CONFIRMATION_LOW_VOLUME",
    "COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW": "COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW",
    "NO_NEXT_TWO_3M_CONFIRMATION": "NO_NEXT_TWO_3M_CONFIRMATION",
    "ALPHA_CANDIDATE_FORMED": "ALPHA_CANDIDATE_FORMED",
    "WAITING_3M_CONFIRMATION": "WAITING_3M_CONFIRMATION",
    "FIRST_CONFIRMATION_NOT_QUALIFIED": "FIRST_CONFIRMATION_NOT_QUALIFIED",
    "ALPHA_3M_CONFIRMED": "ALPHA_3M_CONFIRMED",
    "SETUP_EXPIRED": "SETUP_EXPIRED",
    "WAITING_FOR_1M_TRIGGER": "WAITING_FOR_1M_TRIGGER",
    "CANDLE_DATA_UNAVAILABLE": "CANDLE_DATA_UNAVAILABLE",
}

# JP Rejection Reason Constants
JP_REASONS = {
    "MARKET_REGIME_BLOCKED": "MARKET_REGIME_BLOCKED",
    "COUNTERTREND_MOVE_TOO_SMALL": "COUNTERTREND_MOVE_TOO_SMALL",
    "NEUTRAL_REGIME_MOVE_TOO_SMALL": "NEUTRAL_REGIME_MOVE_TOO_SMALL",
    "MOVE_TOO_SMALL": "MOVE_TOO_SMALL",
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
    "JP_CONFIRMATION_LOW_VOLUME": "JP_CONFIRMATION_LOW_VOLUME",
    "COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW": "COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW",
    "SETUP_EXPIRED": "SETUP_EXPIRED",
    "WAITING_FOR_1M_TRIGGER": "WAITING_FOR_1M_TRIGGER",
    "CANDLE_DATA_UNAVAILABLE": "CANDLE_DATA_UNAVAILABLE",
    "INSUFFICIENT_HISTORY": "INSUFFICIENT_HISTORY",
}

# In-memory deduplication tracker: (symbol, strategy, side) -> (last_pattern_time, last_reason, last_status)
_LAST_DIAGNOSTIC_STATE: Dict[Tuple[str, str, str], Tuple[str, str, str]] = {}


def market_regime_allows_setup(
    direction: str,
    market_regime: str,
    pct_change: float,
    confirmation_volume_ratio: float | None = None,
):
    """
    Return (allowed: bool, mode: str, reason: str).
    Direction is BUY or SELL.
    Market regime is BULLISH, BEARISH, NEUTRAL, or UNKNOWN.
    """
    market_regime = (market_regime or "UNKNOWN").upper()
    direction = direction.upper()
    abs_move = abs(float(pct_change or 0.0))

    aligned = (
        (direction == "BUY" and market_regime == "BULLISH")
        or (direction == "SELL" and market_regime == "BEARISH")
    )

    if aligned:
        if abs_move >= config.REGIME_ALIGNED_MIN_STOCK_MOVE_PCT:
            return True, "ALIGNED", "REGIME_ALIGNED"
        return False, "ALIGNED", "MOVE_TOO_SMALL"

    if market_regime in ("UNKNOWN", "NEUTRAL"):
        if abs_move >= config.REGIME_NEUTRAL_MIN_STOCK_MOVE_PCT:
            return True, "NEUTRAL", "REGIME_NEUTRAL_STRENGTH_OK"
        return False, "NEUTRAL", "NEUTRAL_REGIME_MOVE_TOO_SMALL"

    # Countertrend: allowed only with substantial relative strength/weakness.
    if abs_move < config.REGIME_COUNTERTREND_MIN_STOCK_MOVE_PCT:
        return False, "COUNTERTREND", "COUNTERTREND_MOVE_TOO_SMALL"

    return True, "COUNTERTREND", "COUNTERTREND_PENDING_STRONG_CONFIRMATION"


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

    regime_mode = "ALIGNED"
    if getattr(config, "ALPHA_USE_MARKET_REGIME_FILTER", True):
        allowed, regime_mode, regime_reason = market_regime_allows_setup(
            direction=side,
            market_regime=regime,
            pct_change=pct_change,
        )
        diag_base["regime_mode"] = regime_mode
        if not allowed:
            diag_base["reason"] = regime_reason
            req_thresh = (
                config.REGIME_ALIGNED_MIN_STOCK_MOVE_PCT if regime_mode == "ALIGNED"
                else (config.REGIME_NEUTRAL_MIN_STOCK_MOVE_PCT if regime_mode == "NEUTRAL"
                      else config.REGIME_COUNTERTREND_MIN_STOCK_MOVE_PCT)
            )
            diag_base["metrics"] = {
                "market_regime": regime,
                "pct_change": pct_change,
                "required_move_pct": req_thresh,
                "regime_mode": regime_mode,
            }
            return diag_base
    elif config.ALPHA_REQUIRE_MARKET_BULLISH and regime == "BEARISH":
        diag_base["reason"] = "MARKET_REGIME_BLOCKED"
        diag_base["metrics"] = {"market_regime": regime}
        return diag_base

    if today_pattern_candles is None or today_pattern_candles.empty:
        diag_base["reason"] = "CANDLE_DATA_UNAVAILABLE"
        return diag_base

    df = today_pattern_candles.reset_index(drop=True)
    min_run = config.ALPHA_MIN_TREND_CANDLES
    if len(df) < min_run + 1:
        diag_base["reason"] = "INSUFFICIENT_HISTORY"
        diag_base["metrics"] = {"candles_count": len(df), "required": min_run + 1}
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

        # Calculate SMMA bands for alpha if available
        smma_len = getattr(config, "JP_SMMA_LENGTH", 10)
        df_smma_high = jp_pattern.smma(df["high"], smma_len)
        df_smma_close = jp_pattern.smma(df["close"], smma_len)
        b_low = min(float(df_smma_high.iloc[a_idx]), float(df_smma_close.iloc[a_idx])) if pd.notna(df_smma_high.iloc[a_idx]) and pd.notna(df_smma_close.iloc[a_idx]) else float(alpha.low)
        b_high = max(float(df_smma_high.iloc[a_idx]), float(df_smma_close.iloc[a_idx])) if pd.notna(df_smma_high.iloc[a_idx]) and pd.notna(df_smma_close.iloc[a_idx]) else float(alpha.high)

        if not pattern.valid_alpha_pullback_candle(alpha, t_vols, band_low=b_low, band_high=b_high):
            rej = {
                "reason": "ALPHA_EXCESS_REVERSAL_VOLUME",
                "pattern_time": p_time,
                "metrics": {
                    "relative_volume": round(rel_vol, 3),
                    "max_allowed": config.ALPHA_MAX_ALPHA_VOLUME_RATIO,
                    "close_position": round(pattern.close_position_in_range(alpha), 3),
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
            cand_b_low = min(float(df_smma_high.iloc[idx]), float(df_smma_close.iloc[idx])) if pd.notna(df_smma_high.iloc[idx]) and pd.notna(df_smma_close.iloc[idx]) else b_low
            cand_b_high = max(float(df_smma_high.iloc[idx]), float(df_smma_close.iloc[idx])) if pd.notna(df_smma_high.iloc[idx]) and pd.notna(df_smma_close.iloc[idx]) else b_high

            is_valid, t_reason, t_metrics = pattern.valid_alpha_trend_candle(c, rec_vols, band_low=cand_b_low, band_high=cand_b_high)
            if not is_valid:
                trend_broken_reason = t_reason
                trend_broken_metrics = {"candle_time": str(c.timestamp), **t_metrics}
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
        deadline_bars = getattr(config, "ALPHA_CONFIRMATION_DEADLINE_BARS", getattr(config, "ALPHA_CONFIRMATION_PATTERN_BARS", 2))
        confirmations = df.iloc[a_idx + 1:a_idx + 1 + deadline_bars]
        bars_seen = len(confirmations)

        confirmation = confirmations[
            (confirmations["high"] > float(alpha.high))
            & (confirmations["close"] > float(alpha.high))
        ] if not confirmations.empty else pd.DataFrame()

        # If no bars exist yet or 1 non-confirming bar exists (and deadline has not passed), it is WAITING for confirmation!
        if confirmation.empty:
            if bars_seen < deadline_bars:
                diag_base["status"] = "WAITING"
                diag_base["reason"] = "FIRST_CONFIRMATION_NOT_QUALIFIED" if bars_seen == 1 else "ALPHA_CANDIDATE_FORMED"
                diag_base["pattern_time"] = p_time
                diag_base["metrics"] = {
                    "alpha_high": float(alpha.high),
                    "alpha_low": float(alpha.low),
                    "confirmation_bars_seen": bars_seen,
                    "confirmation_bars_required": deadline_bars,
                    "trend_length": len(reversed_run),
                }
                return diag_base

            rej = {
                "reason": "NO_NEXT_TWO_3M_CONFIRMATION",
                "pattern_time": p_time,
                "metrics": {
                    "alpha_high": float(alpha.high),
                    "bars_checked": bars_seen,
                    "max_bars": deadline_bars,
                },
            }
            best_rejection = rej
            continue

        conf_candle = confirmation.iloc[0]
        ref_vols = [float(alpha.volume)] + [float(c.volume) for c in reversed_run[:3] if float(c.volume) > 0]
        conf_vol_ratio = pattern.relative_volume(conf_candle, ref_vols)
        min_conf_vol = (
            getattr(config, "REGIME_COUNTERTREND_MIN_CONFIRMATION_VOLUME_RATIO", 1.50)
            if regime_mode == "COUNTERTREND"
            else getattr(config, "ALPHA_CONFIRMATION_MIN_VOLUME_RATIO", 1.20)
        )
        if conf_vol_ratio < min_conf_vol:
            if bars_seen < deadline_bars:
                diag_base["status"] = "WAITING"
                diag_base["reason"] = "FIRST_CONFIRMATION_NOT_QUALIFIED"
                diag_base["pattern_time"] = p_time
                diag_base["metrics"] = {
                    "alpha_high": float(alpha.high),
                    "confirmation_bars_seen": bars_seen,
                    "confirmation_volume_ratio": round(conf_vol_ratio, 3),
                    "min_required": min_conf_vol,
                }
                return diag_base

            reason = (
                "COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW"
                if regime_mode == "COUNTERTREND"
                else "ALPHA_CONFIRMATION_LOW_VOLUME"
            )
            rej = {
                "reason": reason,
                "pattern_time": p_time,
                "metrics": {
                    "confirmation_volume_ratio": round(conf_vol_ratio, 3),
                    "min_required": min_conf_vol,
                    "regime_mode": regime_mode,
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
    session_date=None,
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

    regime_mode = "ALIGNED"
    if getattr(config, "JP_USE_MARKET_REGIME_FILTER", True):
        allowed, regime_mode, regime_reason = market_regime_allows_setup(
            direction=side,
            market_regime=regime,
            pct_change=pct_change,
        )
        diag_base["regime_mode"] = regime_mode
        if not allowed:
            diag_base["reason"] = regime_reason
            req_thresh = (
                config.REGIME_ALIGNED_MIN_STOCK_MOVE_PCT if regime_mode == "ALIGNED"
                else (config.REGIME_NEUTRAL_MIN_STOCK_MOVE_PCT if regime_mode == "NEUTRAL"
                      else config.REGIME_COUNTERTREND_MIN_STOCK_MOVE_PCT)
            )
            diag_base["metrics"] = {
                "market_regime": regime,
                "pct_change": pct_change,
                "required_move_pct": req_thresh,
                "regime_mode": regime_mode,
            }
            return diag_base
    elif config.JP_REQUIRE_MARKET_REGIME:
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
    compression_valid = False
    if prior_touch_count > config.JP_MAX_PRIOR_BAND_TOUCHES:
        if getattr(config, "JP_ALLOW_CONSTRUCTIVE_COMPRESSION", True) and jp_pattern.constructive_compression(
            prior, band_low, band_high, bullish=is_bullish_setup
        ):
            compression_valid = True
        else:
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

        conf_vol = float(confirmation.volume)
        conf_ref = float(prior["volume"].median()) if not prior.empty and float(prior["volume"].median()) > 0 else float(candle.volume)
        conf_vol_ratio = conf_vol / conf_ref if conf_ref > 0 else 1.0
        min_conf_vol = (
            getattr(config, "REGIME_COUNTERTREND_MIN_CONFIRMATION_VOLUME_RATIO", 1.50)
            if regime_mode == "COUNTERTREND"
            else getattr(config, "JP_CONFIRMATION_MIN_VOLUME_RATIO", 1.20)
        )
        if conf_vol_ratio < min_conf_vol:
            reason = (
                "COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW"
                if regime_mode == "COUNTERTREND"
                else "JP_CONFIRMATION_LOW_VOLUME"
            )
            diag_base["reason"] = reason
            diag_base["metrics"] = {
                "confirmation_volume_ratio": round(conf_vol_ratio, 3),
                "min_required": min_conf_vol,
                "regime_mode": regime_mode,
            }
            return diag_base
    else:
        conf_vol_ratio = 1.0

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


def diagnose_opening_momentum_candidate(
    symbol: str,
    security_id: int,
    candidate_info: dict,
    today_pattern_candles: Optional[pd.DataFrame],
    regime: str,
    now: datetime,
    session_date=None,
) -> dict:
    """
    Evaluates Opening Momentum setup rules and returns structured diagnostic dict.
    """
    rank = candidate_info.get("rank")
    pct_change = candidate_info.get("pct_change", 0.0)
    volume = candidate_info.get("volume", 0.0)
    is_bullish_setup = candidate_info.get("is_bullish_setup", True)
    side = "BUY" if is_bullish_setup else "SELL"
    strategy = "OPENING_MOMENTUM"

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

    if not getattr(config, "OPENING_MOMENTUM_ENABLED", True):
        diag_base["reason"] = "OPENING_MOMENTUM_DISABLED"
        return diag_base

    if today_pattern_candles is None or today_pattern_candles.empty:
        diag_base["reason"] = "CANDLE_DATA_UNAVAILABLE"
        return diag_base

    df = today_pattern_candles.copy().sort_values("timestamp").reset_index(drop=True)
    if session_date is not None:
        df["_cdate"] = df["timestamp"].apply(lambda t: t.date() if hasattr(t, "date") else pd.to_datetime(t).date())
        df = df[df["_cdate"] == session_date].reset_index(drop=True)
        if df.empty:
            diag_base["reason"] = "INSUFFICIENT_HISTORY"
            return diag_base

    first_bar = df.iloc[0]
    p_time = str(first_bar.get("timestamp"))
    diag_base["pattern_time"] = p_time

    if is_bullish_setup and not pattern.is_green(first_bar):
        diag_base["reason"] = "OPENING_CANDLE_NOT_GREEN"
        return diag_base
    if not is_bullish_setup and not pattern.is_red(first_bar):
        diag_base["reason"] = "OPENING_CANDLE_NOT_RED"
        return diag_base

    if pattern.is_doji(first_bar):
        diag_base["reason"] = "OPENING_CANDLE_DOJI"
        return diag_base

    br = pattern.candle_body_ratio(first_bar)
    bwr = pattern.body_to_wick_ratio(first_bar)
    min_br = getattr(config, "OPENING_MOMENTUM_MIN_BODY_RATIO", getattr(config, "ALPHA_MIN_TREND_BODY_RATIO", 0.40))
    min_bwr = getattr(config, "OPENING_MOMENTUM_MIN_BODY_TO_WICK_RATIO", getattr(config, "ALPHA_MIN_TREND_BODY_TO_WICK_RATIO", 1.0))

    if br < min_br:
        diag_base["reason"] = "OPENING_BODY_RATIO"
        diag_base["metrics"] = {"body_ratio": round(br, 3), "required": min_br}
        return diag_base

    if bwr < min_bwr:
        diag_base["reason"] = "OPENING_BODY_TO_WICK"
        diag_base["metrics"] = {"body_to_wick_ratio": round(bwr, 3), "required": min_bwr}
        return diag_base

    diag_base["status"] = "WAITING"
    diag_base["reason"] = "WAITING_FOR_1M_TRIGGER"
    diag_base["metrics"] = {
        "trigger_price": float(first_bar["high"]) if is_bullish_setup else float(first_bar["low"]),
        "stop_price": float(first_bar["low"]) if is_bullish_setup else float(first_bar["high"]),
        "body_ratio": round(br, 3),
        "body_to_wick_ratio": round(bwr, 3),
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

