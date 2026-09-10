"""
AlphaCandle - Main Runner (isolated, self-contained project).

Run with:
    python main.py

Wires together universe.py, discovery.py, pattern.py, engine.py, broker.py,
notifier.py, state.py, timing.py - all inside this single project folder.
No dependency on any file outside this folder.
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from logging import getLogger
from logger import setup_logging
setup_logging()
logger = getLogger(__name__)

import pandas as pd

import config
import state
import timing
import universe
import discovery
import pattern
import jp_pattern
import signal_diagnostics
import engine
import notifier
from broker import DhanBroker

_stop_event = threading.Event()
_cache_lock = threading.Lock()
_candle_cache = {}
_pattern_candle_cache = {}
_jp_pattern_candle_cache = {}
_processed_alpha_1m_candles = set()
_processed_alpha_lock = threading.Lock()


def validate_runtime_config():
    missing = [name for name in config.STARTUP_REQUIRED_CONFIG if not hasattr(config, name)]
    if missing:
        raise RuntimeError(f"Missing required config attributes: {', '.join(missing)}")
    if config.ENTRY_TIMEFRAME != 1 or config.SOURCE_CANDLE_TIMEFRAME != 1:
        raise RuntimeError("ENTRY_TIMEFRAME and SOURCE_CANDLE_TIMEFRAME must both be 1")
    if config.ALPHA_TIMEFRAME != 3 or config.JP_TIMEFRAME != 3 or config.PATTERN_TIMEFRAME != 3:
        raise RuntimeError("Alpha, JP, and pattern timeframes must all be 3")


def get_cached_candles(broker, security_id, prev_trade_date, timeframe, ttl_seconds):
    key = f"{security_id}_{timeframe}"
    now = time.time()
    with _cache_lock:
        cached = _candle_cache.get(key)
        if cached and now - cached["fetched_at"] < ttl_seconds:
            return cached["data"]

    candles = broker.get_intraday_candles(
        security_id=security_id,
        exchange_segment=config.EXCHANGE,
        instrument_type="EQUITY",
        from_dt=prev_trade_date,
        timeframe=timeframe,
    )
    if candles is not None and not candles.empty:
        with _cache_lock:
            _candle_cache[key] = {"fetched_at": now, "data": candles}
    return candles


def get_cached_pattern_candles(broker, security_id, prev_trade_date, timeframe):
    key = f"{security_id}_{timeframe}"
    now = time.time()
    with _cache_lock:
        cached = _pattern_candle_cache.get(key)
        if cached and now - cached["fetched_at"] < config.CANDLE_CACHE_TTL_PATTERN_SEC:
            return cached["data"]
    candles = broker.get_pattern_candles(
        security_id=security_id, exchange_segment=config.EXCHANGE,
        instrument_type="EQUITY", from_dt=prev_trade_date,
        pattern_timeframe=timeframe,
    )
    if candles is not None and not candles.empty:
        with _cache_lock:
            _pattern_candle_cache[key] = {"fetched_at": now, "data": candles}
    return candles


def get_cached_jp_pattern_candles_with_warmup(broker, security_id, prev_trade_date):
    if not getattr(config, "JP_USE_PREVIOUS_SESSION_WARMUP", True):
        return get_cached_pattern_candles(broker, security_id, prev_trade_date, config.JP_TIMEFRAME)

    key = f"JP_WARMUP_{security_id}"
    now = time.time()
    with _cache_lock:
        cached = _jp_pattern_candle_cache.get(key)
        if cached and now - cached["fetched_at"] < config.CANDLE_CACHE_TTL_PATTERN_SEC:
            return cached["data"]

    candles = broker.get_pattern_candles_with_warmup(
        security_id=security_id,
        exchange_segment=config.EXCHANGE,
        instrument_type="EQUITY",
        prev_trade_date=prev_trade_date,
        pattern_timeframe=config.JP_TIMEFRAME,
    )
    if candles is not None and not candles.empty:
        with _cache_lock:
            _jp_pattern_candle_cache[key] = {"fetched_at": now, "data": candles}
    return candles


def check_alpha_entry(broker, security_id, symbol, is_bullish_setup,
                      watchlist_entry, prev_trade_date):
    alpha_close_time = datetime.fromisoformat(watchlist_entry["alpha_close_time"])
    hold_expires_at = datetime.fromisoformat(watchlist_entry["hold_expires_at"])
    now = datetime.now(config.TIME_ZONE)
    completed_cutoff = now.replace(second=0, microsecond=0)
    candles_1m = get_cached_candles(
        broker=broker,
        security_id=security_id,
        prev_trade_date=prev_trade_date,
        timeframe=config.ENTRY_TIMEFRAME,
        ttl_seconds=config.ALPHA_1M_CACHE_TTL_SEC,
    )
    if candles_1m is None or candles_1m.empty:
        return None

    eligible = candles_1m[
        (candles_1m["timestamp"] >= alpha_close_time)
        & (candles_1m["timestamp"] < completed_cutoff)
        & (candles_1m["timestamp"] <= hold_expires_at)
    ].copy()
    if eligible.empty:
        return None

    alpha_key = watchlist_entry["alpha_key"]
    for _, candle in eligible.iterrows():
        candle_time = candle["timestamp"].isoformat()
        processed_key = f"{alpha_key}_{candle_time}"
        with _processed_alpha_lock:
            if processed_key in _processed_alpha_1m_candles:
                continue
            _processed_alpha_1m_candles.add(processed_key)

        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        if is_bullish_setup:
            triggered = high > float(watchlist_entry["alpha_high"])
            level = float(watchlist_entry["alpha_high"])
            trigger_text = "high"
        else:
            triggered = low < float(watchlist_entry["alpha_low"])
            level = float(watchlist_entry["alpha_low"])
            trigger_text = "low"

        state.add_log(
            f"{symbol}: Alpha 1-min checked time={candle_time}, high={high:.2f}, "
            f"low={low:.2f}, close={close:.2f}, required_{trigger_text}={level:.2f}, "
            f"triggered={triggered}"
        )
        if triggered:
            state.add_log(
                f"{symbol}: ALPHA WICK ENTRY CONFIRMED "
                f"direction={'BUY' if is_bullish_setup else 'SELL'}, "
                f"alpha_time={watchlist_entry['alpha_open_time']}, "
                f"entry_1m_time={candle_time}, high={high:.2f}, low={low:.2f}, close={close:.2f}"
            )
            return candle
    return None


def process_new_1m_bar_for_setup(broker, setup, latest_completed_1m_bar):
    """
    Process exactly one completed 1-minute bar for exactly one matching setup.
    Never use one symbol's candle to trigger another symbol's setup.
    """
    if not setup or latest_completed_1m_bar is None:
        return None

    security_id = str(setup.get("security_id"))
    if not security_id:
        return None

    bar_time = latest_completed_1m_bar.get("timestamp")
    if bar_time is None:
        return None

    if isinstance(bar_time, str):
        try:
            bar_time = datetime.fromisoformat(bar_time)
        except ValueError:
            return None

    bar_time_iso = bar_time.isoformat()
    if setup.get("last_processed_1m_time") == bar_time_iso:
        return None

    if security_id in state.snapshot().get("open_positions", {}):
        return None

    strategy = (setup.get("strategy") or "ALPHA").upper()
    symbol = setup.get("symbol", security_id)
    direction = (setup.get("direction") or "").upper()
    if direction not in ("BUY", "SELL"):
        return None

    pattern_close_time = setup.get("pattern_close_time") or setup.get("jp_close_time") or setup.get("alpha_close_time")
    bar_was_naive = bar_time.tzinfo is None
    if bar_was_naive:
        bar_time = bar_time.replace(tzinfo=config.TIME_ZONE)
    if pattern_close_time:
        pattern_close = datetime.fromisoformat(pattern_close_time)
        if pattern_close.tzinfo is None or bar_was_naive:
            pattern_close = pattern_close.replace(tzinfo=config.TIME_ZONE)
        if bar_time < pattern_close:
            return None

    expires_at = setup.get("expires_at")
    if expires_at:
        exp_dt = datetime.fromisoformat(expires_at)
        if exp_dt.tzinfo is None:
            exp_dt = exp_dt.replace(tzinfo=config.TIME_ZONE)
        if bar_time > exp_dt:
            strategy_name = strategy
            state.log_setup_outcome(setup, "SETUP_EXPIRED", f"expired_at={expires_at}")
            if strategy_name == "JP":
                state.remove_jp_setup(security_id)
            else:
                state.remove_alpha_setup(security_id)
            return "SETUP_EXPIRED"

    high = float(latest_completed_1m_bar.get("high", 0.0))
    low = float(latest_completed_1m_bar.get("low", 0.0))
    close = float(latest_completed_1m_bar.get("close", 0.0))

    trigger_level = float(setup.get("trigger_price") or setup.get(
        "pattern_high" if direction == "BUY" else "pattern_low"
    ) or setup.get("alpha_high" if direction == "BUY" else "alpha_low") or setup.get(
        "jp_high" if direction == "BUY" else "jp_low"
    ) or 0.0)
    if trigger_level <= 0:
        return None

    if strategy == "JP":
        crossed = (high > trigger_level and close > trigger_level) if direction == "BUY" else (low < trigger_level and close < trigger_level)
    else:
        crossed = high > trigger_level if direction == "BUY" else low < trigger_level
    setup["last_processed_1m_time"] = bar_time_iso
    if strategy == "JP":
        state.set_jp_watchlist_item(security_id, setup)
    else:
        state.set_alpha_watchlist_item(security_id, setup)

    if strategy == "JP" and config.JP_DETECTION_ONLY:
        state.log_setup_outcome(setup, "JP_TRIGGER_DETECTED_ONLY", "")
        state.remove_jp_setup(security_id)
        return "JP_TRIGGER_DETECTED_ONLY"

    if strategy == "OPENING_MOMENTUM" and getattr(config, "OPENING_MOMENTUM_DETECTION_ONLY", True):
        if crossed:
            state.log_setup_outcome(setup, "OPENING_MOMENTUM_TRIGGER_DETECTED_ONLY", f"trigger={trigger_level:.2f}, 1m_high={high:.2f}, 1m_close={close:.2f}")
            state.remove_alpha_setup(security_id)
            state.add_log(f"{symbol}: OPENING_MOMENTUM trigger detected only at {bar_time_iso} (no order placed)")
            return "OPENING_MOMENTUM_TRIGGER_DETECTED_ONLY"
        return None

    if not crossed:
        return None

    # Check countertrend 1m close requirement if enabled
    regime_mode = setup.get("regime_mode")
    if regime_mode == "COUNTERTREND" and getattr(config, "REGIME_COUNTERTREND_REQUIRE_1M_CLOSE_CONFIRMATION", True):
        close_confirmed = (close > trigger_level) if direction == "BUY" else (close < trigger_level)
        if not close_confirmed:
            state.log_setup_outcome(setup, "COUNTERTREND_1M_CLOSE_NOT_CONFIRMED", f"close={close:.2f}, trigger={trigger_level:.2f}")
            state.add_log(f"{symbol}: Entry rejected - countertrend 1m close {close:.2f} did not confirm beyond trigger {trigger_level:.2f}")
            if strategy == "JP":
                state.remove_jp_setup(security_id)
            else:
                state.remove_alpha_setup(security_id)
            return "COUNTERTREND_1M_CLOSE_NOT_CONFIRMED"

    now = datetime.now(config.TIME_ZONE)
    bar_close_at = bar_time + timedelta(minutes=1)
    delay_seconds = (now - bar_close_at).total_seconds()

    if delay_seconds > config.MAX_ENTRY_DELAY_SECONDS:
        state.log_setup_outcome(setup, "SKIPPED_LATE_TRIGGER", f"delay={delay_seconds:.1f}s")
        if strategy == "JP":
            state.remove_jp_setup(security_id)
        else:
            state.remove_alpha_setup(security_id)
        return "SKIPPED_LATE_TRIGGER"

    ltp = broker.get_ltp(security_id, config.EXCHANGE)
    if ltp is None:
        state.log_setup_outcome(setup, "SKIPPED_NO_LTP", "")
        if strategy == "JP":
            state.remove_jp_setup(security_id)
        else:
            state.remove_alpha_setup(security_id)
        return "SKIPPED_NO_LTP"

    fill = max(float(ltp), close) if direction == "BUY" else min(float(ltp), close)

    # Check entry extension percentage (prevent chasing price when it has moved too far past trigger)
    ext_pct = pattern.entry_extension_pct(fill, trigger_level, side=direction)
    max_ext_pct = getattr(config, "ALPHA_MAX_ENTRY_EXTENSION_PCT", 0.35)
    if ext_pct > max_ext_pct:
        state.log_setup_outcome(setup, "ENTRY_REJECTED_EXTENSION", f"ext={ext_pct:.2f}% > max={max_ext_pct:.2f}%")
        state.add_log(f"{symbol}: Entry rejected - extension {ext_pct:.2f}% exceeds {max_ext_pct:.2f}%")
        if strategy == "JP":
            state.remove_jp_setup(security_id)
        else:
            state.remove_alpha_setup(security_id)
        return "ENTRY_REJECTED_EXTENSION"

    # For countertrend setups, verify confirmation volume meets minimum threshold
    signal_quality = setup.get("signal_quality", {})
    conf_vol = signal_quality.get("confirmation_volume_ratio", 1.0)
    if regime_mode == "COUNTERTREND":
        min_counter_vol = getattr(config, "REGIME_COUNTERTREND_MIN_CONFIRMATION_VOLUME_RATIO", 1.50)
        if conf_vol < min_counter_vol:
            state.log_setup_outcome(setup, "COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW", f"conf_vol={conf_vol:.2f} < min={min_counter_vol:.2f}")
            state.add_log(f"{symbol}: Entry rejected - countertrend confirmation volume {conf_vol:.2f}x below {min_counter_vol:.2f}x")
            if strategy == "JP":
                state.remove_jp_setup(security_id)
            else:
                state.remove_alpha_setup(security_id)
            return "COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW"

    alpha_high = float(setup.get("alpha_high") if setup.get("alpha_high") is not None else setup.get("jp_high") or trigger_level)
    alpha_low = float(setup.get("alpha_low") if setup.get("alpha_low") is not None else setup.get("jp_low") or trigger_level)
    alpha_open_time = setup.get("alpha_open_time") or setup.get("jp_open_time")
    alpha_close_time = setup.get("alpha_close_time") or setup.get("jp_close_time")
    alpha_key = setup.get("alpha_key") or setup.get("jp_key")
    pattern_confirmation_time = setup.get("pattern_confirmation_time")

    position = engine.enter_trade(
        broker=broker,
        security_id=int(security_id),
        symbol=symbol,
        is_bullish_setup=(direction == "BUY"),
        strategy=strategy,
        alpha_high=alpha_high,
        alpha_low=alpha_low,
        entry_candle=type("_EntryBar", (), {
            "open": float(latest_completed_1m_bar.get("open", fill)),
            "high": high,
            "low": low,
            "close": fill,
            "timestamp": bar_time,
        })(),
        alpha_open_time=alpha_open_time,
        alpha_close_time=alpha_close_time,
        alpha_key=alpha_key,
        signal_quality=signal_quality,
        pattern_confirmation_time=pattern_confirmation_time,
        regime_mode=regime_mode,
        regime_reason=setup.get("regime_reason"),
    )

    if position is not None:
        state.log_setup_outcome(setup, "TRADE_ENTERED", f"fill={fill:.2f}")
        state.remove_alpha_setup(security_id)
        state.remove_jp_setup(security_id)
        return "TRADE_ENTERED"

    state.log_setup_outcome(setup, "ENTRY_REJECTED", "")
    return "ENTRY_REJECTED"


def run_flag():
    return timing.is_within_run_window() and not _stop_event.is_set()


def wait_until(hms_tuple):
    now = datetime.now(config.TIME_ZONE)
    target = now.replace(hour=hms_tuple[0], minute=hms_tuple[1], second=hms_tuple[2], microsecond=0)
    wait_s = max(0, (target - now).total_seconds())
    if wait_s > 0:
        logger.info(f"Waiting {wait_s:.0f}s until {hms_tuple}")
        time.sleep(wait_s)


def get_prev_trading_day(broker):
    try:
        to_date = datetime.now(config.TIME_ZONE).date()
        from_date = to_date - timedelta(days=7)
        hist = broker.get_historical_daily_candles(
            security_id=config.INDEX_SECURITY_ID, exchange_segment="IDX_I",
            instrument_type="INDEX", from_dt=from_date, to_dt=to_date,
        )
        if hist is not None and len(hist) > 1:
            hist["timestamp"] = hist["timestamp"].dt.date
            hist = hist.sort_values("timestamp", ascending=False)
            return hist.iloc[1]["timestamp"]
    except Exception:
        logger.exception("Failed to fetch previous trading day")
    return datetime.now(config.TIME_ZONE).date() - timedelta(days=1)


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


def update_market_regime(broker):
    try:
        net_change = broker.get_net_change("IDX_I", config.INDEX_SECURITY_ID)
        ltp = broker.get_ltp_from_api("IDX_I", config.INDEX_SECURITY_ID)
        pct = None
        if ltp and net_change is not None:
            prev = ltp - net_change
            pct = round((net_change / prev) * 100, 3) if prev else None
        regime = "BULLISH" if (net_change is not None and net_change > 0) else ("BEARISH" if net_change is not None else "UNKNOWN")
        state.update({"nifty_ltp": ltp, "nifty_pct_change": pct, "market_regime": regime})
    except Exception:
        logger.exception("Failed to update market regime")


def regime_loop(broker):
    while run_flag():
        update_market_regime(broker)
        time.sleep(10)


def heartbeat_loop():
    if config.SEND_TELEGRAM_HEARTBEAT_MIN <= 0:
        return
    while run_flag():
        time.sleep(config.SEND_TELEGRAM_HEARTBEAT_MIN * 60)
        snap = state.snapshot()
        notifier.notify_heartbeat(len(snap["open_positions"]), snap["daily_pnl"], snap["daily_trade_count"])


_last_scan_status_log_at = 0.0


def build_candidate_universe():
    """
    Builds a deduplicated candidate universe:
    - Top 20 Gainers (Source: TOP_GAINER, direction: BUY)
    - Top 20 Losers (Source: TOP_LOSER, direction: SELL)
    - Top 5 Volume Leaders (Source: TOP_VOLUME, direction based on pct_change >= 0)
    Returns list of deduplicated candidate dicts with priority tiers.
    """
    snap = state.snapshot()
    candidates = []

    gainers = snap.get("top_gainers", [])[:config.TOP_N_GAINERS]
    for row in gainers:
        candidates.append({
            **row,
            "candidate_source": "TOP_GAINER",
            "is_bullish_setup": True,
            "priority_tier": 1 if row.get("rank", 99) <= 10 else 2,
        })

    losers = snap.get("top_losers", [])[:config.TOP_N_LOSERS]
    for row in losers:
        candidates.append({
            **row,
            "candidate_source": "TOP_LOSER",
            "is_bullish_setup": False,
            "priority_tier": 1 if row.get("rank", 99) <= 10 else 2,
        })

    if getattr(config, "INCLUDE_VOLUME_LEADER_POOL", True):
        for row in snap.get("top_volume_leaders", [])[:getattr(config, "TOP_N_VOLUME_LEADERS", 5)]:
            pct = float(row.get("pct_change", 0.0))
            candidates.append({
                **row,
                "candidate_source": "TOP_VOLUME",
                "is_bullish_setup": (pct >= 0),
                "priority_tier": 2,
            })

    # Deduplicate by security ID, keeping higher priority if present in both
    deduped = {}
    for cand in candidates:
        sid = str(cand["SECURITY_ID"])
        if sid not in deduped:
            deduped[sid] = cand
        elif cand.get("priority_tier", 2) < deduped[sid].get("priority_tier", 2):
            deduped[sid] = cand

    return list(deduped.values())


def build_scan_candidates():
    return build_candidate_universe()


def build_jp_scan_candidates():
    return build_candidate_universe()


def scan_jp_candidate(broker, candidate, prev_trade_date):
    if not config.JP_ENABLED:
        return

    security_id = int(candidate["SECURITY_ID"])
    symbol = candidate["display_name"]
    is_bullish_setup = candidate["is_bullish_setup"]
    direction = "BUY" if is_bullish_setup else "SELL"
    regime = state.snapshot().get("market_regime")
    pct_change = candidate.get("pct_change", 0.0)

    regime_mode = "ALIGNED"
    regime_reason = "REGIME_ALIGNED"
    if getattr(config, "JP_USE_MARKET_REGIME_FILTER", True):
        allowed, regime_mode, regime_reason = market_regime_allows_setup(
            direction=direction,
            market_regime=regime,
            pct_change=pct_change,
        )
        if not allowed:
            return
    elif config.JP_REQUIRE_MARKET_REGIME and ((is_bullish_setup and regime != "BULLISH") or (not is_bullish_setup and regime != "BEARISH")):
        return

    if state.jp_signal_count(security_id) >= config.JP_MAX_SIGNALS_PER_SYMBOL_PER_DAY:
        return
    # Check collision protection: do not overwrite active Alpha setup with JP
    if state.has_active_setup_for_security(security_id):
        active_alpha = state.snapshot().get("alpha_watchlist", {}).get(str(security_id)) or state.snapshot().get("watchlist", {}).get(str(security_id))
        if active_alpha and active_alpha.get("stage") in ("WAITING_3M_CONFIRMATION", "WAITING_SECOND_3M_CONFIRMATION", "AWAITING_BREAKOUT", "AWAITING_1M_TRIGGER"):
            return

    try:
        candles_3m = get_cached_jp_pattern_candles_with_warmup(broker, security_id, prev_trade_date)
    except Exception:
        logger.exception(f"JP pattern candle fetch failed for {symbol}")
        return
    if candles_3m is None or candles_3m.empty:
        return

    today = datetime.now(config.TIME_ZONE).date()
    result = jp_pattern.find_jp_setup(candles_3m, is_bullish_setup, session_date=today)
    if result is None:
        return

    # If it's only an observation, log diagnostic but do NOT store in tradeable watchlist and do NOT send Telegram alert
    if result.get("status") == "OBSERVATION" or not result.get("is_candidate", True):
        state.add_log(f"{symbol}: JP Observation ({result['direction']} - {result.get('band_interaction')}) Q={result.get('quality_score')} Notes={result.get('quality_warnings')}")
        return

    # If countertrend, verify stricter confirmation volume threshold
    conf_vol_ratio = result.get("confirmation_volume_ratio", 1.0)
    if regime_mode == "COUNTERTREND":
        min_counter_vol = getattr(config, "REGIME_COUNTERTREND_MIN_CONFIRMATION_VOLUME_RATIO", 1.25)
        if conf_vol_ratio < min_counter_vol:
            return

    now = datetime.now(config.TIME_ZONE)
    age_minutes = (now - result["jp_close_time"]).total_seconds() / 60.0
    if result["jp_close_time"] > now or age_minutes > config.JP_MAX_ENTRY_MINUTES:
        return

    key = f"JP_{security_id}_{result['direction']}_{result['jp_open_time'].isoformat()}"
    if state.has_alerted_jp(key):
        return

    stop_pct = jp_pattern.jp_stop_distance_pct(result["trigger_price"], result["stop_price"])
    if stop_pct > config.JP_MAX_STOP_DISTANCE_PCT:
        state.add_log(
            f"{symbol}: JP rejected; stop distance {stop_pct:.2f}% exceeds "
            f"{config.JP_MAX_STOP_DISTANCE_PCT:.2f}%"
        )
        state.mark_jp_alerted(key)
        return

    item = {
        "strategy": "JP", "symbol": symbol, "security_id": str(security_id),
        "direction": result["direction"], "stage": "AWAITING_1M_TRIGGER",
        "pattern_timeframe": config.JP_TIMEFRAME,
        "pattern_open_time": result["jp_open_time"].isoformat(),
        "pattern_close_time": result["jp_close_time"].isoformat(),
        "pattern_high": result["pattern_high"], "pattern_low": result["pattern_low"],
        "jp_high": result["trigger_price"] if result["direction"] == "BUY" else result["pattern_high"],
        "jp_low": result["trigger_price"] if result["direction"] == "SELL" else result["pattern_low"],
        "jp_open_time": result["jp_open_time"].isoformat(),
        "jp_close_time": result["jp_close_time"].isoformat(),
        "trigger_price": result["trigger_price"], "stop_price": result["stop_price"],
        "smma_band_low": result["band_low"], "smma_band_high": result["band_high"],
        "stop_distance_pct": round(stop_pct, 3), "detected_at": now.isoformat(),
        "expires_at": (result["jp_close_time"] + timedelta(minutes=config.JP_MAX_ENTRY_MINUTES)).isoformat(),
        "jp_key": key,
        "candidate_source": candidate.get("candidate_source", "MOVER"),
        "band_interaction": result.get("band_interaction", "BAND_TOUCH"),
        "quality_score": result.get("quality_score", 100),
        "quality_warnings": result.get("quality_warnings", []),
        "regime_mode": regime_mode,
        "regime_reason": regime_reason,
        "signal_quality": {
            "compression_valid": result.get("compression_valid", False),
            "prior_band_touch_count": result.get("prior_band_touch_count", 0),
            "confirmation_volume_ratio": result.get("confirmation_volume_ratio", 1.0),
            "pullback_volume_ratio": result.get("volume_ratio", 1.0),
            "body_ratio": round(result.get("body_ratio", 0.0), 3),
        },
    }
    state.set_jp_watchlist_item(security_id, item)
    state.mark_jp_alerted(key)
    state.add_log(
        f"{symbol}: JP {result['direction']} candidate detected at {result['jp_open_time'].isoformat()} "
        f"trigger={result['trigger_price']:.2f} SL={result['stop_price']:.2f} [{regime_mode}] Q={item['quality_score']}"
    )
    state.increment_jp_signal_count(security_id)
    if config.SEND_TELEGRAM_ON_JP_SETUP:
        notifier.notify_jp_candle_detected(
            symbol=symbol, direction=result["direction"],
            jp_time=result["jp_open_time"].isoformat(),
            trigger_price=result["trigger_price"], stop_price=result["stop_price"],
            band_low=result["band_low"], band_high=result["band_high"],
            band_interaction=result.get("band_interaction", "TOUCH"),
            quality_score=result.get("quality_score"),
            warnings=result.get("quality_warnings"),
        )


def scan_opening_momentum_candidate(broker, candidate, prev_trade_date):
    if not getattr(config, "OPENING_MOMENTUM_ENABLED", True):
        return

    now_time = datetime.now(config.TIME_ZONE).time()
    start_time = datetime.strptime(getattr(config, "OPENING_MOMENTUM_START_TIME", "09:18"), "%H:%M").time()
    end_time = datetime.strptime(getattr(config, "OPENING_MOMENTUM_END_TIME", "09:45"), "%H:%M").time()
    if not (start_time <= now_time <= end_time):
        return

    security_id = int(candidate["SECURITY_ID"])
    symbol = candidate["display_name"]
    is_bullish_setup = candidate["is_bullish_setup"]
    direction = "BUY" if is_bullish_setup else "SELL"
    regime = state.snapshot().get("market_regime")
    pct_change = candidate.get("pct_change", 0.0)

    regime_mode = "ALIGNED"
    regime_reason = "REGIME_ALIGNED"
    if getattr(config, "ALPHA_USE_MARKET_REGIME_FILTER", True):
        allowed, regime_mode, regime_reason = market_regime_allows_setup(
            direction=direction,
            market_regime=regime,
            pct_change=pct_change,
        )
        if not allowed:
            return

    snap = state.snapshot()
    if str(security_id) in snap.get("open_positions", {}) or str(security_id) in snap.get("watchlist", {}):
        return

    try:
        pattern_candles = get_cached_pattern_candles(broker, security_id, prev_trade_date, config.PATTERN_TIMEFRAME)
    except Exception:
        return

    if pattern_candles is None or pattern_candles.empty:
        return

    today = datetime.now(config.TIME_ZONE).date()
    res = pattern.find_opening_momentum_setup(pattern_candles, is_bullish_setup=is_bullish_setup, session_date=today)
    if res is None:
        return

    key = f"OPENING_MOMENTUM_{security_id}_{direction}_{res['pattern_open_time'].isoformat()}"
    if state.has_alerted(key):
        return

    now = datetime.now(config.TIME_ZONE)
    item = {
        "strategy": "OPENING_MOMENTUM",
        "symbol": symbol,
        "security_id": str(security_id),
        "direction": direction,
        "stage": "AWAITING_1M_TRIGGER",
        "pattern_timeframe": config.PATTERN_TIMEFRAME,
        "pattern_open_time": res["pattern_open_time"].isoformat(),
        "pattern_close_time": res["pattern_close_time"].isoformat(),
        "pattern_high": res["pattern_high"],
        "pattern_low": res["pattern_low"],
        "trigger_price": res["trigger_price"],
        "stop_price": res["stop_price"],
        "detected_at": now.isoformat(),
        "expires_at": (res["pattern_close_time"] + timedelta(minutes=int(getattr(config, "OPENING_MOMENTUM_HOLD_MINUTES", 15)))).isoformat(),
        "alpha_key": key,
        "regime_mode": regime_mode,
        "regime_reason": regime_reason,
        "signal_quality": {
            "body_ratio": res.get("body_ratio", 0.0),
            "body_to_wick_ratio": res.get("body_to_wick_ratio", 0.0),
        },
    }
    state.set_alpha_watchlist_item(security_id, item)
    state.mark_alerted(key)
    state.add_log(f"{symbol}: OPENING_MOMENTUM {direction} setup detected at {res['pattern_open_time'].isoformat()} trigger={res['trigger_price']:.2f}")


def scan_candidate(broker, candidate, prev_trade_date):
    security_id = int(candidate["SECURITY_ID"])
    symbol = candidate["display_name"]
    is_bullish_setup = candidate["is_bullish_setup"]
    direction = "BUY" if is_bullish_setup else "SELL"
    regime = state.snapshot().get("market_regime")
    pct_change = candidate.get("pct_change", 0.0)

    regime_mode = "ALIGNED"
    regime_reason = "REGIME_ALIGNED"
    if getattr(config, "ALPHA_USE_MARKET_REGIME_FILTER", True):
        allowed, regime_mode, regime_reason = market_regime_allows_setup(
            direction=direction,
            market_regime=regime,
            pct_change=pct_change,
        )
        if not allowed:
            return
    elif config.ALPHA_REQUIRE_MARKET_REGIME:
        if (is_bullish_setup and regime == "BEARISH") or (not is_bullish_setup and regime == "BULLISH"):
            return

    if str(security_id) in state.snapshot()["open_positions"]:
        return

    watchlist_entry = state.snapshot()["watchlist"].get(str(security_id))

    if state.is_blacklisted(security_id):
        signal_vol = watchlist_entry.get("alpha_volume") if watchlist_entry else 0
        if not engine.can_requalify_blacklisted(security_id, signal_vol or 0):
            return

    try:
        pattern_candles = get_cached_pattern_candles(
            broker, security_id, prev_trade_date, config.PATTERN_TIMEFRAME
        )
        if pattern_candles is None or pattern_candles.empty:
            return
        today_pattern = pattern_candles[pattern_candles["timestamp"].dt.date == datetime.now(config.TIME_ZONE).date()]
    except Exception:
        logger.exception(f"Pattern-timeframe candle fetch failed for {symbol}")
        return

    # If setup is already confirmed and awaiting 1m breakout, let process_new_1m_bar_for_setup handle it
    if watchlist_entry is not None and watchlist_entry.get("stage") == "AWAITING_BREAKOUT":
        return

    # If setup is waiting for 3m confirmation, evaluate newly completed 3m bars against the stored Alpha candle
    if watchlist_entry is not None and watchlist_entry.get("stage") == "WAITING_3M_CONFIRMATION":
        eval_res = pattern.evaluate_alpha_confirmation(today_pattern, watchlist_entry)
        if eval_res.get("status") == "CONFIRMED":
            watchlist_entry["stage"] = "AWAITING_BREAKOUT"
            watchlist_entry["pattern_confirmation_status"] = "CONFIRMED"
            watchlist_entry["pattern_confirmation_time"] = eval_res.get("pattern_confirmation_time")
            conf_vol = eval_res.get("confirmation_volume_ratio", 1.0)
            watchlist_entry.setdefault("signal_quality", {})["confirmation_volume_ratio"] = conf_vol
            state.set_watchlist_item(security_id, watchlist_entry)
            dedup_key = watchlist_entry.get("alpha_key", str(security_id)) + "_CONFIRMED"
            if not state.has_alerted(dedup_key):
                if config.SEND_TELEGRAM_ON_SETUP_WATCH:
                    notifier.notify_alpha_candle_confirmed(
                        symbol=symbol,
                        direction=direction,
                        alpha_time=watchlist_entry.get("alpha_candle_time", ""),
                        alpha_high=float(watchlist_entry.get("alpha_high", 0.0)),
                        alpha_low=float(watchlist_entry.get("alpha_low", 0.0)),
                        timeframe=config.PATTERN_TIMEFRAME,
                        quality_score=watchlist_entry.get("quality_score"),
                        warnings=watchlist_entry.get("quality_warnings"),
                    )
                state.mark_alerted(dedup_key)
                state.add_log(f"{symbol}: Alpha Candle confirmed & ready ({direction}) [{regime_mode}] Q={watchlist_entry.get('quality_score')}")
            return
        elif eval_res.get("status") == "EXPIRED":
            state.remove_watchlist_item(security_id)
            state.log_setup_outcome(watchlist_entry, "EXPIRED_NO_3M_CONFIRMATION", "")
            return
        # Still waiting for confirmation within deadline; do not overwrite with fresh run yet
        return

    result = pattern.find_trend_run_and_alpha(today_pattern, is_bullish_setup)
    if result is None:
        return

    alpha = result["alpha_candle"]
    alpha_range_pct = ((float(alpha.high) - float(alpha.low)) / float(alpha.close)) * 100
    if alpha_range_pct > config.MAX_ALPHA_RANGE_PCT:
        state.add_log(
            f"{symbol}: Alpha rejected - range {alpha_range_pct:.2f}% "
            f"exceeds {config.MAX_ALPHA_RANGE_PCT:.2f}%"
        )
        return
    direction = result.get("direction") or ("BUY" if is_bullish_setup else "SELL")
    alpha_open_time = result["alpha_open_time"]
    alpha_close_time = result["alpha_close_time"]
    trend_volumes = [float(c.volume) for c in result["trend_run"] if float(c.volume) > 0]
    trend_volume_ratio = float(alpha.volume) / (sum(trend_volumes) / len(trend_volumes)) if trend_volumes else 1.0
    now = datetime.now(config.TIME_ZONE)
    alpha_age_minutes = (now - alpha_close_time).total_seconds() / 60.0
    alpha_key = f"{security_id}_{direction}_{alpha_open_time.isoformat()}"

    if alpha_close_time > now:
        state.add_log(f"{symbol}: Alpha ignored - pattern candle has not closed yet ({alpha_open_time.isoformat()})")
        return
    if alpha_age_minutes > config.ALPHA_MAX_ENTRY_MINUTES:
        state.add_log(f"{symbol}: stale Alpha ignored - {alpha_age_minutes:.1f} min old, candle={alpha_open_time.isoformat()}")
        return
    if state.is_expired_alpha(alpha_key):
        state.add_log(f"{symbol}: expired Alpha ignored - key={alpha_key}")
        return

    stage = "AWAITING_BREAKOUT" if result.get("pattern_confirmation_status") == "CONFIRMED" else "WAITING_3M_CONFIRMATION"
    dedup_key = alpha_key
    conf_time = result.get("pattern_confirmation_time")
    conf_time_str = conf_time.isoformat() if conf_time is not None and hasattr(conf_time, "isoformat") else str(conf_time) if conf_time else None

    item = {
        "strategy": "ALPHA",
        "symbol": symbol,
        "security_id": str(security_id),
        "is_bullish_setup": is_bullish_setup,
        "direction": direction,
        "stage": stage,
        "is_candidate": result.get("is_candidate", True),
        "alpha_high": float(alpha.high),
        "alpha_low": float(alpha.low),
        "trigger_price": float(alpha.high if is_bullish_setup else alpha.low),
        "pattern_high": float(alpha.high),
        "pattern_low": float(alpha.low),
        "alpha_volume": float(alpha.volume),
        "alpha_open_time": alpha_open_time.isoformat(),
        "alpha_close_time": alpha_close_time.isoformat(),
        "pattern_open_time": alpha_open_time.isoformat(),
        "pattern_close_time": alpha_close_time.isoformat(),
        "alpha_detected_at": now.isoformat(),
        "alpha_candle_time": str(alpha.timestamp),
        "trend_candle_count": len(result["trend_run"]),
        "current_ltp": None,
        "distance_to_trigger_pct": None,
        "hold_expires_at": (alpha_close_time + timedelta(
            minutes=config.ALPHA_MAX_ENTRY_MINUTES
        )).isoformat(),
        "expires_at": (alpha_close_time + timedelta(minutes=config.ALPHA_MAX_ENTRY_MINUTES)).isoformat(),
        "pattern_confirmation_time": conf_time_str,
        "pattern_confirmation_status": result.get("pattern_confirmation_status"),
        "quality_score": result.get("quality_score", 100),
        "quality_warnings": result.get("quality_warnings", []),
        "candidate_source": candidate.get("candidate_source", "MOVER"),
        "signal_quality": {
            "trend_volume_ratio": round(trend_volume_ratio, 3),
            "pullback_volume_ratio": round(result.get("pullback_volume_ratio", trend_volume_ratio), 3),
            "confirmation_volume_ratio": round(result.get("confirmation_volume_ratio", 1.0), 3),
            "close_position_in_range": round(result.get("close_position_in_range", pattern.close_position(alpha)), 3),
            "body_ratio": round(pattern.candle_body_ratio(alpha), 3),
            "body_to_wick_ratio": round(pattern.body_to_wick_ratio(alpha), 3),
        },
        "alpha_key": alpha_key,
        "regime_mode": regime_mode,
        "regime_reason": regime_reason,
    }
    state.set_watchlist_item(security_id, item)

    # Only promote to Telegram watch if it meets candidate quality thresholds
    if stage == "WAITING_3M_CONFIRMATION" and not state.has_alerted(dedup_key):
        if result.get("is_candidate", True):
            if config.SEND_TELEGRAM_ON_SETUP_WATCH:
                notifier.notify_alpha_candle_detected(
                    symbol=symbol,
                    direction=direction,
                    alpha_time=str(alpha.timestamp),
                    alpha_high=float(alpha.high),
                    alpha_low=float(alpha.low),
                    timeframe=config.PATTERN_TIMEFRAME,
                    quality_score=item.get("quality_score"),
                    warnings=item.get("quality_warnings"),
                )
            state.mark_alerted(dedup_key)
            state.add_log(f"{symbol}: Alpha Candidate detected ({direction}) at {alpha.timestamp} [{regime_mode}] Q={item.get('quality_score')}")
        else:
            state.add_log(f"{symbol}: Alpha Observation ({direction}) at {alpha.timestamp} [{regime_mode}] Q={item.get('quality_score')} Notes={item.get('quality_warnings')}")
    elif stage == "AWAITING_BREAKOUT":
        conf_dedup = dedup_key + "_CONFIRMED"
        if not state.has_alerted(conf_dedup):
            if config.SEND_TELEGRAM_ON_SETUP_WATCH:
                notifier.notify_alpha_candle_confirmed(
                    symbol=symbol,
                    direction=direction,
                    alpha_time=str(alpha.timestamp),
                    alpha_high=float(alpha.high),
                    alpha_low=float(alpha.low),
                    timeframe=config.PATTERN_TIMEFRAME,
                    quality_score=item.get("quality_score"),
                    warnings=item.get("quality_warnings"),
                )
            state.mark_alerted(conf_dedup)
            state.mark_alerted(dedup_key)
            state.add_log(f"{symbol}: Alpha Candle confirmed & ready ({direction}) at {alpha.timestamp} [{regime_mode}] Q={item.get('quality_score')}")
    return

    if "alpha_close_time" not in watchlist_entry or "alpha_key" not in watchlist_entry:
        state.remove_watchlist_item(security_id)
        state.add_log(f"{symbol}: legacy Alpha watch removed; awaiting a fresh setup")
        return

    alpha_close_time = datetime.fromisoformat(watchlist_entry["alpha_close_time"])
    trigger_candle = check_alpha_entry(
        broker=broker, security_id=security_id, symbol=symbol,
        is_bullish_setup=is_bullish_setup,
        watchlist_entry=watchlist_entry, prev_trade_date=prev_trade_date,
    )
    if trigger_candle is None:
        if datetime.now(config.TIME_ZONE) > datetime.fromisoformat(watchlist_entry["hold_expires_at"]):
            state.remove_watchlist_item(security_id)
            expiry_time = datetime.now(config.TIME_ZONE).isoformat()
            state.mark_expired_alpha(watchlist_entry["alpha_key"], expires_at=expiry_time)
            state.add_log(
                f"{symbol}: Alpha expired after checking all completed 1-min candles. "
                f"alpha={watchlist_entry['alpha_open_time']}"
            )
        return

    state.add_log(
        f"{symbol}: Alpha wick-break confirmed. "
        f"Direction={'BUY' if is_bullish_setup else 'SELL'} | "
        f"AlphaHigh={float(watchlist_entry['alpha_high']):.2f} | "
        f"AlphaLow={float(watchlist_entry['alpha_low']):.2f} | "
        f"1mTime={trigger_candle.timestamp} | "
        f"1mOpen={float(trigger_candle.open):.2f} | "
        f"1mHigh={float(trigger_candle.high):.2f} | "
        f"1mLow={float(trigger_candle.low):.2f} | "
        f"1mClose={float(trigger_candle.close):.2f}"
    )

    position = engine.enter_trade(
        broker=broker, security_id=security_id, symbol=symbol,
        is_bullish_setup=is_bullish_setup,
        alpha_high=watchlist_entry["alpha_high"], alpha_low=watchlist_entry["alpha_low"],
        entry_candle=trigger_candle, tick_size=0.05,
        alpha_open_time=watchlist_entry["alpha_open_time"],
        alpha_close_time=watchlist_entry["alpha_close_time"],
        alpha_key=watchlist_entry["alpha_key"],
    )
    if position is not None:
        state.remove_watchlist_item(security_id)
        state.add_log(f"{symbol}: Alpha watch consumed; paper position {position['order_id']} created")


_last_diagnostic_3m_boundary: str = ""


def evaluate_diagnostics_for_top_candidates(broker, prev_trade_date):
    """
    Evaluates signal diagnostics for the top 5 Alpha BUY candidates and top 5 JP BUY/SELL candidates.
    Runs once per completed 3-minute pattern bar boundary.
    """
    global _last_diagnostic_3m_boundary
    now = datetime.now(config.TIME_ZONE)
    # Calculate completed 3-minute boundary (e.g. at 10:24:10, last completed boundary is 10:24:00)
    minute_bucket = (now.minute // 3) * 3
    boundary_dt = now.replace(minute=minute_bucket, second=0, microsecond=0)
    boundary_key = boundary_dt.isoformat()

    if _last_diagnostic_3m_boundary == boundary_key:
        return

    _last_diagnostic_3m_boundary = boundary_key
    snap = state.snapshot()
    regime = snap.get("market_regime", "UNKNOWN")
    gainers = snap.get("top_gainers", [])
    losers = snap.get("top_losers", [])

    # Top 5 Alpha BUY candidates (from top gainers)
    top_alpha = gainers[:5]
    for cand in top_alpha:
        try:
            sid = int(cand["SECURITY_ID"])
            sym = cand.get("display_name", str(sid))
            candles = get_cached_pattern_candles(broker, sid, prev_trade_date, config.PATTERN_TIMEFRAME)
            today_candles = None
            if candles is not None and not candles.empty:
                today_candles = candles[candles["timestamp"].dt.date == now.date()].copy()
            diag = signal_diagnostics.diagnose_alpha_candidate(
                symbol=sym,
                security_id=sid,
                candidate_info=cand,
                today_pattern_candles=today_candles,
                regime=regime,
                now=now,
            )
            signal_diagnostics.record_diagnostic(diag)
        except Exception:
            logger.exception("Error diagnosing Alpha candidate %s", cand.get("display_name"))

    # Top 5 JP BUY candidates (from top gainers)
    top_jp_buy = gainers[:5]
    for cand in top_jp_buy:
        try:
            sid = int(cand["SECURITY_ID"])
            sym = cand.get("display_name", str(sid))
            cand_info = {**cand, "is_bullish_setup": True}
            candles = get_cached_jp_pattern_candles_with_warmup(broker, sid, prev_trade_date)
            diag = signal_diagnostics.diagnose_jp_candidate(
                symbol=sym,
                security_id=sid,
                candidate_info=cand_info,
                today_pattern_candles=candles,
                regime=regime,
                now=now,
                session_date=now.date(),
            )
            signal_diagnostics.record_diagnostic(diag)
        except Exception:
            logger.exception("Error diagnosing JP BUY candidate %s", cand.get("display_name"))

    # Top 5 JP SELL candidates (from top losers)
    top_jp_sell = losers[:5]
    for cand in top_jp_sell:
        try:
            sid = int(cand["SECURITY_ID"])
            sym = cand.get("display_name", str(sid))
            cand_info = {**cand, "is_bullish_setup": False}
            candles = get_cached_jp_pattern_candles_with_warmup(broker, sid, prev_trade_date)
            diag = signal_diagnostics.diagnose_jp_candidate(
                symbol=sym,
                security_id=sid,
                candidate_info=cand_info,
                today_pattern_candles=candles,
                regime=regime,
                now=now,
                session_date=now.date(),
            )
            signal_diagnostics.record_diagnostic(diag)
        except Exception:
            logger.exception("Error diagnosing JP SELL candidate %s", cand.get("display_name"))


def scan_loop(broker, prev_trade_date):
    while run_flag():
        try:
            if timing.is_entry_allowed():
                evaluate_diagnostics_for_top_candidates(broker, prev_trade_date)
                active_setups = state.get_active_alpha_setups() + state.get_active_jp_setups()
                setups_by_security = {}
                for setup in active_setups:
                    if not setup or not setup.get("security_id"):
                        continue
                    sid = str(setup["security_id"])
                    setups_by_security.setdefault(sid, []).append(setup)

                for sid, symbol_setups in setups_by_security.items():
                    try:
                        candles_1m = get_cached_candles(
                            broker=broker,
                            security_id=int(sid),
                            prev_trade_date=prev_trade_date,
                            timeframe=config.ENTRY_TIMEFRAME,
                            ttl_seconds=max(config.CANDLE_CACHE_TTL_1M_SEC, config.ALPHA_1M_CACHE_TTL_SEC),
                        )
                        if candles_1m is None or candles_1m.empty:
                            continue
                        now = datetime.now(config.TIME_ZONE)
                        cutoff = now.replace(second=0, microsecond=0)
                        completed = candles_1m[(candles_1m["timestamp"] < cutoff)].copy()
                        if completed.empty:
                            continue
                        latest = completed.iloc[-1].to_dict()
                        for setup in symbol_setups:
                            process_new_1m_bar_for_setup(broker, setup, latest)
                    except Exception:
                        logger.exception(f"1m event processing failed for {symbol_setups[0].get('symbol', sid)}")

                candidates = build_scan_candidates()
                jp_candidates = build_jp_scan_candidates()

                # Parallel scan across candidates universe (Top 20 Gainers + Top 20 Losers + Top 5 Volume Leaders)
                with ThreadPoolExecutor(max_workers=6) as executor:
                    futures = []
                    for cand in candidates:
                        futures.append(executor.submit(scan_candidate, broker, cand, prev_trade_date))
                        if getattr(config, "OPENING_MOMENTUM_ENABLED", True):
                            futures.append(executor.submit(scan_opening_momentum_candidate, broker, cand, prev_trade_date))
                    for cand in jp_candidates:
                        futures.append(executor.submit(scan_jp_candidate, broker, cand, prev_trade_date))

                    for f in as_completed(futures):
                        try:
                            f.result()
                        except Exception:
                            logger.exception("Error during parallel candidate scan")

            for sid in list(state.snapshot()["open_positions"].keys()):
                if config.PAPER_MODE:
                    ltp = broker.get_ltp_from_api(config.EXCHANGE, sid)
                else:
                    ltp = broker.get_ltp(sid, config.EXCHANGE)
                engine.manage_open_position(broker, sid, ltp)

            if timing.is_past_squareoff():
                engine.square_off_all(broker)
                break
        except Exception:
            logger.exception("Error in scan loop iteration")
        time.sleep(1.0)


def main():
    validate_runtime_config()
    state.reset_daily()
    state.update({"started_at": datetime.now(config.TIME_ZONE).isoformat(), "paper_mode": config.PAPER_MODE})
    state.add_log(f"AlphaCandle starting. PAPER_MODE={config.PAPER_MODE}")

    wait_until(config.START_TIME)

    broker = DhanBroker()
    margin_data = broker.get_fund_limits()
    if margin_data:
        notifier.notify_login(margin_data.get("availabelBalance"))
    else:
        notifier.notify_login_failed()

    universe_df = universe.build_fno_universe()
    state.update({"universe_size": len(universe_df)})

    broker.start_websocket()
    try:
        from dhanhq import MarketFeed
        mode = getattr(MarketFeed, "Quote", 17)
        subscribe_list = [(MarketFeed.NSE, str(r["SECURITY_ID"]), mode) for _, r in universe_df.iterrows()]
    except ImportError:
        from dhanhq.marketfeed import NSE
        subscribe_list = [(NSE, str(r["SECURITY_ID"]), 17) for _, r in universe_df.iterrows()]

    logger.info("Subscribing %d F&O universe symbols on WebSocket (Quote Data mode)...", len(subscribe_list))
    WS_SUBSCRIBE_BATCH_SIZE = 100
    for start in range(0, len(subscribe_list), WS_SUBSCRIBE_BATCH_SIZE):
        batch = subscribe_list[start:start + WS_SUBSCRIBE_BATCH_SIZE]
        broker.subscribe_symbols(batch)
        time.sleep(0.25)

    bootstrap_wait_sec = getattr(config, "WEBSOCKET_BOOTSTRAP_WAIT_SEC", 20)
    min_prev_close_pct = getattr(config, "MIN_PREV_CLOSE_COVERAGE_PCT", 90.0)

    logger.info(
        "Waiting up to %ss for WebSocket Previous Close packets to populate (%s%% target)...",
        bootstrap_wait_sec,
        min_prev_close_pct,
    )
    ready = broker.wait_for_prev_close_coverage(
        expected_count=len(universe_df),
        minimum_pct=min_prev_close_pct,
        timeout_sec=bootstrap_wait_sec,
    )

    if not ready and getattr(config, "ENABLE_QUOTE_BOOTSTRAP", True):
        logger.info("WebSocket previous close coverage incomplete; executing ONE single REST quote bootstrap...")
        snapshot = broker.bootstrap_prior_closes_once(
            universe_df["SECURITY_ID"].astype(int).tolist()
        )
        if snapshot:
            discovery.refresh_from_livefeed_full_universe(
                broker, universe_df, allow_stale_cache=True,
            )
        else:
            logger.warning("Initial quote bootstrap unavailable; retaining WebSocket-only cache")
    else:
        # Prime discovery from WebSocket feed immediately
        discovery.refresh_from_livefeed_full_universe(
            broker, universe_df, allow_stale_cache=True,
        )

    prev_trade_date = get_prev_trading_day(broker)

    wait_until(config.DISCOVERY_WARMUP_END)

    threading.Thread(target=discovery.discovery_loop, args=(broker, universe_df, run_flag), daemon=True).start()
    threading.Thread(target=regime_loop, args=(broker,), daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    state.update({"run_process": True})

    try:
        scan_loop(broker, prev_trade_date)
    except KeyboardInterrupt:
        logger.info("AlphaCandle stopped by user")
    finally:
        engine.square_off_all(broker)
        state.update({"run_process": False})
        broker.close_connection()
        notifier.send_telegram("🔻 AlphaCandle engine stopped")


if __name__ == "__main__":
    main()
