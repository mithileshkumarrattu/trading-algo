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
import engine
import notifier
from broker import DhanBroker

_stop_event = threading.Event()
_candle_cache = {}
_processed_alpha_1m_candles = set()


def get_cached_candles(broker, security_id, prev_trade_date, timeframe, ttl_seconds):
    key = f"{security_id}_{timeframe}"
    now = time.time()
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
        _candle_cache[key] = {"fetched_at": now, "data": candles}
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

    high = float(latest_completed_1m_bar.get("high", 0.0))
    low = float(latest_completed_1m_bar.get("low", 0.0))
    close = float(latest_completed_1m_bar.get("close", 0.0))

    trigger_level = float(
        setup.get("alpha_high" if direction == "BUY" else "alpha_low")
        or setup.get("jp_high" if direction == "BUY" else "jp_low")
        or 0.0
    )
    if trigger_level <= 0:
        return None

    crossed = high > trigger_level if direction == "BUY" else low < trigger_level
    setup["last_processed_1m_time"] = bar_time_iso
    if strategy == "JP":
        state.set_jp_watchlist_item(security_id, setup)
    else:
        state.set_alpha_watchlist_item(security_id, setup)

    if not crossed:
        return None

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

    position = engine.enter_trade(
        broker=broker,
        security_id=int(security_id),
        symbol=symbol,
        is_bullish_setup=(direction == "BUY"),
        alpha_high=float(setup.get("alpha_high") or setup.get("jp_high") or trigger_level),
        alpha_low=float(setup.get("alpha_low") or setup.get("jp_low") or trigger_level),
        entry_candle=type("_EntryBar", (), {
            "open": float(latest_completed_1m_bar.get("open", fill)),
            "high": high,
            "low": low,
            "close": fill,
            "timestamp": bar_time,
        })(),
        alpha_open_time=setup.get("alpha_open_time") or setup.get("jp_open_time"),
        alpha_close_time=setup.get("alpha_close_time") or setup.get("jp_close_time"),
        alpha_key=setup.get("alpha_key") or setup.get("jp_key"),
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


def build_scan_candidates():
    snap = state.snapshot()
    gainers = [{**item, "is_bullish_setup": True} for item in snap.get("top_gainers", [])[:config.TOP_N_GAINERS]]
    losers = [{**item, "is_bullish_setup": False} for item in snap.get("top_losers", [])[:config.TOP_N_LOSERS]]
    candidates = gainers + losers
    state.add_log(
        f"Scan universe: LIVE, gainers={len(gainers)}, losers={len(losers)}, active={len(snap.get('watchlist', {})) + len(snap.get('open_positions', {}))}"
    )
    return candidates


def build_jp_scan_candidates():
    snap = state.snapshot()
    gainers = [{**row, "is_bullish_setup": True} for row in snap.get("top_gainers", [])[:config.JP_TOP_N_PER_SIDE]]
    losers = [{**row, "is_bullish_setup": False} for row in snap.get("top_losers", [])[:config.JP_TOP_N_PER_SIDE]]
    return gainers + losers


def scan_jp_candidate(broker, candidate, prev_trade_date):
    if not config.JP_ENABLED:
        return

    security_id = int(candidate["SECURITY_ID"])
    symbol = candidate["display_name"]
    is_bullish_setup = candidate["is_bullish_setup"]

    if state.jp_signal_count(security_id) >= config.JP_MAX_SIGNALS_PER_SYMBOL_PER_DAY:
        return
    snap = state.snapshot()
    if str(security_id) in snap.get("open_positions", {}):
        return

    try:
        candles_5m = get_cached_candles(
            broker=broker, security_id=security_id, prev_trade_date=prev_trade_date,
            timeframe=config.JP_TIMEFRAME, ttl_seconds=25,
        )
    except Exception:
        logger.exception(f"JP pattern candle fetch failed for {symbol}")
        return
    if candles_5m is None or candles_5m.empty:
        return

    today = datetime.now(config.TIME_ZONE).date()
    candles_5m = candles_5m[candles_5m["timestamp"].dt.date == today].copy()
    result = jp_pattern.find_jp_setup(candles_5m, is_bullish_setup)
    if result is None:
        return

    now = datetime.now(config.TIME_ZONE)
    age_minutes = (now - result["jp_close_time"]).total_seconds() / 60.0
    if result["jp_close_time"] > now or age_minutes > config.JP_MAX_AGE_MINUTES:
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
        "direction": result["direction"], "stage": "AWAITING_NEXT_5M_CONFIRMATION",
        "jp_open_time": result["jp_open_time"].isoformat(),
        "jp_close_time": result["jp_close_time"].isoformat(),
        "trigger_price": result["trigger_price"], "stop_price": result["stop_price"],
        "smma_band_low": result["band_low"], "smma_band_high": result["band_high"],
        "stop_distance_pct": round(stop_pct, 3), "detected_at": now.isoformat(),
        "jp_key": key,
    }
    state.set_jp_watchlist_item(security_id, item)
    state.mark_jp_alerted(key)
    state.add_log(
        f"{symbol}: JP {result['direction']} detected at {result['jp_open_time'].isoformat()} "
        f"trigger={result['trigger_price']:.2f} SL={result['stop_price']:.2f}"
    )
    state.increment_jp_signal_count(security_id)
    if config.SEND_TELEGRAM_ON_JP_SETUP:
        notifier.notify_jp_candle_detected(
            symbol=symbol, direction=result["direction"],
            jp_time=result["jp_open_time"].isoformat(),
            trigger_price=result["trigger_price"], stop_price=result["stop_price"],
            band_low=result["band_low"], band_high=result["band_high"],
        )


def scan_candidate(broker, candidate, prev_trade_date):
    security_id = int(candidate["SECURITY_ID"])
    symbol = candidate["display_name"]
    is_bullish_setup = candidate["is_bullish_setup"]

    if str(security_id) in state.snapshot()["open_positions"]:
        return

    watchlist_entry = state.snapshot()["watchlist"].get(str(security_id))

    if state.is_blacklisted(security_id):
        signal_vol = watchlist_entry.get("alpha_volume") if watchlist_entry else 0
        if not engine.can_requalify_blacklisted(security_id, signal_vol or 0):
            return

    try:
        candles_5m = get_cached_candles(
            broker=broker, security_id=security_id, prev_trade_date=prev_trade_date,
            timeframe=config.PATTERN_TIMEFRAME, ttl_seconds=25,
        )
        if candles_5m is None or candles_5m.empty:
            return
        today_5m = candles_5m[candles_5m["timestamp"].dt.date == datetime.now(config.TIME_ZONE).date()]
    except Exception:
        logger.exception(f"Pattern-timeframe candle fetch failed for {symbol}")
        return

    if watchlist_entry is None or watchlist_entry.get("stage") != "AWAITING_BREAKOUT":
        result = pattern.find_trend_run_and_alpha(today_5m, is_bullish_setup)
        if result is None:
            state.remove_watchlist_item(security_id)
            return

        alpha = result["alpha_candle"]
        alpha_range_pct = ((float(alpha.high) - float(alpha.low)) / float(alpha.close)) * 100
        if alpha_range_pct > config.MAX_ALPHA_RANGE_PCT:
            state.add_log(
                f"{symbol}: Alpha rejected - range {alpha_range_pct:.2f}% "
                f"exceeds {config.MAX_ALPHA_RANGE_PCT:.2f}%"
            )
            return
        direction = "BUY" if is_bullish_setup else "SELL"
        alpha_open_time = alpha.timestamp.to_pydatetime()
        alpha_close_time = alpha_open_time + timedelta(minutes=config.PATTERN_TIMEFRAME)
        now = datetime.now(config.TIME_ZONE)
        alpha_age_minutes = (now - alpha_close_time).total_seconds() / 60.0
        alpha_key = f"{security_id}_{direction}_{alpha_open_time.isoformat()}"

        if alpha_close_time > now:
            state.add_log(f"{symbol}: Alpha ignored - pattern candle has not closed yet ({alpha_open_time.isoformat()})")
            return
        if alpha_age_minutes > config.MAX_ALPHA_AGE_MINUTES:
            state.add_log(f"{symbol}: stale Alpha ignored - {alpha_age_minutes:.1f} min old, candle={alpha_open_time.isoformat()}")
            return
        if state.is_expired_alpha(alpha_key):
            state.add_log(f"{symbol}: expired Alpha ignored - key={alpha_key}")
            return

        dedup_key = alpha_key
        state.set_watchlist_item(security_id, {
            "symbol": symbol,
            "security_id": str(security_id),
            "is_bullish_setup": is_bullish_setup,
            "direction": direction,
            "stage": "AWAITING_BREAKOUT",
            "alpha_high": float(alpha.high),
            "alpha_low": float(alpha.low),
            "alpha_volume": float(alpha.volume),
            "alpha_open_time": alpha_open_time.isoformat(),
            "alpha_close_time": alpha_close_time.isoformat(),
            "alpha_detected_at": now.isoformat(),
            "alpha_candle_time": str(alpha.timestamp),
            "trend_candle_count": len(result["trend_run"]),
            "current_ltp": None,
            "distance_to_trigger_pct": None,
            "hold_expires_at": (alpha_close_time + timedelta(
                minutes=config.HOLD_CANDLES_5MIN * config.PATTERN_TIMEFRAME
            )).isoformat(),
            "alpha_key": alpha_key,
        })
        if not state.has_alerted(dedup_key):
            if config.SEND_TELEGRAM_ON_SETUP_WATCH:
                notifier.notify_alpha_candle_detected(symbol, direction, str(alpha.timestamp),
                                                       float(alpha.high), float(alpha.low), config.PATTERN_TIMEFRAME)
            state.mark_alerted(dedup_key)
            state.add_log(f"{symbol}: Alpha Candle detected ({direction}) at {alpha.timestamp}")
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


def scan_loop(broker, prev_trade_date):
    while run_flag():
        try:
            if timing.is_entry_allowed():
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
                for cand in candidates:
                    scan_candidate(broker, cand, prev_trade_date)
                    time.sleep(0.2)
                for cand in build_jp_scan_candidates():
                    scan_jp_candidate(broker, cand, prev_trade_date)
                    time.sleep(0.2)

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

    try:
        from dhanhq import MarketFeed
        subscribe_list = [(MarketFeed.NSE, str(r["SECURITY_ID"]), MarketFeed.Ticker) for _, r in universe_df.iterrows()]
    except ImportError:
        from dhanhq.marketfeed import NSE, Ticker
        subscribe_list = [(NSE, str(r["SECURITY_ID"]), Ticker) for _, r in universe_df.iterrows()]
    broker.start_websocket()
    time.sleep(2)
    broker.subscribe_symbols(subscribe_list)

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
