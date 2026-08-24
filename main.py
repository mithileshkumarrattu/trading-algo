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
import engine
import notifier
from broker import DhanBroker

_stop_event = threading.Event()


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
    now = datetime.now(config.TIME_ZONE).time()
    lock_time = timing.get_time(config.FINAL_UNIVERSE_LOCK_TIME)
    use_locked = now >= lock_time
    gainers = snap.get("final_top_gainers" if use_locked else "top_gainers", [])
    losers = snap.get("final_top_losers" if use_locked else "top_losers", [])
    active_count = len(snap.get("watchlist", {})) + len(snap.get("open_positions", {}))
    group_size = config.SCAN_GROUP_SIZE_PER_SIDE
    group_index = 0 if active_count < config.MAX_ACTIVE_SETUPS_BEFORE_EXPAND else 1
    start = group_index * group_size
    end = start + group_size
    selected_gainers = gainers[start:end]
    selected_losers = losers[start:end]
    candidates = [{**row, "is_bullish_setup": True} for row in selected_gainers]
    candidates.extend({**row, "is_bullish_setup": False} for row in selected_losers)
    state.add_log(
        f"Scan universe: {'FINAL LOCKED' if use_locked else 'LIVE'} "
        f"group={group_index + 1}, gainers={len(selected_gainers)}, "
        f"losers={len(selected_losers)}, active={active_count}"
    )
    return candidates


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
        candles_5m = broker.get_intraday_candles(
            security_id=security_id, exchange_segment=config.EXCHANGE, instrument_type="EQUITY",
            from_dt=prev_trade_date, timeframe=config.PATTERN_TIMEFRAME,
        )
        if candles_5m is None or candles_5m.empty:
            return
        today_5m = candles_5m[candles_5m["timestamp"].dt.date == datetime.now(config.TIME_ZONE).date()]
    except Exception:
        logger.exception(f"5-min candle fetch failed for {symbol}")
        return

    if watchlist_entry is None or watchlist_entry.get("stage") != "AWAITING_BREAKOUT":
        result = pattern.find_trend_run_and_alpha(today_5m, is_bullish_setup)
        if result is None:
            state.remove_watchlist_item(security_id)
            return

        alpha = result["alpha_candle"]
        direction = "BUY" if is_bullish_setup else "SELL"
        alpha_open_time = alpha.timestamp.to_pydatetime()
        alpha_close_time = alpha_open_time + timedelta(minutes=config.PATTERN_TIMEFRAME)
        now = datetime.now(config.TIME_ZONE)
        alpha_age_minutes = (now - alpha_close_time).total_seconds() / 60.0
        alpha_key = f"{security_id}_{direction}_{alpha_open_time.isoformat()}"

        if alpha_close_time > now:
            state.add_log(f"{symbol}: Alpha ignored - 5-min candle has not closed yet ({alpha_open_time.isoformat()})")
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
    if pattern.hold_window_expired(alpha_close_time):
        state.remove_watchlist_item(security_id)
        expiry_time = datetime.now(config.TIME_ZONE).isoformat()
        state.mark_expired_alpha(watchlist_entry["alpha_key"], expires_at=expiry_time)
        state.add_log(f"{symbol}: Alpha expired without breakout. 5-min candle={watchlist_entry['alpha_open_time']} expired_at={expiry_time}")
        return

    try:
        candles_1m = broker.get_intraday_candles(
            security_id=security_id, exchange_segment=config.EXCHANGE, instrument_type="EQUITY",
            from_dt=prev_trade_date, timeframe=config.ENTRY_TIMEFRAME,
        )
        if candles_1m is None or candles_1m.empty:
            return
        today_1m = candles_1m[candles_1m["timestamp"] >= alpha_close_time].copy()
    except Exception:
        logger.exception(f"1-min candle fetch failed for {symbol}")
        return

    if not today_1m.empty:
        latest_ltp = float(today_1m.iloc[-1].close)
        alpha_ref = watchlist_entry["alpha_high"] if is_bullish_setup else watchlist_entry["alpha_low"]
        dist_pct = round(((alpha_ref - latest_ltp) / latest_ltp) * 100, 3) if is_bullish_setup \
                   else round(((latest_ltp - alpha_ref) / latest_ltp) * 100, 3)
        updated_entry = dict(watchlist_entry)
        updated_entry["current_ltp"] = latest_ltp
        updated_entry["distance_to_trigger_pct"] = dist_pct
        state.set_watchlist_item(security_id, updated_entry)

    trigger_candle = pattern.check_1min_breakout(
        today_1m, watchlist_entry["alpha_high"], watchlist_entry["alpha_low"], is_bullish_setup
    )
    if trigger_candle is None:
        return

    tick_size = 0.05
    try:
        universe_df = state.snapshot()
    except Exception:
        pass

    position = engine.enter_trade(
        broker=broker, security_id=security_id, symbol=symbol,
        is_bullish_setup=is_bullish_setup,
        alpha_high=watchlist_entry["alpha_high"], alpha_low=watchlist_entry["alpha_low"],
        entry_candle=trigger_candle, tick_size=tick_size,
        alpha_open_time=watchlist_entry["alpha_open_time"],
        alpha_close_time=watchlist_entry["alpha_close_time"],
        alpha_key=watchlist_entry["alpha_key"],
    )
    if position is not None:
        state.remove_watchlist_item(security_id)


def scan_loop(broker, prev_trade_date):
    while run_flag():
        try:
            if timing.is_entry_allowed():
                candidates = build_scan_candidates()
                for cand in candidates:
                    scan_candidate(broker, cand, prev_trade_date)
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
