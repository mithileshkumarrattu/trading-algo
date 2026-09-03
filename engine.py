"""
AlphaCandle - Execution Engine.

Owns the full trade lifecycle exactly per spec:
  - Quantity sized from fixed rupee risk per trade (RISK_PER_TRADE / SL distance).
  - Entry (simulated in PAPER_MODE, real via broker.py in LIVE mode).
  - Initial SL = 2-3 points from Alpha Candle low/high.
  - At 1:2 R -> book 50% qty, move remaining SL to BREAKEVEN (entry price) -
    profit already locked in can NEVER be given back.
  - Remaining 50% ("runner") watched for stall (no new favourable high/low
    for STALL_CANDLES_FOR_REVERSAL_EXIT consecutive 1-min candles after the
    1:2 target) -> exit rest rather than risk giving back gains.
  - Daily loss governor: once cumulative daily PnL <= -MAX_LOSS_PER_DAY
    across ALL stocks combined, "cost-to-cost mode" activates - every trade
    taken after that point is forced to exit at ENTRY PRICE (0 PnL), never
    adding further loss, even if it could recover.
  - Per-stock one-trade-per-day blacklist; requalifies only after a cooldown
    AND a stronger (higher-volume) new signal.

PAPER_MODE guard: every order-placing/modifying/cancelling broker call is
routed through this module's own logic, gated by config.PAPER_MODE - in
paper mode we never call broker.place_order / modify_order / cancel_order /
close_position_by_security. LTP/quote/candle reads (read-only) still happen
so the simulation reflects real prices.
"""
import logging
import math
from datetime import datetime, timedelta

import config
import state
import pattern
import notifier

logger = logging.getLogger(__name__)

_paper_seq = 0


def _next_paper_id():
    global _paper_seq
    _paper_seq += 1
    return f"PAPER-{int(datetime.now().timestamp())}-{_paper_seq}"


def calc_quantity(entry_price, sl_price):
    risk_per_share = abs(entry_price - sl_price)
    if risk_per_share <= 0:
        return 0
    return max(1, math.floor(config.RISK_PER_TRADE / risk_per_share))


def apply_paper_entry_slippage(price, transaction_type):
    bps = config.PAPER_SLIPPAGE_BPS / 10_000.0
    return price * (1 + bps) if transaction_type == "BUY" else price * (1 - bps)


def can_take_new_trade():
    import timing
    snap = state.snapshot()
    if snap["daily_trade_count"] >= config.MAX_TRADES_PER_DAY:
        return False, "Max trades/day reached"
    if len(snap["open_positions"]) >= config.MAX_OPEN_POSITIONS:
        return False, "Max open positions reached"
    if not timing.is_entry_allowed():
        return False, "Outside entry window"
    return True, None


def enter_trade(broker, security_id, symbol, is_bullish_setup, alpha_high, alpha_low, entry_candle,
                tick_size=0.05, alpha_open_time=None, alpha_close_time=None, alpha_key=None,
                strategy="ALPHA", signal_quality=None, pattern_confirmation_time=None):
    ok, reason = can_take_new_trade()
    if not ok:
        state.add_log(f"{symbol}: entry blocked - {reason}")
        return None

    transaction_type = "BUY" if is_bullish_setup else "SELL"
    entry_price = float(entry_candle.close)
    if config.PAPER_MODE:
        entry_price = apply_paper_entry_slippage(entry_price, transaction_type)
    sl_price = pattern.initial_stop_loss(alpha_high, alpha_low, is_bullish_setup, entry_price)
    max_stop_pct = config.JP_MAX_STOP_DISTANCE_PCT if strategy.upper() == "JP" else config.MAX_STOP_DISTANCE_PCT
    stop_distance_pct = abs(entry_price - sl_price) / entry_price * 100
    if stop_distance_pct > max_stop_pct:
        state.add_log(
            f"{symbol}: entry rejected - stop distance "
            f"{stop_distance_pct:.2f}% exceeds {max_stop_pct:.2f}%"
        )
        return None
    qty = calc_quantity(entry_price, sl_price)
    if qty <= 0:
        state.add_log(f"{symbol}: quantity calculation failed, skipping entry")
        return None

    risk_distance = abs(entry_price - sl_price)
    target_r2_price = (entry_price + config.DESIRED_TARGET_R * risk_distance) if is_bullish_setup \
        else (entry_price - config.DESIRED_TARGET_R * risk_distance)

    cost_to_cost_trade = state.snapshot()["cost_to_cost_mode"]

    if config.PAPER_MODE:
        order_id = _next_paper_id()
        sl_order_id = _next_paper_id()
        state.add_log(f"[PAPER] {transaction_type} entry simulated: {symbol} qty={qty} @ {entry_price:.2f}")
        logger.warning(
            f"PAPER ENTRY CREATED | {transaction_type} | {symbol} | "
            f"qty={qty} | entry={entry_price:.2f} | sl={sl_price:.2f} | "
            f"target={target_r2_price:.2f} | alpha={alpha_open_time} | "
            f"trigger_1m={entry_candle.timestamp}"
        )
    else:
        limit_price = broker.get_limit_price(config.EXCHANGE, security_id, transaction_type) or entry_price
        order_id = broker.place_order(security_id, transaction_type, config.EXCHANGE, qty,
                                       order_type="LIMIT", product_type=config.PRODUCT_TYPE,
                                       limit_price=limit_price, tick_size=tick_size)
        if order_id is None:
            state.add_log(f"{symbol}: LIVE entry order placement FAILED")
            return None
        entry_price = limit_price
        sl_transaction = "SELL" if is_bullish_setup else "BUY"
        sl_order_id = broker.place_order(security_id, sl_transaction, config.EXCHANGE, qty,
                                          order_type="STOP_LOSS", product_type=config.PRODUCT_TYPE,
                                          limit_price=sl_price, trigger_price=sl_price, tick_size=tick_size)

    position = {
        "strategy": strategy,
        "entry_reason": "1_MIN_WICK_BREAK",
        "trigger_price_level": float(alpha_high if is_bullish_setup else alpha_low),
        "security_id": str(security_id),
        "symbol": symbol,
        "transaction_type": transaction_type,
        "entry_price": entry_price,
        "initial_sl": sl_price,
        "current_sl": sl_price,
        "target_r2_price": target_r2_price,
        "quantity": qty,
        "remaining_qty": qty,
        "order_id": order_id,
        "sl_order_id": sl_order_id,
        "entered_at": datetime.now(config.TIME_ZONE).isoformat(),
        "partial_booked": False,
        "stall_counter": 0,
        "best_price_since_r2": entry_price,
        "cost_to_cost_trade": cost_to_cost_trade,
        "alpha_key": alpha_key,
        "setup_key": alpha_key,
        "pattern_timeframe": config.PATTERN_TIMEFRAME,
        "pattern_open_time": alpha_open_time,
        "pattern_close_time": alpha_close_time,
        "pattern_high": float(alpha_high),
        "pattern_low": float(alpha_low),
        "alpha_open_time": alpha_open_time,
        "alpha_close_time": alpha_close_time,
        "alpha_high": float(alpha_high),
        "alpha_low": float(alpha_low),
        "trigger_1m_time": str(entry_candle.timestamp),
        "trigger_1m_open": float(entry_candle.open),
        "trigger_1m_high": float(entry_candle.high),
        "trigger_1m_low": float(entry_candle.low),
        "trigger_1m_close": float(entry_candle.close),
        "status": "OPEN",
        "signal_quality": signal_quality or {},
        "pattern_confirmation_time": pattern_confirmation_time,
    }
    state.set_open_position(security_id, position)
    notifier.notify_entry(
        symbol=symbol, direction=transaction_type, qty=qty,
        entry_price=entry_price, sl_price=sl_price, target_price=target_r2_price,
        paper_mode=config.PAPER_MODE, alpha_open_time=alpha_open_time,
        alpha_high=alpha_high, alpha_low=alpha_low,
        trigger_1m_time=str(entry_candle.timestamp),
        strategy=strategy,
    )
    return position


def _close_position(broker, security_id, exit_price, exit_qty, reason):
    snap = state.snapshot()
    pos = snap["open_positions"].get(str(security_id))
    if not pos:
        return

    transaction_type = pos["transaction_type"]
    direction = 1 if transaction_type == "BUY" else -1
    if config.PAPER_MODE and not pos.get("cost_to_cost_trade"):
        exit_transaction = "SELL" if transaction_type == "BUY" else "BUY"
        exit_price = apply_paper_entry_slippage(float(exit_price), exit_transaction)
    pnl = direction * (exit_price - pos["entry_price"]) * exit_qty

    if pos.get("cost_to_cost_trade"):
        pnl = 0.0
        exit_price = pos["entry_price"]

    if not config.PAPER_MODE:
        try:
            broker.close_position_by_security(security_id, exit_qty, transaction_type)
        except Exception:
            logger.exception(f"Error closing live position {security_id}")

    trade_record = {
        "strategy": pos.get("strategy", "ALPHA"),
        "symbol": pos["symbol"],
        "security_id": str(security_id),
        "transaction_type": transaction_type,
        "entry_price": pos["entry_price"],
        "exit_price": exit_price,
        "quantity": exit_qty,
        "pnl": round(pnl, 2),
        "reason": reason,
        "closed_at": datetime.now(config.TIME_ZONE).isoformat(),
    }
    state.add_closed_trade(trade_record)

    remaining = pos["remaining_qty"] - exit_qty
    if remaining <= 0:
        state.remove_open_position(security_id)
        state.set_blacklist(security_id, reason=f"Traded today ({reason})", signal_volume=pos.get("signal_volume", 0))
        notifier.notify_blacklist_locked(pos["symbol"], f"already traded today ({reason})")
        if reason == "STOP_LOSS":
            notifier.notify_sl_hit(pos["symbol"], transaction_type, exit_qty, exit_price, trade_record["pnl"], config.PAPER_MODE)
        elif reason == "STALL_REVERSAL_EXIT":
            notifier.notify_stall_exit(pos["symbol"], transaction_type, exit_qty, exit_price, trade_record["pnl"], config.PAPER_MODE)
        elif reason == "EOD_SQUARE_OFF":
            notifier.notify_eod_squareoff(pos["symbol"], exit_qty, exit_price, trade_record["pnl"], config.PAPER_MODE)
    else:
        pos["remaining_qty"] = remaining
        pos["partial_booked"] = True
        state.set_open_position(security_id, pos)
        notifier.notify_partial_book(pos["symbol"], transaction_type, exit_qty, exit_price, trade_record["pnl"], config.PAPER_MODE)

    if state.snapshot()["cost_to_cost_mode"] and not pos.get("cost_to_cost_trade"):
        notifier.notify_daily_loss_cap(state.snapshot()["daily_pnl"])


def manage_open_position(broker, security_id, ltp):
    snap = state.snapshot()
    pos = snap["open_positions"].get(str(security_id))
    if not pos or ltp is None:
        return

    direction = 1 if pos["transaction_type"] == "BUY" else -1
    hit_sl = (direction == 1 and ltp <= pos["current_sl"]) or (direction == -1 and ltp >= pos["current_sl"])
    if hit_sl:
        _close_position(broker, security_id, ltp, pos["remaining_qty"], reason="STOP_LOSS")
        return

    reached_r2 = (direction == 1 and ltp >= pos["target_r2_price"]) or (direction == -1 and ltp <= pos["target_r2_price"])

    if reached_r2 and not pos["partial_booked"]:
        book_qty = max(1, math.floor(pos["quantity"] * config.PARTIAL_BOOK_FRACTION))
        book_qty = min(book_qty, pos["remaining_qty"])
        _close_position(broker, security_id, ltp, book_qty, reason="PARTIAL_1_2_TARGET")

        pos = state.snapshot()["open_positions"].get(str(security_id))
        if pos:
            pos["current_sl"] = pos["entry_price"]
            pos["best_price_since_r2"] = ltp
            pos["stall_counter"] = 0
            state.set_open_position(security_id, pos)
            if not config.PAPER_MODE and pos.get("sl_order_id"):
                try:
                    sl_info = broker.get_order_by_id(pos["sl_order_id"])
                    if sl_info:
                        broker.modify_order(sl_info, pos["entry_price"])
                except Exception:
                    logger.exception(f"Failed to move SL to breakeven for {security_id}")
        return

    if pos["partial_booked"]:
        improved = (direction == 1 and ltp > pos["best_price_since_r2"]) or (direction == -1 and ltp < pos["best_price_since_r2"])
        if improved:
            pos["best_price_since_r2"] = ltp
            pos["stall_counter"] = 0
        else:
            pos["stall_counter"] += 1
        state.set_open_position(security_id, pos)

        if pos["stall_counter"] >= config.STALL_CANDLES_FOR_REVERSAL_EXIT:
            _close_position(broker, security_id, ltp, pos["remaining_qty"], reason="STALL_REVERSAL_EXIT")


def can_requalify_blacklisted(security_id, new_signal_volume):
    entry = state.get_blacklist_entry(security_id)
    if not entry:
        return True
    blacklisted_at = datetime.fromisoformat(entry["blacklisted_at"])
    if datetime.now(config.TIME_ZONE) < blacklisted_at + timedelta(minutes=config.BLACKLIST_COOLDOWN_MIN):
        return False
    prior_volume = entry.get("signal_volume") or 0
    if prior_volume <= 0:
        return True
    return new_signal_volume >= prior_volume * config.BLACKLIST_REQUALIFY_VOL_MULTIPLE


def square_off_all(broker):
    snap = state.snapshot()
    for sid, pos in list(snap["open_positions"].items()):
        try:
            ltp = broker.get_ltp(sid, config.EXCHANGE)
        except Exception:
            ltp = None
        exit_price = float(ltp) if ltp is not None else float(pos["entry_price"])
        _close_position(
            broker=broker, security_id=sid, exit_price=exit_price,
            exit_qty=pos["remaining_qty"], reason="EOD_SQUARE_OFF",
        )
    state.add_log("EOD square-off completed")
