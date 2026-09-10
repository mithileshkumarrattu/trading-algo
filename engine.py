"""
AlphaCandle - Execution Engine.

Owns trade lifecycle and routes orders through ExecutionGateway:
  - Quantity sized from fixed rupee risk per trade: min(risk_qty, notional_qty).
  - Entry routed through ExecutionGateway (Paper, Shadow, or LiveDhan).
  - Target partial booking (1:2 R): books 50%, confirms partial fill, then resizes stop and moves to breakeven.
  - Runner stall exit: watches remaining 50% for reversal stall.
  - Daily loss governor: locks new entries once daily loss threshold reached.
  - Broker-driven EOD square-off.
"""
import math
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import config
import state
import pattern
import notifier
import timing
import journal
from execution import (
    TradeIntent, ExecutionResult, get_execution_gateway,
    round_to_tick, derive_entry_limit
)

logger = logging.getLogger(__name__)


def calc_quantity(entry_price: float, sl_price: float, available_margin: Optional[float] = None) -> int:
    risk_per_share = abs(entry_price - sl_price)
    if risk_per_share <= 0:
        return 0

    risk_budget = getattr(config, "RISK_PER_TRADE", 50.0)
    risk_qty = math.floor(risk_budget / risk_per_share)

    max_notional = getattr(config, "MAX_NOTIONAL_PER_TRADE", 5000.0)
    notional_qty = math.floor(max_notional / entry_price) if entry_price > 0 else 0

    if available_margin is not None and available_margin > 0:
        margin_buffer = getattr(config, "MARGIN_BUFFER_RUPEES", 2000.0)
        usable_margin = max(0.0, available_margin - margin_buffer)
        margin_qty = math.floor(usable_margin / entry_price) if entry_price > 0 else 0
        qty = min(risk_qty, notional_qty, margin_qty)
    else:
        qty = min(risk_qty, notional_qty)

    return max(0, qty)


def can_take_new_trade():
    snap = state.snapshot()
    if snap.get("daily_trade_count", 0) >= getattr(config, "MAX_DAILY_TRADES", config.MAX_TRADES_PER_DAY):
        return False, "Max trades/day reached"
    if len(snap.get("open_positions", {})) >= config.MAX_OPEN_POSITIONS:
        return False, "Max open positions reached"
    if not timing.is_entry_allowed():
        return False, "Outside entry window"
    if snap.get("daily_pnl", 0.0) <= -abs(config.MAX_LOSS_PER_DAY):
        return False, f"Max daily loss reached ({snap.get('daily_pnl'):.2f} <= -{config.MAX_LOSS_PER_DAY})"
    if journal.is_circuit_breaker_active():
        return False, "Circuit breaker active"
    return True, None


def enter_trade(broker, security_id, symbol, is_bullish_setup, alpha_high, alpha_low, entry_candle,
                tick_size=0.05, alpha_open_time=None, alpha_close_time=None, alpha_key=None,
                strategy="ALPHA", signal_quality=None, pattern_confirmation_time=None,
                regime_mode=None, regime_reason=None):
    ok, reason = can_take_new_trade()
    if not ok:
        state.add_log(f"{symbol}: entry blocked - {reason}")
        return None

    transaction_type = "BUY" if is_bullish_setup else "SELL"
    trigger_level = float(alpha_high if is_bullish_setup else alpha_low)
    candle_close = float(entry_candle.close)

    # Calculate stop loss level
    sl_price = pattern.initial_stop_loss(alpha_high, alpha_low, is_bullish_setup, candle_close)
    sl_price = round_to_tick(sl_price, tick_size)

    # Derive marketable limit price capped by max permitted extension
    quote_data = broker.liveFeed.get(str(security_id), {}) if broker else {}
    best_bid = quote_data.get("best_bid") or quote_data.get("bid") or quote_data.get("ltp")
    best_ask = quote_data.get("best_ask") or quote_data.get("ask") or quote_data.get("ltp")
    quote_age = (datetime.now(config.TIME_ZONE) - quote_data.get("updated_at")).total_seconds() if quote_data.get("updated_at") else 0.0

    spread_pct = 0.0
    if best_bid and best_ask and best_bid > 0:
        spread_pct = abs(best_ask - best_bid) / best_bid * 100.0

    entry_limit = derive_entry_limit(
        side=transaction_type,
        trigger=trigger_level,
        best_bid=best_bid,
        best_ask=best_ask,
        tick_size=tick_size,
        max_extension_pct=getattr(config, "MAX_ENTRY_EXTENSION_PCT", 0.15),
    )

    qty = calc_quantity(entry_limit, sl_price)
    if qty <= 0:
        state.add_log(f"{symbol}: quantity calculation resulted in 0 (risk or notional limit), skipping entry")
        return None

    risk_distance = abs(entry_limit - sl_price)
    target_r2_price = (entry_limit + config.DESIRED_TARGET_R * risk_distance) if is_bullish_setup \
        else (entry_limit - config.DESIRED_TARGET_R * risk_distance)
    target_r2_price = round_to_tick(target_r2_price, tick_size)

    # Deterministic signal ID
    pattern_time_str = ""
    if alpha_open_time:
        try:
            pattern_time_str = datetime.fromisoformat(str(alpha_open_time)).strftime("%Y%m%dT%H%M%S")
        except Exception:
            pattern_time_str = str(alpha_open_time).replace(":", "").replace("-", "")
    if not pattern_time_str:
        pattern_time_str = datetime.now(config.TIME_ZONE).strftime("%Y%m%dT%H%M%S")

    signal_id = f"{strategy}_{security_id}_{transaction_type}_{pattern_time_str}"

    intent = TradeIntent(
        signal_id=signal_id,
        strategy=strategy,
        security_id=str(security_id),
        symbol=symbol,
        side=transaction_type,
        exchange_segment=config.EXCHANGE,
        product_type=config.PRODUCT_TYPE,
        qty=qty,
        entry_limit=entry_limit,
        trigger_price=trigger_level,
        protective_stop=sl_price,
        target_r2=target_r2_price,
        tick_size=tick_size,
        signal_time=datetime.now(config.TIME_ZONE),
        signal_quality=signal_quality or {},
        regime_mode=regime_mode,
        regime_reason=regime_reason,
        alpha_key=alpha_key,
        alpha_open_time=str(alpha_open_time) if alpha_open_time else None,
        alpha_close_time=str(alpha_close_time) if alpha_close_time else None,
        pattern_confirmation_time=str(pattern_confirmation_time) if pattern_confirmation_time else None,
        quote_age_sec=quote_age,
        spread_pct=spread_pct,
    )

    gateway = get_execution_gateway(broker)
    res: ExecutionResult = gateway.submit_entry(intent)

    if not res.success or res.status in ("REJECTED", "FAILED", "TIMED_OUT", "STOP_FAILED_EMERGENCY_EXIT"):
        return None

    pos = state.snapshot()["open_positions"].get(str(security_id))
    if not pos:
        pos = journal.get_position(str(security_id))

    if pos:
        notifier.notify_entry(
            symbol=symbol,
            direction=transaction_type,
            qty=res.filled_qty or qty,
            entry_price=res.avg_price or entry_limit,
            sl_price=sl_price,
            target_price=target_r2_price,
            paper_mode=config.PAPER_MODE,
            alpha_open_time=alpha_open_time,
            alpha_high=alpha_high,
            alpha_low=alpha_low,
            trigger_1m_time=str(entry_candle.timestamp),
            strategy=strategy,
            pattern_confirmation_time=pattern_confirmation_time,
            entry_extension_pct=round(abs(entry_limit - trigger_level) / trigger_level * 100, 3),
            regime_mode=regime_mode,
        )
    return pos


def _close_position(broker, security_id, exit_price, exit_qty, reason):
    snap = state.snapshot()
    pos = snap["open_positions"].get(str(security_id)) or journal.get_position(str(security_id))
    if not pos:
        return

    gateway = get_execution_gateway(broker)
    exit_res = gateway.submit_exit(pos, exit_qty, reason)
    if not exit_res.success and config.EXECUTION_MODE == "LIVE":
        logger.error(f"Failed to submit exit for {security_id}: {exit_res.message}")
        return

    transaction_type = pos["transaction_type"] if "transaction_type" in pos else pos.get("direction", "BUY")
    direction = 1 if transaction_type == "BUY" else -1
    entry_price = float(pos.get("entry_price") or pos.get("entry_average", 0.0))
    pnl = direction * (exit_price - entry_price) * exit_qty

    trade_record = {
        "strategy": pos.get("strategy", "ALPHA"),
        "symbol": pos.get("symbol", str(security_id)),
        "security_id": str(security_id),
        "transaction_type": transaction_type,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "quantity": exit_qty,
        "pnl": round(pnl, 2),
        "reason": reason,
        "closed_at": datetime.now(config.TIME_ZONE).isoformat(),
    }
    state.add_closed_trade(trade_record)
    journal.record_trade(trade_record)

    remaining = pos.get("remaining_qty", pos.get("net_quantity", exit_qty)) - exit_qty
    if remaining <= 0:
        state.remove_open_position(security_id)
        pos["status"] = "CLOSED"
        pos["net_quantity"] = 0
        journal.upsert_position(pos)
        state.set_blacklist(security_id, reason=f"Traded today ({reason})", signal_volume=pos.get("signal_volume", 0))
        notifier.notify_blacklist_locked(pos.get("symbol", str(security_id)), f"already traded today ({reason})")
        if reason == "STOP_LOSS":
            notifier.notify_sl_hit(pos.get("symbol", str(security_id)), transaction_type, exit_qty, exit_price, trade_record["pnl"], config.PAPER_MODE)
        elif reason == "STALL_REVERSAL_EXIT":
            notifier.notify_stall_exit(pos.get("symbol", str(security_id)), transaction_type, exit_qty, exit_price, trade_record["pnl"], config.PAPER_MODE)
        elif reason == "EOD_SQUARE_OFF":
            notifier.notify_eod_squareoff(pos.get("symbol", str(security_id)), exit_qty, exit_price, trade_record["pnl"], config.PAPER_MODE)
    else:
        pos["remaining_qty"] = remaining
        pos["net_quantity"] = remaining
        pos["partial_booked"] = True
        state.set_open_position(security_id, pos)
        journal.upsert_position(pos)
        notifier.notify_partial_book(pos.get("symbol", str(security_id)), transaction_type, exit_qty, exit_price, trade_record["pnl"], config.PAPER_MODE)

    if state.snapshot().get("daily_pnl", 0.0) <= -abs(config.MAX_LOSS_PER_DAY):
        journal.set_circuit_breaker("DAILY_LOSS_LOCK", locked=True, reason=f"Daily loss limit reached ({state.snapshot().get('daily_pnl'):.2f})")
        notifier.notify_daily_loss_cap(state.snapshot().get("daily_pnl", 0.0))


def manage_open_position(broker, security_id, ltp):
    snap = state.snapshot()
    pos = snap["open_positions"].get(str(security_id)) or journal.get_position(str(security_id))
    if not pos or ltp is None:
        return

    direction = 1 if pos.get("transaction_type", pos.get("direction")) == "BUY" else -1
    current_sl = float(pos.get("current_sl", pos.get("protective_stop_price", 0.0)))
    target_r2 = float(pos.get("target_r2_price", 0.0))
    entry_price = float(pos.get("entry_price", pos.get("entry_average", 0.0)))
    remaining_qty = int(pos.get("remaining_qty", pos.get("net_quantity", 0)))
    partial_booked = bool(pos.get("partial_booked", False))

    hit_sl = (direction == 1 and ltp <= current_sl) or (direction == -1 and ltp >= current_sl)
    if hit_sl:
        _close_position(broker, security_id, ltp, remaining_qty, reason="STOP_LOSS")
        return

    reached_r2 = (direction == 1 and ltp >= target_r2) or (direction == -1 and ltp <= target_r2)

    if reached_r2 and not partial_booked:
        book_qty = max(1, math.floor(int(pos.get("quantity", remaining_qty)) * config.PARTIAL_BOOK_FRACTION))
        book_qty = min(book_qty, remaining_qty)

        # 1. Close partial quantity
        _close_position(broker, security_id, ltp, book_qty, reason="PARTIAL_1_2_TARGET")

        # 2. Only after partial exit fill is confirmed, resize stop & move to breakeven
        pos = state.snapshot()["open_positions"].get(str(security_id)) or journal.get_position(str(security_id))
        if pos and pos.get("net_quantity", 0) > 0:
            gateway = get_execution_gateway(broker)
            new_stop = entry_price
            amend_res = gateway.amend_protective_stop(pos, new_stop=new_stop, quantity=pos["net_quantity"])
            if amend_res.success:
                pos["current_sl"] = new_stop
                pos["protective_stop_price"] = new_stop
                pos["best_price_since_r2"] = ltp
                pos["stall_counter"] = 0
                state.set_open_position(security_id, pos)
                journal.upsert_position(pos)
        return

    if partial_booked:
        best_price = float(pos.get("best_price_since_r2", entry_price))
        improved = (direction == 1 and ltp > best_price) or (direction == -1 and ltp < best_price)
        if improved:
            pos["best_price_since_r2"] = ltp
            pos["stall_counter"] = 0
        else:
            pos["stall_counter"] = pos.get("stall_counter", 0) + 1
        state.set_open_position(security_id, pos)
        journal.upsert_position(pos)

        if pos.get("stall_counter", 0) >= config.STALL_CANDLES_FOR_REVERSAL_EXIT:
            _close_position(broker, security_id, ltp, remaining_qty, reason="STALL_REVERSAL_EXIT")


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
    """
    Broker-driven EOD Square-Off:
      1. Freeze new entries.
      2. In LIVE mode: cancel pending entry orders -> exit broker net positions -> confirm fills -> cancel stops.
      3. In PAPER mode: close simulated open positions.
      4. Reconcile final state.
    """
    state.add_log("Starting EOD square-off sequence...")

    if config.EXECUTION_MODE == "LIVE" and broker:
        try:
            # 1. Cancel pending bot orders
            broker_orders = broker.get_order_list() or []
            for o in broker_orders:
                if str(o.get("orderStatus", "")).upper() in ("PENDING", "OPEN", "TRIGGER_PENDING"):
                    ord_type = str(o.get("orderType", "")).upper()
                    if ord_type != "STOP_LOSS":  # keep stop loss until position exited
                        broker.cancel_order(str(o.get("orderId")))

            # 2. Exit all broker net positions
            broker_positions = broker.get_positions() or []
            for p in broker_positions:
                net_qty = int(p.get("netQty", 0))
                sec_id = str(p.get("securityId"))
                if net_qty != 0:
                    exit_side = "SELL" if net_qty > 0 else "BUY"
                    broker.place_order(
                        security_id=sec_id,
                        transaction_type=exit_side,
                        exchange_segment=p.get("exchangeSegment", config.EXCHANGE),
                        quantity=abs(net_qty),
                        order_type="MARKET",
                        product_type=p.get("productType", config.PRODUCT_TYPE),
                    )

            # 3. Cancel residual stop orders
            for o in broker.get_order_list() or []:
                if str(o.get("orderStatus", "")).upper() in ("PENDING", "TRIGGER_PENDING", "OPEN"):
                    broker.cancel_order(str(o.get("orderId")))
        except Exception:
            logger.exception("Error during live EOD broker square-off")

    # Clean local positions
    snap = state.snapshot()
    for sid, pos in list(snap.get("open_positions", {}).items()):
        try:
            ltp = broker.get_ltp(sid, config.EXCHANGE) if broker else None
        except Exception:
            ltp = None
        exit_price = float(ltp) if ltp is not None else float(pos.get("entry_price", pos.get("entry_average", 0.0)))
        _close_position(
            broker=broker, security_id=sid, exit_price=exit_price,
            exit_qty=pos.get("remaining_qty", pos.get("net_quantity", 0)), reason="EOD_SQUARE_OFF",
        )

    state.add_log("EOD square-off completed")
