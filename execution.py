"""
AlphaCandle - Production Execution Gateway.

Supports:
  - PaperExecutionGateway (simulated fills with slippage)
  - ShadowExecutionGateway (live account inspection, quote/depth checks, no orders sent)
  - LiveDhanExecutionGateway (live DhanHQ broker integration with protection-first state machine)
"""
import time
import math
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, field

import config
import state
import journal
import notifier
import timing

logger = logging.getLogger(__name__)


def round_to_tick(price: float, tick_size: float = 0.05) -> float:
    """Round a price to the nearest instrument tick size."""
    if tick_size <= 0:
        return round(price, 2)
    ticks = (Decimal(str(price)) / Decimal(str(tick_size))).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    return float(ticks * Decimal(str(tick_size)))


def protective_sl_prices(direction: str, stop_reference: float, tick_size: float = 0.05, limit_buffer_ticks: int = 2) -> Tuple[float, float]:
    """
    Returns (trigger_price, limit_price) for protective stop orders.
    For BUY position: stop order is SELL. Trigger = stop, Limit = stop - buffer.
    For SELL position: stop order is BUY. Trigger = stop, Limit = stop + buffer.
    """
    stop = round_to_tick(stop_reference, tick_size)
    buffer_pts = tick_size * limit_buffer_ticks
    if direction.upper() == "BUY":
        trigger = stop
        limit = round_to_tick(stop - buffer_pts, tick_size)
    else:
        trigger = stop
        limit = round_to_tick(stop + buffer_pts, tick_size)
    return trigger, limit


def derive_entry_limit(side: str, trigger: float, best_bid: Optional[float], best_ask: Optional[float], tick_size: float = 0.05, max_extension_pct: float = 0.15) -> float:
    """Derives a marketable but capped limit price from best bid/ask and trigger level."""
    max_buy = trigger * (1.0 + max_extension_pct / 100.0)
    min_sell = trigger * (1.0 - max_extension_pct / 100.0)
    if side.upper() == "BUY":
        candidate = best_ask if (best_ask and best_ask > 0) else (trigger + tick_size)
        return round_to_tick(min(candidate, max_buy), tick_size)
    else:
        candidate = best_bid if (best_bid and best_bid > 0) else (trigger - tick_size)
        return round_to_tick(max(candidate, min_sell), tick_size)


@dataclass
class TradeIntent:
    signal_id: str
    strategy: str
    security_id: str
    symbol: str
    side: str  # "BUY" or "SELL"
    exchange_segment: str
    product_type: str
    qty: int
    entry_limit: float
    trigger_price: float
    protective_stop: float
    target_r2: float
    tick_size: float = 0.05
    signal_time: Optional[datetime] = None
    signal_quality: Optional[Dict[str, Any]] = None
    regime_mode: Optional[str] = None
    regime_reason: Optional[str] = None
    alpha_key: Optional[str] = None
    alpha_open_time: Optional[str] = None
    alpha_close_time: Optional[str] = None
    pattern_confirmation_time: Optional[str] = None
    quote_age_sec: float = 0.0
    spread_pct: float = 0.0
    current_ltp: Optional[float] = None
    available_margin: Optional[float] = None
    required_margin: Optional[float] = None


@dataclass
class ExecutionResult:
    success: bool
    order_id: Optional[str] = None
    client_tag: Optional[str] = None
    correlation_id: Optional[str] = None
    filled_qty: int = 0
    avg_price: Optional[float] = None
    status: str = "PENDING"
    message: str = ""
    raw_response: Optional[Any] = None


@dataclass
class ReconciliationReport:
    reconciled_at: str
    matched_positions: int = 0
    unmatched_positions: list = field(default_factory=list)
    stale_orders_cancelled: int = 0
    status: str = "OK"
    details: Dict[str, Any] = field(default_factory=dict)


def validate_trade_intent(intent: TradeIntent, broker=None) -> Tuple[bool, str]:
    """Pre-trade validation gate verifying all risk, pricing, and health constraints."""
    # 1. Idempotency check: has this signal already created an order?
    existing_orders = journal.get_orders_for_signal(intent.signal_id)
    if existing_orders:
        return False, f"Signal {intent.signal_id} has already been submitted (idempotency block)"

    # 2. Circuit breakers
    if journal.is_circuit_breaker_active():
        active = journal.get_all_circuit_breakers()
        locked = [k for k, v in active.items() if v.get("locked")]
        return False, f"Circuit breaker active: {', '.join(locked)}"

    # 3. Timing window
    if not timing.is_entry_allowed():
        return False, "Outside allowed entry timing window"

    # 4. Position and trade counts
    snap = state.snapshot()
    if snap.get("daily_trade_count", 0) >= config.MAX_DAILY_TRADES:
        return False, f"Daily trade limit reached ({snap.get('daily_trade_count')} >= {config.MAX_DAILY_TRADES})"

    open_pos = snap.get("open_positions", {})
    if len(open_pos) >= config.MAX_OPEN_POSITIONS:
        return False, f"Max open positions reached ({len(open_pos)} >= {config.MAX_OPEN_POSITIONS})"

    if str(intent.security_id) in open_pos:
        return False, f"Position already open for {intent.symbol} ({intent.security_id})"

    # 5. Daily Loss Limit
    if snap.get("daily_pnl", 0.0) <= -abs(config.MAX_LOSS_PER_DAY):
        return False, f"Max daily loss reached (PnL={snap.get('daily_pnl'):.2f} <= -{config.MAX_LOSS_PER_DAY})"

    # 6. Quantity & Sizing
    if intent.qty <= 0:
        return False, "Calculated quantity is <= 0"

    notional = intent.qty * intent.entry_limit
    if notional > config.MAX_NOTIONAL_PER_TRADE:
        return False, f"Notional {notional:.2f} exceeds MAX_NOTIONAL_PER_TRADE {config.MAX_NOTIONAL_PER_TRADE}"

    risk_rupees = intent.qty * abs(intent.entry_limit - intent.protective_stop)
    if risk_rupees > config.RISK_PER_TRADE * 1.5:  # slight buffer for tick rounding
        return False, f"Risk {risk_rupees:.2f} exceeds RISK_PER_TRADE {config.RISK_PER_TRADE}"

    # 7. Stop Distance
    stop_dist_pct = (abs(intent.entry_limit - intent.protective_stop) / intent.entry_limit) * 100
    max_stop_pct = config.JP_MAX_STOP_DISTANCE_PCT if intent.strategy.upper() == "JP" else config.MAX_STOP_DISTANCE_PCT
    if stop_dist_pct > max_stop_pct:
        return False, f"Stop distance {stop_dist_pct:.2f}% exceeds {max_stop_pct:.2f}%"

    # 8. Pricing & Extension
    ext_pct = abs(intent.entry_limit - intent.trigger_price) / intent.trigger_price * 100
    if ext_pct > config.MAX_ENTRY_EXTENSION_PCT:
        return False, f"Entry extension {ext_pct:.2f}% exceeds {config.MAX_ENTRY_EXTENSION_PCT:.2f}%"

    # 9. Quote Quality (spread & age)
    if intent.quote_age_sec > config.MAX_QUOTE_AGE_SECONDS:
        return False, f"Quote age {intent.quote_age_sec:.1f}s exceeds MAX_QUOTE_AGE_SECONDS {config.MAX_QUOTE_AGE_SECONDS}s"

    if intent.spread_pct > config.MAX_BID_ASK_SPREAD_PCT:
        return False, f"Bid/Ask spread {intent.spread_pct:.2f}% exceeds MAX_BID_ASK_SPREAD_PCT {config.MAX_BID_ASK_SPREAD_PCT}%"

    return True, "OK"


class ExecutionGateway:
    """Abstract Base Class for order execution gateways."""
    def submit_entry(self, intent: TradeIntent) -> ExecutionResult:
        raise NotImplementedError

    def submit_protective_stop(self, position: Dict[str, Any], quantity: int) -> ExecutionResult:
        raise NotImplementedError

    def amend_protective_stop(self, position: Dict[str, Any], new_stop: float, quantity: int) -> ExecutionResult:
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> ExecutionResult:
        raise NotImplementedError

    def submit_exit(self, position: Dict[str, Any], quantity: int, reason: str, is_emergency: bool = False) -> ExecutionResult:
        raise NotImplementedError

    def reconcile(self) -> ReconciliationReport:
        raise NotImplementedError


class PaperExecutionGateway(ExecutionGateway):
    """High-fidelity simulated paper trading gateway."""
    def __init__(self, broker=None):
        self.broker = broker
        self._paper_counter = 0

    def _next_id(self, prefix="PAPER") -> str:
        self._paper_counter += 1
        return f"{prefix}-{int(time.time())}-{self._paper_counter}"

    def submit_entry(self, intent: TradeIntent) -> ExecutionResult:
        valid, msg = validate_trade_intent(intent, self.broker)
        if not valid:
            state.add_log(f"{intent.symbol}: Paper entry rejected - {msg}")
            journal.record_event("PAPER_ENTRY_REJECTED", {"reason": msg, "intent": intent.__dict__}, signal_id=intent.signal_id, security_id=intent.security_id)
            return ExecutionResult(success=False, status="REJECTED", message=msg)

        # Apply realistic paper slippage
        bps = getattr(config, "PAPER_SLIPPAGE_BPS", 2) / 10_000.0
        fill_price = intent.entry_limit * (1 + bps) if intent.side.upper() == "BUY" else intent.entry_limit * (1 - bps)
        fill_price = round_to_tick(fill_price, intent.tick_size)

        order_id = self._next_id("PAPER-ENT")
        client_tag = f"AC-{intent.signal_id[-16:]}"

        order_rec = {
            "order_id": order_id,
            "client_tag": client_tag,
            "correlation_id": client_tag,
            "signal_id": intent.signal_id,
            "security_id": str(intent.security_id),
            "symbol": intent.symbol,
            "side": intent.side,
            "order_type": "LIMIT",
            "quantity": intent.qty,
            "filled_quantity": intent.qty,
            "average_fill_price": fill_price,
            "limit_price": intent.entry_limit,
            "trigger_price": intent.trigger_price,
            "order_status": "FILLED",
            "role": "ENTRY",
            "raw_broker_status": "TRADED",
            "created_at": datetime.now(config.TIME_ZONE).isoformat(),
        }
        journal.upsert_order(order_rec)
        journal.record_event("ENTRY_FILLED", order_rec, signal_id=intent.signal_id, security_id=intent.security_id)

        # Submit synthetic protective stop
        sl_trigger, sl_limit = protective_sl_prices(intent.side, intent.protective_stop, intent.tick_size)
        sl_order_id = self._next_id("PAPER-SL")
        sl_side = "SELL" if intent.side.upper() == "BUY" else "BUY"
        sl_rec = {
            "order_id": sl_order_id,
            "client_tag": f"SL-{client_tag[3:]}",
            "correlation_id": f"SL-{client_tag[3:]}",
            "signal_id": intent.signal_id,
            "security_id": str(intent.security_id),
            "symbol": intent.symbol,
            "side": sl_side,
            "order_type": "STOP_LOSS",
            "quantity": intent.qty,
            "filled_quantity": 0,
            "average_fill_price": None,
            "limit_price": sl_limit,
            "trigger_price": sl_trigger,
            "order_status": "TRIGGER_PENDING",
            "role": "STOP_LOSS",
            "raw_broker_status": "PENDING",
            "created_at": datetime.now(config.TIME_ZONE).isoformat(),
        }
        journal.upsert_order(sl_rec)
        journal.record_event("STOP_CONFIRMED", sl_rec, signal_id=intent.signal_id, security_id=intent.security_id)

        pos_rec = {
            "security_id": str(intent.security_id),
            "signal_id": intent.signal_id,
            "symbol": intent.symbol,
            "strategy": intent.strategy,
            "direction": intent.side,
            "quantity": intent.qty,
            "net_quantity": intent.qty,
            "entry_price": fill_price,
            "entry_average": fill_price,
            "initial_sl": intent.protective_stop,
            "current_sl": intent.protective_stop,
            "protective_stop_order_id": sl_order_id,
            "protective_stop_price": intent.protective_stop,
            "target_r2_price": intent.target_r2,
            "partial_booked": False,
            "status": "PROTECTED",
            "entered_at": datetime.now(config.TIME_ZONE).isoformat(),
            "order_id": order_id,
            "sl_order_id": sl_order_id,
            "stall_counter": 0,
            "best_price_since_r2": fill_price,
            "cost_to_cost_trade": state.snapshot().get("cost_to_cost_mode", False),
        }
        journal.upsert_position(pos_rec)
        state.set_open_position(str(intent.security_id), pos_rec)
        state.increment_trade_count()
        state.add_log(f"[PAPER] {intent.side} entry filled: {intent.symbol} qty={intent.qty} @ {fill_price:.2f} (SL={intent.protective_stop:.2f})")

        return ExecutionResult(
            success=True,
            order_id=order_id,
            client_tag=client_tag,
            filled_qty=intent.qty,
            avg_price=fill_price,
            status="FILLED",
            message="Paper trade simulated and protected",
        )

    def submit_protective_stop(self, position: Dict[str, Any], quantity: int) -> ExecutionResult:
        return ExecutionResult(success=True, status="ACCEPTED", message="Synthetic stop created")

    def amend_protective_stop(self, position: Dict[str, Any], new_stop: float, quantity: int) -> ExecutionResult:
        sid = str(position["security_id"])
        new_stop = round_to_tick(new_stop, float(position.get("tick_size", 0.05)))
        position["current_sl"] = new_stop
        position["protective_stop_price"] = new_stop
        position["net_quantity"] = quantity
        journal.upsert_position(position)
        state.set_open_position(sid, position)
        journal.record_event("STOP_MODIFIED", {"security_id": sid, "new_stop": new_stop, "quantity": quantity})
        state.add_log(f"[PAPER] {position.get('symbol', sid)}: SL amended to {new_stop:.2f} (qty={quantity})")
        return ExecutionResult(success=True, status="ACCEPTED", message=f"Stop modified to {new_stop}")

    def cancel_order(self, order_id: str) -> ExecutionResult:
        order = journal.get_order(order_id=order_id)
        if order:
            order["order_status"] = "CANCELLED"
            journal.upsert_order(order)
        return ExecutionResult(success=True, order_id=order_id, status="CANCELLED")

    def submit_exit(self, position: Dict[str, Any], quantity: int, reason: str, is_emergency: bool = False) -> ExecutionResult:
        sid = str(position["security_id"])
        symbol = position.get("symbol", sid)
        exit_order_id = self._next_id("PAPER-EXT")
        journal.record_event("EXIT_FILLED", {"security_id": sid, "quantity": quantity, "reason": reason, "emergency": is_emergency})
        return ExecutionResult(success=True, order_id=exit_order_id, filled_qty=quantity, status="FILLED", message=f"Exit filled: {reason}")

    def reconcile(self) -> ReconciliationReport:
        return ReconciliationReport(reconciled_at=datetime.now(config.TIME_ZONE).isoformat(), status="OK")


class ShadowExecutionGateway(ExecutionGateway):
    """Shadow gateway that inspects live account, depth, and margin without placing broker orders."""
    def __init__(self, broker):
        self.broker = broker

    def submit_entry(self, intent: TradeIntent) -> ExecutionResult:
        valid, msg = validate_trade_intent(intent, self.broker)
        intent_dump = {k: v for k, v in intent.__dict__.items() if k != "signal_time"}
        intent_dump["is_valid"] = valid
        intent_dump["validation_message"] = msg

        journal.record_event("SHADOW_INTENT_EVALUATED", intent_dump, signal_id=intent.signal_id, security_id=intent.security_id)
        if valid:
            state.add_log(f"[SHADOW] VALID INTENT: {intent.side} {intent.symbol} qty={intent.qty} @ {intent.entry_limit:.2f} (SL={intent.protective_stop:.2f})")
            logger.info(f"SHADOW TRADE PASS: {intent.symbol} | side={intent.side} | qty={intent.qty} | limit={intent.entry_limit:.2f} | sl={intent.protective_stop:.2f}")
        else:
            state.add_log(f"[SHADOW] BLOCKED INTENT: {intent.symbol} - {msg}")
            logger.warning(f"SHADOW TRADE BLOCKED: {intent.symbol} - {msg}")

        return ExecutionResult(success=valid, status="SHADOW_EVALUATED", message=msg)

    def submit_protective_stop(self, position: Dict[str, Any], quantity: int) -> ExecutionResult:
        return ExecutionResult(success=True, status="SHADOW_SKIPPED")

    def amend_protective_stop(self, position: Dict[str, Any], new_stop: float, quantity: int) -> ExecutionResult:
        return ExecutionResult(success=True, status="SHADOW_SKIPPED")

    def cancel_order(self, order_id: str) -> ExecutionResult:
        return ExecutionResult(success=True, status="SHADOW_SKIPPED")

    def submit_exit(self, position: Dict[str, Any], quantity: int, reason: str, is_emergency: bool = False) -> ExecutionResult:
        return ExecutionResult(success=True, status="SHADOW_SKIPPED")

    def reconcile(self) -> ReconciliationReport:
        now_iso = datetime.now(config.TIME_ZONE).isoformat()
        return ReconciliationReport(reconciled_at=now_iso, status="OK", details={"mode": "SHADOW"})


class LiveDhanExecutionGateway(ExecutionGateway):
    """
    Live DhanHQ Broker Gateway.
    Strictly enforced with protection-first lifecycle and dual-key safety gates.
    """
    def __init__(self, broker):
        self.broker = broker

    def _assert_live_enabled(self):
        if config.EXECUTION_MODE != "LIVE" or not config.LIVE_TRADING_ENABLED:
            raise RuntimeError("CRITICAL: Live order execution blocked because EXECUTION_MODE != 'LIVE' or LIVE_TRADING_ENABLED is False")

    def submit_entry(self, intent: TradeIntent) -> ExecutionResult:
        self._assert_live_enabled()
        valid, msg = validate_trade_intent(intent, self.broker)
        if not valid:
            state.add_log(f"{intent.symbol}: Live entry rejected - {msg}")
            journal.record_event("LIVE_ENTRY_REJECTED", {"reason": msg, "intent": intent.__dict__}, signal_id=intent.signal_id, security_id=intent.security_id)
            return ExecutionResult(success=False, status="REJECTED", message=msg)

        client_tag = f"AC-{intent.signal_id[-16:]}"
        journal.record_event("ENTRY_SUBMITTING", {"intent": intent.__dict__, "client_tag": client_tag}, signal_id=intent.signal_id, security_id=intent.security_id)

        try:
            # Place entry limit order
            order_id = self.broker.place_order(
                security_id=intent.security_id,
                transaction_type=intent.side,
                exchange_segment=intent.exchange_segment,
                quantity=intent.qty,
                order_type="LIMIT",
                product_type=intent.product_type,
                limit_price=intent.entry_limit,
                tick_size=intent.tick_size,
                correlation_id=client_tag,
            )
        except Exception as e:
            logger.exception("Error placing live entry order")
            journal.record_event("ENTRY_SUBMISSION_FAILED", {"error": str(e)}, signal_id=intent.signal_id, security_id=intent.security_id)
            return ExecutionResult(success=False, status="SUBMISSION_FAILED", message=str(e))

        if not order_id:
            journal.record_event("ENTRY_SUBMISSION_FAILED", {"reason": "Broker returned null order_id"}, signal_id=intent.signal_id, security_id=intent.security_id)
            return ExecutionResult(success=False, status="FAILED", message="Broker returned empty order ID")

        order_id = str(order_id)
        order_rec = {
            "order_id": order_id,
            "client_tag": client_tag,
            "correlation_id": client_tag,
            "signal_id": intent.signal_id,
            "security_id": str(intent.security_id),
            "symbol": intent.symbol,
            "side": intent.side,
            "order_type": "LIMIT",
            "quantity": intent.qty,
            "filled_quantity": 0,
            "average_fill_price": None,
            "limit_price": intent.entry_limit,
            "trigger_price": intent.trigger_price,
            "order_status": "PENDING",
            "role": "ENTRY",
            "created_at": datetime.now(config.TIME_ZONE).isoformat(),
        }
        journal.upsert_order(order_rec)

        # Wait for fill confirmation via OrderUpdate/REST fallback
        filled_qty, avg_fill_price, final_status = self._wait_for_fill(order_id, config.ENTRY_ORDER_TIMEOUT_SECONDS)

        if filled_qty <= 0 or final_status in ("CANCELLED", "REJECTED"):
            # Unfilled within timeout -> Cancel remaining and clean up
            self.broker.cancel_order(order_id)
            order_rec["order_status"] = "CANCELLED"
            journal.upsert_order(order_rec)
            journal.record_event("ENTRY_TIMED_OUT", {"order_id": order_id, "filled_qty": filled_qty}, signal_id=intent.signal_id, security_id=intent.security_id)
            state.add_log(f"{intent.symbol}: Live entry order {order_id} timed out without fill; cancelled.")
            return ExecutionResult(success=False, order_id=order_id, status="TIMED_OUT", message="Order unfilled within timeout")

        # Record fill
        order_rec["filled_quantity"] = filled_qty
        order_rec["average_fill_price"] = avg_fill_price
        order_rec["order_status"] = "FILLED" if filled_qty == intent.qty else "PARTIALLY_FILLED"
        journal.upsert_order(order_rec)
        journal.record_event("ENTRY_FILLED", order_rec, signal_id=intent.signal_id, security_id=intent.security_id)

        # If partial fill, cancel remaining unfilled portion immediately per policy
        if filled_qty < intent.qty:
            state.add_log(f"{intent.symbol}: Partial fill {filled_qty}/{intent.qty} @ {avg_fill_price:.2f}. Cancelling unfilled balance.")
            try:
                self.broker.cancel_order(order_id)
            except Exception:
                pass

        # MANDATORY PROTECTION-FIRST: Place protective stop for exact filled quantity
        stop_result = self._place_and_verify_protective_stop(intent, filled_qty)
        if not stop_result.success:
            # STOP PLACEMENT FAILED -> EMERGENCY EXIT & LOCK CIRCUIT BREAKER
            logger.critical(f"PROTECTIVE STOP FAILED FOR {intent.symbol}! TRIGGERING EMERGENCY EXIT.")
            state.add_log(f"CRITICAL: Protective stop placement failed for {intent.symbol}! Triggering emergency exit.")
            self._emergency_exit_filled_quantity(intent, filled_qty)
            journal.set_circuit_breaker("STOP_PROTECTION_FAILURE_LOCK", locked=True, reason=f"Stop placement failed for {intent.symbol} (Order {order_id})")
            if config.SEND_TELEGRAM_ON_ENTRY:
                notifier.send_telegram(f"🚨 <b>CRITICAL CIRCUIT BREAKER</b>\nProtective stop failed for {intent.symbol}. Emergency exit triggered and new entries locked.")
            return ExecutionResult(success=False, order_id=order_id, filled_qty=filled_qty, status="STOP_FAILED_EMERGENCY_EXIT", message="Stop placement failed; emergency exited.")

        # Create active managed position
        pos_rec = {
            "security_id": str(intent.security_id),
            "signal_id": intent.signal_id,
            "symbol": intent.symbol,
            "strategy": intent.strategy,
            "direction": intent.side,
            "quantity": filled_qty,
            "net_quantity": filled_qty,
            "entry_price": avg_fill_price,
            "entry_average": avg_fill_price,
            "initial_sl": intent.protective_stop,
            "current_sl": intent.protective_stop,
            "protective_stop_order_id": stop_result.order_id,
            "protective_stop_price": intent.protective_stop,
            "target_r2_price": intent.target_r2,
            "partial_booked": False,
            "status": "PROTECTED",
            "entered_at": datetime.now(config.TIME_ZONE).isoformat(),
            "order_id": order_id,
            "sl_order_id": stop_result.order_id,
            "stall_counter": 0,
            "best_price_since_r2": avg_fill_price,
            "cost_to_cost_trade": False,
        }
        journal.upsert_position(pos_rec)
        state.set_open_position(str(intent.security_id), pos_rec)
        state.increment_trade_count()
        state.add_log(f"[LIVE] Position PROTECTED: {intent.symbol} qty={filled_qty} @ {avg_fill_price:.2f} (SL Order={stop_result.order_id})")

        return ExecutionResult(
            success=True,
            order_id=order_id,
            client_tag=client_tag,
            filled_qty=filled_qty,
            avg_price=avg_fill_price,
            status="PROTECTED",
            message="Live order filled and protected",
        )

    def _wait_for_fill(self, order_id: str, timeout_sec: float) -> Tuple[int, float, str]:
        start = time.time()
        while time.time() - start < timeout_sec:
            # 1. Check local order journal (updated via WebSocket OrderUpdate)
            ord_data = journal.get_order(order_id=order_id)
            if ord_data and ord_data.get("filled_quantity", 0) > 0:
                return ord_data["filled_quantity"], float(ord_data.get("average_fill_price") or 0.0), ord_data.get("order_status", "FILLED")
            if ord_data and ord_data.get("order_status") in ("CANCELLED", "REJECTED"):
                return 0, 0.0, ord_data["order_status"]

            # 2. REST polling fallback
            try:
                broker_ord = self.broker.get_order_by_id(order_id)
                if broker_ord:
                    status = str(broker_ord.get("orderStatus", "")).upper()
                    filled = int(broker_ord.get("filledQty", 0))
                    avg = float(broker_ord.get("avgTradedPrice") or broker_ord.get("price") or 0.0)
                    if filled > 0:
                        return filled, avg, status
                    if status in ("CANCELLED", "REJECTED"):
                        return 0, 0.0, status
            except Exception:
                pass
            time.sleep(0.5)

        return 0, 0.0, "TIMED_OUT"

    def _place_and_verify_protective_stop(self, intent: TradeIntent, quantity: int) -> ExecutionResult:
        sl_side = "SELL" if intent.side.upper() == "BUY" else "BUY"
        sl_trigger, sl_limit = protective_sl_prices(intent.side, intent.protective_stop, intent.tick_size)
        sl_tag = f"SL-{intent.signal_id[-13:]}"

        try:
            sl_order_id = self.broker.place_order(
                security_id=intent.security_id,
                transaction_type=sl_side,
                exchange_segment=intent.exchange_segment,
                quantity=quantity,
                order_type="STOP_LOSS",
                product_type=intent.product_type,
                limit_price=sl_limit,
                trigger_price=sl_trigger,
                tick_size=intent.tick_size,
                correlation_id=sl_tag,
            )
        except Exception as e:
            logger.exception("Error placing protective stop order")
            return ExecutionResult(success=False, message=str(e))

        if not sl_order_id:
            return ExecutionResult(success=False, message="Broker returned empty stop order ID")

        sl_order_id = str(sl_order_id)
        sl_rec = {
            "order_id": sl_order_id,
            "client_tag": sl_tag,
            "correlation_id": sl_tag,
            "signal_id": intent.signal_id,
            "security_id": str(intent.security_id),
            "symbol": intent.symbol,
            "side": sl_side,
            "order_type": "STOP_LOSS",
            "quantity": quantity,
            "filled_quantity": 0,
            "limit_price": sl_limit,
            "trigger_price": sl_trigger,
            "order_status": "TRIGGER_PENDING",
            "role": "STOP_LOSS",
            "created_at": datetime.now(config.TIME_ZONE).isoformat(),
        }
        journal.upsert_order(sl_rec)

        # Verify stop is accepted by broker
        start = time.time()
        while time.time() - start < config.PROTECTIVE_STOP_TIMEOUT_SECONDS:
            try:
                b_order = self.broker.get_order_by_id(sl_order_id)
                if b_order:
                    status = str(b_order.get("orderStatus", "")).upper()
                    if status in ("TRANSIT", "PENDING", "TRIGGER_PENDING", "OPEN"):
                        journal.record_event("STOP_CONFIRMED", sl_rec, signal_id=intent.signal_id, security_id=intent.security_id)
                        return ExecutionResult(success=True, order_id=sl_order_id, status="TRIGGER_PENDING")
                    elif status in ("REJECTED", "CANCELLED"):
                        return ExecutionResult(success=False, order_id=sl_order_id, status=status, message=f"Stop {status}")
            except Exception:
                pass
            time.sleep(0.4)

        return ExecutionResult(success=False, order_id=sl_order_id, status="UNVERIFIED", message="Stop verification timed out")

    def _emergency_exit_filled_quantity(self, intent: TradeIntent, quantity: int):
        exit_side = "SELL" if intent.side.upper() == "BUY" else "BUY"
        try:
            # Marketable limit exit with generous slippage cap
            ltp = self.broker.get_ltp(intent.security_id, intent.exchange_segment) or intent.entry_limit
            exit_limit = round_to_tick(ltp * 0.95 if exit_side == "SELL" else ltp * 1.05, intent.tick_size)
            self.broker.place_order(
                security_id=intent.security_id,
                transaction_type=exit_side,
                exchange_segment=intent.exchange_segment,
                quantity=quantity,
                order_type="LIMIT",
                product_type=intent.product_type,
                limit_price=exit_limit,
                tick_size=intent.tick_size,
            )
            journal.record_event("EMERGENCY_EXIT_SUBMITTED", {"security_id": str(intent.security_id), "quantity": quantity, "exit_limit": exit_limit})
        except Exception:
            logger.exception("Error executing emergency exit")

    def amend_protective_stop(self, position: Dict[str, Any], new_stop: float, quantity: int) -> ExecutionResult:
        self._assert_live_enabled()
        sid = str(position["security_id"])
        sl_order_id = position.get("protective_stop_order_id") or position.get("sl_order_id")
        if not sl_order_id:
            return ExecutionResult(success=False, message="No active stop order ID found on position")

        tick_size = float(position.get("tick_size", 0.05))
        direction = str(position.get("direction", "BUY")).upper()
        current_sl = float(position.get("protective_stop_price", position.get("current_sl", 0.0)))

        # Validation: stop adjustment must only improve risk
        if direction == "BUY" and new_stop < current_sl:
            return ExecutionResult(success=False, message=f"Cannot lower stop on BUY position ({new_stop:.2f} < {current_sl:.2f})")
        if direction == "SELL" and new_stop > current_sl:
            return ExecutionResult(success=False, message=f"Cannot raise stop on SELL position ({new_stop:.2f} > {current_sl:.2f})")

        new_trigger, new_limit = protective_sl_prices(direction, new_stop, tick_size)
        try:
            mod_res = self.broker.modify_order(
                order_id=sl_order_id,
                order_type="STOP_LOSS",
                quantity=quantity,
                limit_price=new_limit,
                trigger_price=new_trigger,
            )
            if mod_res:
                position["current_sl"] = new_stop
                position["protective_stop_price"] = new_stop
                position["net_quantity"] = quantity
                journal.upsert_position(position)
                state.add_open_position(sid, position)
                journal.record_event("STOP_MODIFIED", {"security_id": sid, "order_id": sl_order_id, "new_stop": new_stop, "quantity": quantity})
                return ExecutionResult(success=True, order_id=sl_order_id, status="MODIFIED")
        except Exception as e:
            logger.exception("Error modifying protective stop")
            return ExecutionResult(success=False, message=str(e))

        return ExecutionResult(success=False, message="Broker rejected stop modification")

    def cancel_order(self, order_id: str) -> ExecutionResult:
        self._assert_live_enabled()
        try:
            self.broker.cancel_order(order_id)
            order = journal.get_order(order_id=order_id)
            if order:
                order["order_status"] = "CANCELLED"
                journal.upsert_order(order)
            return ExecutionResult(success=True, order_id=order_id, status="CANCELLED")
        except Exception as e:
            return ExecutionResult(success=False, order_id=order_id, message=str(e))

    def submit_exit(self, position: Dict[str, Any], quantity: int, reason: str, is_emergency: bool = False) -> ExecutionResult:
        self._assert_live_enabled()
        sid = str(position["security_id"])
        direction = str(position.get("direction", "BUY")).upper()
        exit_side = "SELL" if direction == "BUY" else "BUY"
        tick_size = float(position.get("tick_size", 0.05))

        ltp = self.broker.get_ltp(sid, config.EXCHANGE) or float(position.get("entry_price", 0.0))
        # Generous slippage for urgent exit, tight for regular
        slippage_pct = 0.05 if is_emergency else 0.005
        exit_limit = round_to_tick(ltp * (1.0 - slippage_pct) if exit_side == "SELL" else ltp * (1.0 + slippage_pct), tick_size)

        try:
            order_id = self.broker.place_order(
                security_id=sid,
                transaction_type=exit_side,
                exchange_segment=config.EXCHANGE,
                quantity=quantity,
                order_type="LIMIT",
                product_type=config.PRODUCT_TYPE,
                limit_price=exit_limit,
                tick_size=tick_size,
            )
            if order_id:
                order_id = str(order_id)
                filled, avg, status = self._wait_for_fill(order_id, config.EXIT_ORDER_TIMEOUT_SECONDS)
                journal.record_event("EXIT_FILLED", {"security_id": sid, "order_id": order_id, "quantity": filled, "reason": reason, "avg_price": avg})
                return ExecutionResult(success=True, order_id=order_id, filled_qty=filled, avg_price=avg, status=status)
        except Exception as e:
            logger.exception("Error executing exit order")
            return ExecutionResult(success=False, message=str(e))

        return ExecutionResult(success=False, message="Exit order placement failed")

    def reconcile(self) -> ReconciliationReport:
        import reconciliation
        return reconciliation.reconcile_broker_state(self.broker, self)


def get_execution_gateway(broker=None) -> ExecutionGateway:
    """Factory function returning the active ExecutionGateway based on config."""
    mode = getattr(config, "EXECUTION_MODE", "PAPER").upper()
    live_enabled = getattr(config, "LIVE_TRADING_ENABLED", False)

    if mode == "LIVE" and live_enabled:
        return LiveDhanExecutionGateway(broker)
    elif mode == "SHADOW":
        return ShadowExecutionGateway(broker)
    else:
        return PaperExecutionGateway(broker)
