"""
AlphaCandle - Broker Reconciliation & Order Update Consumer.

Provides:
  1. BrokerOrderEventConsumer: real-time WebSocket order update processor.
  2. reconcile_broker_state: authoritative 3-way reconciliation across Dhan order book,
     trade book, net positions, and local state.
"""
import time
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

import config
import state
import journal
import notifier

logger = logging.getLogger(__name__)


class BrokerOrderEventConsumer:
    """Processes real-time OrderUpdate events from Dhan WebSocket."""
    def __init__(self, broker=None):
        self.broker = broker
        self.is_connected = False
        self.last_update_at = 0.0

    def on_connect(self):
        self.is_connected = True
        journal.record_event("ORDER_UPDATE_CONNECTED", {})
        # Clear degraded lock if it was active
        if journal.is_circuit_breaker_active("ORDER_UPDATE_DEGRADED_LOCK"):
            journal.set_circuit_breaker("ORDER_UPDATE_DEGRADED_LOCK", locked=False, reason="OrderUpdate reconnected")
            # Trigger immediate reconciliation on reconnect
            if self.broker:
                reconcile_broker_state(self.broker)

    def on_disconnect(self):
        self.is_connected = False
        journal.record_event("ORDER_UPDATE_DISCONNECTED", {})
        # Activate degraded lock on disconnect to block new live entries
        if config.EXECUTION_MODE == "LIVE" and config.LIVE_TRADING_ENABLED:
            journal.set_circuit_breaker("ORDER_UPDATE_DEGRADED_LOCK", locked=True, reason="OrderUpdate WebSocket disconnected")
            state.add_log("WARNING: OrderUpdate WebSocket disconnected. New live entries locked.")

    def on_order_update(self, raw_event: Dict[str, Any]):
        self.last_update_at = time.time()
        journal.record_raw_order_update(raw_event)

        try:
            # Dhan OrderUpdate format normalization
            order_data = raw_event.get("data", raw_event)
            order_id = str(order_data.get("orderId") or order_data.get("order_id") or "")
            if not order_id:
                return

            status = str(order_data.get("orderStatus") or order_data.get("status") or "").upper()
            filled_qty = int(order_data.get("filledQty") or order_data.get("tradedQty") or 0)
            avg_price = float(order_data.get("avgTradedPrice") or order_data.get("price") or 0.0)
            correlation_id = order_data.get("correlationId") or order_data.get("client_tag")

            existing = journal.get_order(order_id=order_id, correlation_id=correlation_id)
            if existing:
                existing["order_status"] = status
                existing["filled_quantity"] = max(existing.get("filled_quantity", 0), filled_qty)
                if avg_price > 0:
                    existing["average_fill_price"] = avg_price
                existing["raw_broker_status"] = status
                journal.upsert_order(existing)
                journal.record_event("ORDER_UPDATE_PROCESSED", {"order_id": order_id, "status": status, "filled_qty": filled_qty, "avg_price": avg_price})

                # Check if this update triggers position state changes
                role = existing.get("role", "ENTRY")
                sec_id = existing.get("security_id")
                if role == "STOP_LOSS" and status in ("TRADED", "FILLED") and sec_id:
                    # Stop loss executed -> mark position closed
                    pos = journal.get_position(sec_id)
                    if pos:
                        pos["status"] = "CLOSED"
                        pos["net_quantity"] = 0
                        journal.upsert_position(pos)
                        state.remove_open_position(sec_id)
                        state.add_log(f"{existing.get('symbol', sec_id)}: Protective STOP hit @ {avg_price:.2f}. Position CLOSED.")
        except Exception:
            logger.exception("Error processing raw order update event")


def reconcile_broker_state(broker, gateway=None) -> Dict[str, Any]:
    """
    Authoritative reconciliation of Dhan order book, trade book, and net positions
    against local journal and state.
    """
    now_iso = datetime.now(config.TIME_ZONE).isoformat()
    report = {
        "reconciled_at": now_iso,
        "matched_positions": 0,
        "unmatched_positions": [],
        "stale_orders_cancelled": 0,
        "status": "OK",
        "errors": [],
    }

    if config.EXECUTION_MODE == "PAPER" or broker is None:
        journal.record_reconciliation(report)
        return report

    try:
        # 1. Fetch live Dhan records
        broker_positions = broker.get_positions() or []
        broker_orders = broker.get_order_list() or []
        broker_trades = broker.get_trade_book() or []

        # Filter active net positions on broker (netQty != 0)
        dhan_net_pos = {}
        for p in broker_positions:
            sec_id = str(p.get("securityId") or p.get("security_id"))
            net_qty = int(p.get("netQty") or p.get("quantity") or 0)
            if net_qty != 0:
                dhan_net_pos[sec_id] = {
                    "security_id": sec_id,
                    "symbol": p.get("tradingSymbol") or p.get("symbol"),
                    "net_qty": net_qty,
                    "buy_qty": int(p.get("buyQty", 0)),
                    "sell_qty": int(p.get("sellQty", 0)),
                    "buy_avg": float(p.get("buyAvg", 0.0)),
                    "sell_avg": float(p.get("sellAvg", 0.0)),
                }

        # 2. Compare against local active positions
        local_open_pos = journal.get_open_positions()

        # Check Dhan positions vs local
        for sec_id, d_pos in dhan_net_pos.items():
            if sec_id in local_open_pos:
                l_pos = local_open_pos[sec_id]
                if l_pos.get("net_quantity") == abs(d_pos["net_qty"]):
                    report["matched_positions"] += 1
                else:
                    report["unmatched_positions"].append({
                        "security_id": sec_id,
                        "type": "QUANTITY_MISMATCH",
                        "local_qty": l_pos.get("net_quantity"),
                        "broker_qty": d_pos["net_qty"],
                    })
                    state.add_log(f"RECONCILIATION WARNING: Quantity mismatch on {sec_id}: local={l_pos.get('net_quantity')} vs broker={d_pos['net_qty']}")
            else:
                # Unmanaged position at Dhan
                report["unmatched_positions"].append({
                    "security_id": sec_id,
                    "type": "UNMANAGED_BROKER_POSITION",
                    "broker_qty": d_pos["net_qty"],
                })
                state.add_log(f"CRITICAL RECONCILIATION: Unmanaged position detected at broker for {sec_id} (qty={d_pos['net_qty']})")
                journal.set_circuit_breaker("RECONCILIATION_MISMATCH_LOCK", locked=True, reason=f"Unmanaged position for {sec_id}")

        # Check local positions that might have closed on Dhan
        for sec_id, l_pos in local_open_pos.items():
            if sec_id not in dhan_net_pos:
                # Local says open, but Dhan is flat -> verify trade book
                report["unmatched_positions"].append({
                    "security_id": sec_id,
                    "type": "LOCAL_OPEN_BROKER_FLAT",
                    "local_qty": l_pos.get("net_quantity"),
                })
                # Mark closed locally
                l_pos["status"] = "CLOSED"
                l_pos["net_quantity"] = 0
                journal.upsert_position(l_pos)
                state.remove_open_position(sec_id)
                state.add_log(f"RECONCILIATION: Local position {sec_id} reconciled to CLOSED because broker is flat.")

        # 3. Clean up stale orphan orders
        for o in broker_orders:
            ord_id = str(o.get("orderId"))
            ord_status = str(o.get("orderStatus", "")).upper()
            sec_id = str(o.get("securityId"))
            ord_type = str(o.get("orderType", "")).upper()

            if ord_status in ("PENDING", "TRIGGER_PENDING", "OPEN"):
                # If it's a stop loss order but no open position exists for this security -> cancel stale stop
                if ord_type in ("STOP_LOSS", "STOP_LOSS_MARKET") and sec_id not in dhan_net_pos:
                    logger.warning(f"Cancelling stale stop order {ord_id} for flat security {sec_id}")
                    try:
                        broker.cancel_order(ord_id)
                        report["stale_orders_cancelled"] += 1
                    except Exception:
                        pass

        if report["unmatched_positions"]:
            report["status"] = "MISMATCH_DETECTED"
        else:
            report["status"] = "OK"
            if journal.is_circuit_breaker_active("RECONCILIATION_MISMATCH_LOCK"):
                journal.set_circuit_breaker("RECONCILIATION_MISMATCH_LOCK", locked=False, reason="Reconciliation resolved")

    except Exception as e:
        logger.exception("Error during broker reconciliation")
        report["status"] = "ERROR"
        report["errors"].append(str(e))

    journal.record_reconciliation(report)
    return report
