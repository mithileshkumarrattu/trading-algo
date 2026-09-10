"""
Comprehensive Test Suite for Live Execution Safety, Broker Reconciliation,
Protective Stop Lifecycle, Risk Gate Enforcement, and Single-Flight Caching.
"""
import time
import math
import threading
from decimal import Decimal
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import config
import state
import journal
import timing
import reconciliation
from execution import (
    TradeIntent, ExecutionResult, PaperExecutionGateway,
    LiveDhanExecutionGateway, ShadowExecutionGateway,
    round_to_tick, protective_sl_prices, derive_entry_limit,
    validate_trade_intent
)
from engine import calc_quantity, manage_open_position
import main


@pytest.fixture(autouse=True)
def reset_test_state(monkeypatch):
    """Reset state and database before each test."""
    monkeypatch.setattr(timing, "is_entry_allowed", lambda: True)
    state.reset_daily()
    state.update({
        "open_positions": {},
        "daily_trade_count": 0,
        "daily_pnl": 0.0,
        "websocket_connected": True,
        "livefeed_coverage": 100,
    })
    journal.reset_circuit_breakers()
    # Clean positions and orders in SQLite
    journal._init_db()
    with journal._DB_LOCK:
        conn = journal.sqlite3.connect(journal._DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM orders")
        cur.execute("DELETE FROM positions")
        cur.execute("DELETE FROM execution_events")
        conn.commit()
        conn.close()
    yield


# ============================================================================
# 1. PRICE & TICK MATH TESTS
# ============================================================================

def test_tick_size_rounding():
    assert round_to_tick(100.024, 0.05) == 100.00
    assert round_to_tick(100.026, 0.05) == 100.05
    assert round_to_tick(692.14, 0.05) == 692.15
    assert round_to_tick(692.12, 0.05) == 692.10


def test_protective_sl_prices_directionality():
    # Long BUY position -> Stop order is SELL. Limit must be BELOW trigger
    trig_buy, lim_buy = protective_sl_prices("BUY", stop_reference=100.0, tick_size=0.05, limit_buffer_ticks=2)
    assert trig_buy == 100.0
    assert lim_buy == 99.90
    assert lim_buy < trig_buy

    # Short SELL position -> Stop order is BUY. Limit must be ABOVE trigger
    trig_sell, lim_sell = protective_sl_prices("SELL", stop_reference=100.0, tick_size=0.05, limit_buffer_ticks=2)
    assert trig_sell == 100.0
    assert lim_sell == 100.10
    assert lim_sell > trig_sell


def test_derive_entry_limit_capping():
    # BUY trigger = 100.0, max ext = 0.15% -> max limit = 100.15
    buy_limit = derive_entry_limit("BUY", trigger=100.0, best_bid=99.95, best_ask=100.05, tick_size=0.05, max_extension_pct=0.15)
    assert buy_limit == 100.05

    # If ask is extended beyond 100.15 (e.g. 100.30), limit is capped at 100.15
    buy_limit_capped = derive_entry_limit("BUY", trigger=100.0, best_bid=100.10, best_ask=100.30, tick_size=0.05, max_extension_pct=0.15)
    assert buy_limit_capped == 100.15

    # SELL trigger = 100.0, max ext = 0.15% -> min limit = 99.85
    sell_limit = derive_entry_limit("SELL", trigger=100.0, best_bid=99.90, best_ask=100.05, tick_size=0.05, max_extension_pct=0.15)
    assert sell_limit == 99.90


# ============================================================================
# 2. POSITION SIZING & RISK GATES TESTS
# ============================================================================

def test_calc_quantity_risk_and_notional_caps():
    # Risk budget = 50, SL distance = 2.0 -> Risk qty = 25
    # Stock price = 100, Max notional = 5000 -> Notional qty = 50 -> min is 25
    q1 = calc_quantity(entry_price=100.0, sl_price=98.0)
    assert q1 == 25

    # Stock price = 1000, SL distance = 5.0 -> Risk qty = 10
    # Max notional = 5000 / 1000 = 5 -> Notional qty = 5 -> min is 5
    q2 = calc_quantity(entry_price=1000.0, sl_price=995.0)
    assert q2 == 5


def test_validate_trade_intent_stale_quote_and_spread():
    intent = TradeIntent(
        signal_id="ALPHA_1001_BUY_20260910T100000",
        strategy="ALPHA",
        security_id="1001",
        symbol="TESTSTOCK",
        side="BUY",
        exchange_segment="NSE_EQ",
        product_type="INTRADAY",
        qty=10,
        entry_limit=100.05,
        trigger_price=100.0,
        protective_stop=99.50,
        target_r2=101.10,
        quote_age_sec=0.5,
        spread_pct=0.05,
    )

    valid, msg = validate_trade_intent(intent)
    assert valid is True

    # Stale quote > 2.0s
    intent.quote_age_sec = 3.5
    valid, msg = validate_trade_intent(intent)
    assert valid is False
    assert "Quote age" in msg

    # Wide spread > 0.10%
    intent.quote_age_sec = 0.5
    intent.spread_pct = 0.25
    valid, msg = validate_trade_intent(intent)
    assert valid is False
    assert "spread" in msg


def test_validate_trade_intent_daily_loss_and_circuit_breaker():
    intent = TradeIntent(
        signal_id="ALPHA_1002_BUY_20260910T100000",
        strategy="ALPHA",
        security_id="1002",
        symbol="TESTSTOCK",
        side="BUY",
        exchange_segment="NSE_EQ",
        product_type="INTRADAY",
        qty=10,
        entry_limit=100.05,
        trigger_price=100.0,
        protective_stop=99.50,
        target_r2=101.10,
    )

    # Trigger circuit breaker
    journal.set_circuit_breaker("DAILY_LOSS_LOCK", locked=True, reason="Daily loss limit")
    valid, msg = validate_trade_intent(intent)
    assert valid is False
    assert "Circuit breaker active" in msg

    journal.set_circuit_breaker("DAILY_LOSS_LOCK", locked=False)
    valid, msg = validate_trade_intent(intent)
    assert valid is True


# ============================================================================
# 3. IDEMPOTENCY & DUPLICATE SIGNAL REJECTION
# ============================================================================

def test_duplicate_signal_idempotency():
    intent = TradeIntent(
        signal_id="ALPHA_1003_BUY_20260910T100000",
        strategy="ALPHA",
        security_id="1003",
        symbol="TESTSTOCK",
        side="BUY",
        exchange_segment="NSE_EQ",
        product_type="INTRADAY",
        qty=10,
        entry_limit=100.05,
        trigger_price=100.0,
        protective_stop=99.50,
        target_r2=101.10,
    )

    gateway = PaperExecutionGateway()
    res1 = gateway.submit_entry(intent)
    assert res1.success is True

    # Second submission with the exact same signal_id
    res2 = gateway.submit_entry(intent)
    assert res2.success is False
    assert "idempotency block" in res2.message or "already open" in res2.message


# ============================================================================
# 4. LIVE DHAN GATEWAY: ENTRY TIMEOUT & PROTECTION-FIRST
# ============================================================================

def test_live_entry_unfilled_times_out_and_cancels():
    mock_broker = MagicMock()
    mock_broker.place_order.return_value = "ORD-99901"
    mock_broker.get_order_by_id.return_value = {
        "orderId": "ORD-99901",
        "orderStatus": "PENDING",
        "filledQty": 0,
        "avgTradedPrice": 0.0,
    }

    intent = TradeIntent(
        signal_id="ALPHA_1004_BUY_20260910T100000",
        strategy="ALPHA",
        security_id="1004",
        symbol="TESTSTOCK",
        side="BUY",
        exchange_segment="NSE_EQ",
        product_type="INTRADAY",
        qty=10,
        entry_limit=100.05,
        trigger_price=100.0,
        protective_stop=99.50,
        target_r2=101.10,
    )

    with patch.object(config, "EXECUTION_MODE", "LIVE"), \
         patch.object(config, "LIVE_TRADING_ENABLED", True), \
         patch.object(config, "ENTRY_ORDER_TIMEOUT_SECONDS", 0.5):
        gateway = LiveDhanExecutionGateway(mock_broker)
        res = gateway.submit_entry(intent)

        assert res.success is False
        assert res.status == "TIMED_OUT"
        # Verify cancel order was called on timeout
        mock_broker.cancel_order.assert_called_with("ORD-99901")
        # Ensure position was NOT marked open
        assert "1004" not in state.snapshot()["open_positions"]


def test_live_entry_stop_rejected_triggers_emergency_exit_and_lock():
    mock_broker = MagicMock()
    # Entry succeeds and fills
    mock_broker.place_order.side_effect = ["ORD-ENT-1", "ORD-SL-1", "ORD-EMERG-1"]
    mock_broker.get_order_by_id.side_effect = [
        {"orderId": "ORD-ENT-1", "orderStatus": "FILLED", "filledQty": 10, "avgTradedPrice": 100.0},
        {"orderId": "ORD-SL-1", "orderStatus": "REJECTED"},
    ]
    mock_broker.get_ltp.return_value = 99.80

    intent = TradeIntent(
        signal_id="ALPHA_1005_BUY_20260910T100000",
        strategy="ALPHA",
        security_id="1005",
        symbol="TESTSTOCK",
        side="BUY",
        exchange_segment="NSE_EQ",
        product_type="INTRADAY",
        qty=10,
        entry_limit=100.05,
        trigger_price=100.0,
        protective_stop=99.50,
        target_r2=101.10,
    )

    with patch.object(config, "EXECUTION_MODE", "LIVE"), \
         patch.object(config, "LIVE_TRADING_ENABLED", True), \
         patch.object(config, "PROTECTIVE_STOP_TIMEOUT_SECONDS", 0.5):
        gateway = LiveDhanExecutionGateway(mock_broker)
        res = gateway.submit_entry(intent)

        assert res.success is False
        assert res.status == "STOP_FAILED_EMERGENCY_EXIT"
        # Verify circuit breaker was locked
        assert journal.is_circuit_breaker_active("STOP_PROTECTION_FAILURE_LOCK") is True


def test_live_partial_fill_protects_only_filled_quantity():
    mock_broker = MagicMock()
    mock_broker.place_order.side_effect = ["ORD-ENT-2", "ORD-SL-2"]
    # Partial fill of 6 out of 10
    mock_broker.get_order_by_id.side_effect = [
        {"orderId": "ORD-ENT-2", "orderStatus": "PARTIALLY_FILLED", "filledQty": 6, "avgTradedPrice": 100.0},
        {"orderId": "ORD-SL-2", "orderStatus": "TRIGGER_PENDING", "filledQty": 0},
    ]

    intent = TradeIntent(
        signal_id="ALPHA_1006_BUY_20260910T100000",
        strategy="ALPHA",
        security_id="1006",
        symbol="TESTSTOCK",
        side="BUY",
        exchange_segment="NSE_EQ",
        product_type="INTRADAY",
        qty=10,
        entry_limit=100.05,
        trigger_price=100.0,
        protective_stop=99.50,
        target_r2=101.10,
    )

    with patch.object(config, "EXECUTION_MODE", "LIVE"), \
         patch.object(config, "LIVE_TRADING_ENABLED", True), \
         patch.object(config, "ENTRY_ORDER_TIMEOUT_SECONDS", 0.5), \
         patch.object(config, "PROTECTIVE_STOP_TIMEOUT_SECONDS", 0.5):
        gateway = LiveDhanExecutionGateway(mock_broker)
        res = gateway.submit_entry(intent)

        assert res.success is True
        assert res.filled_qty == 6
        # Check that remaining entry portion was cancelled
        mock_broker.cancel_order.assert_called_with("ORD-ENT-2")
        # Check active position net quantity is 6
        pos = state.snapshot()["open_positions"]["1006"]
        assert pos["quantity"] == 6
        assert pos["net_quantity"] == 6


# ============================================================================
# 5. TARGET PARTIAL BOOKING & STOP RESIZING
# ============================================================================

def test_target_partial_exit_resizes_stop_and_moves_breakeven():
    mock_broker = MagicMock()
    mock_broker.get_ltp.return_value = 104.0

    # Start with open paper position of qty 10
    pos = {
        "security_id": "1007",
        "symbol": "TESTSTOCK",
        "strategy": "ALPHA",
        "transaction_type": "BUY",
        "direction": "BUY",
        "quantity": 10,
        "remaining_qty": 10,
        "net_quantity": 10,
        "entry_price": 100.0,
        "entry_average": 100.0,
        "initial_sl": 98.0,
        "current_sl": 98.0,
        "target_r2_price": 104.0,
        "partial_booked": False,
        "sl_order_id": "SL-1007",
        "protective_stop_order_id": "SL-1007",
        "tick_size": 0.05,
    }
    state.set_open_position("1007", pos)
    journal.upsert_position(pos)

    # Manage position at LTP 104.0 (hits target R2)
    manage_open_position(mock_broker, "1007", ltp=104.0)

    updated_pos = state.snapshot()["open_positions"]["1007"]
    # 50% booked -> 5 remaining
    assert updated_pos["remaining_qty"] == 5
    assert updated_pos["partial_booked"] is True
    # Stop moved to breakeven (entry price 100.0)
    assert updated_pos["current_sl"] == 100.0


# ============================================================================
# 6. BROKER RECONCILIATION TESTS
# ============================================================================

def test_reconciliation_detects_unmanaged_position_and_locks():
    mock_broker = MagicMock()
    mock_broker.get_positions.return_value = [
        {"securityId": "1008", "symbol": "ORPHAN_STOCK", "netQty": 20, "buyQty": 20, "sellQty": 0}
    ]
    mock_broker.get_order_list.return_value = []
    mock_broker.get_trade_book.return_value = []

    with patch.object(config, "EXECUTION_MODE", "LIVE"):
        report = reconciliation.reconcile_broker_state(mock_broker)
        assert report["status"] == "MISMATCH_DETECTED"
        assert len(report["unmatched_positions"]) == 1
        assert report["unmatched_positions"][0]["type"] == "UNMANAGED_BROKER_POSITION"
        # Circuit breaker engaged
        assert journal.is_circuit_breaker_active("RECONCILIATION_MISMATCH_LOCK") is True


def test_reconciliation_cleans_local_position_if_broker_flat():
    mock_broker = MagicMock()
    mock_broker.get_positions.return_value = []  # Broker has 0 positions
    mock_broker.get_order_list.return_value = []
    mock_broker.get_trade_book.return_value = []

    pos = {
        "security_id": "1009",
        "symbol": "GHOST_STOCK",
        "strategy": "ALPHA",
        "direction": "BUY",
        "quantity": 10,
        "net_quantity": 10,
        "entry_price": 100.0,
        "status": "OPEN",
    }
    journal.upsert_position(pos)
    state.set_open_position("1009", pos)

    with patch.object(config, "EXECUTION_MODE", "LIVE"):
        report = reconciliation.reconcile_broker_state(mock_broker)
        assert report["status"] == "MISMATCH_DETECTED"
        # Local position is cleaned to CLOSED
        assert "1009" not in state.snapshot()["open_positions"]
        assert journal.get_position("1009")["status"] == "CLOSED"


# ============================================================================
# 7. SINGLE-FLIGHT CACHE FETCHING CONCURRENCY TEST
# ============================================================================

def test_single_flight_cache_duplicate_fetch_prevention():
    mock_broker = MagicMock()
    fetch_count = [0]
    lock = threading.Lock()

    def fake_intraday_candles(*args, **kwargs):
        with lock:
            fetch_count[0] += 1
        time.sleep(0.1)  # Simulate network latency
        import pandas as pd
        return pd.DataFrame([{"timestamp": datetime.now(), "close": 100.0}])

    mock_broker.get_intraday_candles.side_effect = fake_intraday_candles

    main._candle_cache.clear()
    main._cache_inflight.clear()

    # Launch 6 concurrent worker threads for the same uncached symbol
    threads = []
    results = [None] * 6

    def worker(idx):
        res = main.get_cached_candles(
            broker=mock_broker,
            security_id=9999,
            prev_trade_date=datetime.now().date(),
            timeframe=1,
            ttl_seconds=15,
        )
        results[idx] = res

    for i in range(6):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # All 6 threads must receive the valid candle data
    for r in results:
        assert r is not None and not r.empty

    # EXACTLY ONE underlying REST call must have been issued
    assert fetch_count[0] == 1
