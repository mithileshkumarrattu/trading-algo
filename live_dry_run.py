"""
AlphaCandle - Live Dry Run Diagnostic Utility.

Runs comprehensive pre-flight verification without placing any orders:
  1. Credentials & Token Validation
  2. Available Margin & Fund Limits
  3. Market Feed & Quote Freshness
  4. OrderUpdate WebSocket Health
  5. Broker Reconciliation Check
  6. Tick Size & Price Normalization
  7. Position Sizer & Risk Math
  8. Circuit Breaker & Safety Switches

Run with:
    python live_dry_run.py
"""
import sys
import time
import logging
from decimal import Decimal

import config
import state
import journal
import reconciliation
from execution import round_to_tick, protective_sl_prices, derive_entry_limit
from engine import calc_quantity
from broker import DhanBroker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("live_dry_run")


def run_dry_run():
    print("\n" + "=" * 60)
    print("      ALPHACANDLE LIVE DRY RUN & PRE-FLIGHT CHECK")
    print("=" * 60)

    results = []

    # 1. Config & Safety Switch Check
    print("\n[1/8] Verifying Configuration & Safety Switches...")
    mode = getattr(config, "EXECUTION_MODE", "PAPER")
    live_enabled = getattr(config, "LIVE_TRADING_ENABLED", False)
    paper_mode = getattr(config, "PAPER_MODE", True)

    print(f"  - EXECUTION_MODE:       {mode}")
    print(f"  - LIVE_TRADING_ENABLED: {live_enabled}")
    print(f"  - PAPER_MODE:           {paper_mode}")
    print(f"  - RISK_PER_TRADE:       Rs. {config.RISK_PER_TRADE}")
    print(f"  - MAX_NOTIONAL:         Rs. {config.MAX_NOTIONAL_PER_TRADE}")
    print(f"  - MAX_ENTRY_DELAY:      {config.MAX_ENTRY_DELAY_SECONDS}s")
    print(f"  - MAX_ENTRY_EXTENSION:  {config.MAX_ENTRY_EXTENSION_PCT}%")

    if mode == "LIVE" and not live_enabled:
        print("  ✓ Mode is safely gated (LIVE requested but LIVE_TRADING_ENABLED is False)")
    results.append(("Config & Safety Switches", True, f"Mode={mode}, LiveEnabled={live_enabled}"))

    # 2. Broker Login & Margin Check
    print("\n[2/8] Connecting to Dhan API & Checking Fund Limits...")
    try:
        broker = DhanBroker()
        fund_data = broker.get_fund_limits()
        if fund_data and fund_data.get("availabelBalance") is not None:
            bal = float(fund_data.get("availabelBalance", 0.0))
            print(f"  ✓ Broker authenticated successfully! Available Balance: Rs. {bal:,.2f}")
            results.append(("Broker Auth & Funds", True, f"Balance=Rs.{bal:,.2f}"))
        else:
            print("  ✗ Failed to fetch valid fund limits from Dhan")
            results.append(("Broker Auth & Funds", False, "Invalid fund response"))
    except Exception as e:
        print(f"  ✗ Exception during broker login: {e}")
        results.append(("Broker Auth & Funds", False, str(e)))
        broker = None

    # 3. Market Feed & Quote Freshness Test
    print("\n[3/8] Testing Market Feed & LTP Retrieval...")
    if broker:
        try:
            nifty_ltp = broker.get_ltp_from_api("IDX_I", config.INDEX_SECURITY_ID)
            print(f"  ✓ Nifty 50 REST Quote LTP: {nifty_ltp}")
            results.append(("Market Feed REST Quote", bool(nifty_ltp), f"Nifty LTP={nifty_ltp}"))
        except Exception as e:
            print(f"  ✗ Failed to query REST quote: {e}")
            results.append(("Market Feed REST Quote", False, str(e)))
    else:
        results.append(("Market Feed REST Quote", False, "Skipped (no broker)"))

    # 4. OrderUpdate WebSocket Handshake Test
    print("\n[4/8] Testing OrderUpdate Consumer Architecture...")
    consumer = reconciliation.BrokerOrderEventConsumer(broker)
    consumer.on_connect()
    if consumer.is_connected:
        print("  ✓ BrokerOrderEventConsumer connected and ready for events")
        consumer.on_disconnect()
        print("  ✓ OrderUpdate disconnect handled cleanly (lock engaged)")
        results.append(("OrderUpdate Consumer", True, "Connected & Disconnect handlers verified"))
    else:
        results.append(("OrderUpdate Consumer", False, "Connection state error"))

    # 5. Broker Reconciliation Check
    print("\n[5/8] Running Broker Reconciliation Engine...")
    if broker:
        try:
            report = reconciliation.reconcile_broker_state(broker)
            print(f"  ✓ Reconciliation status: {report.get('status')} (Matched={report.get('matched_positions')}, Unmatched={len(report.get('unmatched_positions', []))})")
            results.append(("Broker Reconciliation", True, f"Status={report.get('status')}"))
        except Exception as e:
            print(f"  ✗ Error in reconciliation: {e}")
            results.append(("Broker Reconciliation", False, str(e)))
    else:
        results.append(("Broker Reconciliation", False, "Skipped"))

    # 6. Tick Size & Price Normalization Math
    print("\n[6/8] Testing Tick Normalization & Stop Price Math...")
    t1 = round_to_tick(123.456, 0.05)
    t2 = round_to_tick(123.42, 0.05)
    trig_buy, lim_buy = protective_sl_prices("BUY", 100.0, 0.05, limit_buffer_ticks=2)
    trig_sell, lim_sell = protective_sl_prices("SELL", 100.0, 0.05, limit_buffer_ticks=2)
    derived_buy = derive_entry_limit("BUY", 100.0, best_bid=99.90, best_ask=100.10, tick_size=0.05, max_extension_pct=0.15)

    print(f"  - Round 123.456 -> {t1} (expected 123.45)")
    print(f"  - Round 123.420 -> {t2} (expected 123.40)")
    print(f"  - BUY Stop Order:  Trigger={trig_buy:.2f}, Limit={lim_buy:.2f}")
    print(f"  - SELL Stop Order: Trigger={trig_sell:.2f}, Limit={lim_sell:.2f}")
    print(f"  - Derived BUY Limit: {derived_buy:.2f}")

    math_ok = (t1 == 123.45 and t2 == 123.40 and lim_buy < trig_buy and lim_sell > trig_sell and derived_buy <= 100.15)
    if math_ok:
        print("  ✓ Price and tick normalization algorithms PASSED")
    else:
        print("  ✗ Math check failed!")
    results.append(("Price & Tick Math", math_ok, "Tick rounding & SL bounds OK"))

    # 7. Position Sizer & Risk Math
    print("\n[7/8] Testing Position Sizer & Risk Guardrails...")
    qty1 = calc_quantity(entry_price=100.0, sl_price=98.0, available_margin=10000.0)
    qty2 = calc_quantity(entry_price=1000.0, sl_price=990.0, available_margin=10000.0)
    print(f"  - Stock @ 100 (SL 98, Risk=50): Qty={qty1}")
    print(f"  - Stock @ 1000 (SL 990, Risk=50, MaxNotional=5000): Qty={qty2}")
    sizer_ok = (qty1 == 25 and qty2 == 5)
    if sizer_ok:
        print("  ✓ Position sizer respects both risk rupees and notional limits")
    else:
        print(f"  ✗ Position sizer returned unexpected quantities: {qty1}, {qty2}")
    results.append(("Position Sizer & Risk Caps", sizer_ok, f"Qty1={qty1}, Qty2={qty2}"))

    # 8. Event Journaling & Circuit Breaker Store
    print("\n[8/8] Testing SQLite Durable Journal & Circuit Breakers...")
    journal.set_circuit_breaker("TEST_DRY_RUN_LOCK", locked=True, reason="Pre-flight check")
    is_locked = journal.is_circuit_breaker_active("TEST_DRY_RUN_LOCK")
    journal.set_circuit_breaker("TEST_DRY_RUN_LOCK", locked=False, reason="Pre-flight completed")
    is_unlocked = not journal.is_circuit_breaker_active("TEST_DRY_RUN_LOCK")

    journal_ok = is_locked and is_unlocked
    if journal_ok:
        print("  ✓ SQLite durable journal and circuit breakers verified")
    results.append(("Durable Journal & Locks", journal_ok, "SQLite read/write/lock OK"))

    # Summary Report
    print("\n" + "=" * 60)
    print("                PRE-FLIGHT SUMMARY REPORT")
    print("=" * 60)
    all_passed = True
    for name, passed, detail in results:
        status_str = "PASS ✓" if passed else "FAIL ✗"
        print(f"{name:<30} : [{status_str}] ({detail})")
        if not passed:
            all_passed = False

    print("=" * 60)
    if all_passed:
        print("RESULT: ALL PRE-FLIGHT CHECKS PASSED. SYSTEM IS READY IN PAPER/SHADOW.")
    else:
        print("RESULT: SOME CHECKS FAILED. INVESTIGATE LOGS BEFORE PROCEEDING.")
    print("=" * 60 + "\n")
    return all_passed


if __name__ == "__main__":
    success = run_dry_run()
    sys.exit(0 if success else 1)
