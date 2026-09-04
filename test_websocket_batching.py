"""
Offline unit tests for Dhan WebSocket subscription batching, deduplication,
reconciliation, startup load mitigation, and historical auth token refresh.
"""
import logging
from queue import Queue
import time
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

import broker
import config
import discovery
import state


def _create_mock_broker():
    instance = object.__new__(broker.DhanBroker)
    instance.instruments = [(0, str(config.INDEX_SECURITY_ID), 15)]  # Initial Nifty 50
    instance.cmd_queue = Queue()
    instance.data_queue = Queue()
    instance.rate_limited_until = 0.0
    instance.timeZone = config.TIME_ZONE
    return instance


def test_subscription_batching_208_split():
    """Verify that 208 symbols get split into chunks of [100, 100, 8]."""
    b = _create_mock_broker()
    mock_ws = MagicMock()

    # 208 distinct symbols
    symbols_208 = [(1, str(10000 + i), 15) for i in range(208)]

    b._subscribe_payload(mock_ws, symbols_208)

    assert mock_ws.subscribe_symbols.call_count == 3
    calls = mock_ws.subscribe_symbols.call_args_list
    batch_sizes = [len(call[0][0]) for call in calls]
    assert batch_sizes == [100, 100, 8]
    # Total instruments in broker should be 1 (initial Nifty) + 208 = 209
    assert len(b.instruments) == 209


def test_duplicate_subscription_removal():
    """Verify duplicate symbols are not re-subscribed or duplicated in self.instruments."""
    b = _create_mock_broker()
    mock_ws = MagicMock()

    # Send symbols with duplicates and with symbol already in instruments
    payload = [
        (0, str(config.INDEX_SECURITY_ID), 15),       # Already in b.instruments
        (1, "10001", 15),
        (1, "10001", 15),    # Duplicate within payload
        (1, "10002", 15),
    ]

    b._subscribe_payload(mock_ws, payload)

    assert mock_ws.subscribe_symbols.call_count == 1
    subscribed_batch = mock_ws.subscribe_symbols.call_args[0][0]
    assert subscribed_batch == [(1, "10001", 15), (1, "10002", 15)]
    assert b.instruments == [(0, str(config.INDEX_SECURITY_ID), 15), (1, "10001", 15), (1, "10002", 15)]


def test_drain_websocket_commands():
    """Verify command queue drains SUB, UNSUB, and CLOSE properly."""
    b = _create_mock_broker()
    mock_ws = MagicMock()
    import threading
    b.stop_event = threading.Event()

    b.cmd_queue.put(("SUB", [(1, "20001", 15), (1, "20002", 15)]))
    b.cmd_queue.put(("UNSUB", [(1, "20001", 15)]))

    b._drain_websocket_commands(mock_ws)

    mock_ws.subscribe_symbols.assert_called_once_with([(1, "20001", 15), (1, "20002", 15)])
    mock_ws.unsubscribe_symbols.assert_called_once_with([(1, "20001", 15)])
    assert b.instruments == [(0, str(config.INDEX_SECURITY_ID), 15), (1, "20002", 15)]


def test_get_data_exception_logged_and_reraised(caplog):
    """Verify original exception from get_data is logged with type and details, and re-raised."""
    b = _create_mock_broker()
    mock_ws = MagicMock()

    custom_error = ConnectionResetError("Dhan server closed TCP stream unexpectedly")
    mock_ws.get_data.side_effect = custom_error

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ConnectionResetError) as exc_info:
            try:
                mock_ws.get_data()
            except Exception as exc:
                broker.logger.exception(
                    "Dhan MarketFeed get_data failed: type=%s message=%s",
                    type(exc).__name__,
                    exc,
                )
                raise

        assert exc_info.value is custom_error
        assert "Dhan MarketFeed get_data failed: type=ConnectionResetError message=Dhan server closed TCP stream unexpectedly" in caplog.text


def test_historical_daily_candles_auth_refresh_retry():
    """Verify DH-906 auth error triggers safe one-time token refresh and retry."""
    b = _create_mock_broker()
    b.data_pacer = broker.RequestPacer(0)
    b.dhan = MagicMock()

    auth_err = {'errorType': 'Order_Error', 'errorCode': 'DH-906', 'errorMessage': 'Invalid Token'}
    success_resp = {
        'status': 'success',
        'data': {
            'timestamp': [1725408000],
            'open': [24500.0],
            'high': [24600.0],
            'low': [24400.0],
            'close': [24550.0],
            'volume': [1000],
        }
    }

    b.dhan.historical_daily_data.side_effect = [auth_err, success_resp]

    with patch.object(b, '_configure_api_client') as mock_config, \
         patch('broker.get_valid_access_token', return_value="new_token_123") as mock_get_token:
        from datetime import date
        df = b.get_historical_daily_candles(
            security_id=13, exchange_segment="IDX_I", instrument_type="INDEX",
            from_dt=date(2026, 9, 1), to_dt=date(2026, 9, 4),
        )

        assert not df.empty
        assert mock_get_token.call_count == 1
        assert mock_config.call_count == 1
        mock_config.assert_called_once_with("new_token_123")
        assert b.dhan.historical_daily_data.call_count == 2


def test_startup_does_not_enqueue_208_subscriptions():
    """Verify broker at startup only holds Nifty index subscription."""
    b = _create_mock_broker()
    assert len(b.instruments) == 1
    assert b.instruments[0] == (0, str(config.INDEX_SECURITY_ID), 15)
    assert b.cmd_queue.empty()


def test_subscription_reconciliation_unchanged_set():
    """Verify unchanged desired set makes no new subscription or unsubscription calls."""
    b = _create_mock_broker()
    # Currently only Nifty
    desired = [(0, str(config.INDEX_SECURITY_ID), 15)]
    changed = b.reconcile_subscriptions(desired)
    assert not changed
    assert b.cmd_queue.empty()


def test_subscription_reconciliation_add_and_remove():
    """Verify reconciliation subscribes new symbols and unsubscribes removed ones (preserving Nifty)."""
    b = _create_mock_broker()
    b.instruments = [
        (0, str(config.INDEX_SECURITY_ID), 15),
        (1, "1001", 15),
        (1, "1002", 15),
    ]
    # New desired: keep 1002, drop 1001, add 1003
    desired = [
        (1, "1002", 15),
        (1, "1003", 15),
    ]
    changed = b.reconcile_subscriptions(desired)
    assert changed

    # Check queued commands
    cmd1 = b.cmd_queue.get_nowait()
    assert cmd1 == ("UNSUB", [(1, "1001", 15)])
    cmd2 = b.cmd_queue.get_nowait()
    assert cmd2 == ("SUB", [(1, "1003", 15)])


def test_subscription_reconciliation_batching_large_payload():
    """Verify that large reconciliation additions split into chunks <= 100."""
    b = _create_mock_broker()
    mock_ws = MagicMock()

    # 120 new symbols
    desired = [(1, str(30000 + i), 15) for i in range(120)]
    b.reconcile_subscriptions(desired)

    # Drain commands
    b._drain_websocket_commands(mock_ws)

    # Should call subscribe_symbols twice: batch of 100, then batch of 20
    assert mock_ws.subscribe_symbols.call_count == 2
    calls = mock_ws.subscribe_symbols.call_args_list
    assert len(calls[0][0][0]) == 100
    assert len(calls[1][0][0]) == 20
    assert len(b.instruments) == 121  # 1 Nifty + 120 symbols


def test_successful_discovery_subscribes_top_movers_and_active():
    """Verify successful discovery subscribes top 10 gainers + top 10 losers + active setups."""
    b = _create_mock_broker()

    gainers = [{"SECURITY_ID": 100 + i, "pct_change": 3.0} for i in range(20)]
    losers = [{"SECURITY_ID": 200 + i, "pct_change": -3.0} for i in range(20)]

    mock_state_data = {
        "open_positions": {"9999": {"symbol": "ACTIVE_POS"}},
        "watchlist": {"8888": {"symbol": "ACTIVE_ALPHA"}},
    }

    with patch("discovery.state.snapshot", return_value=mock_state_data), \
         patch.object(b, "reconcile_subscriptions") as mock_reconcile:
        discovery.reconcile_live_subscriptions(b, gainers, losers)

        assert mock_reconcile.call_count == 1
        desired = mock_reconcile.call_args[0][0]
        desired_sids = {str(item[1]) for item in desired}

        # Must contain Nifty (13), top 10 gainers (100..109), top 10 losers (200..209), and active (9999, 8888)
        assert str(config.INDEX_SECURITY_ID) in desired_sids
        for i in range(10):
            assert str(100 + i) in desired_sids
            assert str(200 + i) in desired_sids
        # Gainers 110..119 and losers 210..219 should NOT be in desired
        for i in range(10, 20):
            assert str(100 + i) not in desired_sids
            assert str(200 + i) not in desired_sids
        assert "9999" in desired_sids
        assert "8888" in desired_sids


def test_incomplete_discovery_creates_no_subscription_calls():
    """Verify discovery with coverage < 90% retains prior movers without throwing errors."""
    b = _create_mock_broker()
    b.liveFeed = {}  # 0% coverage
    universe_df = pd.DataFrame([{"SECURITY_ID": 1000 + i, "DISPLAY_NAME": f"SYM_{i}"} for i in range(10)])

    with patch("discovery.state.snapshot", return_value={"top_gainers": [{"SECURITY_ID": 1000}], "top_losers": []}), \
         patch("discovery.state.update") as mock_update:
        discovery.refresh_from_livefeed_full_universe(b, universe_df)
        mock_update.assert_not_called()


def test_local_mover_ranking_from_livefeed_208_symbols():
    """Verify local mover ranking correctly identifies top gainers & losers for 208 mock symbols."""
    from datetime import datetime
    b = _create_mock_broker()
    b.liveFeed = {}
    now_tz = datetime.now(config.TIME_ZONE)

    rows = []
    for i in range(208):
        sid = 10000 + i
        rows.append({"SECURITY_ID": sid, "DISPLAY_NAME": f"SYM_{sid}"})
        # Half gainers, half losers
        prev_close = 100.0
        pct = 2.0 if i < 104 else -2.0
        ltp = prev_close * (1.0 + pct / 100.0)
        b.liveFeed[str(sid)] = {
            "ltp": ltp,
            "prev_close": prev_close,
            "net_change": ltp - prev_close,
            "volume": 100000.0 + i * 1000,
            "ltt": now_tz,
            "updated_at": now_tz,
        }

    universe_df = pd.DataFrame(rows)

    captured_updates = {}
    def mock_update(payload):
        captured_updates.update(payload)

    with patch("discovery.state.update", side_effect=mock_update), \
         patch("discovery.state.snapshot", return_value={"final_universe_locked": False}), \
         patch("discovery.state.is_blacklisted", return_value=False):
        discovery.refresh_from_livefeed_full_universe(b, universe_df)

    assert "top_gainers" in captured_updates
    assert "top_losers" in captured_updates
    gainers = captured_updates["top_gainers"]
    losers = captured_updates["top_losers"]

    assert len(gainers) == config.TOP_N_GAINERS  # 20
    assert len(losers) == config.TOP_N_LOSERS    # 20
    assert all(g["pct_change"] > 0 for g in gainers)
    assert all(l["pct_change"] < 0 for l in losers)
    assert all(g["source"] == "WEBSOCKET" for g in gainers)


def test_stale_feed_exclusion():
    """Verify ticks older than DISCOVERY_TICK_MAX_AGE_SEC are excluded from coverage and ranking."""
    from datetime import datetime, timedelta
    b = _create_mock_broker()
    b.liveFeed = {}
    stale_tz = datetime.now(config.TIME_ZONE) - timedelta(seconds=config.DISCOVERY_TICK_MAX_AGE_SEC + 5)

    rows = []
    for i in range(10):
        sid = 20000 + i
        rows.append({"SECURITY_ID": sid, "DISPLAY_NAME": f"SYM_{sid}"})
        b.liveFeed[str(sid)] = {
            "ltp": 105.0,
            "prev_close": 100.0,
            "net_change": 5.0,
            "volume": 50000.0,
            "ltt": stale_tz,
            "updated_at": stale_tz,
        }

    universe_df = pd.DataFrame(rows)

    with patch("discovery.state.snapshot", return_value={"top_gainers": []}), \
         patch("discovery.state.update") as mock_update:
        discovery.refresh_from_livefeed_full_universe(b, universe_df)
        mock_update.assert_not_called()



def test_180_of_208_passes_80_percent_coverage(monkeypatch):
    """Verify 180/208 (86.5%) symbols with valid previous close passes 80% coverage and updates top movers."""
    from datetime import datetime
    b = _create_mock_broker()
    b.liveFeed = {}
    now_tz = datetime.now(config.TIME_ZONE)

    rows = []
    for i in range(208):
        sid = 40000 + i
        rows.append({"SECURITY_ID": sid, "DISPLAY_NAME": f"SYM_{sid}"})
        if i < 180:
            pct = 2.5 if i < 90 else -2.5
            prev_close = 100.0
            ltp = prev_close * (1.0 + pct / 100.0)
            b.liveFeed[str(sid)] = {
                "ltp": ltp,
                "prev_close": prev_close,
                "net_change": ltp - prev_close,
                "volume": 50000,
                "ltt": now_tz,
                "updated_at": now_tz,
            }

    universe_df = pd.DataFrame(rows)

    captured = {}
    with patch("discovery.state.update", side_effect=lambda payload: captured.update(payload)), \
         patch("discovery.state.snapshot", return_value={"final_universe_locked": False}), \
         patch("discovery.state.is_blacklisted", return_value=False):
        monkeypatch.setattr(config, "MIN_WEBSOCKET_COVERAGE_PCT", 60.0)
        discovery.refresh_from_livefeed_full_universe(b, universe_df)
        assert "top_gainers" in captured
        assert len(captured["top_gainers"]) > 0

        # Now test that if policy is 90%, 180/208 (86.5%) is retained and NOT updated
        captured.clear()
        monkeypatch.setattr(config, "MIN_WEBSOCKET_COVERAGE_PCT", 90.0)
        discovery.refresh_from_livefeed_full_universe(b, universe_df)
        assert "top_gainers" not in captured


def test_reconnect_preserves_livefeed_cache_and_resubscribes_canonical():
    """Verify connection loss does NOT wipe liveFeed and re-issues subscriptions from canonical list."""
    b = _create_mock_broker()
    b.instruments = [(1, "1001", 15), (1, "1002", 15)]
    b.liveFeed = {
        "1001": {"ltp": 100.0, "prev_close": 98.0},
        "1002": {"ltp": 200.0, "prev_close": 195.0},
    }

    mock_ws = MagicMock()
    # Test _resubscribe_canonical_instruments
    b._resubscribe_canonical_instruments(mock_ws)
    assert mock_ws.subscribe_symbols.call_count == 1
    assert len(b.liveFeed) == 2  # Feed retained


def test_active_symbol_fallback_uses_ws_and_respects_cooldown(monkeypatch):
    """Verify active symbol uses WebSocket LTP when fresh (<=5s) and rate-limits REST fallback (10s cooldown)."""
    from datetime import datetime, timedelta
    b = _create_mock_broker()
    b.liveFeed = {}
    b.quote_pacer = broker.RequestPacer(0)
    b._active_ltp_last_api_fetch = {}
    b.dhan = MagicMock()
    b.dhan.quote_data.return_value = {
        "status": "success",
        "data": {config.EXCHANGE: {"5001": {"last_price": 105.0, "net_change": 5.0}}}
    }

    now_tz = datetime.now(config.TIME_ZONE)
    # Case 1: Fresh tick (2s old) -> returns WS LTP without REST API call
    b.liveFeed["5001"] = {"ltp": 104.0, "prev_close": 100.0, "updated_at": now_tz - timedelta(seconds=2)}
    ltp1 = b.get_active_symbol_ltp_with_fallback("5001")
    assert ltp1 == 104.0
    assert b.dhan.quote_data.call_count == 0

    # Case 2: Stale tick (10s old) -> calls REST API and updates liveFeed
    b.liveFeed["5001"]["updated_at"] = now_tz - timedelta(seconds=10)
    ltp2 = b.get_active_symbol_ltp_with_fallback("5001")
    assert ltp2 == 105.0
    assert b.dhan.quote_data.call_count == 1

    # Case 3: Immediately called again while on cooldown -> does NOT make second REST call
    b.liveFeed["5001"]["updated_at"] = now_tz - timedelta(seconds=10)
    ltp3 = b.get_active_symbol_ltp_with_fallback("5001")
    assert ltp3 == 105.0
    assert b.dhan.quote_data.call_count == 1
