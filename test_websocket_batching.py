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
    """Verify incomplete discovery returns early and creates no subscription updates."""
    b = _create_mock_broker()
    universe_df = pd.DataFrame([{"SECURITY_ID": i} for i in range(10)])

    # Force _fetch_batch_quotes to return empty
    with patch("discovery._fetch_batch_quotes", return_value=({}, 10, [{"offset": 0, "count": 10, "sample": ["0"]}])):
        with patch.object(b, "reconcile_subscriptions") as mock_reconcile:
            discovery.run_full_universe_scan(b, universe_df)
            mock_reconcile.assert_not_called()
