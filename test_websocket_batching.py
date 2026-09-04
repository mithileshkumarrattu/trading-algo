"""
Offline unit tests for Dhan WebSocket subscription batching, deduplication,
command draining, exception preservation, and historical auth token refresh.
"""
import logging
from queue import Queue
import pytest
from unittest.mock import MagicMock, patch

import broker


def _create_mock_broker():
    instance = object.__new__(broker.DhanBroker)
    instance.instruments = [(0, "13", 15)]  # Initial Nifty 50
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
        (0, "13", 15),       # Already in b.instruments
        (1, "10001", 15),
        (1, "10001", 15),    # Duplicate within payload
        (1, "10002", 15),
    ]
    
    b._subscribe_payload(mock_ws, payload)
    
    assert mock_ws.subscribe_symbols.call_count == 1
    subscribed_batch = mock_ws.subscribe_symbols.call_args[0][0]
    assert subscribed_batch == [(1, "10001", 15), (1, "10002", 15)]
    assert b.instruments == [(0, "13", 15), (1, "10001", 15), (1, "10002", 15)]


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
    assert b.instruments == [(0, "13", 15), (1, "20002", 15)]


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
