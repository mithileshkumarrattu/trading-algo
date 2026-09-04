from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pandas as pd

import config
import discovery
import state
import main
import pattern


def test_runtime_config_contract():
    main.validate_runtime_config()


def _candle(open_price, high, low, close, volume=100):
    return SimpleNamespace(open=open_price, high=high, low=low, close=close, volume=volume)


def test_alpha_quality_helpers_reject_weak_candles():
    doji = _candle(100, 110, 90, 100.5)
    weak_wick = _candle(100, 110, 90, 101)
    assert pattern.is_doji(doji)
    assert pattern.candle_body_ratio(weak_wick) < config.ALPHA_MIN_TREND_BODY_RATIO
    assert pattern.body_to_wick_ratio(weak_wick) < config.ALPHA_MIN_TREND_BODY_TO_WICK_RATIO


def test_alpha_is_buy_only():
    assert pattern.find_trend_run_and_alpha([], False) is None


def test_alpha_pullback_rejects_extreme_volume():
    pullback = _candle(110, 111, 100, 101, volume=1000)
    assert not pattern.valid_alpha_pullback_candle(pullback, [100, 100, 100])


def test_alpha_run_must_be_immediately_before_pullback():
    candles = [
        _candle(100, 102, 99, 101.5),
        _candle(101.5, 104, 101, 103.5),
        _candle(103.5, 106, 103, 105.5),
        _candle(105.5, 106, 104, 105.7),
        _candle(105.7, 106, 101, 102, volume=100),
        _candle(102, 107, 101, 106, volume=100),
    ]
    frame = __import__("pandas").DataFrame([vars(candle) for candle in candles])
    assert pattern.find_alpha_buy_setup(frame) is None


def test_quote_change_normalization_produces_positive_move():
    response = {
        "status": "success",
        "data": {
            "data": {
                "NSE_EQ": {
                    "1333": {"last_price": 104.63, "net_change": 4.63}
                }
            }
        },
    }
    parsed = main.DhanBroker._extract_quote_data(response, "NSE_EQ")
    quote = parsed["1333"]
    pct_change = quote["net_change"] / (quote["last_price"] - quote["net_change"]) * 100
    assert round(pct_change, 2) == 4.63
    assert config.MIN_PCT_MOVE <= pct_change <= config.MAX_PCT_MOVE


def test_websocket_discovery_reports_coverage_and_retains_movers(monkeypatch):
    b = MagicMock()
    b.liveFeed = {
        "11": {"ltp": 100.0, "prev_close": 98.0, "net_change": 2.0, "volume": 1000, "updated_at": datetime.now(config.TIME_ZONE)},
    }
    universe_df = pd.DataFrame([{"SECURITY_ID": sid, "DISPLAY_NAME": f"SYM_{sid}"} for sid in [11, 12, 13, 14]])

    with patch("discovery.state.snapshot", return_value={"top_gainers": [{"SECURITY_ID": 11}]}), \
         patch("discovery.state.update") as mock_update:
        # Coverage is 1/4 = 25% < 90%, so update should not be called and movers retained
        discovery.refresh_from_livefeed_full_universe(b, universe_df)
        mock_update.assert_not_called()


def test_discovery_loop_no_rest_quote_calls(monkeypatch):
    """Verify live discovery loop calls refresh_from_livefeed_full_universe without invoking get_quote_batch."""
    b = MagicMock()
    b.liveFeed = {}
    b.get_quote_batch = MagicMock()
    universe_df = pd.DataFrame([{"SECURITY_ID": 1000, "DISPLAY_NAME": "SYM"}])

    with patch("discovery.state.snapshot", return_value={"final_universe_locked": False}), \
         patch("discovery.state.update"):
        # Run 1 iteration of discovery
        discovery.refresh_from_livefeed_full_universe(b, universe_df)
        b.get_quote_batch.assert_not_called()



def test_top_movers_throttle_uses_time_interval(monkeypatch):
    monkeypatch.setattr(config, "SEND_TELEGRAM_TOP_MOVERS", True)
    monkeypatch.setattr(discovery.state, "snapshot", lambda: {})
    monkeypatch.setattr(discovery, "_last_top_movers_telegram_at", 0.0)
    assert discovery.should_send_top_movers() is True

    monkeypatch.setattr(discovery, "_last_top_movers_telegram_at", 9999999999.0)
    assert discovery.should_send_top_movers() is False


def test_jp_detection_only_keeps_detector_and_skips_entry(monkeypatch):
    monkeypatch.setattr(config, "JP_DETECTION_ONLY", True)

    captured = {}

    def fake_log_setup_outcome(setup, outcome, details=""):
        captured["outcome"] = outcome

    def fake_remove_jp_setup(security_id):
        captured["removed"] = str(security_id)

    def fake_enter_trade(*args, **kwargs):
        raise AssertionError("JP entry should be skipped when detection-only is enabled")

    monkeypatch.setattr(state, "log_setup_outcome", fake_log_setup_outcome)
    monkeypatch.setattr(state, "remove_jp_setup", fake_remove_jp_setup)
    monkeypatch.setattr(main.engine, "enter_trade", fake_enter_trade)

    setup = {
        "security_id": "42",
        "symbol": "TEST",
        "strategy": "JP",
        "direction": "BUY",
        "jp_high": 101.0,
        "jp_low": 99.0,
        "jp_open_time": "2026-09-01T09:15:00+00:00",
        "jp_close_time": "2026-09-01T09:20:00+00:00",
        "jp_key": "JP_42_BUY_2026-09-01T09:15:00+00:00",
        "last_processed_1m_time": None,
    }
    latest_bar = {
        "timestamp": datetime(2026, 9, 1, 9, 21, 0),
        "open": 100.0,
        "high": 102.5,
        "low": 98.8,
        "close": 101.8,
    }

    monkeypatch.setattr(state, "snapshot", lambda: {"open_positions": {}})
    result = main.process_new_1m_bar_for_setup(None, setup, latest_bar)

    assert result == "JP_TRIGGER_DETECTED_ONLY"
    assert captured["outcome"] == "JP_TRIGGER_DETECTED_ONLY"
    assert captured["removed"] == "42"


def test_jp_trigger_routes_to_shared_paper_entry(monkeypatch):
    monkeypatch.setattr(config, "JP_DETECTION_ONLY", False)
    captured = {}

    def fake_enter_trade(*args, **kwargs):
        captured.update(kwargs)
        return {"order_id": "PAPER-JP"}

    monkeypatch.setattr(main.engine, "enter_trade", fake_enter_trade)
    monkeypatch.setattr(main.state, "snapshot", lambda: {"open_positions": {}})
    monkeypatch.setattr(main.state, "set_jp_watchlist_item", lambda sid, item: None)
    monkeypatch.setattr(main.state, "remove_alpha_setup", lambda sid: None)
    monkeypatch.setattr(main.state, "remove_jp_setup", lambda sid: None)
    monkeypatch.setattr(main.state, "log_setup_outcome", lambda *args, **kwargs: None)
    monkeypatch.setattr(main.state, "add_log", lambda *args, **kwargs: None)
    class DateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 3, 10, 5, tzinfo=tz)

    monkeypatch.setattr(main, "datetime", DateTime)

    setup = {
        "security_id": "42", "symbol": "TEST", "strategy": "JP", "direction": "BUY",
        "trigger_price": 101.0, "pattern_high": 101.0, "pattern_low": 99.0,
        "pattern_open_time": "2026-09-03T10:00:00+05:30",
        "pattern_close_time": "2026-09-03T10:03:00+05:30", "setup_key": "JP-42",
    }
    bar = {"timestamp": datetime(2026, 9, 3, 10, 4, tzinfo=DateTime.now().tzinfo),
           "open": 100, "high": 102, "low": 100, "close": 101.5}

    class Broker:
        def get_ltp(self, security_id, exchange):
            return 101.5

    result = main.process_new_1m_bar_for_setup(Broker(), setup, bar)
    assert result == "TRADE_ENTERED"
    assert captured["strategy"] == "JP"


def test_diagnose_alpha_rejection_reasons():
    import signal_diagnostics
    now = datetime(2026, 9, 4, 10, 30, tzinfo=config.TIME_ZONE)
    cand_info = {"SECURITY_ID": 101, "rank": 1, "pct_change": 2.5, "volume": 50000}

    # Case 1: Market regime bearish rejects Alpha BUY
    diag_regime = signal_diagnostics.diagnose_alpha_candidate(
        symbol="SWIGGY",
        security_id=101,
        candidate_info=cand_info,
        today_pattern_candles=pd.DataFrame(),
        regime="BEARISH",
        now=now,
    )
    assert diag_regime["status"] == "REJECTED"
    assert diag_regime["reason"] == "MARKET_REGIME_BLOCKED"

    # Case 2: Candle with insufficient trend body-to-wick ratio
    candles = [
        {"timestamp": now - timedelta(minutes=15), "open": 100.0, "high": 102.0, "low": 99.5, "close": 101.8, "volume": 1000},
        {"timestamp": now - timedelta(minutes=12), "open": 101.8, "high": 104.0, "low": 101.5, "close": 103.8, "volume": 1000},
        # candle with body=1.0, wicks=2.0 (high=105, low=102, open=103, close=104 -> body_ratio = 1/3 = 0.33 < 0.55, body_to_wick = 1/2 = 0.5 < 1.0, not doji since body_ratio 0.33 > 0.18)
        {"timestamp": now - timedelta(minutes=9), "open": 103.0, "high": 105.0, "low": 102.0, "close": 104.0, "volume": 1000},
        {"timestamp": now - timedelta(minutes=6), "open": 104.0, "high": 104.2, "low": 102.5, "close": 103.0, "volume": 1000}, # Alpha candidate
        {"timestamp": now - timedelta(minutes=3), "open": 103.0, "high": 105.0, "low": 102.8, "close": 104.8, "volume": 1000}, # Confirmation
    ]
    df = pd.DataFrame(candles)
    diag_trend = signal_diagnostics.diagnose_alpha_candidate(
        symbol="SWIGGY",
        security_id=101,
        candidate_info=cand_info,
        today_pattern_candles=df,
        regime="BULLISH",
        now=now,
    )
    assert diag_trend["status"] == "REJECTED"
    assert diag_trend["reason"] in ("TREND_BODY_TO_WICK", "TREND_BODY_RATIO", "TREND_DOJI", "TREND_RUN_TOO_SHORT")


def test_diagnose_jp_rejection_reasons():
    import signal_diagnostics
    now = datetime(2026, 9, 4, 10, 30, tzinfo=config.TIME_ZONE)
    cand_info = {"SECURITY_ID": 202, "rank": 1, "pct_change": 3.0, "volume": 80000, "is_bullish_setup": True}

    # Case 1: Market regime bearish rejects JP BUY
    diag_regime = signal_diagnostics.diagnose_jp_candidate(
        symbol="HAL",
        security_id=202,
        candidate_info=cand_info,
        today_pattern_candles=pd.DataFrame(),
        regime="BEARISH",
        now=now,
    )
    assert diag_regime["status"] == "REJECTED"
    assert diag_regime["reason"] == "MARKET_REGIME_BLOCKED"

    # Case 2: Insufficient candles
    diag_hist = signal_diagnostics.diagnose_jp_candidate(
        symbol="HAL",
        security_id=202,
        candidate_info=cand_info,
        today_pattern_candles=pd.DataFrame([{"timestamp": now, "high": 100, "low": 90, "open": 95, "close": 98, "volume": 100}]),
        regime="BULLISH",
        now=now,
    )
    assert diag_hist["status"] == "REJECTED"
    assert diag_hist["reason"] == "INSUFFICIENT_HISTORY"
