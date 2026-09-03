from datetime import datetime, timedelta

import config
import discovery
import state
import main


def test_fetch_batch_quotes_reports_coverage(monkeypatch):
    monkeypatch.setattr(discovery.config, "DISCOVERY_QUOTE_CHUNK", 2)
    monkeypatch.setattr(discovery.time, "sleep", lambda _: None)

    class Broker:
        def get_quote_batch(self, exchange, security_ids):
            if security_ids[0] == 13:
                return {}
            return {str(security_ids[0]): {"last_price": 100.0, "net_change": 1.0}}

    quotes, requested, failed = discovery._fetch_batch_quotes(Broker(), [11, 12, 13, 14])

    assert requested == 4
    assert len(quotes) == 1
    assert failed == [{"offset": 2, "count": 2, "sample": ["13", "14"]}]


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
