from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pandas as pd

import config
import discovery
import state
import main
import pattern
import jp_pattern


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
           "open": 100, "high": 101.3, "low": 100, "close": 101.2}

    class Broker:
        def get_ltp(self, security_id, exchange):
            return 101.2

    result = main.process_new_1m_bar_for_setup(Broker(), setup, bar)
    assert result == "TRADE_ENTERED"
    assert captured["strategy"] == "JP"


def test_diagnose_alpha_rejection_reasons():
    import signal_diagnostics
    now = datetime(2026, 9, 4, 10, 30, tzinfo=config.TIME_ZONE)
    cand_info = {"SECURITY_ID": 101, "rank": 1, "pct_change": 2.5, "volume": 50000}

    # Case 1: Market regime bearish with small move (< 2.0%) rejects Alpha BUY
    cand_small = {"SECURITY_ID": 101, "rank": 1, "pct_change": 1.2, "volume": 50000}
    diag_regime = signal_diagnostics.diagnose_alpha_candidate(
        symbol="SWIGGY",
        security_id=101,
        candidate_info=cand_small,
        today_pattern_candles=pd.DataFrame(),
        regime="BEARISH",
        now=now,
    )
    assert diag_regime["status"] == "REJECTED"
    assert diag_regime["reason"] == "COUNTERTREND_MOVE_TOO_SMALL"
    assert diag_regime["regime_mode"] == "COUNTERTREND"

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

    # Case 1: Market regime bearish with small move (< 2.0%) rejects JP BUY
    cand_small = {"SECURITY_ID": 202, "rank": 1, "pct_change": 1.2, "volume": 80000, "is_bullish_setup": True}
    diag_regime = signal_diagnostics.diagnose_jp_candidate(
        symbol="HAL",
        security_id=202,
        candidate_info=cand_small,
        today_pattern_candles=pd.DataFrame(),
        regime="BEARISH",
        now=now,
    )
    assert diag_regime["status"] == "REJECTED"
    assert diag_regime["reason"] == "COUNTERTREND_MOVE_TOO_SMALL"
    assert diag_regime["regime_mode"] == "COUNTERTREND"

    # Case 2: Insufficient candles
    cand_info = {"SECURITY_ID": 202, "rank": 1, "pct_change": 3.0, "volume": 80000, "is_bullish_setup": True}
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
    assert diag_hist["reason"] == "INSUFFICIENT_HISTORY"


def test_alpha_high_volume_shakeout_exception():
    # 1. High-volume red candle closing weakly below band -> Rejected
    weak_pullback = _candle(105.0, 105.5, 100.0, 100.5, volume=2000)
    assert not pattern.valid_alpha_pullback_candle(weak_pullback, [1000, 1000, 1000], band_low=102.0, band_high=104.0)

    # 2. High-volume red shakeout holding band and closing in upper 50% of range -> Passes exception
    # Red candle with lower shadow: open=105.0, close=104.0, high=105.1, low=102.5 -> range=2.6, body=1.0, upper_wick=0.1, lower_wick=1.5 -> close_pos=(104.0-102.5)/2.6=0.577 >= 0.50
    strong_pullback = _candle(105.0, 105.1, 102.5, 104.0, volume=2000)
    assert pattern.valid_alpha_pullback_candle(strong_pullback, [1000, 1000, 1000], band_low=102.0, band_high=104.0)


def test_jp_constructive_compression_and_confirmation_volume():
    # 1. Repeated band touches with wide ranges / bad closes -> Rejected
    bad_context = pd.DataFrame([
        {"high": 110, "low": 90, "open": 105, "close": 92, "volume": 1000},
        {"high": 112, "low": 88, "open": 93, "close": 89, "volume": 1200},
        {"high": 115, "low": 85, "open": 90, "close": 86, "volume": 1500},
    ])
    assert not jp_pattern.constructive_compression(bad_context, band_low=95.0, band_high=100.0, bullish=True)

    # 2. Constructive compression: contracting ranges, closes >= band_low, controlled volume -> Passes
    good_context = pd.DataFrame([
        {"high": 105, "low": 98, "open": 104, "close": 101, "volume": 1000}, # range 7
        {"high": 103, "low": 98.5, "open": 101, "close": 100, "volume": 900}, # range 4.5
        {"high": 102, "low": 99.0, "open": 100, "close": 100.5, "volume": 800}, # range 3 (last range <= median range)
    ])
    assert jp_pattern.constructive_compression(good_context, band_low=98.0, band_high=102.0, bullish=True)


def test_alpha_controlled_compression_and_confirmation_volume():
    # 1. Strong normal green trend candle (body_ratio >= 0.45, rel_vol >= 0.90) passes
    strong_candle = _candle(100.0, 102.0, 99.8, 101.5, volume=1000) # body=1.5, rng=2.2, body_ratio=0.68, body_to_wick=1.5/0.7=2.14
    is_valid, reason, _ = pattern.valid_alpha_trend_candle(strong_candle, [1000, 1000])
    assert is_valid
    assert reason == "TREND_STRONG"

    # 2. Weak candle with body < wick fails (body=0.8, range=2.0 -> wick=1.2 -> body_to_wick=0.67 < 1.0)
    weak_wick_candle = _candle(100.5, 102.0, 100.0, 101.3, volume=1000) # upper=0.7, lower=0.5 -> wick=1.2
    is_valid, reason, _ = pattern.valid_alpha_trend_candle(weak_wick_candle, [1000, 1000], band_low=100.0, band_high=101.0)
    assert not is_valid
    assert reason == "TREND_BODY_TO_WICK"

    # 3. Doji fails even if near SMMA
    doji_candle = _candle(100.0, 102.0, 98.0, 100.2, volume=1000) # body_ratio=0.2/4=0.05 < 0.18
    is_valid, reason, _ = pattern.valid_alpha_trend_candle(doji_candle, [1000, 1000], band_low=99.0, band_high=101.0)
    assert not is_valid
    assert reason == "TREND_DOJI"

    # 4. Controlled compression bar (rel_vol=0.80, near SMMA band within 0.35%) passes when within parameters
    near_candle = _candle(100.0, 100.5, 99.8, 100.2, volume=800)
    assert round(pattern.distance_to_band_pct(near_candle, band_low=99.5, band_high=100.0), 3) == 0.20
    far_candle = _candle(100.0, 102.0, 99.8, 101.5, volume=800)
    assert round(pattern.distance_to_band_pct(far_candle, band_low=99.5, band_high=100.0), 3) == 1.50


def test_alpha_buy_and_sell_state_transitions():
    # Test BUY Setup (Green run -> Red pullback -> Green confirmation)
    now = datetime(2026, 9, 4, 10, 0, tzinfo=config.TIME_ZONE)
    buy_candles = [
        {"timestamp": now + timedelta(minutes=0), "open": 100.0, "high": 102.0, "low": 99.8, "close": 101.8, "volume": 1000},
        {"timestamp": now + timedelta(minutes=3), "open": 101.8, "high": 104.0, "low": 101.5, "close": 103.8, "volume": 1000},
        {"timestamp": now + timedelta(minutes=6), "open": 103.8, "high": 106.0, "low": 103.5, "close": 105.8, "volume": 1000},
        # Candidate Red Alpha Pullback
        {"timestamp": now + timedelta(minutes=9), "open": 105.8, "high": 106.0, "low": 104.5, "close": 104.8, "volume": 1000},
        # Confirmation bar breaks and closes above Alpha high (106.0) with >= 1.20x volume
        {"timestamp": now + timedelta(minutes=12), "open": 104.8, "high": 107.5, "low": 104.5, "close": 107.0, "volume": 1400},
    ]
    df_buy = pd.DataFrame(buy_candles)
    res_buy = pattern.find_alpha_setup(df_buy, side="BUY")
    assert res_buy is not None
    assert res_buy["side"] == "BUY"
    assert res_buy["stage"] == "AWAITING_1M_TRIGGER"
    assert res_buy["pattern_confirmation_status"] == "CONFIRMED"
    assert res_buy["alpha_high"] == 106.0

    # Test SELL Setup (Red run -> Green pullback -> Red confirmation)
    sell_candles = [
        {"timestamp": now + timedelta(minutes=0), "open": 100.0, "high": 100.2, "low": 98.0, "close": 98.2, "volume": 1000},
        {"timestamp": now + timedelta(minutes=3), "open": 98.2, "high": 98.5, "low": 96.0, "close": 96.2, "volume": 1000},
        {"timestamp": now + timedelta(minutes=6), "open": 96.2, "high": 96.5, "low": 94.0, "close": 94.2, "volume": 1000},
        # Candidate Green Alpha Pullback
        {"timestamp": now + timedelta(minutes=9), "open": 94.2, "high": 95.5, "low": 94.0, "close": 95.2, "volume": 1000},
        # Confirmation bar breaks and closes below Alpha low (94.0) with >= 1.20x volume
        {"timestamp": now + timedelta(minutes=12), "open": 95.2, "high": 95.5, "low": 92.5, "close": 93.0, "volume": 1400},
    ]
    df_sell = pd.DataFrame(sell_candles)
    res_sell = pattern.find_alpha_setup(df_sell, side="SELL")
    assert res_sell is not None
    assert res_sell["side"] == "SELL"
    assert res_sell["stage"] == "AWAITING_1M_TRIGGER"
    assert res_sell["pattern_confirmation_status"] == "CONFIRMED"
    assert res_sell["alpha_low"] == 94.0


def test_alpha_candidate_persists_even_with_later_bars():
    # Verify that an earlier Alpha candidate remains detected and waiting for confirmation
    # even when subsequent pattern bars are formed (1 non-confirming bar -> WAITING_SECOND_3M_CONFIRMATION)
    now = datetime(2026, 9, 4, 10, 0, tzinfo=config.TIME_ZONE)
    candles = [
        {"timestamp": now + timedelta(minutes=0), "open": 100.0, "high": 102.0, "low": 99.8, "close": 101.8, "volume": 1000},
        {"timestamp": now + timedelta(minutes=3), "open": 101.8, "high": 104.0, "low": 101.5, "close": 103.8, "volume": 1000},
        {"timestamp": now + timedelta(minutes=6), "open": 103.8, "high": 106.0, "low": 103.5, "close": 105.8, "volume": 1000},
        # Candidate Red Alpha Pullback
        {"timestamp": now + timedelta(minutes=9), "open": 105.8, "high": 106.0, "low": 104.5, "close": 104.8, "volume": 1000},
        # Intermediate bar that does not yet confirm (inside bar)
        {"timestamp": now + timedelta(minutes=12), "open": 104.8, "high": 105.5, "low": 104.2, "close": 105.0, "volume": 900},
    ]
    df = pd.DataFrame(candles)
    res = pattern.find_alpha_setup(df, side="BUY")
    assert res is not None
    assert res["stage"] == "WAITING_SECOND_3M_CONFIRMATION"
    assert res["pattern_confirmation_status"] == "WAITING_3M_CONFIRMATION"
    assert res["alpha_high"] == 106.0


def test_alpha_candidate_expires_after_two_non_confirming_bars():
    # If 2 pattern bars pass without breakout confirmation, setup expires / is not returned
    now = datetime(2026, 9, 4, 10, 0, tzinfo=config.TIME_ZONE)
    candles = [
        {"timestamp": now + timedelta(minutes=0), "open": 100.0, "high": 102.0, "low": 99.8, "close": 101.8, "volume": 1000},
        {"timestamp": now + timedelta(minutes=3), "open": 101.8, "high": 104.0, "low": 101.5, "close": 103.8, "volume": 1000},
        {"timestamp": now + timedelta(minutes=6), "open": 103.8, "high": 106.0, "low": 103.5, "close": 105.8, "volume": 1000},
        # Candidate Red Alpha Pullback
        {"timestamp": now + timedelta(minutes=9), "open": 105.8, "high": 106.0, "low": 104.5, "close": 104.8, "volume": 1000},
        # Two non-confirming bars
        {"timestamp": now + timedelta(minutes=12), "open": 104.8, "high": 105.5, "low": 104.2, "close": 105.0, "volume": 900},
        {"timestamp": now + timedelta(minutes=15), "open": 105.0, "high": 105.8, "low": 104.0, "close": 105.2, "volume": 850},
    ]
    df = pd.DataFrame(candles)
    res = pattern.find_alpha_setup(df, side="BUY")
    assert res is None


def test_entry_extension_rejection():
    assert pattern.entry_extension_pct(100.50, 100.0, "BUY") == 0.50
    assert pattern.entry_extension_pct(99.50, 100.0, "BUY") == 0.0
    assert pattern.entry_extension_pct(99.50, 100.0, "SELL") == 0.50


def test_market_regime_allows_setup_policy():
    import main
    import signal_diagnostics

    # 1. Aligned BUY (BUY + BULLISH)
    ok, mode, reason = main.market_regime_allows_setup("BUY", "BULLISH", 1.5)
    assert ok is True
    assert mode == "ALIGNED"
    assert reason == "REGIME_ALIGNED"

    # Aligned BUY with move < 0.75% -> MOVE_TOO_SMALL
    ok, mode, reason = main.market_regime_allows_setup("BUY", "BULLISH", 0.5)
    assert ok is False
    assert mode == "ALIGNED"
    assert reason == "MOVE_TOO_SMALL"

    # 2. Aligned SELL (SELL + BEARISH)
    ok, mode, reason = main.market_regime_allows_setup("SELL", "BEARISH", -1.8)
    assert ok is True
    assert mode == "ALIGNED"

    # 3. Neutral / Unknown Regime
    ok, mode, reason = main.market_regime_allows_setup("SELL", "UNKNOWN", -1.6)
    assert ok is True
    assert mode == "NEUTRAL"
    assert reason == "REGIME_NEUTRAL_STRENGTH_OK"

    # Neutral with move < 1.00% -> NEUTRAL_REGIME_MOVE_TOO_SMALL
    ok, mode, reason = main.market_regime_allows_setup("SELL", "UNKNOWN", -0.8)
    assert ok is False
    assert mode == "NEUTRAL"
    assert reason == "NEUTRAL_REGIME_MOVE_TOO_SMALL"

    ok, mode, reason = main.market_regime_allows_setup("BUY", "NEUTRAL", 1.7)
    assert ok is True
    assert mode == "NEUTRAL"

    # 4. Countertrend JP SELL under BULLISH Nifty
    # Stock down -2.5% -> allowed to proceed to pattern checks (COUNTERTREND)
    ok, mode, reason = main.market_regime_allows_setup("SELL", "BULLISH", -2.5)
    assert ok is True
    assert mode == "COUNTERTREND"
    assert reason == "COUNTERTREND_PENDING_STRONG_CONFIRMATION"

    # Stock down -1.2% -> rejected as COUNTERTREND_MOVE_TOO_SMALL
    ok, mode, reason = main.market_regime_allows_setup("SELL", "BULLISH", -1.2)
    assert ok is False
    assert mode == "COUNTERTREND"
    assert reason == "COUNTERTREND_MOVE_TOO_SMALL"

    # 5. Countertrend Alpha BUY under BEARISH Nifty
    # Stock up +2.5% -> allowed to proceed
    ok, mode, reason = main.market_regime_allows_setup("BUY", "BEARISH", 2.5)
    assert ok is True
    assert mode == "COUNTERTREND"


def test_jp_countertrend_in_opposing_regime_full_flow():
    import signal_diagnostics
    now = datetime(2026, 9, 4, 11, 0, tzinfo=config.TIME_ZONE)

    # Construct candles for a JP BUY setup under BEARISH regime
    candles = []
    price = 100.0
    for i in range(13):
        t = now - timedelta(minutes=(15 - i) * 3)
        candles.append({
            "timestamp": t,
            "open": price,
            "high": price + 2.0,
            "low": price - 0.5,
            "close": price + 1.8,
            "volume": 1000,
        })
        price += 1.8

    # Pullback drops to band (band is around price - 8.0)
    t_pullback = now - timedelta(minutes=6)
    candles.append({
        "timestamp": t_pullback,
        "open": price,
        "high": price + 0.2,
        "low": price - 10.0, # touches band!
        "close": price - 4.0, # close above band_low
        "volume": 1000,
    })

    # Confirmation bar with standard volume 1.25x (fails countertrend 1.50x requirement)
    t_conf = now - timedelta(minutes=3)
    candles.append({
        "timestamp": t_conf,
        "open": price - 3.5,
        "high": price + 2.0,
        "low": price - 3.6,
        "close": price + 1.5,
        "volume": 1250,
    })

    df = pd.DataFrame(candles)

    # 1. Countertrend with +2.5% move and 1.25x conf volume -> REJECTED COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW
    cand_info = {"SECURITY_ID": 303, "rank": 1, "pct_change": 2.5, "volume": 100000, "is_bullish_setup": True}
    diag = signal_diagnostics.diagnose_jp_candidate(
        symbol="ATHER",
        security_id=303,
        candidate_info=cand_info,
        today_pattern_candles=df,
        regime="BEARISH",
        now=now,
    )
    assert diag["regime_mode"] == "COUNTERTREND"
    assert diag["reason"] == "COUNTERTREND_CONFIRMATION_VOLUME_TOO_LOW"
    assert diag["metrics"]["min_required"] == 1.50

    # 2. Increase confirmation volume to 1.60x -> WAITING for 1m trigger!
    df.loc[df.index[-1], "volume"] = 1600
    diag_pass = signal_diagnostics.diagnose_jp_candidate(
        symbol="ATHER",
        security_id=303,
        candidate_info=cand_info,
        today_pattern_candles=df,
        regime="BEARISH",
        now=now,
    )
    assert diag_pass["status"] == "WAITING"
    assert diag_pass["reason"] == "WAITING_FOR_1M_TRIGGER"
    assert diag_pass["regime_mode"] == "COUNTERTREND"


def test_jp_warmup_session_resampling_and_concatenation():
    """Verify get_pattern_candles_with_warmup resamples previous day and today separately and joins them."""
    broker = main.DhanBroker()
    prev_date = datetime(2026, 9, 8).date()
    today_date = datetime(2026, 9, 9).date()

    # Generate 60 mins of 1m bars on previous day (14:30 to 15:29)
    prev_1m_rows = []
    base_t_prev = datetime(2026, 9, 8, 14, 30, tzinfo=config.TIME_ZONE)
    for m in range(60):
        t = base_t_prev + timedelta(minutes=m)
        prev_1m_rows.append({
            "timestamp": t, "open": 100 + m*0.1, "high": 100.5 + m*0.1, "low": 99.8 + m*0.1, "close": 100.2 + m*0.1, "volume": 500
        })
    df_prev_1m = pd.DataFrame(prev_1m_rows)

    # Generate 12 mins of 1m bars today (09:15 to 09:26) -> 4 completed 3m bars (09:15, 09:18, 09:21, 09:24)
    today_1m_rows = []
    base_t_today = datetime(2026, 9, 9, 9, 15, tzinfo=config.TIME_ZONE)
    for m in range(12):
        t = base_t_today + timedelta(minutes=m)
        today_1m_rows.append({
            "timestamp": t, "open": 106 + m*0.1, "high": 106.5 + m*0.1, "low": 105.8 + m*0.1, "close": 106.2 + m*0.1, "volume": 800
        })
    df_today_1m = pd.DataFrame(today_1m_rows)

    def mock_get_intraday(security_id, exchange_segment, instrument_type, from_dt, to_dt, timeframe, skip_incomplete):
        if from_dt == prev_date:
            return df_prev_1m
        return df_today_1m

    with patch.object(broker, "get_intraday_candles", side_effect=mock_get_intraday), \
         patch("broker.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 9, 9, 9, 27, tzinfo=config.TIME_ZONE)
        res = broker.get_pattern_candles_with_warmup(
            security_id=123, exchange_segment="NSE_EQ", instrument_type="EQUITY", prev_trade_date=prev_date
        )

        assert not res.empty
        # 20 bars from previous day + 4 completed 3m bars today = 24 bars total
        assert len(res) == 24
        # First bar should be from previous day
        assert res.iloc[0]["timestamp"].date() == prev_date
        # Last bar should be 09:24 today
        assert res.iloc[-1]["timestamp"].date() == today_date
        assert res.iloc[-1]["timestamp"].time() == datetime.strptime("09:24", "%H:%M").time()


def test_jp_morning_setup_detected_with_warmup():
    """Verify a morning JP setup at 09:27 (4 today bars) successfully finds a setup and does not trigger on previous-day bars."""
    import signal_diagnostics
    prev_date = datetime(2026, 9, 8).date()
    today_date = datetime(2026, 9, 9).date()
    now = datetime(2026, 9, 9, 9, 27, tzinfo=config.TIME_ZONE)

    candles = []
    # 15 flat baseline bars + 5 trend bars on previous day
    for i in range(15):
        t = datetime(2026, 9, 8, 14, 15, tzinfo=config.TIME_ZONE) + timedelta(minutes=i * 3)
        candles.append({
            "timestamp": t, "open": 100.0, "high": 101.5, "low": 99.5, "close": 101.0, "volume": 1000
        })

    price = 101.0
    for i in range(5):
        t = datetime(2026, 9, 8, 15, 0, tzinfo=config.TIME_ZONE) + timedelta(minutes=i * 3)
        candles.append({
            "timestamp": t, "open": price, "high": price + 1.5, "low": price - 0.2, "close": price + 1.2, "volume": 1000
        })
        price += 1.2

    # Today 4 bars:
    # 09:15 trend continue
    candles.append({
        "timestamp": datetime(2026, 9, 9, 9, 15, tzinfo=config.TIME_ZONE),
        "open": price, "high": price + 1.5, "low": price - 0.2, "close": price + 1.2, "volume": 1000
    })
    price += 1.2

    # 09:18 trend continue
    candles.append({
        "timestamp": datetime(2026, 9, 9, 9, 18, tzinfo=config.TIME_ZONE),
        "open": price, "high": price + 1.5, "low": price - 0.2, "close": price + 1.2, "volume": 1000
    })
    price += 1.2

    # Calculate SMMA to place 09:21 pullback cleanly touching band
    df_temp = pd.DataFrame(candles)
    df_temp["jp_smma_close"] = jp_pattern.smma(df_temp["close"], config.JP_SMMA_LENGTH)
    band_low = df_temp.iloc[-1]["jp_smma_close"]

    # 09:21 pullback to band
    pullback_low = band_low - 0.2
    pullback_close = band_low + 0.1
    candles.append({
        "timestamp": datetime(2026, 9, 9, 9, 21, tzinfo=config.TIME_ZONE),
        "open": price, "high": price + 0.05, "low": pullback_low, "close": pullback_close, "volume": 1000
    })

    # 09:24 confirmation breakout
    candles.append({
        "timestamp": datetime(2026, 9, 9, 9, 24, tzinfo=config.TIME_ZONE),
        "open": pullback_close, "high": price + 1.0, "low": pullback_close - 0.2, "close": price + 0.8, "volume": 1600
    })

    df = pd.DataFrame(candles)

    # 1. With session_date=today_date, the 09:21 pullback setup is found
    setup = jp_pattern.find_jp_setup(df, is_bullish_setup=True, session_date=today_date)
    assert setup is not None
    assert setup["direction"] == "BUY"
    assert setup["jp_open_time"].time() == datetime.strptime("09:21", "%H:%M").time()

    # 2. If session_date is set to yesterday, no setup is detected (today's setup is ignored)
    setup_prev = jp_pattern.find_jp_setup(df, is_bullish_setup=True, session_date=prev_date)
    assert setup_prev is None

    # 3. Diagnostic check at 09:27 passes with WAITING status instead of INSUFFICIENT_HISTORY
    cand_info = {"SECURITY_ID": 555, "rank": 1, "pct_change": 2.5, "volume": 100000, "is_bullish_setup": True}
    diag = signal_diagnostics.diagnose_jp_candidate(
        symbol="PRESTIGE",
        security_id=555,
        candidate_info=cand_info,
        today_pattern_candles=df,
        regime="BULLISH",
        now=now,
        session_date=today_date,
    )
    assert diag["status"] == "WAITING"
    assert diag["reason"] == "WAITING_FOR_1M_TRIGGER"


def test_alpha_candidate_zero_later_bars_returns_waiting():
    """Verify newly formed Alpha pullback with 0 subsequent bars returns WAITING_3M_CONFIRMATION, not rejected."""
    now = datetime(2026, 9, 9, 9, 45, tzinfo=config.TIME_ZONE)
    candles = [
        {"timestamp": now - timedelta(minutes=12), "open": 3000.0, "high": 3020.0, "low": 2995.0, "close": 3018.0, "volume": 1000},
        {"timestamp": now - timedelta(minutes=9), "open": 3018.0, "high": 3040.0, "low": 3015.0, "close": 3038.0, "volume": 1000},
        {"timestamp": now - timedelta(minutes=6), "open": 3038.0, "high": 3060.0, "low": 3035.0, "close": 3058.0, "volume": 1000},
        # Candidate Red Alpha Pullback candle at 09:45 (high=3069.60, open=3068.0, close=3048.0, low=3047.0)
        {"timestamp": now - timedelta(minutes=3), "open": 3068.0, "high": 3069.60, "low": 3047.0, "close": 3048.0, "volume": 1000},
    ]
    df = pd.DataFrame(candles)
    res = pattern.find_alpha_setup(df, side="BUY")
    assert res is not None
    assert res["status"] == "WAITING_3M_CONFIRMATION"
    assert res["stage"] == "WAITING_3M_CONFIRMATION"
    assert res["confirmation_bars_seen"] == 0
    assert res["confirmation_bars_required"] == 2
    assert res["alpha_high"] == 3069.60

    # Diagnostic check reports WAITING with ALPHA_CANDIDATE_FORMED (not NO_NEXT_TWO_3M_CONFIRMATION)
    import signal_diagnostics
    cand_info = {"SECURITY_ID": 777, "rank": 1, "pct_change": 3.5, "volume": 200000}
    diag = signal_diagnostics.diagnose_alpha_candidate(
        symbol="ADANIENT",
        security_id=777,
        candidate_info=cand_info,
        today_pattern_candles=df,
        regime="BULLISH",
        now=now,
    )
    assert diag["status"] == "WAITING"
    assert diag["reason"] == "ALPHA_CANDIDATE_FORMED"
    assert diag["metrics"]["confirmation_bars_seen"] == 0
    assert diag["metrics"]["alpha_high"] == 3069.60


def test_alpha_candidate_first_bar_non_confirming_stays_waiting():
    """Verify Alpha candidate with 1 non-confirming bar stays in WAITING_3M_CONFIRMATION."""
    now = datetime(2026, 9, 9, 9, 48, tzinfo=config.TIME_ZONE)
    candles = [
        {"timestamp": now - timedelta(minutes=15), "open": 3000.0, "high": 3020.0, "low": 2995.0, "close": 3018.0, "volume": 1000},
        {"timestamp": now - timedelta(minutes=12), "open": 3018.0, "high": 3040.0, "low": 3015.0, "close": 3038.0, "volume": 1000},
        {"timestamp": now - timedelta(minutes=9), "open": 3038.0, "high": 3060.0, "low": 3035.0, "close": 3058.0, "volume": 1000},
        # Candidate Red Alpha at 09:42
        {"timestamp": now - timedelta(minutes=6), "open": 3068.0, "high": 3069.60, "low": 3047.0, "close": 3048.0, "volume": 1000},
        # 09:45: First confirmation bar fails to break alpha_high (high=3065 < 3069.60)
        {"timestamp": now - timedelta(minutes=3), "open": 3048.0, "high": 3065.0, "low": 3046.0, "close": 3060.0, "volume": 1100},
    ]
    df = pd.DataFrame(candles)
    res = pattern.find_alpha_setup(df, side="BUY")
    assert res is not None
    assert res["status"] == "WAITING_3M_CONFIRMATION"
    assert res["confirmation_bars_seen"] == 1

    import signal_diagnostics
    cand_info = {"SECURITY_ID": 777, "rank": 1, "pct_change": 3.5, "volume": 200000}
    diag = signal_diagnostics.diagnose_alpha_candidate(
        symbol="ADANIENT",
        security_id=777,
        candidate_info=cand_info,
        today_pattern_candles=df,
        regime="BULLISH",
        now=now,
    )
    assert diag["status"] == "WAITING"
    assert diag["reason"] == "FIRST_CONFIRMATION_NOT_QUALIFIED"
    assert diag["metrics"]["confirmation_bars_seen"] == 1


def test_alpha_candidate_second_bar_confirmation_progresses():
    """Verify Alpha candidate with second-bar breakout becomes CONFIRMED."""
    now = datetime(2026, 9, 9, 9, 51, tzinfo=config.TIME_ZONE)
    candles = [
        {"timestamp": now - timedelta(minutes=18), "open": 3000.0, "high": 3020.0, "low": 2995.0, "close": 3018.0, "volume": 1000},
        {"timestamp": now - timedelta(minutes=15), "open": 3018.0, "high": 3040.0, "low": 3015.0, "close": 3038.0, "volume": 1000},
        {"timestamp": now - timedelta(minutes=12), "open": 3038.0, "high": 3060.0, "low": 3035.0, "close": 3058.0, "volume": 1000},
        # Candidate Red Alpha at 09:39
        {"timestamp": now - timedelta(minutes=9), "open": 3068.0, "high": 3069.60, "low": 3047.0, "close": 3048.0, "volume": 1000},
        # Bar 1 at 09:42: inside bar
        {"timestamp": now - timedelta(minutes=6), "open": 3048.0, "high": 3065.0, "low": 3046.0, "close": 3060.0, "volume": 1000},
        # Bar 2 at 09:45: confirmed breakout above 3069.60 with 1.40x volume
        {"timestamp": now - timedelta(minutes=3), "open": 3060.0, "high": 3078.0, "low": 3058.0, "close": 3075.0, "volume": 1400},
    ]
    df = pd.DataFrame(candles)
    res = pattern.find_alpha_setup(df, side="BUY")
    assert res is not None
    assert res["status"] == "CONFIRMED"
    assert res["stage"] == "AWAITING_1M_TRIGGER"
    assert res["confirmation_volume_ratio"] >= 1.20

    # Also test evaluate_alpha_confirmation helper
    alpha_close_iso = (now - timedelta(minutes=9) + timedelta(minutes=3)).isoformat()
    watchlist_entry = {
        "direction": "BUY",
        "alpha_high": 3069.60,
        "alpha_low": 3045.0,
        "alpha_close_time": alpha_close_iso,
        "alpha_volume": 1000,
    }
    eval_res = pattern.evaluate_alpha_confirmation(df, watchlist_entry)
    assert eval_res["status"] == "CONFIRMED"
    assert eval_res["bars_seen"] == 2


def test_alpha_4_stage_state_machine_transitions():
    """Test full Alpha 4-stage lifecycle across 0, 1, 2 confirmation bars and expiration."""
    now = datetime(2026, 9, 9, 9, 45, tzinfo=config.TIME_ZONE)
    base_run = [
        {"timestamp": now - timedelta(minutes=12), "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.5, "volume": 1000},
        {"timestamp": now - timedelta(minutes=9), "open": 101.5, "high": 104.0, "low": 101.0, "close": 103.5, "volume": 1000},
        {"timestamp": now - timedelta(minutes=6), "open": 103.5, "high": 106.0, "low": 103.0, "close": 105.5, "volume": 1000},
        # Alpha candle (Red)
        {"timestamp": now - timedelta(minutes=3), "open": 105.5, "high": 106.0, "low": 103.8, "close": 104.0, "volume": 1000},
    ]

    # Stage 1: 0 confirmation bars seen -> WAITING_3M_CONFIRMATION
    df_0 = pd.DataFrame(base_run)
    res_0 = pattern.find_alpha_setup(df_0, side="BUY")
    assert res_0 is not None
    assert res_0["stage"] == "WAITING_3M_CONFIRMATION"
    assert res_0["confirmation_bars_seen"] == 0

    # Stage 2: 1 non-confirming bar seen -> WAITING_SECOND_3M_CONFIRMATION
    bar_1 = {"timestamp": now, "open": 104.0, "high": 105.0, "low": 103.5, "close": 104.5, "volume": 900}
    df_1 = pd.DataFrame(base_run + [bar_1])
    res_1 = pattern.find_alpha_setup(df_1, side="BUY")
    assert res_1 is not None
    assert res_1["stage"] == "WAITING_SECOND_3M_CONFIRMATION"
    assert res_1["confirmation_bars_seen"] == 1

    # Stage 3: 2 bars with successful confirmation on 2nd bar -> AWAITING_1M_TRIGGER
    bar_2_confirm = {"timestamp": now + timedelta(minutes=3), "open": 104.5, "high": 107.5, "low": 104.0, "close": 107.0, "volume": 1500}
    df_2_conf = pd.DataFrame(base_run + [bar_1, bar_2_confirm])
    res_2_conf = pattern.find_alpha_setup(df_2_conf, side="BUY")
    assert res_2_conf is not None
    assert res_2_conf["status"] == "CONFIRMED"
    assert res_2_conf["stage"] == "AWAITING_1M_TRIGGER"

    # Stage 4: 2 bars both non-confirming -> Expired
    bar_2_fail = {"timestamp": now + timedelta(minutes=3), "open": 104.5, "high": 105.5, "low": 104.0, "close": 104.8, "volume": 900}
    df_2_fail = pd.DataFrame(base_run + [bar_1, bar_2_fail])
    res_2_fail = pattern.find_alpha_setup(df_2_fail, side="BUY")
    assert res_2_fail is None

    # Test evaluate_alpha_confirmation returns EXPIRED on 2 non-confirming bars
    alpha_close_iso = (now - timedelta(minutes=3) + timedelta(minutes=3)).isoformat()
    w_entry = {"direction": "BUY", "alpha_high": 106.0, "alpha_low": 103.8, "alpha_close_time": alpha_close_iso, "alpha_volume": 1000}
    eval_exp = pattern.evaluate_alpha_confirmation(df_2_fail, w_entry)
    assert eval_exp["status"] == "EXPIRED"
    assert eval_exp["reason"] == "NO_NEXT_TWO_3M_CONFIRMATION"


def test_opening_momentum_detection_only(monkeypatch):
    """Test Opening Momentum detection only logs trigger and does not call enter_trade."""
    monkeypatch.setattr(config, "OPENING_MOMENTUM_ENABLED", True)
    monkeypatch.setattr(config, "OPENING_MOMENTUM_DETECTION_ONLY", True)

    captured = {}
    def fake_log_outcome(setup, outcome, details=""):
        captured["outcome"] = outcome
        captured["details"] = details

    def fake_enter_trade(*args, **kwargs):
        raise AssertionError("enter_trade must NEVER be called when OPENING_MOMENTUM_DETECTION_ONLY is True")

    monkeypatch.setattr(state, "log_setup_outcome", fake_log_outcome)
    monkeypatch.setattr(main.engine, "enter_trade", fake_enter_trade)
    monkeypatch.setattr(state, "snapshot", lambda: {"open_positions": {}})

    setup = {
        "security_id": "999",
        "symbol": "TATACHEM",
        "strategy": "OPENING_MOMENTUM",
        "direction": "BUY",
        "trigger_price": 1000.0,
        "stop_price": 980.0,
        "pattern_open_time": "2026-09-09T09:15:00+05:30",
        "pattern_close_time": "2026-09-09T09:18:00+05:30",
        "expires_at": "2026-09-09T09:33:00+05:30",
        "alpha_key": "OPENING_MOMENTUM_999_BUY_2026-09-09T09:15:00+05:30",
    }

    # 1-minute breakout candle at 09:19 (high=1005 > trigger=1000)
    latest_1m = {
        "timestamp": datetime(2026, 9, 9, 9, 19, tzinfo=config.TIME_ZONE),
        "open": 998.0,
        "high": 1005.0,
        "low": 997.0,
        "close": 1003.0,
    }

    res = main.process_new_1m_bar_for_setup(None, setup, latest_1m)
    assert res == "OPENING_MOMENTUM_TRIGGER_DETECTED_ONLY"
    assert captured["outcome"] == "OPENING_MOMENTUM_TRIGGER_DETECTED_ONLY"


def test_selective_thresholds_applied():
    """Verify selective thresholds: Alpha trend 0.40, Alpha pullback 0.25, Alpha vol 0.80, JP vol 0.75-2.25."""
    assert config.ALPHA_MIN_TREND_BODY_RATIO == 0.40
    assert config.ALPHA_MIN_ALPHA_BODY_RATIO == 0.25
    assert config.ALPHA_MIN_TREND_VOLUME_RATIO == 0.80
    assert config.JP_MIN_VOLUME_RATIO == 0.75
    assert config.JP_MAX_VOLUME_RATIO == 2.25
    assert config.DOJI_BODY_RATIO == 0.18
    assert config.ALPHA_MIN_TREND_BODY_TO_WICK_RATIO == 1.0
    assert config.ALPHA_MIN_ALPHA_BODY_TO_WICK_RATIO == 1.0
    assert config.JP_MIN_BODY_TO_WICK_RATIO == 1.0
    assert config.JP_MIN_BODY_RATIO == 0.25


def test_countertrend_policy_and_1m_close_requirement(monkeypatch):
    """Verify countertrend policy moves and 1m close requirement rejection."""
    # 1. Market regime allow function moves
    allowed_aligned, mode_al, _ = main.market_regime_allows_setup("BUY", "BULLISH", 0.80)
    assert allowed_aligned is True and mode_al == "ALIGNED"

    allowed_aligned_fail, _, _ = main.market_regime_allows_setup("BUY", "BULLISH", 0.60)
    assert allowed_aligned_fail is False

    allowed_neutral, mode_neu, _ = main.market_regime_allows_setup("BUY", "NEUTRAL", 1.10)
    assert allowed_neutral is True and mode_neu == "NEUTRAL"

    allowed_counter, mode_ct, _ = main.market_regime_allows_setup("BUY", "BEARISH", 1.60)
    assert allowed_counter is True and mode_ct == "COUNTERTREND"

    allowed_counter_fail, _, _ = main.market_regime_allows_setup("BUY", "BEARISH", 1.30)
    assert allowed_counter_fail is False

    # 2. Countertrend 1m close rejection when high crosses but close does not
    monkeypatch.setattr(config, "REGIME_COUNTERTREND_REQUIRE_1M_CLOSE_CONFIRMATION", True)
    captured = {}
    monkeypatch.setattr(state, "log_setup_outcome", lambda setup, outcome, details="": captured.update({"outcome": outcome}))
    monkeypatch.setattr(state, "snapshot", lambda: {"open_positions": {}})

    setup = {
        "security_id": "888",
        "symbol": "CANBK",
        "strategy": "ALPHA",
        "direction": "BUY",
        "trigger_price": 100.0,
        "pattern_close_time": "2026-09-09T09:24:00+05:30",
        "expires_at": "2026-09-09T09:40:00+05:30",
        "regime_mode": "COUNTERTREND",
        "signal_quality": {"confirmation_volume_ratio": 1.60},
    }

    # Wick crossed (high=101.0 > 100.0) but close did not (close=99.5 <= 100.0)
    bar = {
        "timestamp": datetime(2026, 9, 9, 9, 25, tzinfo=config.TIME_ZONE),
        "open": 99.0,
        "high": 101.0,
        "low": 98.5,
        "close": 99.5,
    }
    res = main.process_new_1m_bar_for_setup(None, setup, bar)
    assert res == "COUNTERTREND_1M_CLOSE_NOT_CONFIRMED"
    assert captured["outcome"] == "COUNTERTREND_1M_CLOSE_NOT_CONFIRMED"


def test_terminal_outcome_logging_for_all_paths(monkeypatch):
    """Verify explicit terminal outcomes logged on expiration, late trigger, extension, and entry."""
    outcomes = []
    monkeypatch.setattr(state, "log_setup_outcome", lambda setup, outcome, details="": outcomes.append(outcome))
    monkeypatch.setattr(state, "snapshot", lambda: {"open_positions": {}})

    # 1. SETUP_EXPIRED
    now = datetime.now(config.TIME_ZONE)
    setup_exp = {
        "security_id": "101", "symbol": "S1", "strategy": "JP", "direction": "BUY",
        "trigger_price": 100.0, "pattern_close_time": (now - timedelta(minutes=30)).isoformat(),
        "expires_at": (now - timedelta(minutes=15)).isoformat(),
    }
    bar_exp = {"timestamp": now - timedelta(minutes=10), "high": 99.0, "close": 98.0}
    main.process_new_1m_bar_for_setup(None, setup_exp, bar_exp)
    assert "SETUP_EXPIRED" in outcomes

    # 2. SKIPPED_LATE_TRIGGER
    monkeypatch.setattr(config, "MAX_ENTRY_DELAY_SECONDS", 10)
    setup_late = {
        "security_id": "102", "symbol": "S2", "strategy": "ALPHA", "direction": "BUY",
        "trigger_price": 100.0, "pattern_close_time": (now - timedelta(minutes=20)).isoformat(),
        "expires_at": (now + timedelta(minutes=10)).isoformat(),
    }
    # Bar from 5 minutes ago (delay = 4 mins > 10 seconds)
    old_bar = {"timestamp": now - timedelta(minutes=5), "high": 102.0, "close": 101.0}
    main.process_new_1m_bar_for_setup(None, setup_late, old_bar)
    assert "SKIPPED_LATE_TRIGGER" in outcomes

    # 3. ENTRY_REJECTED_EXTENSION
    mock_broker = MagicMock()
    mock_broker.get_ltp.return_value = 105.0  # 5% extension above 100.0 > max 0.35%
    setup_ext = {
        "security_id": "103", "symbol": "S3", "strategy": "ALPHA", "direction": "BUY",
        "trigger_price": 100.0, "pattern_close_time": (now - timedelta(minutes=10)).isoformat(),
        "expires_at": (now + timedelta(minutes=10)).isoformat(),
    }
    now_bar = {"timestamp": now - timedelta(seconds=2), "high": 101.0, "close": 100.8}
    main.process_new_1m_bar_for_setup(mock_broker, setup_ext, now_bar)
    assert "ENTRY_REJECTED_EXTENSION" in outcomes

