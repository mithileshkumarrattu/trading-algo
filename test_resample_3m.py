from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

import broker
import config


IST = ZoneInfo("Asia/Kolkata")


def _bars(times, offset=0):
    return pd.DataFrame({
        "timestamp": [pd.Timestamp(day, tz=IST) for day in times],
        "open": [100 + offset + i for i in range(len(times))],
        "high": [101 + offset + i for i in range(len(times))],
        "low": [99 + offset + i for i in range(len(times))],
        "close": [100.5 + offset + i for i in range(len(times))],
        "volume": [10] * len(times),
    })


def _resample(df):
    instance = object.__new__(broker.DhanBroker)
    instance.timeZone = IST
    return instance._resample_session_aligned_3m(df)


def test_nse_session_aligned_3m():
    df = _bars([
        datetime(2026, 9, 2, 9, 15), datetime(2026, 9, 2, 9, 16),
        datetime(2026, 9, 2, 9, 17), datetime(2026, 9, 2, 9, 18),
        datetime(2026, 9, 2, 9, 19), datetime(2026, 9, 2, 9, 20),
    ])
    result = _resample(df)
    assert list(result.timestamp.dt.strftime("%H:%M")) == ["09:15", "09:18"]
    first = result.iloc[0]
    assert first.open == 100
    assert first.high == 103
    assert first.low == 99
    assert first.close == 102.5
    assert first.volume == 30
    assert result.timestamp.dt.tz is not None


def test_missing_minute_excludes_bucket():
    df = _bars([
        datetime(2026, 9, 2, 9, 15), datetime(2026, 9, 2, 9, 16),
        datetime(2026, 9, 2, 9, 18), datetime(2026, 9, 2, 9, 19),
        datetime(2026, 9, 2, 9, 20),
    ])
    result = _resample(df)
    assert list(result.timestamp.dt.strftime("%H:%M")) == ["09:18"]


def test_out_of_session_bars_excluded():
    df = _bars([
        datetime(2026, 9, 2, 9, 15), datetime(2026, 9, 2, 9, 16),
        datetime(2026, 9, 2, 9, 17), datetime(2026, 9, 2, 15, 30),
        datetime(2026, 9, 2, 15, 31),
    ])
    result = _resample(df)
    assert list(result.timestamp.dt.strftime("%H:%M")) == ["09:15"]


def test_dhan_interval_three_is_rejected():
    instance = object.__new__(broker.DhanBroker)
    try:
        instance.get_intraday_candles(1, config.EXCHANGE, "EQUITY", datetime.now(IST), timeframe=3)
    except ValueError as exc:
        assert "interval must be one of" in str(exc)
    else:
        raise AssertionError("interval=3 must never be sent to Dhan")
