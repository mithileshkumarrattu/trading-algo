"""
AlphaCandle - Isolated Dhan Broker Adapter.

Same underlying dhanhq SDK calls Ali's original brokerClass.py used (proven
working methods: place_order, modify_order, cancel_order, quote_data,
intraday_minute_data, historical_daily_data, get_positions, get_fund_limits,
MarketFeed, OrderUpdate) - rewritten standalone here with zero import
dependency on the original folder.
"""
from time import time, sleep
from zoneinfo import ZoneInfo
import json
import asyncio
import math
import logging
import threading
from collections import defaultdict
from decimal import Decimal
from datetime import datetime, timedelta
from queue import Queue, Empty
from typing import Any

import pandas as pd

try:
    from dhanhq import DhanContext, dhanhq, MarketFeed, OrderUpdate
    DhanSDK_LEGACY = True
except ImportError:
    from dhanhq.dhanhq import dhanhq as DhanApiClient
    from dhanhq.marketfeed import DhanFeed as FeedClient
    from dhanhq.orderupdate import OrderSocket as OrderUpdate

    class MarketFeed:
        IDX = 0
        NSE = 1
        Ticker = 15

    DhanContext = None
    DhanSDK_LEGACY = False
    dhanhq = DhanApiClient

import config
from dhan_token_manager import get_valid_access_token
from notifier import send_telegram

logger = logging.getLogger(__name__)
_malformed_candle_warned_at = {}
_candle_retry_after = {}

WS_MAX_SUBSCRIBE_BATCH = 100
WS_RECONNECT_DELAYS = [3, 5, 10, 20, 30]


class RequestPacer:
    def __init__(self, min_interval_sec: float):
        self.min_interval_sec = float(min_interval_sec)
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self):
        with self._lock:
            now = time()
            delay = max(0.0, self._next_allowed - now)
            self._next_allowed = max(now, self._next_allowed) + self.min_interval_sec
        if delay:
            sleep(delay)


def _reject_candle_response(security_id, timeframe, message):
    key = f"{security_id}_{timeframe}"
    now_ts = time()
    last_warn = _malformed_candle_warned_at.get(key, 0)
    if now_ts - last_warn >= 60:
        logger.warning(
            f"Candle data for {security_id}, timeframe={timeframe} "
            f"{message}; skipping this symbol for 60 seconds"
        )
        _malformed_candle_warned_at[key] = now_ts
    _candle_retry_after[key] = now_ts + 60
    return None


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DhanBroker(metaclass=SingletonMeta):
    def __init__(self):
        self.dhan: Any = None
        self.dhan_context = None
        self.liveFeed = {}
        self.orderPool = {}
        self.accessToken = None
        self.instruments = [(MarketFeed.IDX, str(config.INDEX_SECURITY_ID), MarketFeed.Ticker)]
        self.timeZone = ZoneInfo("Asia/Kolkata")
        self.ordertag = "ALPHACANDLE"
        self.data_queue = Queue()
        self.cmd_queue = Queue()
        self.stop_event = threading.Event()
        self.quote_pacer = RequestPacer(1.0 / config.QUOTE_API_MAX_REQUESTS_PER_SECOND)
        self.data_pacer = RequestPacer(1.0 / config.DATA_API_MAX_REQUESTS_PER_SECOND)
        self.rate_limited_until = 0.0
        self._quote_cooldown_warned_until = 0.0
        self.start = time()
        self.login()

    def _wait_for_api(self, pacer):
        cooldown = max(0.0, self.rate_limited_until - time())
        if cooldown:
            sleep(cooldown)
        pacer.wait()

    def _mark_rate_limited(self):
        self.rate_limited_until = max(
            self.rate_limited_until,
            time() + config.QUOTE_RATE_LIMIT_COOLDOWN_SEC,
        )

    def is_quote_cooldown_active(self):
        return time() < self.rate_limited_until

    def login(self):
        accessToken = get_valid_access_token()
        self._configure_api_client(accessToken)
        fundRes = self.dhan.get_fund_limits()
        if self._is_auth_failure(fundRes):
            logger.warning("Cached Dhan token rejected; requesting a fresh access token")
            try:
                self._configure_api_client(get_valid_access_token(force_refresh=True))
                fundRes = self.dhan.get_fund_limits()
            except Exception:
                logger.exception("Dhan access-token refresh failed during login")
        if fundRes is not None and fundRes.get("status") == "success":
            bal = fundRes["data"].get("availabelBalance")
            logger.info(f"Available Margin: {bal}")
            if config.SEND_TELEGRAM_ON_LOGIN:
                send_telegram(f"AlphaCandle login successful. Available margin: Rs.{bal}")
        else:
            logger.error(f"Login failed: {fundRes}")
            if config.SEND_TELEGRAM_ON_LOGIN:
                send_telegram("AlphaCandle login FAILED. Check server immediately.")

    @staticmethod
    def _is_auth_failure(response):
        if not isinstance(response, dict):
            return False
        response_text = str(response)
        return "DH-906" in response_text or "DH-808" in response_text or "Invalid Token" in response_text or "Authentication Failed" in response_text

    def _configure_api_client(self, access_token):
        self.accessToken = access_token
        if DhanSDK_LEGACY:
            self.dhan_context = DhanContext(config.CLIENT_ID, access_token)
            self.dhan = dhanhq(self.dhan_context)
        else:
            self.dhan_context = {"client_id": str(config.CLIENT_ID), "access_token": access_token}
            self.dhan = dhanhq(str(config.CLIENT_ID), access_token)

    def get_fund_limits(self):
        try:
            res = self.dhan.get_fund_limits()
            if res and res.get("status") == "success":
                return res["data"]
        except Exception:
            logger.exception("Error fetching fund limits")
        return None

    def truncate(self, number, tick_size=0.05, floor_or_ceil=None):
        if not number:
            return number
        tick_size = Decimal(str(tick_size))
        number = Decimal(str(number))
        remainder = number % tick_size
        if remainder == 0:
            return float(number)
        if floor_or_ceil is None:
            floor_or_ceil = "ceil" if (remainder >= tick_size / 2) else "floor"
        if floor_or_ceil == "ceil":
            number = number - remainder + tick_size
        else:
            number = number - remainder
        decimals = len(format(Decimal(repr(float(tick_size))), "f").split(".")[1])
        return float(round(number, decimals))

    def get_limit_price(self, exchange_segment: str, security_id: int, transaction_type: str):
        for _ in range(4):
            try:
                self._wait_for_api(self.quote_pacer)
                res = self.dhan.quote_data(securities={exchange_segment: [int(security_id)]})
                depth = res["data"]["data"][exchange_segment][str(security_id)]["depth"]
                if transaction_type == "BUY":
                    return float(depth["sell"][2]["price"])
                return float(depth["buy"][2]["price"])
            except Exception:
                logger.exception("Error getting limit price")
            sleep(2)
        return None

    def place_order(self, security_id, transaction_type, exchange_segment, qty,
                     order_type="LIMIT", product_type="INTRADAY", limit_price=0,
                     trigger_price=0, tick_size=0.05):
        try:
            res = self.dhan.place_order(
                security_id=str(security_id),
                exchange_segment=exchange_segment,
                transaction_type=transaction_type,
                quantity=int(qty),
                order_type=order_type,
                product_type=product_type,
                price=self.truncate(limit_price, tick_size),
                trigger_price=self.truncate(trigger_price, tick_size),
                tag=self.ordertag,
            )
            logger.info(f"place_order -> {security_id} {transaction_type} {qty} {order_type} limit={limit_price} trigger={trigger_price} :: {res}")
            if res and res.get("status") == "success":
                return res["data"]["orderId"]
        except Exception:
            logger.exception(f"Error placing order for {security_id}")
        return None

    def modify_order(self, order_info: dict, new_trigger_price, tick_size=0.05, new_limit_price=None):
        try:
            if new_limit_price is None:
                new_limit_price = (new_trigger_price + 10 * tick_size) if order_info["transactionType"] == "BUY" else (new_trigger_price - 10 * tick_size)
            order_id = order_info.get("orderId") or order_info.get("orderNo")
            res = self.dhan.modify_order(
                order_id=order_id,
                order_type=order_info["orderType"],
                leg_name=order_info.get("legName", "NA"),
                quantity=order_info["quantity"],
                price=new_limit_price,
                trigger_price=self.truncate(new_trigger_price, tick_size),
                disclosed_quantity=order_info.get("disclosedQuantity", 0),
                validity=order_info.get("validity", "DAY"),
            )
            logger.info(f"modify_order -> {order_id} new_trigger={new_trigger_price} :: {res}")
            return res
        except Exception:
            logger.exception(f"Error modifying order {order_info}")
        return None

    def cancel_order(self, order_id):
        try:
            res = self.dhan.cancel_order(order_id)
            logger.info(f"cancel_order -> {order_id} :: {res}")
            return res
        except Exception:
            logger.exception(f"Error cancelling order {order_id}")
        return None

    def get_order_by_id(self, order_id):
        for _ in range(6):
            try:
                res = self.dhan.get_order_by_id(order_id)
                if res and res.get("status") == "success":
                    return res["data"][0]
            except Exception:
                logger.exception(f"Error fetching order {order_id}")
            sleep(1)
        return None

    def get_order_status(self, order_id):
        order_id = str(order_id)
        if order_id in self.orderPool:
            return self.orderPool[order_id]
        return self.get_order_by_id(order_id)

    def get_positions(self):
        try:
            res = self.dhan.get_positions()
            if res and res.get("status") == "success":
                return res["data"]
        except Exception:
            logger.exception("Error fetching positions")
        return []

    def close_position_by_security(self, security_id, qty, transaction_type):
        positions = self.get_positions()
        for position in positions:
            try:
                net_qty = int(position["netQty"])
                if position["netQty"] != 0 and str(position["securityId"]) == str(security_id) and \
                   ((transaction_type == "BUY" and net_qty > 0) or (transaction_type == "SELL" and net_qty < 0)):
                    close_type = "SELL" if net_qty > 0 else "BUY"
                    limit_price = self.get_limit_price(position["exchangeSegment"], position["securityId"], close_type)
                    return self.place_order(
                        security_id=position["securityId"], transaction_type=close_type,
                        exchange_segment=position["exchangeSegment"],
                        qty=min(abs(net_qty), qty), order_type="LIMIT",
                        product_type=position["productType"], limit_price=limit_price,
                    )
            except Exception:
                logger.exception(f"Error closing position {security_id}")
        return None

    def get_ltp_from_api(self, exchange_segment, security_id):
        try:
            self._wait_for_api(self.quote_pacer)
            res = self.dhan.quote_data(securities={exchange_segment: [int(security_id)]})
            if res and res.get("status") == "success":
                return float(res["data"]["data"][exchange_segment][str(security_id)]["last_price"])
        except Exception:
            logger.exception(f"Error getting LTP for {security_id}")
        return None

    def get_ltp(self, security_id, exchange_segment="NSE_EQ"):
        security_id = str(security_id)
        try:
            if security_id in self.liveFeed:
                feed = self.liveFeed[security_id]
                delay = (datetime.now(self.timeZone) - feed["ltt"]).total_seconds()
                if delay > 120:
                    return self.get_ltp_from_api(exchange_segment, security_id)
                return feed["ltp"]
            return self.get_ltp_from_api(exchange_segment, security_id)
        except Exception:
            logger.exception("Error in get_ltp")
        return None

    def get_quote_batch(self, exchange_segment, security_ids):
        security_ids = [str(s) for s in security_ids if s is not None]
        if not security_ids:
            logger.warning("Quote batch skipped: no security IDs")
            return {}

        logger.info(
            "Quote batch request: segment=%s ids=%s sample=%s",
            exchange_segment,
            len(security_ids),
            security_ids[:3],
        )
        if time() < self.rate_limited_until:
            if time() >= self._quote_cooldown_warned_until:
                logger.warning("Quote API cooldown active; skipping requests")
                self._quote_cooldown_warned_until = self.rate_limited_until
            return {}

        request = {exchange_segment: [int(s) for s in security_ids]}
        refreshed = False
        for attempt in range(3):
            try:
                if time() < self.rate_limited_until:
                    if time() >= self._quote_cooldown_warned_until:
                        logger.warning("Quote API cooldown active; skipping requests")
                        self._quote_cooldown_warned_until = self.rate_limited_until
                    return {}
                self._wait_for_api(self.quote_pacer)
                res = self.dhan.quote_data(securities=request)
                if not isinstance(res, dict):
                    logger.warning(
                        "Quote batch unexpected response type: %s",
                        type(res).__name__,
                    )
                    return {}

                logger.info("Quote batch response: keys=%s", list(res.keys())[:10])
                if res.get("status") == "success":
                    quotes = self._extract_quote_data(res, exchange_segment)
                    logger.info(
                        "Quote batch parsed: %s/%s usable quotes",
                        len(quotes),
                        len(security_ids),
                    )
                    return quotes

                logger.warning("Dhan quote request failed: %s", res.get("data") if isinstance(res, dict) else res)
                error_data = res.get("data") if isinstance(res, dict) else None
                error_text = str(error_data)
                if "805" in error_text or "904" in error_text or "too many" in error_text.lower():
                    self._mark_rate_limited()
                    logger.warning(
                        "Dhan quote API rate-limited; pausing quote discovery for %ss",
                        config.QUOTE_RATE_LIMIT_COOLDOWN_SEC,
                    )
                    return {}
                if not refreshed and ("808" in error_text or "Authentication Failed" in error_text):
                    refreshed = True
                    logger.warning("Dhan quote authentication failed; refreshing access token")
                    try:
                        self._configure_api_client(get_valid_access_token(force_refresh=True))
                        continue
                    except Exception:
                        logger.exception("Dhan access-token refresh failed")
                        break
                sleep(2)
            except Exception:
                logger.exception("Error in get_quote_batch")
        return {}

    @staticmethod
    def _extract_quote_data(response, exchange_segment):
        candidates = [response]
        data = response.get("data")
        if isinstance(data, dict):
            candidates.append(data)
            nested_data = data.get("data")
            if isinstance(nested_data, dict):
                candidates.append(nested_data)

        raw_quotes = None
        for candidate in candidates:
            segment_quotes = candidate.get(exchange_segment)
            if isinstance(segment_quotes, dict):
                raw_quotes = segment_quotes
                break
            security_wise = candidate.get("securityWise")
            if isinstance(security_wise, dict):
                raw_quotes = security_wise
                break

        if raw_quotes is None:
            return {}

        diagnostic_logged = getattr(DhanBroker, "_quote_schema_logged", False)
        if raw_quotes:
            first_security_id, first_quote = next(iter(raw_quotes.items()))
            if not diagnostic_logged:
                logger.info(
                    "Dhan quote sample: security_id=%s keys=%s values=%s",
                    first_security_id,
                    list(first_quote.keys()) if isinstance(first_quote, dict) else type(first_quote).__name__,
                    {
                        key: first_quote.get(key)
                        for key in (
                            "last_price", "lastPrice", "ltp", "net_change", "netChange",
                            "change", "change_percent", "net_change_percent",
                            "previous_close", "prev_close", "previousClose", "close",
                        )
                        if isinstance(first_quote, dict) and key in first_quote
                    },
                )
                DhanBroker._quote_schema_logged = True

        normalized = {}
        for security_id, quote in raw_quotes.items():
            if not isinstance(quote, dict):
                continue
            last_price = quote.get("last_price") or quote.get("lastPrice") or quote.get("ltp")
            net_change = quote.get("net_change")
            if net_change is None:
                net_change = quote.get("netChange", quote.get("change"))
            prev_close = (
                quote.get("previous_close") or quote.get("prev_close")
                or quote.get("previousClose") or quote.get("close")
            )
            if last_price is None:
                continue
            try:
                last_price = float(last_price)
                if net_change is None and prev_close is not None:
                    net_change = last_price - float(prev_close)
                if net_change is None:
                    continue
                normalized[str(security_id)] = {
                    "last_price": last_price,
                    "net_change": float(net_change),
                    "volume": float(quote.get("volume", 0.0) or 0.0),
                }
            except (TypeError, ValueError):
                logger.warning("Skipping malformed quote for %s", security_id)
        return normalized

    def get_net_change(self, exchange_segment, security_id):
        for _ in range(5):
            try:
                self._wait_for_api(self.quote_pacer)
                res = self.dhan.quote_data(securities={exchange_segment: [int(security_id)]})
                if res and res.get("status") == "success":
                    return float(res["data"]["data"][exchange_segment][str(security_id)]["net_change"])
            except Exception:
                logger.exception("Error in get_net_change")
            sleep(2)
        return None

    def get_intraday_candles(self, security_id, exchange_segment, instrument_type,
                              from_dt, to_dt=None, timeframe=1, skip_incomplete=True, tz="Asia/Kolkata"):
        native_intervals = {1, 5, 15, 25, 60}
        if int(timeframe) not in native_intervals:
            raise ValueError(
                f"Dhan native interval must be one of {sorted(native_intervals)}; got {timeframe}. "
                "Use get_pattern_candles() for derived 3-minute candles."
            )
        retry_key = f"{security_id}_{timeframe}"
        if time() < _candle_retry_after.get(retry_key, 0):
            return None

        tzinfo = ZoneInfo(tz)
        from_str = from_dt.strftime("%Y-%m-%d")
        to_dt = to_dt or datetime.now()
        to_str = to_dt.strftime("%Y-%m-%d")
        refreshed = False
        for _ in range(3):
            try:
                self._wait_for_api(self.data_pacer)
                res = self.dhan.intraday_minute_data(
                    security_id=security_id, exchange_segment=exchange_segment,
                    instrument_type=instrument_type, from_date=from_str, to_date=to_str,
                    interval=timeframe,
                )
                if not isinstance(res, dict) or res.get("status") != "success":
                    if not refreshed and self._is_auth_failure(res):
                        refreshed = True
                        logger.warning("Intraday candle request authentication failed; refreshing access token once")
                        try:
                            self._configure_api_client(get_valid_access_token(force_refresh=True))
                            continue
                        except Exception:
                            logger.exception("Dhan access-token refresh failed during intraday candle retry")
                    return _reject_candle_response(
                        security_id, timeframe, f"failed: {res.get('data') if isinstance(res, dict) else res}"
                    )

                data = res.get("data")
                if not data or not isinstance(data, dict):
                    return _reject_candle_response(security_id, timeframe, "returned empty or malformed data")

                # Ensure every value is a list (guards against scalar-only responses)
                if not all(isinstance(value, list) for value in data.values()):
                    return _reject_candle_response(security_id, timeframe, "is malformed")

                lengths = {len(values) for values in data.values()}
                if len(lengths) > 1 or (lengths and next(iter(lengths)) == 0):
                    return _reject_candle_response(security_id, timeframe, "has inconsistent or empty columns")

                df = pd.DataFrame(data)
                if df.empty:
                    return _reject_candle_response(security_id, timeframe, "returned no rows")
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(tz)
                if skip_incomplete:
                    now = datetime.now(tzinfo)
                    day_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
                    minutes_from_start = math.floor((now - day_start).seconds / 60)
                    last_tf = int(minutes_from_start / timeframe) * timeframe - timeframe
                    last_ts = day_start + timedelta(minutes=last_tf)
                    df = df[df.timestamp <= last_ts]
                return df
            except Exception:
                logger.exception(f"Error fetching intraday candles for {security_id}")
            sleep(1)
        return pd.DataFrame()

    def _completed_1m_only(self, df):
        current_open = datetime.now(self.timeZone).replace(second=0, microsecond=0)
        return df[df["timestamp"] < current_open].copy()

    def _resample_session_aligned_3m(self, one_min_df):
        columns = ["timestamp", "open", "high", "low", "close", "volume"]
        df = one_min_df.copy().sort_values("timestamp").drop_duplicates("timestamp")
        df = df.set_index("timestamp").between_time("09:15", "15:29")
        if df.empty:
            return pd.DataFrame(columns=columns)

        pieces = []
        for session_date, day in df.groupby(df.index.date):
            session_open = pd.Timestamp(session_date, tz=self.timeZone).replace(
                hour=9, minute=15, second=0, microsecond=0
            )
            offset = ((day.index - session_open).total_seconds() // 60).astype(int)
            day = day.assign(_bucket=(offset // 3) * 3)
            grouped = day.groupby("_bucket", sort=True).agg(
                open=("open", "first"), high=("high", "max"), low=("low", "min"),
                close=("close", "last"), volume=("volume", "sum"), _count=("close", "size")
            ).reset_index()
            grouped["timestamp"] = session_open + pd.to_timedelta(grouped["_bucket"], unit="min")
            if config.DERIVED_CANDLE_REQUIRE_FULL_BUCKET:
                grouped = grouped[grouped["_count"] == config.DERIVED_CANDLE_MINUTE_COUNT]
            pieces.append(grouped[columns])
        if not pieces:
            return pd.DataFrame(columns=columns)
        return pd.concat(pieces, ignore_index=True).sort_values("timestamp").reset_index(drop=True)

    def get_pattern_candles(self, security_id, exchange_segment, instrument_type,
                            from_dt, pattern_timeframe, to_dt=None):
        source = self.get_intraday_candles(
            security_id=security_id, exchange_segment=exchange_segment,
            instrument_type=instrument_type, from_dt=from_dt, to_dt=to_dt,
            timeframe=config.SOURCE_CANDLE_TIMEFRAME, skip_incomplete=False,
        )
        if source is None or source.empty:
            return pd.DataFrame()
        if int(pattern_timeframe) == 1:
            return self._completed_1m_only(source)
        if int(pattern_timeframe) == 3:
            return self._resample_session_aligned_3m(self._completed_1m_only(source))
        return self.get_intraday_candles(
            security_id=security_id, exchange_segment=exchange_segment,
            instrument_type=instrument_type, from_dt=from_dt, to_dt=to_dt,
            timeframe=int(pattern_timeframe), skip_incomplete=True,
        )

    def get_historical_daily_candles(self, security_id, exchange_segment, instrument_type, from_dt, to_dt, tz="Asia/Kolkata"):
        from_str = from_dt.strftime("%Y-%m-%d")
        to_str = to_dt.strftime("%Y-%m-%d")
        refreshed = False
        for _ in range(5):
            try:
                self._wait_for_api(self.data_pacer)
                res = self.dhan.historical_daily_data(
                    security_id=security_id, exchange_segment=exchange_segment,
                    instrument_type=instrument_type, from_date=from_str, to_date=to_str,
                )
                if not isinstance(res, dict) or res.get("status") != "success":
                    logger.warning(
                        "Historical candle request failed for %s: %s",
                        security_id,
                        res.get("data") if isinstance(res, dict) else res,
                    )
                    if not refreshed and self._is_auth_failure(res):
                        refreshed = True
                        logger.warning("Historical candle request authentication failed; refreshing access token once")
                        try:
                            self._configure_api_client(get_valid_access_token(force_refresh=True))
                            continue
                        except Exception:
                            logger.exception("Dhan access-token refresh failed during historical candle retry")
                    return pd.DataFrame()
                candle_data = res.get("data")
                if not isinstance(candle_data, (dict, list)):
                    logger.warning(
                        "Historical candle response malformed for %s: data_type=%s",
                        security_id,
                        type(candle_data).__name__,
                    )
                    return pd.DataFrame()
                df = pd.DataFrame(candle_data)
                if df.empty:
                    return df
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(tz)
                return df
            except Exception:
                logger.exception(f"Error fetching historical candles for {security_id}")
            sleep(2)
        return pd.DataFrame()

    def subscribe_symbols(self, symbols):
        self.cmd_queue.put(("SUB", symbols))

    def unsubscribe_symbols(self, symbols):
        self.cmd_queue.put(("UNSUB", symbols))

    def close_connection(self):
        self.cmd_queue.put(("CLOSE", None))
        self.stop_event.set()

    def _subscribe_payload(self, dhanDataws, payload):
        if not payload:
            return
        existing = {(item[0], str(item[1]), item[2]) for item in self.instruments}
        new_symbols = []
        for item in payload:
            key = (item[0], str(item[1]), item[2])
            if key not in existing:
                existing.add(key)
                new_symbols.append(item)
        if not new_symbols:
            return

        total_batches = (len(new_symbols) + WS_MAX_SUBSCRIBE_BATCH - 1) // WS_MAX_SUBSCRIBE_BATCH
        for idx in range(0, len(new_symbols), WS_MAX_SUBSCRIBE_BATCH):
            batch_num = (idx // WS_MAX_SUBSCRIBE_BATCH) + 1
            batch = new_symbols[idx:idx + WS_MAX_SUBSCRIBE_BATCH]
            logger.info(
                "WebSocket subscribe batch %d/%d: %d instruments",
                batch_num,
                total_batches,
                len(batch),
            )
            if dhanDataws:
                try:
                    dhanDataws.subscribe_symbols(batch)
                except Exception:
                    logger.exception(
                        "Failed to subscribe batch %d/%d to Dhan MarketFeed",
                        batch_num,
                        total_batches,
                    )
                    raise
            self.instruments.extend(batch)

    def _drain_websocket_commands(self, dhanDataws):
        while True:
            try:
                cmd, payload = self.cmd_queue.get_nowait()
            except Empty:
                break
            if cmd == "SUB":
                self._subscribe_payload(dhanDataws, payload)
            elif cmd == "UNSUB":
                if payload:
                    to_remove = {(item[0], str(item[1]), item[2]) for item in payload}
                    self.instruments = [
                        item for item in self.instruments
                        if (item[0], str(item[1]), item[2]) not in to_remove
                    ]
                    if dhanDataws:
                        try:
                            dhanDataws.unsubscribe_symbols(payload)
                        except Exception:
                            logger.exception("Failed to unsubscribe symbols from Dhan MarketFeed")
            elif cmd == "CLOSE":
                if dhanDataws:
                    try:
                        dhanDataws.close_connection()
                    except Exception:
                        pass
                self.stop_event.set()
                break

    def _market_feed_worker(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        version = "v2"
        dhanDataws = None
        backoff_idx = 0
        while not self.stop_event.is_set():
            try:
                logger.info("AlphaCandle websocket connecting...")
                if DhanSDK_LEGACY:
                    dhanDataws = MarketFeed(self.dhan_context, self.instruments, version)
                else:
                    dhanDataws = FeedClient(str(config.CLIENT_ID), self.accessToken, self.instruments, version)
                dhanDataws.run_forever()
                logger.info("AlphaCandle websocket connected")

                self._drain_websocket_commands(dhanDataws)
                logger.info("WebSocket connected; subscribed=%d", len(self.instruments))

                healthy_ticks = 0
                while not self.stop_event.is_set():
                    self._drain_websocket_commands(dhanDataws)
                    try:
                        tick = dhanDataws.get_data()
                        if tick:
                            self.data_queue.put(tick)
                            healthy_ticks += 1
                            if healthy_ticks >= 5:
                                backoff_idx = 0
                    except Exception as exc:
                        logger.exception(
                            "Dhan MarketFeed get_data failed: type=%s message=%s",
                            type(exc).__name__,
                            exc,
                        )
                        raise
            except Exception as e:
                logger.error("AlphaCandle websocket error: %s", e)
                if dhanDataws:
                    try:
                        dhanDataws.close_connection()
                    except Exception:
                        pass
                delay = WS_RECONNECT_DELAYS[min(backoff_idx, len(WS_RECONNECT_DELAYS) - 1)]
                backoff_idx += 1
                logger.info("Reconnecting websocket in %ss (backoff level %d)...", delay, backoff_idx)
                sleep(delay)

    def _data_consumer(self):
        while not self.stop_event.is_set():
            try:
                response = self.data_queue.get(timeout=1)
                if response and response.get("type") in ["Ticker Data", "Full Data"]:
                    sec_id = str(response["security_id"])
                    ltp = float(response["LTP"])
                    ltt = datetime.strptime(
                        f"{datetime.now(self.timeZone).date()} {response['LTT']}", "%Y-%m-%d %H:%M:%S"
                    ).replace(tzinfo=self.timeZone)
                    self.liveFeed[sec_id] = {"ltp": ltp, "ltt": ltt}
            except Empty:
                pass
            except Exception as e:
                logger.error(f"AlphaCandle data consumer error: {e}")

    async def _on_order_update(self, order_data: dict):
        if order_data.get("Type") == "order_alert":
            data = order_data.get("Data", {})
            if "orderNo" in data:
                order_id = data["orderNo"]
                status = data.get("status", "Unknown")
                data.update({"orderStatus": status.upper()})
                self.orderPool[order_id] = data
                logger.info(f"Order update {order_id}: {status}")

    def _run_order_update(self):
        if DhanSDK_LEGACY:
            order_client = OrderUpdate(self.dhan_context)
        else:
            order_client = OrderUpdate(str(config.CLIENT_ID), self.accessToken)
        order_client.handle_order_update = self._on_order_update
        while not self.stop_event.is_set():
            try:
                order_client.connect_to_dhan_websocket_sync()
            except Exception as e:
                logger.error(f"Order update websocket error: {e}. Reconnecting in 5s...")
                sleep(5)

    def start_websocket(self):
        threading.Thread(target=self._market_feed_worker, daemon=True).start()
        threading.Thread(target=self._data_consumer, daemon=True).start()
        if not config.PAPER_MODE:
            # Order Update websocket only needed when REAL orders are placed.
            # Skipping it in PAPER_MODE avoids the dhanhq SDK's known
            # "Extra data" JSON-parse reconnect-loop bug on this feed, since
            # there are no real order status events to listen for anyway.
            threading.Thread(target=self._run_order_update, daemon=True).start()
        else:
            logger.info("PAPER_MODE active - skipping Order Update websocket (not needed for simulated orders)")