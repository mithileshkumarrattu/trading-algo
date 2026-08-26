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
        self.dhan: dhanhq | None = None
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
        self.start = time()
        self.login()

    def login(self):
        accessToken = get_valid_access_token()
        self.accessToken = accessToken
        if DhanSDK_LEGACY:
            self.dhan_context = DhanContext(config.CLIENT_ID, accessToken)
            self.dhan = dhanhq(self.dhan_context)
        else:
            self.dhan_context = {"client_id": str(config.CLIENT_ID), "access_token": accessToken}
            self.dhan = dhanhq(str(config.CLIENT_ID), accessToken)
        fundRes = self.dhan.get_fund_limits()
        if fundRes is not None and fundRes.get("status") == "success":
            bal = fundRes["data"].get("availabelBalance")
            logger.info(f"Available Margin: {bal}")
            if config.SEND_TELEGRAM_ON_LOGIN:
                send_telegram(f"AlphaCandle login successful. Available margin: Rs.{bal}")
        else:
            logger.error(f"Login failed: {fundRes}")
            if config.SEND_TELEGRAM_ON_LOGIN:
                send_telegram("AlphaCandle login FAILED. Check server immediately.")

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
        try:
            for _ in range(3):
                res = self.dhan.quote_data(securities={exchange_segment: [int(s) for s in security_ids]})
                if res and res.get("status") == "success":
                    return res["data"]["data"][exchange_segment]
                sleep(2)
        except Exception:
            logger.exception("Error in get_quote_batch")
        return None

    def get_net_change(self, exchange_segment, security_id):
        for _ in range(5):
            try:
                res = self.dhan.quote_data(securities={exchange_segment: [int(security_id)]})
                if res and res.get("status") == "success":
                    return float(res["data"]["data"][exchange_segment][str(security_id)]["net_change"])
            except Exception:
                logger.exception("Error in get_net_change")
            sleep(2)
        return None

    def get_intraday_candles(self, security_id, exchange_segment, instrument_type,
                              from_dt, to_dt=None, timeframe=1, skip_incomplete=True, tz="Asia/Kolkata"):
        retry_key = f"{security_id}_{timeframe}"
        if time() < _candle_retry_after.get(retry_key, 0):
            return None

        tzinfo = ZoneInfo(tz)
        from_str = from_dt.strftime("%Y-%m-%d")
        to_dt = to_dt or datetime.now()
        to_str = to_dt.strftime("%Y-%m-%d")
        for _ in range(3):
            try:
                res = self.dhan.intraday_minute_data(
                    security_id=security_id, exchange_segment=exchange_segment,
                    instrument_type=instrument_type, from_date=from_str, to_date=to_str,
                    interval=timeframe,
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

    def get_historical_daily_candles(self, security_id, exchange_segment, instrument_type, from_dt, to_dt, tz="Asia/Kolkata"):
        from_str = from_dt.strftime("%Y-%m-%d")
        to_str = to_dt.strftime("%Y-%m-%d")
        for _ in range(5):
            try:
                res = self.dhan.historical_daily_data(
                    security_id=security_id, exchange_segment=exchange_segment,
                    instrument_type=instrument_type, from_date=from_str, to_date=to_str,
                )
                df = pd.DataFrame(res["data"])
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

    def _market_feed_worker(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        version = "v2"
        dhanDataws = None
        while not self.stop_event.is_set():
            try:
                logger.info("AlphaCandle websocket connecting...")
                if DhanSDK_LEGACY:
                    dhanDataws = MarketFeed(self.dhan_context, self.instruments, version)
                else:
                    dhanDataws = FeedClient(str(config.CLIENT_ID), self.accessToken, self.instruments, version)
                dhanDataws.run_forever()
                logger.info("AlphaCandle websocket connected")

                while not self.stop_event.is_set():
                    try:
                        tick = dhanDataws.get_data()
                        if tick:
                            self.data_queue.put(tick)
                    except Exception:
                        raise Exception("Feed dropped")

                    try:
                        cmd, payload = self.cmd_queue.get_nowait()
                        if cmd == "SUB":
                            dhanDataws.subscribe_symbols(payload)
                            self.instruments.extend(payload)
                        elif cmd == "UNSUB":
                            dhanDataws.unsubscribe_symbols(payload)
                        elif cmd == "CLOSE":
                            dhanDataws.close_connection()
                            self.stop_event.set()
                            break
                    except Empty:
                        pass
            except Exception as e:
                logger.error(f"AlphaCandle websocket error: {e}")
                if dhanDataws:
                    try:
                        dhanDataws.close_connection()
                    except Exception:
                        pass
                sleep(3)

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