"""
AlphaCandle - Discovery Engine (v2 - REST-quote driven, works any time).

Key fix vs v1: ranking Top gainers/losers no longer depends on the live
WebSocket feed (which is empty outside market hours). Instead, exactly like
Ali's original discovery.py, we rank directly off the REST quote_data batch
response every scan cycle - Dhan's quote API returns last_price/net_change
for every symbol regardless of whether the market is open (last-known-value
after close, live-updating during market hours). This means:
  - Gainers/losers populate immediately on startup, any time of day.
  - During market hours, values refresh every DISCOVERY_FULL_SCAN_INTERVAL_SEC.
  - After hours, values stay static (reflecting the last close), which is
    exactly the expected/correct behaviour when the market isn't trading.
"""
import time
import logging
import math
from datetime import datetime

import config
import state
import notifier

logger = logging.getLogger(__name__)

_last_full_scan = 0.0
_last_top_movers_telegram_at = 0.0
TOP_MOVERS_TELEGRAM_INTERVAL_SEC = 60 * 60


def should_send_top_movers():
    global _last_top_movers_telegram_at

    if not config.SEND_TELEGRAM_TOP_MOVERS:
        return False

    snap = state.snapshot()
    last_sent = snap.get("last_top_movers_telegram_at")
    if last_sent:
        try:
            _last_top_movers_telegram_at = max(
                _last_top_movers_telegram_at,
                datetime.fromisoformat(last_sent).timestamp(),
            )
        except ValueError:
            pass

    return time.time() - _last_top_movers_telegram_at >= TOP_MOVERS_TELEGRAM_INTERVAL_SEC


def _scheduled_mover_message_due():
    return should_send_top_movers()


def _fetch_batch_quotes(broker, security_ids):
    quotes = {}
    requested = len(security_ids)
    failed_chunks = []

    chunk_size = config.DISCOVERY_QUOTE_CHUNK

    for i in range(0, len(security_ids), chunk_size):
        chunk = security_ids[i:i + chunk_size]
        if hasattr(broker, "is_quote_cooldown_active") and broker.is_quote_cooldown_active():
            failed_chunks.append({"offset": i, "count": len(chunk), "sample": [str(x) for x in chunk[:3]]})
            break

        try:
            res = broker.get_quote_batch(config.EXCHANGE, chunk)
            if res:
                quotes.update(res)
            else:
                failed_chunks.append({
                    "offset": i,
                    "count": len(chunk),
                    "sample": [str(x) for x in chunk[:3]],
                })
                if hasattr(broker, "is_quote_cooldown_active") and broker.is_quote_cooldown_active():
                    break
        except Exception:
            logger.exception(f"Quote chunk fetch failed at offset {i}")
            failed_chunks.append({
                "offset": i,
                "count": len(chunk),
                "sample": [str(x) for x in chunk[:3]],
            })
        time.sleep(config.QUOTE_BATCH_GAP_SEC)

    return quotes, requested, failed_chunks


def run_full_universe_scan(broker, universe_df):
    """
    Single REST pass: fetches quote_data for the whole universe and directly
    computes % change (works any time of day, market open or closed) -
    then ranks and pushes Top-10 gainers/losers straight into state.
    """
    global _last_full_scan

    now = datetime.now(config.TIME_ZONE)
    lock_time = now.replace(
        hour=config.FINAL_UNIVERSE_LOCK_TIME[0],
        minute=config.FINAL_UNIVERSE_LOCK_TIME[1],
        second=config.FINAL_UNIVERSE_LOCK_TIME[2],
        microsecond=0,
    )
    snap = state.snapshot()
    if now >= lock_time and snap.get("final_universe_locked", False):
        return

    id_to_row = {int(r.SECURITY_ID): r for _, r in universe_df.iterrows()}
    sec_ids = list(id_to_row.keys())
    quotes, requested_count, failed_chunks = _fetch_batch_quotes(
        broker,
        sec_ids,
    )

    coverage_pct = (len(quotes) / requested_count * 100.0) if requested_count else 0.0
    if coverage_pct < config.MIN_DISCOVERY_QUOTE_COVERAGE_PCT:
        _last_full_scan = time.time()
        logger.warning(
            "Discovery scan incomplete: %s/%s quotes (%.1f%%); "
            "failed_chunks=%s. Keeping previous movers.",
            len(quotes),
            requested_count,
            coverage_pct,
            failed_chunks,
        )
        state.add_log(
            f"Discovery incomplete: {len(quotes)}/{requested_count} "
            f"quotes ({coverage_pct:.1f}%). Previous movers retained."
        )
        return

    rows = []
    below_min_move = 0
    above_max_move = 0
    invalid_previous_close = 0
    blacklisted = 0
    malformed_quotes = 0
    missing_change_field = 0
    eligible = 0
    for sid_str, data in quotes.items():
        try:
            sid = int(sid_str)
            ltp = float(data.get("last_price", 0.0))
            net_change = float(data.get("net_change", 0.0))
            if "net_change" not in data:
                missing_change_field += 1
                continue
            volume = float(data.get("volume", 0.0)) if data.get("volume") is not None else 0.0
            if ltp < config.MIN_PRICE:
                continue
            prev_close = ltp - net_change
            if prev_close <= 0:
                invalid_previous_close += 1
                continue
            pct_change = (net_change / prev_close) * 100.0

            if abs(pct_change) < config.MIN_PCT_MOVE:
                below_min_move += 1
                continue
            if abs(pct_change) > config.MAX_PCT_MOVE:
                above_max_move += 1
                continue
            if state.is_blacklisted(sid):
                blacklisted += 1
                continue

            eligible += 1

            row = id_to_row.get(sid)
            display_name = row.DISPLAY_NAME if row is not None else str(sid)
            rows.append({
                "SECURITY_ID": sid,
                "display_name": display_name,
                "ltp": ltp,
                "prev_close": round(prev_close, 2),
                "pct_change": round(pct_change, 2),
                "volume": volume,
            })
        except Exception as exc:
            malformed_quotes += 1
            if malformed_quotes <= 3:
                logger.warning("Skipping malformed discovery quote %s: %s", sid_str, exc)
            continue

    discovered_at = datetime.now(config.TIME_ZONE).isoformat()
    volume_values = [math.log1p(max(0.0, row["volume"])) for row in rows]
    max_volume = max(volume_values, default=1.0)
    for row, log_volume in zip(rows, volume_values):
        row["discovered_at"] = discovered_at
        row["priority_score"] = round(
            0.55 * min(1.0, abs(row["pct_change"]) / config.MAX_PCT_MOVE)
            + 0.30 * (log_volume / max_volume if max_volume else 0.0)
            + 0.15,
            4,
        )

    gainers = sorted([r for r in rows if r["pct_change"] > 0], key=lambda x: x["priority_score"], reverse=True)[:config.TOP_N_GAINERS]
    losers = sorted([r for r in rows if r["pct_change"] < 0], key=lambda x: x["priority_score"], reverse=True)[:config.TOP_N_LOSERS]
    for rank, row in enumerate(gainers, start=1):
        row["rank"] = rank
    for rank, row in enumerate(losers, start=1):
        row["rank"] = rank

    state.update({"top_gainers": gainers, "top_losers": losers})
    now = datetime.now(config.TIME_ZONE)
    lock_time = now.replace(
        hour=config.FINAL_UNIVERSE_LOCK_TIME[0],
        minute=config.FINAL_UNIVERSE_LOCK_TIME[1],
        second=config.FINAL_UNIVERSE_LOCK_TIME[2],
        microsecond=0,
    )
    snapshot = state.snapshot()
    if now >= lock_time and not snapshot.get("final_universe_locked", False):
        state.update({
            "final_universe_locked": True,
            "final_top_gainers": gainers[:config.TOP_N_GAINERS],
            "final_top_losers": losers[:config.TOP_N_LOSERS],
            "final_universe_locked_at": now.isoformat(),
        })
        state.add_log(
            f"Final 14:45 universe locked: {len(gainers[:config.TOP_N_GAINERS])} "
            f"gainers / {len(losers[:config.TOP_N_LOSERS])} losers"
        )
    _last_full_scan = time.time()
    state.update({"last_discovery_scan": datetime.now(config.TIME_ZONE).isoformat()})

    logger.info(
        "Discovery scan complete: %s gainers, %s losers qualify "
        "(quoted=%s, parsed=%s, eligible=%s, below_min=%s, above_max=%s, "
        "invalid_prev_close=%s, blacklisted=%s, malformed=%s, missing_change=%s)",
        len(gainers), len(losers), len(quotes), len(quotes), eligible, below_min_move,
        above_max_move, invalid_previous_close, blacklisted, malformed_quotes,
        missing_change_field,
    )
    state.add_log(f"Discovery refreshed: {len(gainers)} gainers / {len(losers)} losers in the {config.MIN_PCT_MOVE}%-{config.MAX_PCT_MOVE}% band")

    if _scheduled_mover_message_due():
        try:
            notifier.notify_top_movers(gainers, losers)
            global _last_top_movers_telegram_at
            _last_top_movers_telegram_at = time.time()
            state.update({"last_top_movers_telegram_at": datetime.now(config.TIME_ZONE).isoformat()})
        except Exception:
            logger.exception("Top Movers Telegram notification failed")


def refresh_from_livefeed(broker, universe_df):
    """
    Supplementary fast path: during market hours, nudge LTP values for
    symbols already in the gainers/losers list using the live WebSocket feed
    so the dashboard feels real-time between REST scans. Silently does
    nothing useful outside market hours (feed will simply be empty), which
    is fine since run_full_universe_scan() already keeps state populated.
    """
    snap = state.snapshot()
    updated_gainers = []
    updated_losers = []

    for r in snap["top_gainers"]:
        feed = broker.liveFeed.get(str(r["SECURITY_ID"]))
        if feed and feed.get("ltp"):
            ltp = float(feed["ltp"])
            pct_change = ((ltp - r["prev_close"]) / r["prev_close"]) * 100.0 if r["prev_close"] > 0 else r["pct_change"]
            updated_gainers.append({**r, "ltp": ltp, "pct_change": round(pct_change, 2)})
        else:
            updated_gainers.append(r)

    for r in snap["top_losers"]:
        feed = broker.liveFeed.get(str(r["SECURITY_ID"]))
        if feed and feed.get("ltp"):
            ltp = float(feed["ltp"])
            pct_change = ((ltp - r["prev_close"]) / r["prev_close"]) * 100.0 if r["prev_close"] > 0 else r["pct_change"]
            updated_losers.append({**r, "ltp": ltp, "pct_change": round(pct_change, 2)})
        else:
            updated_losers.append(r)

    updated_gainers.sort(key=lambda x: x["pct_change"], reverse=True)
    updated_losers.sort(key=lambda x: x["pct_change"])
    state.update({"top_gainers": updated_gainers, "top_losers": updated_losers})


def discovery_loop(broker, universe_df, run_flag_fn):
    run_full_universe_scan(broker, universe_df)
    while run_flag_fn():
        try:
            snap = state.snapshot()
            if snap.get("final_universe_locked", False):
                break
            if time.time() - _last_full_scan > config.DISCOVERY_FULL_SCAN_INTERVAL_SEC:
                run_full_universe_scan(broker, universe_df)
            else:
                refresh_from_livefeed(broker, universe_df)
        except Exception:
            logger.exception("Error in discovery loop iteration")
        time.sleep(1.0)