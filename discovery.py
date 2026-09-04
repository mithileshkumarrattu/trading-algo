"""
AlphaCandle - Discovery Engine (WebSocket Live Feed First).

Primary Discovery Source:
- Computes Top Gainers/Losers locally from streaming ticks in broker.liveFeed.
- Evaluates symbols with ltp > 0, prev_close > 0, and updated within MAX_LIVEFEED_AGE_SEC.
- Ranks candidates by priority score (55% price move + 30% relative volume + 15% base).
- Preserves previous mover lists if live WebSocket coverage falls below MIN_WEBSOCKET_COVERAGE_PCT (90%).
- If streaming ticks lack previous close, performs at most ONE single-request REST quote bootstrap
  for all universe symbols (Dhan allows up to 1,000 instruments in one request).
- Never performs recurring full-universe REST quote polling during market hours.
"""
import time
import logging
import math
from datetime import datetime

import config
import state
import notifier

logger = logging.getLogger(__name__)

_last_top_movers_telegram_at = 0.0
_last_coverage_log_at = 0.0
_bootstrap_attempted = False
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


def reconcile_live_subscriptions(broker, gainers, losers):
    """
    Reconcile active WebSocket subscriptions after mover ranking.
    Subscribes:
      - Nifty 50 index
      - Top WS_TOP_MOVERS_PER_SIDE gainers
      - Top WS_TOP_MOVERS_PER_SIDE losers
      - Active Alpha/JP setups and open positions
    Unsubscribes symbols no longer in this set.
    """
    snap = state.snapshot()
    top_limit = getattr(config, "WS_TOP_MOVERS_PER_SIDE", 10)
    top_movers = (
        gainers[:top_limit]
        + losers[:top_limit]
    )

    desired_ids = {str(r["SECURITY_ID"]) for r in top_movers if isinstance(r, dict) and "SECURITY_ID" in r}

    # Include any active setup / position security IDs
    for key in ("watchlist", "alpha_watchlist", "jp_watchlist", "open_positions"):
        items = snap.get(key, {})
        if isinstance(items, dict):
            for sid in items.keys():
                desired_ids.add(str(sid))
        elif isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and "security_id" in item:
                    desired_ids.add(str(item["security_id"]))
                elif isinstance(item, (str, int)):
                    desired_ids.add(str(item))

    # NSE = 1, Ticker = 15, IDX = 0
    desired_instruments = [
        (0, str(config.INDEX_SECURITY_ID), 15)
    ]
    for sid in sorted(desired_ids):
        desired_instruments.append((1, str(sid), 15))

    if hasattr(broker, "reconcile_subscriptions"):
        broker.reconcile_subscriptions(desired_instruments)


def refresh_from_livefeed_full_universe(broker, universe_df):
    """
    Primary discovery: ranks Top Gainers and Top Losers directly from
    in-memory streaming ticks in broker.liveFeed across the universe.
    """
    global _last_coverage_log_at, _bootstrap_attempted

    now_tz = datetime.now(config.TIME_ZONE)
    lock_time = now_tz.replace(
        hour=config.FINAL_UNIVERSE_LOCK_TIME[0],
        minute=config.FINAL_UNIVERSE_LOCK_TIME[1],
        second=config.FINAL_UNIVERSE_LOCK_TIME[2],
        microsecond=0,
    )
    snap = state.snapshot()
    if now_tz >= lock_time and snap.get("final_universe_locked", False):
        return

    id_to_row = {int(r.SECURITY_ID): r for _, r in universe_df.iterrows()}
    universe_count = len(id_to_row)
    if universe_count == 0:
        return

    # Check how many symbols have prev_close in liveFeed
    feed_dict = getattr(broker, "liveFeed", {})
    symbols_with_prev_close = sum(
        1 for sid in id_to_row.keys()
        if str(sid) in feed_dict and feed_dict[str(sid)].get("prev_close")
    )

    # If < 50% have prior closes and bootstrap not attempted, perform ONE single-request bootstrap
    if symbols_with_prev_close < (universe_count * 0.5) and not _bootstrap_attempted:
        _bootstrap_attempted = True
        logger.info(
            "WebSocket feed has %d/%d prior closes; executing ONE-TIME REST prior-close bootstrap",
            symbols_with_prev_close, universe_count
        )
        if hasattr(broker, "bootstrap_prior_closes"):
            broker.bootstrap_prior_closes(config.EXCHANGE, list(id_to_row.keys()))

    max_age_sec = getattr(config, "MAX_LIVEFEED_AGE_SEC", 15)
    usable_records = {}
    stale_count = 0
    missing_prev_close_count = 0

    for sid, row in id_to_row.items():
        sid_str = str(sid)
        entry = feed_dict.get(sid_str)
        if not entry or not isinstance(entry, dict):
            continue

        ltp = entry.get("ltp")
        prev_close = entry.get("prev_close")
        if ltp is None or prev_close is None or prev_close <= 0:
            missing_prev_close_count += 1
            continue

        updated_at = entry.get("updated_at")
        if updated_at:
            age = (now_tz - updated_at).total_seconds()
            if age > max_age_sec:
                stale_count += 1
                continue

        usable_records[sid] = entry

    coverage_pct = (len(usable_records) / universe_count * 100.0) if universe_count else 0.0
    min_coverage_pct = getattr(config, "MIN_WEBSOCKET_COVERAGE_PCT", 90.0)

    if coverage_pct < min_coverage_pct:
        # Rate-limit coverage logs to at most once per 60 seconds
        if time.time() - _last_coverage_log_at >= 60.0:
            _last_coverage_log_at = time.time()
            logger.info(
                "WebSocket discovery coverage: %d/%d symbols (%.1f%%, min=%.1f%%, stale=%d, missing_prev_close=%d). "
                "Retaining previous movers.",
                len(usable_records),
                universe_count,
                coverage_pct,
                min_coverage_pct,
                stale_count,
                missing_prev_close_count,
            )
            state.add_log(
                f"WebSocket discovery coverage {coverage_pct:.1f}% ({len(usable_records)}/{universe_count}). "
                f"Previous movers retained."
            )
        return

    # Rank usable records
    rows = []
    below_min_move = 0
    above_max_move = 0
    blacklisted = 0
    eligible = 0

    for sid, entry in usable_records.items():
        try:
            ltp = float(entry["ltp"])
            prev_close = float(entry["prev_close"])
            volume = float(entry.get("volume", 0.0) or 0.0)

            if ltp < config.MIN_PRICE:
                continue

            pct_change = ((ltp - prev_close) / prev_close) * 100.0

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
            display_name = row.DISPLAY_NAME if (row is not None and hasattr(row, "DISPLAY_NAME")) else str(sid)
            rows.append({
                "SECURITY_ID": sid,
                "display_name": display_name,
                "ltp": round(ltp, 2),
                "prev_close": round(prev_close, 2),
                "pct_change": round(pct_change, 2),
                "volume": volume,
                "source": "WEBSOCKET",
            })
        except Exception:
            continue

    discovered_at = now_tz.isoformat()
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

    state.update({
        "top_gainers": gainers,
        "top_losers": losers,
        "last_discovery_scan": now_tz.isoformat(),
    })

    if now_tz >= lock_time and not snap.get("final_universe_locked", False):
        state.update({
            "final_universe_locked": True,
            "final_top_gainers": gainers[:config.TOP_N_GAINERS],
            "final_top_losers": losers[:config.TOP_N_LOSERS],
            "final_universe_locked_at": now_tz.isoformat(),
        })
        state.add_log(
            f"Final 14:45 universe locked: {len(gainers[:config.TOP_N_GAINERS])} "
            f"gainers / {len(losers[:config.TOP_N_LOSERS])} losers"
        )

    if _scheduled_mover_message_due():
        try:
            notifier.notify_top_movers(gainers, losers)
            global _last_top_movers_telegram_at
            _last_top_movers_telegram_at = time.time()
            state.update({"last_top_movers_telegram_at": now_tz.isoformat()})
        except Exception:
            logger.exception("Top Movers Telegram notification failed")


# Backward compatibility alias
def run_full_universe_scan(broker, universe_df):
    """Delegate to WebSocket-based universe refresh."""
    return refresh_from_livefeed_full_universe(broker, universe_df)


def refresh_from_livefeed(broker, universe_df):
    """Delegate to WebSocket-based universe refresh."""
    return refresh_from_livefeed_full_universe(broker, universe_df)


def discovery_loop(broker, universe_df, run_flag_fn):
    """
    Continuous local feed discovery loop:
    Evaluates streaming WebSocket ticks every LOCAL_DISCOVERY_REFRESH_SEC (5s).
    """
    refresh_from_livefeed_full_universe(broker, universe_df)
    refresh_sec = getattr(config, "LOCAL_DISCOVERY_REFRESH_SEC", 5)

    while run_flag_fn():
        try:
            snap = state.snapshot()
            if snap.get("final_universe_locked", False):
                break
            refresh_from_livefeed_full_universe(broker, universe_df)
        except Exception:
            logger.exception("Error in discovery loop iteration")
        time.sleep(refresh_sec)