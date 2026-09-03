"""
AlphaCandle - Smart Telegram Notifier.

Produces human-readable, context-rich messages that explain EXACTLY which
stock, which setup direction, which candle timeframe, and what price levels
triggered the event - so you can manually cross-verify on your own chart
without needing to open the dashboard.
"""
import logging
import threading
from datetime import datetime

import requests
import config

logger = logging.getLogger(__name__)


def _send_raw(message: str):
    if not (config.BOT_TOKEN and config.BOT_CHAT_ID) or "PUT_YOUR" in config.BOT_TOKEN:
        logger.warning("Telegram not configured, skipping send")
        return
    url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage"
    params = {
        "chat_id": config.BOT_CHAT_ID,
        "parse_mode": "HTML",
        "text": message,
    }
    try:
        with requests.Session() as session:
            res = session.get(url, params=params, timeout=5)
            body = ""
            try:
                body = res.text
            except Exception:
                body = "<unavailable>"

            if res.status_code >= 400:
                logger.warning(
                    "Telegram HTTP %s for chat_id=%s: %s",
                    res.status_code,
                    config.BOT_CHAT_ID,
                    body[:1000] if body else "(empty body)",
                )
            else:
                logger.info("Telegram response %s", res.status_code)
    except requests.RequestException as e:
        logger.warning("Telegram send error: %s", e)


def send_telegram(message: str):
    logger.info(f"TELEGRAM: {message}")
    threading.Thread(target=_send_raw, args=(message,), daemon=True).start()


def _tag(mode_paper: bool) -> str:
    return "🧪 <b>PAPER</b>" if mode_paper else "🔴 <b>LIVE</b>"


def notify_login(margin):
    send_telegram(f"✅ <b>AlphaCandle Started</b>\nAvailable margin: ₹{margin}\nMode: {'PAPER' if config.PAPER_MODE else 'LIVE'}")


def notify_login_failed():
    send_telegram("❌ <b>AlphaCandle login FAILED.</b> Check server immediately.")


def notify_trend_forming(symbol, direction, candle_count, timeframe):
    arrow = "📈" if direction == "BUY" else "📉"
    message = (
        f"{arrow} <b>{symbol}</b> — {direction} trend forming\n"
        f"{candle_count} consecutive {'green' if direction=='BUY' else 'red'} candles on {timeframe}-min chart, "
        f"good volume, no doji.\nWatching for Alpha Candle next."
    )
    send_telegram(message)


def notify_alpha_candle_detected(symbol, direction, alpha_time, alpha_high, alpha_low, timeframe):
    arrow = "📈" if direction == "BUY" else "📉"
    colour = "red" if direction == "BUY" else "green"
    level = alpha_high if direction == "BUY" else alpha_low
    level_label = "High" if direction == "BUY" else "Low"
    send_telegram(
        f"{arrow} <b>{symbol}</b> — Alpha Candle formed ({direction} setup)\n"
        f"Time: {alpha_time} on {timeframe}-min chart ({colour} candle after the trend run)\n"
        f"Watching for a completed 1-min candle wick to break Alpha "
        f"{level_label} = ₹{level:.2f}\n"
        f"Open your chart at exactly this candle to verify the setup."
    )


def notify_jp_candle_detected(symbol, direction, jp_time, trigger_price, stop_price, band_low, band_high):
    arrow = "🟢" if direction == "BUY" else "🔴"
    level_name = "JP High" if direction == "BUY" else "JP Low"
    mode_line = (
        "<i>Paper execution enabled: a valid completed 1-minute breakout may create a paper order.</i>"
        if not config.JP_DETECTION_ONLY else
        "<i>Detection-only paper validation; no JP order will be placed.</i>"
    )
    send_telegram(
        f"{arrow} <b>{symbol}</b> — JP Pullback setup ({direction})\n"
        f"JP pattern candle: {jp_time}\n"
        f"SMMA band: ₹{band_low:.2f} – ₹{band_high:.2f}\n"
        f"Waiting for the next 1-minute execution candle to cross {level_name}: ₹{trigger_price:.2f}\n"
        f"Structural SL: ₹{stop_price:.2f}\n"
        f"{mode_line}"
    )


def notify_setup_expired(symbol, direction):
    send_telegram(f"⏱ <b>{symbol}</b> — {direction} Alpha setup expired without breakout. Dropped from watch.")


def notify_entry(symbol, direction, qty, entry_price, sl_price, target_price, paper_mode,
                 alpha_open_time=None, alpha_high=None, alpha_low=None, trigger_1m_time=None):
    arrow = "🟢" if direction == "BUY" else "🔴"
    alpha_level_name = "Alpha High" if direction == "BUY" else "Alpha Low"
    alpha_level = alpha_high if direction == "BUY" else alpha_low
    details = []
    if alpha_open_time:
        details.append(f"Alpha pattern candle: {alpha_open_time}")
    if alpha_level is not None:
        details.append(f"{alpha_level_name}: ₹{float(alpha_level):.2f}")
    if trigger_1m_time:
        details.append(f"Breakout 1-min candle: {trigger_1m_time}")
    audit_text = "\n" + "\n".join(details) if details else ""
    send_telegram(
        f"{arrow} {_tag(paper_mode)} <b>ENTRY {direction}</b> — {symbol}\n"
        f"Qty: {qty} | Entry: ₹{entry_price:.2f} | SL: ₹{sl_price:.2f} | Target (1:2): ₹{target_price:.2f}\n"
        f"Risk: ₹{abs(entry_price - sl_price) * qty:.2f}{audit_text}"
    )


def notify_partial_book(symbol, direction, qty, exit_price, pnl, paper_mode):
    send_telegram(
        f"💰 {_tag(paper_mode)} <b>1:2 TARGET HIT — Partial Booked</b> — {symbol}\n"
        f"Booked {qty} qty @ ₹{exit_price:.2f} | PnL: ₹{pnl:.2f}\n"
        f"Remaining quantity's SL moved to breakeven (entry price) — no further downside risk on this trade."
    )


def notify_stall_exit(symbol, direction, qty, exit_price, pnl, paper_mode):
    send_telegram(
        f"⚪ {_tag(paper_mode)} <b>Runner Exited — Momentum Stalled</b> — {symbol}\n"
        f"Qty: {qty} @ ₹{exit_price:.2f} | PnL: ₹{pnl:.2f}\n"
        f"Price went sideways for several 1-min candles after the target — booked before it reversed."
    )


def notify_sl_hit(symbol, direction, qty, exit_price, pnl, paper_mode):
    send_telegram(
        f"🛑 {_tag(paper_mode)} <b>Stop Loss Hit</b> — {symbol}\n"
        f"Qty: {qty} @ ₹{exit_price:.2f} | PnL: ₹{pnl:.2f}"
    )


def notify_eod_squareoff(symbol, qty, exit_price, pnl, paper_mode):
    send_telegram(
        f"🔔 {_tag(paper_mode)} <b>EOD Square-off</b> — {symbol}\n"
        f"Qty: {qty} @ ₹{exit_price:.2f} | PnL: ₹{pnl:.2f}"
    )


def notify_blacklist_locked(symbol, reason):
    send_telegram(f"🔒 <b>{symbol}</b> locked for the day — {reason}")


def notify_daily_loss_cap(pnl):
    send_telegram(
        f"⚠️ <b>Daily loss cap reached</b> (₹{pnl:.2f})\n"
        f"Cost-to-cost mode activated — any further trade today will be managed to exit flat, never adding more loss."
    )


def notify_heartbeat(open_positions, daily_pnl, trade_count):
    send_telegram(
        f"💓 AlphaCandle heartbeat — {datetime.now(config.TIME_ZONE).strftime('%H:%M:%S')}\n"
        f"Open positions: {open_positions} | Daily PnL: ₹{daily_pnl:.2f} | Trades today: {trade_count}"
    )

def notify_top_movers(gainers, losers):
    """Push current Top-10 gainers/losers snapshot to Telegram, clearly
    labelled with symbol, LTP, and % change so it's readable at a glance."""
    lines = ["📊 <b>Top Movers Update</b>"]

    if gainers:
        lines.append("\n🟢 <b>Top Gainers</b>")
        for r in gainers[:10]:
            lines.append(f"{r['display_name']}: ₹{r['ltp']:.2f} ({r['pct_change']:+.2f}%)")
    else:
        lines.append("\n🟢 <b>Top Gainers</b>: none in range right now")

    if losers:
        lines.append("\n🔴 <b>Top Losers</b>")
        for r in losers[:10]:
            lines.append(f"{r['display_name']}: ₹{r['ltp']:.2f} ({r['pct_change']:+.2f}%)")
    else:
        lines.append("\n🔴 <b>Top Losers</b>: none in range right now")

    send_telegram("\n".join(lines))