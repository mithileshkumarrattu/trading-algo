"""
AlphaCandle - Shared State (file-backed, cross-process safe).

CRITICAL DESIGN NOTE: main.py and dashboard.py run as TWO SEPARATE OS
PROCESSES (separate tmux sessions). An in-memory-only dict would give each
process its own private, disconnected copy of "state" - which is exactly
why the dashboard showed "No data" all morning despite main.py actively
finding setups. Every write here persists to a JSON file on disk; every
read loads fresh from that same file - this is how the two processes
actually share live data.
"""
import json
import os
import threading
from datetime import datetime

import config

_LOCK = threading.Lock()
_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "state.json")

_DEFAULT_STATE = {
    "started_at": None,
    "paper_mode": True,
    "run_process": False,
    "nifty_ltp": None,
    "nifty_pct_change": None,
    "market_regime": "UNKNOWN",
    "universe_size": 0,
    "top_gainers": [],
    "top_losers": [],
    "watchlist": {},          # legacy alpha watchlist compatibility
    "alpha_watchlist": {},    # security_id (str) -> Alpha setup dict
    "jp_watchlist": {},       # security_id (str) -> JP setup dict
    "processed_1m_bars": {}, # security_id -> latest processed 1m bar ISO time
    "setup_outcomes": [],     # list of {security_id, strategy, outcome, ...}
    "open_positions": {},     # security_id (str) -> dict
    "closed_trades": [],
    "daily_pnl": 0.0,
    "daily_trade_count": 0,
    "cost_to_cost_mode": False,
    "blacklist": {},          # security_id (str) -> {blacklisted_at, signal_vol}
    "logs": [],
    "last_discovery_scan": None,
    "alerted_alpha_keys": [],  # dedup keys already Telegram-alerted this session
    "expired_alpha_keys": {},  # alpha key -> cooldown expiry timestamp
    "alerted_jp_keys": [],
    "jp_symbol_signal_counts": {},
    "top_mover_telegram_slots_sent": [],
    "final_universe_locked": False,
    "final_top_gainers": [],
    "final_top_losers": [],
    "final_universe_locked_at": None,
}


def _ensure_file():
    os.makedirs(os.path.dirname(_STATE_FILE), exist_ok=True)
    if not os.path.exists(_STATE_FILE):
        with open(_STATE_FILE, "w") as f:
            json.dump(_DEFAULT_STATE, f)


def _read_raw():
    _ensure_file()
    try:
        with open(_STATE_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return dict(_DEFAULT_STATE)


def _write_raw(data):
    _ensure_file()
    tmp_path = _STATE_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, default=str)
    os.replace(tmp_path, _STATE_FILE)


def snapshot():
    with _LOCK:
        return _read_raw()


def update(patch: dict):
    with _LOCK:
        data = _read_raw()
        data.update(patch)
        _write_raw(data)


def add_log(message: str, max_logs: int = 200):
    with _LOCK:
        data = _read_raw()
        ts = datetime.now(config.TIME_ZONE).strftime("%H:%M:%S")
        data["logs"].append(f"[{ts}] {message}")
        data["logs"] = data["logs"][-max_logs:]
        _write_raw(data)


def set_watchlist_item(security_id, item: dict):
    with _LOCK:
        data = _read_raw()
        sid = str(security_id)
        data.setdefault("watchlist", {})[sid] = item
        if item.get("strategy") == "ALPHA" or "alpha_key" in item or "alpha_high" in item:
            data.setdefault("alpha_watchlist", {})[sid] = item
        _write_raw(data)


def remove_watchlist_item(security_id):
    with _LOCK:
        data = _read_raw()
        sid = str(security_id)
        data.setdefault("watchlist", {}).pop(sid, None)
        data.setdefault("alpha_watchlist", {}).pop(sid, None)
        _write_raw(data)


def set_alpha_watchlist_item(security_id, item: dict):
    with _LOCK:
        data = _read_raw()
        sid = str(security_id)
        data.setdefault("alpha_watchlist", {})[sid] = item
        data.setdefault("watchlist", {})[sid] = item
        _write_raw(data)


def remove_alpha_watchlist_item(security_id):
    with _LOCK:
        data = _read_raw()
        sid = str(security_id)
        data.setdefault("alpha_watchlist", {}).pop(sid, None)
        data.setdefault("watchlist", {}).pop(sid, None)
        _write_raw(data)


def get_active_alpha_setups():
    data = snapshot()
    alpha = data.get("alpha_watchlist") or data.get("watchlist") or {}
    return list(alpha.values())


def get_active_jp_setups():
    data = snapshot()
    return list((data.get("jp_watchlist") or {}).values())


def set_open_position(security_id, item: dict):
    with _LOCK:
        data = _read_raw()
        data["open_positions"][str(security_id)] = item
        _write_raw(data)


def remove_open_position(security_id):
    with _LOCK:
        data = _read_raw()
        data["open_positions"].pop(str(security_id), None)
        _write_raw(data)


def append_closed_trade(trade: dict, max_trades: int = 200):
    with _LOCK:
        data = _read_raw()
        data["closed_trades"].append(trade)
        data["closed_trades"] = data["closed_trades"][-max_trades:]
        _write_raw(data)


def add_closed_trade(trade: dict):
    append_closed_trade(trade)


def add_to_daily_pnl(amount: float):
    with _LOCK:
        data = _read_raw()
        data["daily_pnl"] = round(data.get("daily_pnl", 0.0) + amount, 2)
        data["daily_trade_count"] = data.get("daily_trade_count", 0) + 1
        _write_raw(data)


def is_blacklisted(security_id) -> bool:
    data = snapshot()
    entry = data.get("blacklist", {}).get(str(security_id))
    if not entry:
        return False
    cooldown_end = datetime.fromisoformat(entry["blacklisted_at"])
    from datetime import timedelta
    return datetime.now(config.TIME_ZONE) < cooldown_end + timedelta(minutes=config.BLACKLIST_COOLDOWN_MIN)


def add_to_blacklist(security_id, signal_vol):
    with _LOCK:
        data = _read_raw()
        data["blacklist"][str(security_id)] = {
            "blacklisted_at": datetime.now(config.TIME_ZONE).isoformat(),
            "signal_vol": signal_vol,
        }
        _write_raw(data)


def get_blacklist_entry(security_id):
    return snapshot().get("blacklist", {}).get(str(security_id))


def set_blacklist(security_id, reason="", signal_volume=0):
    add_to_blacklist(security_id, signal_volume)


def is_expired_alpha(key: str) -> bool:
    entry = snapshot().get("expired_alpha_keys", {}).get(key)
    if not entry:
        return False
    return datetime.now(config.TIME_ZONE) < datetime.fromisoformat(entry)


def mark_expired_alpha(key: str, expires_at=None):
    from datetime import timedelta
    if expires_at is None:
        expired_at = datetime.now(config.TIME_ZONE)
    else:
        expired_at = datetime.fromisoformat(expires_at)
    cooldown_until = expired_at + timedelta(minutes=config.EXPIRED_ALPHA_COOLDOWN_MINUTES)
    with _LOCK:
        data = _read_raw()
        data.setdefault("expired_alpha_keys", {})[key] = cooldown_until.isoformat()
        _write_raw(data)


def has_alerted(key: str) -> bool:
    data = snapshot()
    return key in data.get("alerted_alpha_keys", [])


def mark_alerted(key: str, max_keys: int = 500):
    with _LOCK:
        data = _read_raw()
        keys = data.get("alerted_alpha_keys", [])
        if key not in keys:
            keys.append(key)
        data["alerted_alpha_keys"] = keys[-max_keys:]
        _write_raw(data)


def reset_daily():
    with _LOCK:
        _write_raw(dict(_DEFAULT_STATE))


def set_jp_watchlist_item(security_id, item: dict):
    with _LOCK:
        data = _read_raw()
        sid = str(security_id)
        data.setdefault("jp_watchlist", {})[sid] = item
        _write_raw(data)


def remove_jp_watchlist_item(security_id):
    with _LOCK:
        data = _read_raw()
        sid = str(security_id)
        data.setdefault("jp_watchlist", {}).pop(sid, None)
        _write_raw(data)


def mark_alpha_bar_processed(security_id, bar_time_iso: str):
    with _LOCK:
        data = _read_raw()
        data.setdefault("processed_1m_bars", {})[str(security_id)] = bar_time_iso
        _write_raw(data)


def mark_jp_bar_processed(security_id, bar_time_iso: str):
    with _LOCK:
        data = _read_raw()
        data.setdefault("processed_1m_bars", {})[str(security_id)] = bar_time_iso
        _write_raw(data)


def log_setup_outcome(setup: dict, outcome: str, details: str = ""):
    with _LOCK:
        data = _read_raw()
        payload = {
            "security_id": str(setup.get("security_id")),
            "strategy": setup.get("strategy"),
            "symbol": setup.get("symbol"),
            "direction": setup.get("direction"),
            "outcome": outcome,
            "details": details,
            "timestamp": datetime.now(config.TIME_ZONE).isoformat(),
        }
        data.setdefault("setup_outcomes", []).append(payload)
        data["setup_outcomes"] = data["setup_outcomes"][-200:]
        _write_raw(data)


def remove_alpha_setup(security_id):
    remove_alpha_watchlist_item(security_id)


def remove_jp_setup(security_id):
    remove_jp_watchlist_item(security_id)


def has_alerted_jp(key: str) -> bool:
    return key in snapshot().get("alerted_jp_keys", [])


def mark_jp_alerted(key: str, max_keys: int = 500):
    with _LOCK:
        data = _read_raw()
        keys = data.setdefault("alerted_jp_keys", [])
        if key not in keys:
            keys.append(key)
        data["alerted_jp_keys"] = keys[-max_keys:]
        _write_raw(data)


def jp_signal_count(security_id):
    return int(snapshot().get("jp_symbol_signal_counts", {}).get(str(security_id), 0))


def increment_jp_signal_count(security_id):
    with _LOCK:
        data = _read_raw()
        counts = data.setdefault("jp_symbol_signal_counts", {})
        key = str(security_id)
        counts[key] = int(counts.get(key, 0)) + 1
        _write_raw(data)