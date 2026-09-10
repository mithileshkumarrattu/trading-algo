"""
AlphaCandle - Execution Event Journal & Durable State Store.

Provides:
  1. Append-only JSONL event logs (events, order_events, trades) for auditability.
  2. Authoritative SQLite database for order lifecycle, positions, and circuit breakers.
"""
import os
import json
import sqlite3
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional

import config

_DB_LOCK = threading.Lock()
_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
_DB_PATH = os.path.join(_DATA_DIR, "execution.db")


def _get_date_str() -> str:
    return datetime.now(config.TIME_ZONE).strftime("%Y-%m-%d")


def _get_iso_now() -> str:
    return datetime.now(config.TIME_ZONE).isoformat()


def _ensure_dirs():
    os.makedirs(_DATA_DIR, exist_ok=True)
    os.makedirs(os.path.join(_DATA_DIR, "events"), exist_ok=True)
    os.makedirs(os.path.join(_DATA_DIR, "order_events"), exist_ok=True)
    os.makedirs(os.path.join(_DATA_DIR, "trades"), exist_ok=True)
    os.makedirs(os.path.join(_DATA_DIR, "reconciliation"), exist_ok=True)


def _init_db():
    _ensure_dirs()
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS execution_events (
                event_id TEXT PRIMARY KEY,
                occurred_at TEXT NOT NULL,
                signal_id TEXT,
                security_id TEXT,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                client_tag TEXT UNIQUE,
                correlation_id TEXT,
                signal_id TEXT NOT NULL,
                security_id TEXT NOT NULL,
                symbol TEXT,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                filled_quantity INTEGER NOT NULL DEFAULT 0,
                average_fill_price REAL,
                limit_price REAL,
                trigger_price REAL,
                order_status TEXT NOT NULL,
                role TEXT NOT NULL,
                raw_broker_status TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                security_id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                strategy TEXT NOT NULL,
                direction TEXT NOT NULL,
                net_quantity INTEGER NOT NULL,
                entry_average REAL,
                protective_stop_order_id TEXT,
                protective_stop_price REAL,
                target_r2_price REAL,
                partial_booked INTEGER DEFAULT 0,
                status TEXT NOT NULL,
                entered_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS circuit_breakers (
                lock_name TEXT PRIMARY KEY,
                locked INTEGER NOT NULL DEFAULT 0,
                reason TEXT,
                locked_at TEXT
            )
        """)
        conn.commit()
        conn.close()


_init_db()


def _append_jsonl(category: str, record: Dict[str, Any]):
    _ensure_dirs()
    date_str = _get_date_str()
    file_path = os.path.join(_DATA_DIR, category, f"{date_str}.jsonl")
    try:
        line = json.dumps(record, default=str) + "\n"
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def record_event(event_type: str, payload: Dict[str, Any], signal_id: Optional[str] = None, security_id: Optional[str] = None):
    now_iso = _get_iso_now()
    event_id = f"EVT-{int(datetime.now().timestamp() * 1000)}-{os.urandom(2).hex()}"
    rec = {
        "event_id": event_id,
        "occurred_at": now_iso,
        "signal_id": signal_id,
        "security_id": str(security_id) if security_id is not None else None,
        "event_type": event_type,
        "payload": payload,
    }
    _append_jsonl("events", rec)

    try:
        with _DB_LOCK:
            conn = sqlite3.connect(_DB_PATH)
            cur = conn.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO execution_events (event_id, occurred_at, signal_id, security_id, event_type, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
                (event_id, now_iso, signal_id, str(security_id) if security_id is not None else None, event_type, json.dumps(payload, default=str)),
            )
            conn.commit()
            conn.close()
    except Exception:
        pass


def record_raw_order_update(raw_update: Dict[str, Any]):
    rec = {
        "received_at": _get_iso_now(),
        "raw": raw_update,
    }
    _append_jsonl("order_events", rec)


def record_trade(trade_data: Dict[str, Any]):
    rec = {
        "occurred_at": _get_iso_now(),
        **trade_data,
    }
    _append_jsonl("trades", rec)


def record_reconciliation(report: Dict[str, Any]):
    rec = {
        "reconciled_at": _get_iso_now(),
        **report,
    }
    _append_jsonl("reconciliation", rec)


def upsert_order(order: Dict[str, Any]):
    now_iso = _get_iso_now()
    order_id = str(order.get("order_id", ""))
    client_tag = order.get("client_tag")
    correlation_id = order.get("correlation_id") or client_tag
    signal_id = str(order.get("signal_id", ""))
    security_id = str(order.get("security_id", ""))
    symbol = order.get("symbol", "")
    side = str(order.get("side", "")).upper()
    order_type = str(order.get("order_type", "")).upper()
    quantity = int(order.get("quantity", 0))
    filled_qty = int(order.get("filled_quantity", 0))
    avg_price = float(order.get("average_fill_price", 0.0)) if order.get("average_fill_price") is not None else None
    limit_price = float(order.get("limit_price", 0.0)) if order.get("limit_price") is not None else None
    trigger_price = float(order.get("trigger_price", 0.0)) if order.get("trigger_price") is not None else None
    order_status = str(order.get("order_status", "PENDING")).upper()
    role = str(order.get("role", "ENTRY")).upper()
    raw_status = str(order.get("raw_broker_status", ""))

    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO orders (
                order_id, client_tag, correlation_id, signal_id, security_id, symbol,
                side, order_type, quantity, filled_quantity, average_fill_price,
                limit_price, trigger_price, order_status, role, raw_broker_status,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                filled_quantity=excluded.filled_quantity,
                average_fill_price=excluded.average_fill_price,
                order_status=excluded.order_status,
                raw_broker_status=excluded.raw_broker_status,
                updated_at=excluded.updated_at
        """, (
            order_id, client_tag, correlation_id, signal_id, security_id, symbol,
            side, order_type, quantity, filled_qty, avg_price,
            limit_price, trigger_price, order_status, role, raw_status,
            order.get("created_at", now_iso), now_iso
        ))
        conn.commit()
        conn.close()


def get_order(order_id: Optional[str] = None, client_tag: Optional[str] = None, correlation_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        row = None
        if order_id:
            cur.execute("SELECT * FROM orders WHERE order_id = ?", (str(order_id),))
            row = cur.fetchone()
        elif client_tag:
            cur.execute("SELECT * FROM orders WHERE client_tag = ?", (str(client_tag),))
            row = cur.fetchone()
        elif correlation_id:
            cur.execute("SELECT * FROM orders WHERE correlation_id = ? OR client_tag = ?", (str(correlation_id), str(correlation_id)))
            row = cur.fetchone()
        conn.close()
        return dict(row) if row else None


def get_orders_for_signal(signal_id: str) -> List[Dict[str, Any]]:
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE signal_id = ? ORDER BY created_at ASC", (str(signal_id),))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]


def get_active_orders() -> List[Dict[str, Any]]:
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE order_status IN ('PENDING', 'SUBMITTED', 'PARTIALLY_FILLED', 'TRIGGER_PENDING', 'OPEN')")
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]


def upsert_position(pos: Dict[str, Any]):
    now_iso = _get_iso_now()
    security_id = str(pos["security_id"])
    signal_id = str(pos.get("signal_id", ""))
    symbol = pos.get("symbol", "")
    strategy = pos.get("strategy", "ALPHA")
    direction = str(pos.get("direction", "BUY")).upper()
    net_qty = int(pos.get("net_quantity", pos.get("quantity", 0)))
    entry_avg = float(pos.get("entry_average", pos.get("entry_price", 0.0)))
    stop_id = pos.get("protective_stop_order_id", pos.get("sl_order_id"))
    stop_price = float(pos.get("protective_stop_price", pos.get("current_sl", 0.0)))
    target_price = float(pos.get("target_r2_price", 0.0)) if pos.get("target_r2_price") else None
    partial = 1 if pos.get("partial_booked") else 0
    status = str(pos.get("status", "OPEN")).upper()

    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO positions (
                security_id, signal_id, symbol, strategy, direction, net_quantity,
                entry_average, protective_stop_order_id, protective_stop_price,
                target_r2_price, partial_booked, status, entered_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(security_id) DO UPDATE SET
                net_quantity=excluded.net_quantity,
                protective_stop_order_id=excluded.protective_stop_order_id,
                protective_stop_price=excluded.protective_stop_price,
                partial_booked=excluded.partial_booked,
                status=excluded.status,
                updated_at=excluded.updated_at
        """, (
            security_id, signal_id, symbol, strategy, direction, net_qty,
            entry_avg, stop_id, stop_price, target_price, partial, status,
            pos.get("entered_at", now_iso), now_iso
        ))
        conn.commit()
        conn.close()


def get_position(security_id: str) -> Optional[Dict[str, Any]]:
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM positions WHERE security_id = ?", (str(security_id),))
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None


def get_open_positions() -> Dict[str, Dict[str, Any]]:
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM positions WHERE status != 'CLOSED' AND net_quantity > 0")
        rows = cur.fetchall()
        conn.close()
        return {r["security_id"]: dict(r) for r in rows}


def delete_position(security_id: str):
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM positions WHERE security_id = ?", (str(security_id),))
        conn.commit()
        conn.close()


def set_circuit_breaker(lock_name: str, locked: bool = True, reason: str = ""):
    now_iso = _get_iso_now()
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO circuit_breakers (lock_name, locked, reason, locked_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(lock_name) DO UPDATE SET
                locked=excluded.locked,
                reason=excluded.reason,
                locked_at=excluded.locked_at
        """, (lock_name, 1 if locked else 0, reason, now_iso if locked else None))
        conn.commit()
        conn.close()
    record_event("CIRCUIT_BREAKER_UPDATED", {"lock_name": lock_name, "locked": locked, "reason": reason})


def is_circuit_breaker_active(lock_name: Optional[str] = None) -> bool:
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        cur = conn.cursor()
        if lock_name:
            cur.execute("SELECT locked FROM circuit_breakers WHERE lock_name = ?", (lock_name,))
            row = cur.fetchone()
            conn.close()
            return bool(row and row[0] == 1)
        else:
            cur.execute("SELECT 1 FROM circuit_breakers WHERE locked = 1 LIMIT 1")
            row = cur.fetchone()
            conn.close()
            return bool(row)


def get_all_circuit_breakers() -> Dict[str, Dict[str, Any]]:
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM circuit_breakers")
        rows = cur.fetchall()
        conn.close()
        return {r["lock_name"]: dict(r) for r in rows}


def reset_circuit_breakers():
    with _DB_LOCK:
        conn = sqlite3.connect(_DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE circuit_breakers SET locked = 0, reason = NULL, locked_at = NULL")
        conn.commit()
        conn.close()
