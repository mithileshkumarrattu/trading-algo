"""
AlphaCandle - Time-window helpers. Fully self-contained.
"""
from datetime import datetime, time as dtime
import config


def get_time(hms_tuple) -> dtime:
    return dtime(hour=hms_tuple[0], minute=hms_tuple[1], second=hms_tuple[2])


def now_time():
    return datetime.now(config.TIME_ZONE).time()


def is_entry_allowed() -> bool:
    t = now_time()
    return get_time(config.START_TIME) <= t <= get_time(config.LAST_ENTRY_TIME)


def is_past_squareoff() -> bool:
    return now_time() >= get_time(config.SQUARE_OFF_TIME)


def is_within_run_window() -> bool:
    return now_time() < get_time(config.EXIT_TIME)
