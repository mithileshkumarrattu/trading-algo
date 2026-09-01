"""
AlphaCandle - Isolated Strategy Config
Fully self-contained. No dependency on any other folder/files on the server.

Fill in your real Dhan credentials and Telegram bot details below before
running. This folder is completely separate from Ali's original algo folder
so nothing there is touched or affected.
"""
from zoneinfo import ZoneInfo

TIME_ZONE = ZoneInfo("Asia/Kolkata")

# ---------------- DHAN CREDENTIALS ----------------
CLIENT_ID = '1108646982'
PIN =      '200649'
TOTP_TOKEN = 'MW3JRYPUBRTUTKEXRIVV4GC7WZWE3DPL'

# ---------------- TELEGRAM ----------------
BOT_TOKEN = "8939945606:AAHA_dLTkJDBHDnX1JznDcw3PXqD654rxrE"
BOT_CHAT_ID = "-5428490798"

# ---------------- SAFETY SWITCH ----------------
# True  -> NO real Dhan order/modify/cancel call is EVER made. Entries/exits
#          are fully simulated in-memory using real live prices.
# False -> Real orders placed. Only flip after verified paper sessions.
PAPER_MODE = True

# ---------------- BROKER / EXCHANGE ----------------
EXCHANGE = "NSE_EQ"
PRODUCT_TYPE = "INTRADAY"
INDEX_SECURITY_ID = 13          # Nifty 50, IDX_I segment

# ---------------- UNIVERSE ----------------
FNO_ONLY = True
MIN_PRICE = 20.0
MAX_SPREAD_PCT = 0.5

# ---------------- TIMEFRAMES ----------------
# Dhan natively supports 1, 5, 15, 25, 60 only.
ALPHA_TIMEFRAME = 5
JP_TIMEFRAME = 5
ENTRY_TIMEFRAME = 1
PATTERN_TIMEFRAME = ALPHA_TIMEFRAME

# ---------------- CANDLE SCHEDULING ----------------
CANDLE_CLOSE_GRACE_SEC = 3
CANDLE_CACHE_TTL_PATTERN_SEC = 25
CANDLE_CACHE_TTL_1M_SEC = 5
QUOTE_MIN_INTERVAL_SEC = 1.10
DISCOVERY_FULL_SCAN_INTERVAL_SEC = 30
DISCOVERY_QUOTE_CHUNK = 45

# ---------------- DISCOVERY (Top movers) ----------------
MIN_PCT_MOVE = 1.0
MAX_PCT_MOVE = 5.0
TOP_N_GAINERS = 10
TOP_N_LOSERS = 10
SCAN_GROUP_SIZE_PER_SIDE = 10
MAX_ACTIVE_SETUPS_BEFORE_EXPAND = 2
FINAL_UNIVERSE_LOCK_TIME = (14, 45, 0)

# ---------------- ALPHA ----------------
ALPHA_ENABLED = True
ALPHA_MAX_CONFIRMATION_3M_CANDLES = 2
ALPHA_ENTRY_WICK_ONLY = True
ALPHA_1M_CACHE_TTL_SEC = 5
ALPHA_5M_CACHE_TTL_SEC = 25

# ---------------- JP (JACKPOT) STRATEGY ----------------
JP_ENABLED = True
JP_DETECTION_ONLY = False
JP_SMMA_LENGTH = 10
JP_MAX_CLOSE_THROUGH_BAND_PCT = 0.15
JP_MAX_STOP_DISTANCE_PCT = 0.60
JP_MAX_AGE_MINUTES = 10
JP_TOP_N_PER_SIDE = 30
JP_MAX_ACTIVE_SETUPS = 5
JP_MAX_SIGNALS_PER_SYMBOL_PER_DAY = 1
JP_CONFIRMATION_3M_CANDLES = 1
JP_CONFIRMATION_CANDLES = 1
JP_COOLDOWN_MINUTES = 30
SEND_TELEGRAM_ON_JP_SETUP = True

# ---------------- CONTROLLED RE-ENTRY ----------------
MAX_TRADES_PER_SYMBOL_PER_DAY = 2
REENTRY_COOLDOWN_MINUTES = 30
REENTRY_VOLUME_MULTIPLE = 1.25

# ---------------- FRESHNESS / STALE-SIGNAL FILTER ----------------
MAX_ALPHA_AGE_MINUTES = 15
EXPIRED_ALPHA_COOLDOWN_MINUTES = 360
MAX_ALPHA_RANGE_PCT = 0.85
MAX_STOP_DISTANCE_PCT = 0.90
MIN_BREAKOUT_BODY_RATIO = 0.35

# ---------------- RISK ----------------
STOP_BUFFER_PCT = 0.10
STOP_BUFFER_MIN_POINTS = 0.05
BREAKEVEN_AT_R = 1.0
PARTIAL_BOOK_AT_R = 2.0
PARTIAL_BOOK_FRACTION = 0.50
TRAIL_DISTANCE_R = 0.75
MAX_ENTRY_DELAY_SECONDS = 10

# ---------------- PAPER EXECUTION ----------------
PAPER_SLIPPAGE_BPS = 2

# ---------------- PATTERN (Alpha Candle) ----------------
MIN_TREND_CANDLES = 3
DOJI_BODY_RATIO = 0.18          # body/range below this = doji, candle rejected
HOLD_CANDLES_5MIN = 3           # abandon setup if no breakout within this many 5-min candles' worth of time

# ---------------- RISK / SIZING ----------------
SL_POINTS_MIN = 2.0
SL_POINTS_MAX = 3.0
RISK_PER_TRADE = 200.0
MAX_LOSS_PER_DAY = 2000.0
MAX_TRADES_PER_DAY = 5
MAX_OPEN_POSITIONS = 3

# ---------------- EXIT MANAGEMENT ----------------
FIRST_TARGET_R_MULTIPLE = 2.0
PARTIAL_BOOK_FRACTION = 0.5
STALL_CANDLES_FOR_REVERSAL_EXIT = 3

# ---------------- BLACKLIST ----------------
BLACKLIST_COOLDOWN_MIN = 60
BLACKLIST_REQUALIFY_VOL_MULTIPLE = 1.5

# ---------------- TIME WINDOWS ----------------
START_TIME = (9, 15, 0)          # market opens 9:15 AM
DISCOVERY_WARMUP_END = (9, 20, 0) # 5 min warmup to build prev_close cache before scanning
LAST_ENTRY_TIME = (15, 0, 0)      # stop taking new entries at 3:00 PM
SQUARE_OFF_TIME = (15, 10, 0)     # force-close all open positions at 3:10 PM
EXIT_TIME = (15, 12, 0)           # engine fully stops at 3:12 PM

COST_TO_COST_AFTER_MAX_LOSS = True

# ---------------- DASHBOARD ----------------
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 8787

# ---------------- TELEGRAM TOGGLES ----------------
SEND_TELEGRAM_ON_SETUP_WATCH = True
SEND_TELEGRAM_ON_ENTRY = True
SEND_TELEGRAM_ON_EXIT = True
SEND_TELEGRAM_ON_LOGIN = True
SEND_TELEGRAM_HEARTBEAT_MIN = 0
SEND_TELEGRAM_TOP_MOVERS = True
TOP_MOVERS_TELEGRAM_TIMES = (
	(9, 20, 0),
	(10, 0, 0),
	(11, 0, 0),
)

# ---------------- PATHS ----------------
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
TOKEN_CACHE_DIR = os.path.join(BASE_DIR, "data")
TOKEN_CACHE_FILE = os.path.join(TOKEN_CACHE_DIR, "dhan_access_token.json")
