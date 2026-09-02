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
BOT_CHAT_ID = "-1004313459571"

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
SOURCE_CANDLE_TIMEFRAME = 1
ALPHA_TIMEFRAME = 3
JP_TIMEFRAME = 3

PATTERN_TIMEFRAME = 3
CANDLE_CACHE_TTL_PATTERN_SEC = 15
CANDLE_CACHE_TTL_1M_SEC = 15
CANDLE_RETRY_COOLDOWN_SEC = 60
DISCOVERY_FULL_SCAN_INTERVAL_SEC = 90
# ---------------- CANDLE SCHEDULING ----------------
DATA_API_MAX_REQUESTS_PER_SECOND = 3
QUOTE_API_MAX_REQUESTS_PER_SECOND = 1
RATE_LIMIT_BACKOFF_SEC = 30
RATE_LIMIT_MAX_BACKOFF_SEC = 300

# ---------------- DERIVED 3-MIN CANDLES ----------------
MARKET_OPEN_TIME = (9, 15)
MARKET_CLOSE_TIME = (15, 30)
DERIVED_CANDLE_REQUIRE_FULL_BUCKET = True
DERIVED_CANDLE_MINUTE_COUNT = 3
CANDLE_CLOSE_GRACE_SEC = 3
ALPHA_MAX_CONFIRMATION_PATTERN_BARS = 2
CANDLE_CACHE_TTL_1M_SEC = 5
JP_MAX_CONFIRMATION_PATTERN_BARS = 1
REQUIRE_ENTRY_CLOSE_BEYOND_TRIGGER = True
DISCOVERY_QUOTE_CHUNK = 45
HOLD_CANDLES_PATTERN = 2        # abandon setup after this many pattern bars
# ---------------- DISCOVERY (Top movers) ----------------
MIN_PCT_MOVE = 1.0

# ---------------- TARGET / REWARD POLICY ----------------
DESIRED_TARGET_R = 2.0
MIN_ACCEPTABLE_TARGET_R = 1.8
ALLOW_REDUCED_TARGET_R = False
PARTIAL_BOOK_R = 2.0
MOVE_SL_TO_BREAKEVEN_AFTER_PARTIAL = True
MAX_PCT_MOVE = 5.0
TOP_N_GAINERS = 10
TOP_N_LOSERS = 10
SCAN_GROUP_SIZE_PER_SIDE = 10
MAX_ACTIVE_SETUPS_BEFORE_EXPAND = 2
FINAL_UNIVERSE_LOCK_TIME = (14, 45, 0)

# ---------------- ALPHA ----------------
ALPHA_ENABLED = True
ALPHA_ENTRY_WICK_ONLY = True
ALPHA_1M_CACHE_TTL_SEC = CANDLE_CACHE_TTL_1M_SEC

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
