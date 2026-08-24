# AlphaCandle — Isolated Alpha-Candle Strategy (Paper-Test Build)

This is a **fully standalone** project folder. It does NOT import or depend
on anything in your existing Ali algo folder (`brokerClass.py`, `config.py`,
`utility.py`, `dhan_token_manager.py`, `logger.py`, `discovery.py`,
`scanner.py`, `positions.py`, `main2.py`). Nothing there is touched.

## Files in this folder

| File | Purpose |
|---|---|
| `config.py` | All credentials + strategy parameters. **Fill in your Dhan/Telegram creds here.** |
| `logger.py` | Isolated logging → this folder's own `logs/` directory. |
| `dhan_token_manager.py` | Isolated Dhan access-token cache/refresh → this folder's own `data/` directory. |
| `broker.py` | Dhan broker adapter (order placement, quotes, candles, WebSocket feed). |
| `notifier.py` | Smart Telegram messages — explains exact symbol, direction, candle time, price levels. |
| `state.py` | Shared in-memory state, read by the dashboard. |
| `timing.py` | Market time-window helpers. |
| `universe.py` | Builds the ~212-stock F&O universe from Dhan's scrip master. |
| `discovery.py` | Top-10 gainer/loser engine (rate-limit safe). |
| `pattern.py` | 5-min trend-run + Alpha Candle detection, 1-min breakout check. |
| `engine.py` | Position sizing, entry, 1:2 partial-book + breakeven lock, stall exit, blacklist, daily loss cap. |
| `main.py` | Main runner — starts everything. |
| `dashboard.py` | Flask dashboard app. |
| `templates/dashboard.html` | Modern clean dashboard UI. |
| `requirements.txt` | Python dependencies. |

## Strategy logic implemented (confirmed understanding)

1. **Discovery**: continuously ranks Top-10 gainers/losers from the ~212 F&O
   stock universe using live WebSocket ticks (batched REST scan only every
   15s to refresh previous-close, so no rate-limit risk).
2. **Alpha Candle (5-min)**: a run of 3+ consecutive same-colour candles
   (each closing beyond the prior candle's high/low, no doji, none the
   lowest-volume candle of the day so far) followed by the first
   counter-colour candle = the Alpha Candle.
3. **1-min entry precision**: after switching to the 1-min chart, we wait for
   a 1-min candle whose high crosses AND closes above the Alpha Candle's
   high (BUY) / below its low (SELL) — regardless of how many 1-min candles
   it takes, within a rolling hold budget of `HOLD_CANDLES_5MIN` (default 3)
   × 5 minutes. If that budget expires with no breakout, the setup is
   dropped so the scanner can move to a fresher, better setup on another
   stock rather than getting stuck.
4. **Exit reassessment at 1:2**: books 50% of quantity, moves the stop-loss
   on the remaining 50% to breakeven (entry price) — so realized profit can
   never be given back. "Reversal" = price stalling sideways for
   `STALL_CANDLES_FOR_REVERSAL_EXIT` (default 3) consecutive 1-min candles
   with no new favourable high/low after the 1:2 target — the runner is
   then exited rather than risking a full round-trip back to breakeven.
5. **Daily risk cap**: ₹2,000 max loss per day, aggregated across ALL
   stocks combined (not per stock). Once breached, "cost-to-cost mode"
   activates — any further trade that day is forced to exit flat (₹0 PnL),
   never adding more loss even while still looking for one more setup.
6. **Blacklist**: a stock already traded once today is locked out for the
   rest of the day. It can only requalify after a `BLACKLIST_COOLDOWN_MIN`
   (default 60 min) cooldown AND a new signal candle with volume at least
   `BLACKLIST_REQUALIFY_VOL_MULTIPLE` (default 1.5×) the original trade's
   signal volume — i.e. a materially stronger setup.

## Setup on your VPS (isolated new folder)

```bash
# 1. On your VPS, create a brand-new isolated folder
mkdir -p ~/alphacandle
cd ~/alphacandle

# 2. Copy all files from this package into that folder (keep templates/ subfolder)
#    e.g. via scp from your PC:
#    scp -r alphacandle/* user@your-vps-ip:~/alphacandle/

# 3. Create an isolated Python virtual environment (does not touch system/Ali's env)
python3 -m venv venv
source venv/bin/activate

# 4. Install dependencies (only inside this venv)
pip install --upgrade pip
pip install -r requirements.txt

# 5. Edit config.py and fill in:
#    - CLIENT_ID, PIN, TOTP_TOKEN  (your Dhan credentials)
#    - BOT_TOKEN, BOT_CHAT_ID       (your Telegram bot)
#    Leave PAPER_MODE = True for tomorrow's testing.
nano config.py

# 6. Run the strategy engine (in a tmux session so it survives SSH disconnect)
tmux new -s alphacandle_engine
source venv/bin/activate
python main.py
# Ctrl+B then D to detach

# 7. Run the dashboard (separate tmux session)
tmux new -s alphacandle_dash
source venv/bin/activate
python dashboard.py
# Ctrl+B then D to detach
```

## Viewing the dashboard from your PC (via VS Code SSH)

1. Connect to the VPS via VS Code Remote-SSH as you already do.
2. Open the **Ports** tab (bottom panel next to Terminal).
3. Click **Forward a Port** → enter `8787`.
4. Open the `localhost:8787` link VS Code gives you, in your browser.

(Alternatively browse `http://<vps-ip>:8787` directly if port 8787 is open
in your VPS firewall — not required if you use VS Code port forwarding.)

## Reattaching to check on running sessions

```bash
tmux attach -t alphacandle_engine
tmux attach -t alphacandle_dash
```

## Before flipping to LIVE mode

1. Run in `PAPER_MODE = True` for several sessions.
2. Cross-check flagged Alpha Candle setups (Telegram message + dashboard
   watchlist row) against your own chart at the exact timestamp shown.
3. Confirm `RISK_PER_TRADE`, `MAX_LOSS_PER_DAY`, `MAX_TRADES_PER_DAY`,
   `MAX_OPEN_POSITIONS` in `config.py` match your comfort level.
4. Only then set `PAPER_MODE = False` in `config.py` and restart `main.py`.
