"""
AlphaCandle - Universe Builder.
Downloads Dhan's scrip master and resolves the ~212-stock F&O underlying
universe on NSE_EQ. Fully self-contained, no dependency on any file outside
this project folder.
"""
import logging
import pandas as pd
import config

logger = logging.getLogger(__name__)

SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"


def build_fno_universe() -> pd.DataFrame:
    """
    Returns a DataFrame with at least SECURITY_ID, DISPLAY_NAME,
    UNDERLYING_SYMBOL, TICK_SIZE columns, restricted to NSE equity shares
    that have F&O (futures/options) contracts trading on them.
    """
    logger.info("Downloading Dhan scrip master for F&O universe resolution...")
    master = pd.read_csv(SCRIP_MASTER_URL, low_memory=False)
    master = master.iloc[:, :16]

    equity_rows = master[
        (master.SEGMENT == "E") & (master.EXCH_ID == "NSE") & (master.SERIES == "EQ")
    ].copy()

    if config.FNO_ONLY:
        fno_underlyings = set(
            master[master["INSTRUMENT_TYPE"].isin(["FUTSTK", "OPTSTK"])]
            ["UNDERLYING_SYMBOL"].dropna().unique()
        )
        equity_rows = equity_rows[equity_rows.UNDERLYING_SYMBOL.isin(fno_underlyings)]

    equity_rows = equity_rows.drop_duplicates(subset=["SECURITY_ID"])
    if "TICK_SIZE" not in equity_rows.columns:
        equity_rows["TICK_SIZE"] = 5  # paise, matches Ali's original row.TICK_SIZE/100 convention

    logger.info(f"F&O universe resolved: {len(equity_rows)} NSE equity symbols")
    return equity_rows.reset_index(drop=True)
