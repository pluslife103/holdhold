"""
Backfill 60 days of historical US stock market-cap data into data/history.db.

Approach:
  1. Fetch current snapshot from NASDAQ screener (price + market_cap + metadata)
  2. Derive shares = market_cap / price  (stable over 60 days)
  3. Download 60-day closing prices + volume via yfinance batch download
  4. historical_market_cap = shares × historical_close
  5. Insert into SQLite snapshots table (skip dates that already exist)
"""

import logging
import sqlite3
import sys
import time
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

DB_PATH    = Path(__file__).parent / "data" / "history.db"
PERIOD     = "65d"   # slightly over 60 to cover weekends/holidays
BATCH_SIZE = 200     # tickers per yfinance download call
_HEADERS   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            date       TEXT NOT NULL,
            ticker     TEXT NOT NULL,
            name       TEXT,
            sector     TEXT,
            industry   TEXT,
            country    TEXT,
            price      REAL,
            market_cap REAL,
            change_pct REAL,
            volume     REAL,
            PRIMARY KEY (date, ticker)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_date   ON snapshots(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_ticker ON snapshots(ticker)")
    conn.commit()
    return conn

def has_date(conn, date):
    return conn.execute(
        "SELECT COUNT(*) FROM snapshots WHERE date=?", (date,)
    ).fetchone()[0] > 0

def insert_batch(conn, records):
    conn.executemany(
        """INSERT OR REPLACE INTO snapshots
           (date,ticker,name,sector,industry,country,price,market_cap,change_pct,volume)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        records,
    )
    conn.commit()


# ── Step 1: NASDAQ screener (current snapshot) ────────────────────────────────

def fetch_nasdaq() -> dict:
    """Returns {ticker: {name,sector,industry,country,price,market_cap,shares}}"""
    log.info("Fetching current NASDAQ snapshot…")
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&download=true"
    r = requests.get(url, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    rows = r.json()["data"]["rows"]

    result = {}
    for row in rows:
        t = row.get("symbol", "").strip()
        if not t or any(c in t for c in ("^", "~", "/", "+", "$", "*")):
            continue
        try:
            price = float(row["lastsale"].replace("$", "").replace(",", "") or 0)
            cap   = float(row["marketCap"].replace(",", "") or 0)
        except (ValueError, AttributeError):
            continue
        if price <= 0 or cap <= 0:
            continue
        result[t] = {
            "name":     row.get("name", "").strip(),
            "sector":   row.get("sector", "") or "Unknown",
            "industry": row.get("industry", "") or "",
            "country":  row.get("country", "") or "",
            "price":    price,
            "cap":      cap,
            "shares":   cap / price,   # derived shares outstanding
        }

    log.info(f"NASDAQ snapshot: {len(result)} tickers with valid price/cap")
    return result


# ── Step 2: Historical prices via yfinance ────────────────────────────────────

def download_prices(tickers: list) -> dict:
    """Returns {date_str: {ticker: (close, volume)}}"""
    log.info(f"Downloading {PERIOD} of price history ({len(tickers)} tickers, batch={BATCH_SIZE})…")
    all_data: dict = {}
    total = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i: i + BATCH_SIZE]
        batch_n = i // BATCH_SIZE + 1
        try:
            df = yf.download(
                batch, period=PERIOD,
                auto_adjust=True, progress=False,
                threads=True,
            )
            if df.empty:
                continue

            # Multi-ticker returns MultiIndex columns; single returns flat
            close_df  = df.get("Close")
            volume_df = df.get("Volume")
            if close_df is None:
                continue
            if isinstance(close_df, pd.Series):
                close_df  = close_df.to_frame(name=batch[0])
                if volume_df is not None and isinstance(volume_df, pd.Series):
                    volume_df = volume_df.to_frame(name=batch[0])

            for ts, row in close_df.iterrows():
                day = ts.strftime("%Y-%m-%d")
                if day not in all_data:
                    all_data[day] = {}
                for col in row.index:
                    price = row[col]
                    if pd.notna(price) and price > 0:
                        vol = None
                        if volume_df is not None and col in volume_df.columns:
                            v = volume_df.loc[ts, col]
                            vol = float(v) if pd.notna(v) else None
                        all_data[day][str(col)] = (float(price), vol)

        except Exception as e:
            log.warning(f"Price batch {batch_n}/{total} error: {e}")

        if batch_n % 5 == 0 or batch_n == total:
            log.info(f"  batch {batch_n}/{total} done")

    days = sorted(all_data.keys())
    log.info(f"Price data ready: {len(days)} trading days  "
             f"({days[0] if days else '?'} → {days[-1] if days else '?'})")
    return all_data


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn        = get_db()
    info_map    = fetch_nasdaq()              # current snapshot
    tickers     = list(info_map.keys())
    price_data  = download_prices(tickers)    # 60-day OHLCV

    sorted_days = sorted(price_data.keys())
    log.info(f"Building records for {len(sorted_days)} trading days…")

    total_inserted = 0
    skipped_days   = 0

    for idx, day in enumerate(sorted_days):
        if has_date(conn, day):
            log.info(f"  {day}: already exists, skipping")
            skipped_days += 1
            continue

        prices_today = price_data[day]
        prices_prev  = price_data[sorted_days[idx - 1]] if idx > 0 else {}

        records = []
        for ticker, (close, vol) in prices_today.items():
            if close <= 0:
                continue
            info   = info_map.get(ticker)
            if not info:
                continue          # no current metadata for this ticker

            shares     = info["shares"]
            market_cap = close * shares

            prev = prices_prev.get(ticker)
            change_pct = (
                round((close - prev[0]) / prev[0] * 100, 4)
                if prev and prev[0] > 0 else None
            )

            records.append((
                day, ticker,
                info["name"],
                info["sector"],
                info["industry"],
                info["country"],
                close,
                market_cap,
                change_pct,
                vol,
            ))

        if records:
            insert_batch(conn, records)
            log.info(f"  {day}: {len(records):,} records inserted")
            total_inserted += len(records)

    conn.close()
    log.info(
        f"\nDone — {total_inserted:,} rows inserted across "
        f"{len(sorted_days) - skipped_days} new days "
        f"({skipped_days} already existed)"
    )


if __name__ == "__main__":
    t0 = time.time()
    main()
    log.info(f"Total time: {(time.time()-t0)/60:.1f} min")
