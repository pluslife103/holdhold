"""
Backfill 60 days of historical US stock market-cap data.

Write target (auto-detected):
  - TURSO_DATABASE_URL=libsql://...  TURSO_AUTH_TOKEN=...  → Turso cloud DB (CI/prod)
  - env vars absent / empty                                  → local data/history.db

Approach:
  1. Fetch current snapshot from NASDAQ screener (price + market_cap + metadata)
  2. Derive shares = market_cap / price  (stable over 60 days)
  3. Download 60-day closing prices + volume via yfinance batch download
  4. historical_market_cap = shares × historical_close
  5. Insert into snapshots table (skip dates that already exist)
"""

import logging
import os
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


# ── Local SQLite helpers ───────────────────────────────────────────────────────

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


# ── Turso HTTP API helpers ─────────────────────────────────────────────────────

def _turso_host(url: str) -> str:
    return url.replace("libsql://", "https://")

def _turso_arg(v):
    if v is None:
        return {"type": "null"}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    return {"type": "text", "value": str(v)}

def _turso_pipeline(session: requests.Session, host: str, token: str,
                    stmts: list, timeout: int = 60) -> list:
    resp = session.post(
        f"{host}/v2/pipeline",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        json={"requests": stmts + [{"type": "close"}]},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    for r in data.get("results", []):
        if r.get("type") == "error":
            raise RuntimeError(f"Turso error: {r['error']['message']}")
    return data.get("results", [])

def turso_ensure_schema(session, host, token):
    _turso_pipeline(session, host, token, [
        {"type": "execute", "stmt": {"sql": (
            "CREATE TABLE IF NOT EXISTS snapshots ("
            "date TEXT NOT NULL, ticker TEXT NOT NULL, "
            "name TEXT, sector TEXT, industry TEXT, country TEXT, "
            "price REAL, market_cap REAL, change_pct REAL, volume REAL, "
            "PRIMARY KEY (date, ticker))"
        ), "args": []}},
        {"type": "execute", "stmt": {
            "sql": "CREATE INDEX IF NOT EXISTS idx_snap_date ON snapshots(date)",
            "args": []}},
        {"type": "execute", "stmt": {
            "sql": "CREATE INDEX IF NOT EXISTS idx_snap_ticker ON snapshots(ticker)",
            "args": []}},
    ])
    log.info("Turso schema ensured")

def turso_has_date(session, host, token, date: str) -> bool:
    results = _turso_pipeline(session, host, token, [
        {"type": "execute", "stmt": {
            "sql": "SELECT COUNT(*) FROM snapshots WHERE date=?",
            "args": [{"type": "text", "value": date}],
        }}
    ])
    try:
        return int(results[0]["response"]["result"]["rows"][0][0]["value"]) > 0
    except (KeyError, IndexError, TypeError):
        return False

def turso_insert_batch(session, host, token, records, chunk_size=200):
    """Batch insert via Turso HTTP pipeline, chunk_size rows per HTTP request."""
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i+chunk_size]
        stmts = [
            {
                "type": "execute",
                "stmt": {
                    "sql": ("INSERT OR REPLACE INTO snapshots "
                            "(date,ticker,name,sector,industry,country,"
                            "price,market_cap,change_pct,volume) "
                            "VALUES (?,?,?,?,?,?,?,?,?,?)"),
                    "args": [_turso_arg(v) for v in row],
                }
            }
            for row in chunk
        ]
        _turso_pipeline(session, host, token, stmts)


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
            "shares":   cap / price,
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
    # Detect write target
    turso_url   = (os.environ.get("TURSO_DATABASE_URL") or "").strip()
    turso_token = (os.environ.get("TURSO_AUTH_TOKEN") or "").strip()
    use_turso   = turso_url.startswith("libsql://") and bool(turso_token)

    if use_turso:
        log.info(f"Write target: Turso  ({turso_url})")
        host    = _turso_host(turso_url)
        session = requests.Session()
        turso_ensure_schema(session, host, turso_token)
        has_date_fn    = lambda d: turso_has_date(session, host, turso_token, d)
        insert_batch_fn = lambda recs: turso_insert_batch(session, host, turso_token, recs)
        close_fn       = lambda: None
    else:
        log.info("Write target: local SQLite")
        conn = get_db()
        has_date_fn    = lambda d: has_date(conn, d)
        insert_batch_fn = lambda recs: insert_batch(conn, recs)
        close_fn       = conn.close

    info_map   = fetch_nasdaq()
    tickers    = list(info_map.keys())
    price_data = download_prices(tickers)

    sorted_days = sorted(price_data.keys())
    log.info(f"Building records for {len(sorted_days)} trading days…")

    total_inserted = 0
    skipped_days   = 0

    for idx, day in enumerate(sorted_days):
        if has_date_fn(day):
            log.info(f"  {day}: already exists, skipping")
            skipped_days += 1
            continue

        prices_today = price_data[day]
        prices_prev  = price_data[sorted_days[idx - 1]] if idx > 0 else {}

        records = []
        for ticker, (close, vol) in prices_today.items():
            if close <= 0:
                continue
            info = info_map.get(ticker)
            if not info:
                continue

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
            insert_batch_fn(records)
            log.info(f"  {day}: {len(records):,} records inserted")
            total_inserted += len(records)

    close_fn()
    log.info(
        f"\nDone — {total_inserted:,} rows inserted across "
        f"{len(sorted_days) - skipped_days} new days "
        f"({skipped_days} already existed)"
    )


if __name__ == "__main__":
    t0 = time.time()
    main()
    log.info(f"Total time: {(time.time()-t0)/60:.1f} min")
