#!/usr/bin/env python3
"""
fill_broker_stock.py — 針對指定股票補齊 broker_chip_history.db 的分點資料

用法:
  python fill_broker_stock.py 2330          # 補最近 90 天
  python fill_broker_stock.py 2330 2454 0050  # 同時補多支
  python fill_broker_stock.py 2330 --days 30  # 只補最近 30 天
"""
import os, sys, time, sqlite3, requests, argparse
from pathlib import Path
from datetime import date, timedelta

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

FINMIND_BASE = "https://api.finmindtrade.com/api/v4/data"
DB_PATH      = "broker_chip_history.db"
DELAY_S      = 0.35


def load_token() -> str:
    t = os.getenv("FINMIND_TOKEN", "")
    if t:
        return t
    for ep in ["finmind_chip_screener/.env", ".env"]:
        p = Path(ep)
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                if "FINMIND_TOKEN" in line and "=" in line:
                    t = line.split("=", 1)[-1].strip().strip('"').strip("'")
                    if t:
                        return t
    return ""


def get_trading_dates(days: int) -> list[str]:
    """回傳最近 days 個工作日（近似值）。"""
    result = []
    d = date.today()
    while len(result) < days:
        if d.weekday() < 5:
            result.append(d.isoformat())
        d -= timedelta(days=1)
    return result  # newest first


def fetch_day(token: str, stock_id: str, date_str: str) -> list | None:
    for attempt in range(1, 4):
        try:
            r = requests.get(FINMIND_BASE, params={
                "dataset":    "TaiwanStockTradingDailyReport",
                "data_id":    stock_id,
                "start_date": date_str,
                "end_date":   date_str,
                "token":      token,
            }, timeout=30)
        except Exception as e:
            print(f"  [{attempt}] 網路錯誤: {e}")
            time.sleep(3 * attempt)
            continue
        if r.status_code == 402:
            print(f"  402 速率限制，等待 120 秒…")
            time.sleep(120)
            continue
        if r.status_code == 200:
            j = r.json()
            if j.get("status") == 402:
                print(f"  FinMind 402，等待 120 秒…")
                time.sleep(120)
                continue
            return j.get("data", [])
        time.sleep(3)
    return None


def upsert(conn: sqlite3.Connection, date_str: str, stock_id: str, api_rows: list) -> int:
    agg: dict = {}
    for row in api_rows:
        bid   = str(row.get("securities_trader_id", "")).strip()
        bname = str(row.get("securities_trader",    "")).strip()
        buy   = int(float(row.get("buy",  0) or 0))
        sell  = int(float(row.get("sell", 0) or 0))
        if not bid:
            continue
        if bid not in agg:
            agg[bid] = {"name": bname, "buy": 0, "sell": 0}
        agg[bid]["buy"]  += buy
        agg[bid]["sell"] += sell

    if not agg:
        return 0

    conn.executemany(
        "INSERT OR IGNORE INTO broker_info (broker_id, broker_name) VALUES (?,?)",
        [(bid, v["name"]) for bid, v in agg.items()],
    )
    conn.executemany(
        "INSERT OR REPLACE INTO broker_chip (date, stock_id, broker_id, buy, sell) VALUES (?,?,?,?,?)",
        [(date_str, stock_id, bid, v["buy"], v["sell"]) for bid, v in agg.items()],
    )
    conn.executemany(
        "INSERT OR REPLACE INTO progress (date, stock_id, rows, done_at) VALUES (?,?,?,?)",
        [(date_str, stock_id, len(agg), date.today().isoformat())],
    )
    conn.commit()
    return len(agg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stocks", nargs="+", help="股票代號，如 2330 2454 0050")
    parser.add_argument("--days", type=int, default=90, help="補最近 N 個工作日（預設 90）")
    args = parser.parse_args()

    token = load_token()
    if not token:
        print("ERROR: 找不到 FINMIND_TOKEN"); sys.exit(1)
    print(f"Token: {token[:8]}...,  DB: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    trading_dates = get_trading_dates(args.days)
    print(f"目標日期: {trading_dates[-1]} ~ {trading_dates[0]} ({len(trading_dates)} 天)")

    # 已完成的 (stock, date) 組合
    done = {(r[0], r[1]) for r in conn.execute("SELECT stock_id, date FROM progress")}

    for stock_id in args.stocks:
        need = [d for d in trading_dates if (stock_id, d) not in done]
        print(f"\n{stock_id}: 需補 {len(need)} 天（跳過 {len(trading_dates)-len(need)} 天已完成）")
        for i, date_str in enumerate(reversed(need), 1):  # oldest first
            rows = fetch_day(token, stock_id, date_str)
            if rows is None:
                print(f"  {date_str}: 失敗，跳過")
                continue
            n = upsert(conn, date_str, stock_id, rows)
            print(f"  {date_str}: {n} 分點", end="\r" if i % 5 != 0 else "\n", flush=True)
            time.sleep(DELAY_S)
        print(f"\n{stock_id}: 完成！")

    conn.close()
    print("\n全部完成。")


if __name__ == "__main__":
    main()
