#!/usr/bin/env python3
"""
fill_broker_stock.py — 針對指定股票補齊 broker_chip_history.db 的分點資料
使用 TWSE bshtm 爬蟲（免費，需 ddddocr 解驗證碼）

用法:
  python fill_broker_stock.py 2330          # 補最近 90 天
  python fill_broker_stock.py 2330 2454 0050  # 同時補多支
  python fill_broker_stock.py 2330 --days 30  # 只補最近 30 天
"""
import sys, time, sqlite3, argparse
from datetime import date, timedelta

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB_PATH = "broker_chip_history.db"
DELAY_S = 3.0   # TWSE 爬蟲需要較長間隔（含驗證碼解碼）


def get_trading_dates(days: int) -> list[str]:
    """回傳最近 days 個工作日（不含今日，由近到遠）。"""
    result = []
    d = date.today() - timedelta(days=1)
    while len(result) < days:
        if d.weekday() < 5:
            result.append(d.isoformat())
        d -= timedelta(days=1)
    return result


def fetch_day_twse(stock_id: str, date_str: str) -> list | None:
    """用 TWSE bshtm 爬蟲抓單日分點資料，回傳 raw rows 或 None（失敗）。"""
    try:
        from twse_bshtm_crawler import query_stock
    except ImportError:
        print("ERROR: 找不到 twse_bshtm_crawler.py，請確認檔案在同一目錄")
        return None

    df = query_stock(stock_id, date_str=date_str, verbose=True)
    if df is None:
        return None
    if df.empty:
        return []
    # 轉成 {bid, name, buy, sell} 格式（股數）
    rows = []
    for _, row in df.iterrows():
        bid   = str(row.get("券商代號", "")).strip()
        bname = str(row.get("券商名稱", "")).strip()
        buy   = int(float(row.get("買進股數", 0) or 0))
        sell  = int(float(row.get("賣出股數", 0) or 0))
        if bid:
            rows.append({"bid": bid, "name": bname, "buy": buy, "sell": sell})
    return rows


def upsert(conn: sqlite3.Connection, date_str: str, stock_id: str, api_rows: list) -> int:
    agg: dict = {}
    for row in api_rows:
        bid  = row["bid"]
        if not bid:
            continue
        if bid not in agg:
            agg[bid] = {"name": row["name"], "buy": 0, "sell": 0}
        agg[bid]["buy"]  += row["buy"]
        agg[bid]["sell"] += row["sell"]

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

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    trading_dates = get_trading_dates(args.days)
    print(f"目標日期: {trading_dates[-1]} ~ {trading_dates[0]} ({len(trading_dates)} 天)")

    done = {(r[0], r[1]) for r in conn.execute("SELECT stock_id, date FROM progress")}

    for stock_id in args.stocks:
        need = [d for d in trading_dates if (stock_id, d) not in done]
        print(f"\n{stock_id}: 需補 {len(need)} 天（跳過 {len(trading_dates)-len(need)} 天已完成）")
        ok = 0
        for i, date_str in enumerate(reversed(need), 1):
            rows = fetch_day_twse(stock_id, date_str)
            if rows is None:
                print(f"  {date_str}: TWSE 爬蟲失敗，跳過")
                time.sleep(DELAY_S)
                continue
            n = upsert(conn, date_str, stock_id, rows)
            ok += 1
            print(f"  {date_str}: {n} 分點 ({ok}/{len(need)})")
            time.sleep(DELAY_S)
        print(f"\n{stock_id}: 完成 {ok}/{len(need)} 天")

    conn.close()
    print("\n全部完成。")


if __name__ == "__main__":
    main()
