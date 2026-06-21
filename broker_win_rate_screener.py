#!/usr/bin/env python3
"""
broker_win_rate_screener.py
觀察期 60 交易日版本

方法：找每個分點對每支股票「首次出現顯著淨買超」的日期，
      計算從那天起 60 個交易日後的股價漲跌幅。
"""
import sqlite3, requests, time, json, sys, io
from collections import defaultdict
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DB_PATH    = "broker_chip_history.db"
OUT_PATH   = "broker_win_rate_result.json"
MIN_NET    = 5    # 首次淨買超至少 5 張
MIN_TRADES = 5    # 至少 5 筆有效進場
OBS_DAYS   = 60   # 觀察 60 個交易日後的報酬

# ── 連接 DB ──────────────────────────────────────────────────────────────────
con = sqlite3.connect(DB_PATH, timeout=30)
con.execute("PRAGMA query_only = ON")

all_dates = [r[0] for r in con.execute(
    "SELECT DISTINCT date FROM broker_chip ORDER BY date")]
date_idx  = {d: i for i, d in enumerate(all_dates)}
print(f"交易日：{len(all_dates)} 個（{all_dates[0]} ~ {all_dates[-1]}）")

# ── 找每個(分點,股票)的首次顯著淨買超日 ─────────────────────────────────────
print("計算首次淨買超日…")
# 用 SQL 找每個(broker, stock)在哪天第一次出現 net >= MIN_NET
entries = con.execute(
    "SELECT broker_id, stock_id, MIN(date) as first_date "
    "FROM broker_chip WHERE (buy-sell)/1000 >= ? "
    "GROUP BY broker_id, stock_id",
    [MIN_NET]).fetchall()
print(f"  有效進場記錄：{len(entries):,} 筆")

# ── 價格快取 ─────────────────────────────────────────────────────────────────
PCACHE_FILE = Path("__broker_win_price_cache.json")
price_cache = json.loads(PCACHE_FILE.read_text("utf-8")) if PCACHE_FILE.exists() else {}
price_pts: dict = {}

def get_month(sid: str, ym: str) -> dict:
    key = f"{sid}|{ym}"
    if key in price_cache:
        r = price_cache[key]
        for d, c in r.items():
            price_pts[(sid, d)] = c
        return r
    try:
        resp = requests.get(
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY",
            params={"response":"json","date":ym+"01","stockNo":sid},
            headers={"User-Agent":"Mozilla/5.0"}, timeout=12)
        j = resp.json()
        if j.get("stat") != "OK":
            price_cache[key] = {}
            return {}
        result = {}
        for row in j.get("data", []):
            try:
                p = row[0].strip().split("/")
                iso = f"{int(p[0])+1911}-{p[1]}-{p[2]}"
                result[iso] = float(str(row[6]).replace(",","") or 0)
                price_pts[(sid, iso)] = result[iso]
            except: pass
        price_cache[key] = result
        time.sleep(0.2)
        return result
    except: return {}

# 收集所有需要的月份
needed = set()
for bid, sid, first_date in entries:
    needed.add((sid, first_date[:7].replace("-","")))
    # 60 交易日後大約是 3 個月
    idx = date_idx.get(first_date, -1)
    if idx >= 0 and idx + OBS_DAYS < len(all_dates):
        exit_date = all_dates[idx + OBS_DAYS]
    elif len(all_dates) > 0:
        exit_date = all_dates[-1]
    else:
        continue
    # 加入所有中間月份
    from datetime import datetime, timedelta
    d0 = datetime.strptime(first_date, "%Y-%m-%d")
    d1 = datetime.strptime(exit_date,  "%Y-%m-%d")
    cur = d0
    while cur <= d1:
        needed.add((sid, cur.strftime("%Y%m")))
        cur += timedelta(days=32)
        cur = cur.replace(day=1)

print(f"需要 {len(needed)} 個 (股票,月份) 的價格資料，開始下載…")
fetched = 0
for sid, ym in sorted(needed):
    get_month(sid, ym)
    fetched += 1
    if fetched % 100 == 0:
        print(f"  {fetched}/{len(needed)}…", flush=True)
PCACHE_FILE.write_text(json.dumps(price_cache, ensure_ascii=False), "utf-8")
print(f"  完成，共 {len(price_pts):,} 個價格點")

# ── 計算每筆進場的 60 日報酬 ─────────────────────────────────────────────────
print("計算 60 日報酬…")
broker_stats = defaultdict(lambda: {"wins":0,"losses":0,"total":0.0,"trades":[]})
broker_names = dict(con.execute("SELECT broker_id, broker_name FROM broker_info").fetchall())
con.close()

for bid, sid, first_date in entries:
    idx = date_idx.get(first_date, -1)
    if idx < 0:
        continue
    exit_idx  = min(idx + OBS_DAYS, len(all_dates) - 1)
    exit_date = all_dates[exit_idx]
    if exit_idx == idx:
        continue  # 沒有觀察空間
    p0 = price_pts.get((sid, first_date))
    p1 = price_pts.get((sid, exit_date))
    if not p0 or not p1 or p0 <= 0:
        continue
    ret = (p1 - p0) / p0
    st  = broker_stats[bid]
    st["total"] += ret
    if ret > 0: st["wins"] += 1
    else:       st["losses"] += 1
    st["trades"].append({"sid":sid,"entry":first_date,"exit":exit_date,
                         "ret_pct":round(ret*100,2),"p0":p0,"p1":p1})

# ── 排名輸出 ─────────────────────────────────────────────────────────────────
results = []
for bid, st in broker_stats.items():
    n = st["wins"] + st["losses"]
    if n < MIN_TRADES: continue
    wr  = st["wins"] / n
    avg = st["total"] / n
    results.append({
        "broker_id":    bid,
        "broker_name":  broker_names.get(bid, bid),
        "win_rate":     round(wr*100, 1),
        "avg_return":   round(avg*100, 2),
        "wins":         st["wins"],
        "losses":       st["losses"],
        "total_trades": n,
        "best_trades":  sorted(st["trades"], key=lambda x: x["ret_pct"], reverse=True)[:5],
    })

results.sort(key=lambda x: (x["win_rate"], x["avg_return"]), reverse=True)

print(f"\n{'='*72}")
print(f"分點買超後 60 交易日（約3個月）報酬率排行")
print(f"{'='*72}")
print(f"{'#':<4} {'分點':24} {'勝率':>6} {'平均報酬':>9} {'進場數':>6}")
print("-"*55)
for i, r in enumerate(results[:30], 1):
    print(f"#{i:<3} {r['broker_id']+' '+r['broker_name']:<24} "
          f"{r['win_rate']:>5.1f}%  {r['avg_return']:>+7.2f}%  {r['total_trades']:>5}次")

with open(OUT_PATH, "w", encoding="utf-8") as f:
    json.dump({"params":{"observe_days":OBS_DAYS,"min_net_lots":MIN_NET,"min_trades":MIN_TRADES},
               "results":results[:60]}, f, ensure_ascii=False, indent=2)
print(f"\n結果已存至 {OUT_PATH}")
