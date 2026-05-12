"""
美國上市 ETF 規模篩選器 — 網頁版
執行: python etf_web.py  然後開啟 http://localhost:8000
"""

from __future__ import annotations
import asyncio, json, time
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncGenerator

import uvicorn
import yfinance as yf
import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse

app = FastAPI()
_cache:           dict = {"df": None, "loaded_at": 0, "loading": False}
_hist_cache:      dict[str, tuple[float, list]] = {}   # "{ticker}_{period}" -> (ts, data)
_overtake_cache:  dict = {"ts": 0.0, "data": [], "days": 0}
_overtake_v2_cache: dict[str, tuple[float, dict]] = {}  # "date_mode" -> (ts, result)
_executor = ThreadPoolExecutor(max_workers=14)

# period 代號 → yfinance period 字串
_YF_PERIOD = {"1m":"1mo", "3m":"3mo", "6m":"6mo", "1y":"1y", "2y":"2y"}

# ── 規模分級 ──────────────────────────────────────────────────────────────────
_TIERS = [
    ("S", 100e9,  float("inf"), "超大型", "≥ $100B"),
    ("A",  10e9,       100e9,  "大型",   "$10B–$100B"),
    ("B",   1e9,        10e9,  "中型",   "$1B–$10B"),
    ("C", 100e6,         1e9,  "小型",   "$100M–$1B"),
    ("D",    0,        100e6,  "微型",   "< $100M"),
]

def _aum_tier(aum: float) -> str:
    for grade, lo, hi, *_ in _TIERS:
        if lo <= aum < hi:
            return grade
    return "D"

def _compute_overtakes(days: int = 30) -> list[dict]:
    """
    比較每個分級內，近 days 天前後的 AUM 排名變化。
    若 ETF X 現在排名高於 Y，但 days 天前排名低於 Y，視為「X 超越了 Y」。
    AUM 用 current_AUM × (price_past / price_now) 估算。
    """
    df = _cache.get("df")
    if df is None or df.empty:
        return []

    results = []
    for tier in ["S", "A", "B", "C", "D"]:
        sub = df[df["tier"] == tier].sort_values("aum", ascending=False).reset_index(drop=True)
        if len(sub) < 2:
            continue
        tickers = sub["ticker"].tolist()[:20]

        try:
            raw   = yf.download(tickers, period=f"{days + 15}d", progress=False, auto_adjust=True)
            close = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw[["Close"]].rename(columns={"Close": tickers[0]})
            if close.empty:
                continue

            # 各 ETF 歷史 AUM 估算
            hist: dict[str, dict] = {}
            for tkr in tickers:
                if tkr not in close.columns:
                    continue
                prices = close[tkr].dropna()
                if len(prices) < max(5, days // 4):
                    continue
                cur_p  = float(prices.iloc[-1])
                past_p = float(prices.iloc[0])
                if cur_p <= 0 or past_p <= 0:
                    continue
                cur_aum  = float(sub.loc[sub["ticker"] == tkr, "aum"].values[0])
                past_aum = cur_aum * past_p / cur_p
                hist[tkr] = {"cur": cur_aum, "past": past_aum}

            if len(hist) < 2:
                continue

            ranked_now  = sorted(hist, key=lambda t: hist[t]["cur"],  reverse=True)
            ranked_past = sorted(hist, key=lambda t: hist[t]["past"], reverse=True)

            for ia, ta in enumerate(ranked_now):
                for ib, tb in enumerate(ranked_now):
                    if ia >= ib:
                        continue
                    # ta 現在排名高於 tb
                    past_a = ranked_past.index(ta)
                    past_b = ranked_past.index(tb)
                    if past_a > past_b:
                        # ta 之前排名低於 tb → ta 超越了 tb
                        diff = hist[ta]["cur"] - hist[tb]["cur"]
                        # 忽略差距小於被超越方 0.5% 的雜訊
                        if diff < hist[tb]["cur"] * 0.005:
                            continue
                        results.append({
                            "overtaker":     ta,
                            "overtaken":     tb,
                            "tier":          tier,
                            "aum_diff":      int(diff),
                            "overtaker_aum": int(hist[ta]["cur"]),
                            "overtaken_aum": int(hist[tb]["cur"]),
                            "days":          days,
                        })
        except Exception as e:
            continue

    return results

# ── ETF 清單 ─────────────────────────────────────────────────────────────────

BUILTIN_ETFS: list[str] = list(dict.fromkeys([
    "SPY","IVV","VOO","VTI","QQQ","DIA","IWM","MDY","IJH","IJR",
    "SCHB","SCHX","VB","VO","VV","ITOT","SPTM","SPLG","IWF","IWD",
    "QUAL","MTUM","VLUE","USMV","FNDX","PRF","RSP","ACWI","VT",
    "VEA","VWO","EFA","EEM","IEFA","IEMG","SCHF","SCHE",
    "EWJ","EWZ","EWC","EWG","EWU","EWA","FXI","MCHI","KWEB","INDA",
    "EWH","EWS","EWT","TUR","EWM","EZA","EPOL","ECH","EWP",
    "BND","AGG","LQD","HYG","JNK","TLT","IEF","SHY","VCSH","VCIT",
    "BNDX","EMB","MUB","TIP","VTIP","SCHZ","VGSH","VGIT","VGLT",
    "FLOT","SHV","BIL","SGOV","USFR","TFLO","GOVT","SPTI","SPTS",
    "MBB","VMBS","GNMA","ANGL","FALN","SHYG","USHY",
    "XLK","XLF","XLV","XLE","XLY","XLP","XLI","XLB","XLU","XLRE",
    "VGT","VFH","VHT","VDE","VCR","VDC","VIS","VAW","VPU","KBWB",
    "IBB","XBI","GLD","SLV","IAU","PDBC","USO","UNG",
    "ICLN","QCLN","SOXX","SMH","IGV","SKYY","CLOU","HACK","CIBR",
    "ROBO","BOTZ","ARKK","ARKW","ARKQ","ARKF","ARKX","ARKG",
    "XOP","OIH","KRE","KBE","IAI","IYF",
    "DGRO","SCHD","VIG","VYM","HDV","DVY","SDY","NOBL","SPHD","DGRW",
    "TQQQ","SQQQ","SPXL","SPXS","UPRO","SSO","SDS","QLD","QID",
    "TNA","TZA","LABU","LABD","FAS","FAZ","NUGT","DUST","SOXL","SOXS",
    "VNQ","IYR","SCHH","RWR","ICF","REET","REZ",
    "ZROZ","EDV","TMF","TBT","TBF","PST","TTT",
    "GLD","GLDM","SGOL","PHYS","PSLV","CEF",
    "USO","UCO","SCO","DBO","DJP","GSG","DBC",
    "VXX","VIXY","UVXY","SVXY","SPLV","LGLV","EEMV",
    "AOR","AOM","AOA","AOK",
]))

# ── 抓取單支 ETF（快照）──────────────────────────────────────────────────────

def _fetch_one(ticker: str) -> dict | None:
    try:
        info = yf.Ticker(ticker).info
        aum = info.get("totalAssets") or info.get("netAssets") or 0
        if not isinstance(aum, (int, float)) or aum <= 0:
            return None
        ytd_raw = info.get("ytdReturn") or 0.0
        ytd = ytd_raw / 100 if abs(ytd_raw) > 1.5 else ytd_raw
        er_raw = info.get("netExpenseRatio") or info.get("annualReportExpenseRatio") or 0.0
        aum_f = float(aum)
        return {
            "ticker":        ticker,
            "name":          info.get("longName") or info.get("shortName") or "",
            "aum":           aum_f,
            "tier":          _aum_tier(aum_f),
            "category":      info.get("category") or "",
            "expense_ratio": round(er_raw / 100, 6),
            "ytd_return":    round(ytd, 6),
            "three_yr_avg":  round(info.get("threeYearAverageReturn") or 0.0, 6),
            "exchange":      info.get("exchange") or "",
        }
    except Exception:
        return None

# ── 抓取歷史 AUM（估算，支援多個時間段）────────────────────────────────────

def _fetch_history(ticker: str, period: str = "3m") -> list[dict]:
    """
    hist_AUM(t) ≈ current_AUM × (price(t) / latest_price)
    假設短期份數不變，趨勢方向準確。
    """
    yf_period = _YF_PERIOD.get(period, "3mo")
    t = yf.Ticker(ticker)
    hist = t.history(period=yf_period)
    if hist.empty:
        return []
    info = t.info
    current_aum = float(info.get("totalAssets") or info.get("netAssets") or 0)
    if current_aum <= 0 or hist.empty:
        return []
    latest_price = float(hist["Close"].iloc[-1])
    if latest_price <= 0:
        return []
    result = []
    prev_aum = None
    for date, row in hist.iterrows():
        price = float(row["Close"])
        if price <= 0:
            continue
        aum = round(current_aum * price / latest_price)
        day_chg = round((aum / prev_aum - 1) * 100, 4) if prev_aum else 0.0
        result.append({
            "date":    date.strftime("%Y-%m-%d"),
            "aum":     aum,
            "day_chg": day_chg,
            "volume":  int(row["Volume"]),
        })
        prev_aum = aum
    return result

# ── 日/週/月超越（支援任意歷史日期）──────────────────────────────────────────

_OVERTAKE_DAYS = {"day": 1, "week": 5, "month": 21}

def _compute_overtakes_v2(date_str: str, mode: str) -> dict:
    """
    比較 date_str 當天 vs. N 個交易日前的 AUM 排名，找出超越事件。
    AUM 以 current_AUM × (price_at_date / price_latest) 估算。
    """
    days_back = _OVERTAKE_DAYS.get(mode, 1)
    df = _cache.get("df")
    if df is None or df.empty:
        return {"error": "ETF 資料尚未載入，請先前往主頁面載入", "events": [],
                "ticker_counts": {}, "tier_counts": {}, "date": date_str, "mode": mode}

    all_tickers = df["ticker"].tolist()
    try:
        raw   = yf.download(all_tickers, period="6mo", progress=False, auto_adjust=True)
        close = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw
    except Exception as e:
        return {"error": f"下載失敗: {e}", "events": [],
                "ticker_counts": {}, "tier_counts": {}, "date": date_str, "mode": mode}

    if close is None or close.empty:
        return {"error": "無價格資料", "events": [],
                "ticker_counts": {}, "tier_counts": {}, "date": date_str, "mode": mode}

    idx_dates = close.index
    target_dt = pd.Timestamp(date_str)
    valid = idx_dates[idx_dates <= target_dt]
    if len(valid) == 0:
        return {"error": f"無此日期的交易資料: {date_str}", "events": [],
                "ticker_counts": {}, "tier_counts": {}, "date": date_str, "mode": mode}

    t_idx   = len(valid) - 1
    p_idx   = max(0, t_idx - days_back)
    actual  = idx_dates[t_idx].strftime("%Y-%m-%d")

    today_px  = close.iloc[t_idx]
    past_px   = close.iloc[p_idx]
    latest_px = close.iloc[-1]

    def safe(series, key):
        try:
            v = float(series.get(key, float("nan")))
            return None if pd.isna(v) or v <= 0 else v
        except Exception:
            return None

    events = []
    for tier in ["S", "A", "B", "C", "D"]:
        sub = df[df["tier"] == tier].sort_values("aum", ascending=False)
        if len(sub) < 2:
            continue
        hist: dict[str, dict] = {}
        for tkr in sub["ticker"]:
            lp = safe(latest_px, tkr)
            tp = safe(today_px,  tkr)
            pp = safe(past_px,   tkr)
            if not all([lp, tp, pp]):
                continue
            cur = float(sub.loc[sub["ticker"] == tkr, "aum"].values[0])
            hist[tkr] = {"today": cur * tp / lp, "past": cur * pp / lp}
        if len(hist) < 2:
            continue
        by_now  = sorted(hist, key=lambda t: hist[t]["today"], reverse=True)
        by_past = sorted(hist, key=lambda t: hist[t]["past"],  reverse=True)
        for ia, ta in enumerate(by_now):
            for ib, tb in enumerate(by_now):
                if ia >= ib:
                    continue
                try:
                    pa, pb = by_past.index(ta), by_past.index(tb)
                except ValueError:
                    continue
                if pa > pb:
                    diff = hist[ta]["today"] - hist[tb]["today"]
                    if diff < hist[tb]["today"] * 0.001:
                        continue
                    events.append({
                        "overtaker":     ta,
                        "overtaken":     tb,
                        "tier":          tier,
                        "aum_diff":      int(diff),
                        "overtaker_aum": int(hist[ta]["today"]),
                        "overtaken_aum": int(hist[tb]["today"]),
                    })

    ticker_counts: dict[str, int] = {}
    tier_counts:   dict[str, int] = {}
    for e in events:
        ticker_counts[e["overtaker"]] = ticker_counts.get(e["overtaker"], 0) + 1
        tier_counts[e["tier"]]        = tier_counts.get(e["tier"], 0)        + 1

    return {"date": actual, "mode": mode,
            "events": events, "ticker_counts": ticker_counts, "tier_counts": tier_counts}

# ── SSE 進度流 ────────────────────────────────────────────────────────────────

async def _sse_fetch(tickers: list[str]) -> AsyncGenerator[str, None]:
    _cache["loading"] = True
    results: list[dict] = []
    total = len(tickers)
    done  = 0
    futures = {_executor.submit(_fetch_one, t): t for t in tickers}
    pending = dict(futures)
    while pending:
        finished = [f for f in list(pending) if f.done()]
        for f in finished:
            done += 1
            r = f.result()
            if r:
                results.append(r)
            del pending[f]
        pct = int(done / total * 100)
        yield f"data: {json.dumps({'pct': pct, 'done': done, 'total': total})}\n\n"
        if pending:
            await asyncio.sleep(0.25)
    df = pd.DataFrame(results).sort_values("aum", ascending=False).reset_index(drop=True)
    _cache["df"]        = df
    _cache["loaded_at"] = time.time()
    _cache["loading"]   = False
    yield f"data: {json.dumps({'pct':100,'done':total,'total':total,'ready':True,'count':len(df)})}\n\n"

# ── API ───────────────────────────────────────────────────────────────────────

@app.get("/api/load")
async def api_load():
    if _cache["loading"]:
        return JSONResponse({"error": "already loading"}, status_code=409)
    return StreamingResponse(
        _sse_fetch(BUILTIN_ETFS),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

@app.get("/api/filter")
def api_filter(
    min_aum:  float = Query(0),
    max_aum:  float = Query(0),
    category: str   = Query(""),
    max_er:   float = Query(0),
    top_n:    int   = Query(0),
    sort_by:  str   = Query("aum"),
    sort_asc: bool  = Query(False),
    tiers:    str   = Query(""),        # 逗號分隔，如 "S,A,B"
):
    df: pd.DataFrame | None = _cache.get("df")
    if df is None or df.empty:
        return JSONResponse({"error": "not loaded"}, status_code=400)
    f = df.copy()
    if min_aum > 0: f = f[f["aum"] >= min_aum]
    if max_aum > 0: f = f[f["aum"] <= max_aum]
    if category:    f = f[f["category"].str.contains(category, case=False, na=False)]
    if max_er > 0:  f = f[f["expense_ratio"] <= max_er / 100]
    tier_list = [t.strip().upper() for t in tiers.split(",") if t.strip()]
    if tier_list:   f = f[f["tier"].isin(tier_list)]
    valid = {"aum","tier","ticker","name","expense_ratio","ytd_return","three_yr_avg","category"}
    col = sort_by if sort_by in valid else "aum"
    f = f.sort_values(col, ascending=sort_asc).reset_index(drop=True)
    if top_n > 0: f = f.head(top_n)
    return JSONResponse(f.to_dict(orient="records"))

@app.get("/api/overtake_events")
async def api_overtake_events(
    date:  str = Query(""),
    mode:  str = Query("day"),
    force: int = Query(0),
):
    """日/週/月超越事件，支援任意歷史日期"""
    from datetime import date as dt_date
    if not date:
        date = dt_date.today().isoformat()
    if mode not in ("day", "week", "month"):
        mode = "day"
    cache_key = f"{date}_{mode}"
    now = time.time()
    ttl = 1800 if date == dt_date.today().isoformat() else 86400
    if not force and cache_key in _overtake_v2_cache:
        ts, data = _overtake_v2_cache[cache_key]
        if now - ts < ttl:
            return JSONResponse(data)
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(_executor, _compute_overtakes_v2, date, mode)
    _overtake_v2_cache[cache_key] = (now, data)
    return JSONResponse(data)

@app.get("/api/overtake")
async def api_overtake(days: int = Query(30)):
    """找出各分級內，近 days 天內發生規模超越的 ETF 配對"""
    days = max(7, min(days, 90))
    now  = time.time()
    if (_overtake_cache["ts"] > 0
            and now - _overtake_cache["ts"] < 3600
            and _overtake_cache["days"] == days):
        return JSONResponse(_overtake_cache["data"])
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(_executor, _compute_overtakes, days)
    _overtake_cache.update({"ts": now, "data": data, "days": days})
    return JSONResponse(data)

@app.get("/api/tier_stats")
def api_tier_stats():
    """回傳各分級的 ETF 數量與平均規模"""
    df: pd.DataFrame | None = _cache.get("df")
    if df is None or df.empty:
        return JSONResponse({})
    result = {}
    for grade, lo, hi, label, rng in _TIERS:
        sub = df[df["tier"] == grade]
        result[grade] = {
            "label": label, "range": rng,
            "count": len(sub),
            "total_aum": int(sub["aum"].sum()),
            "avg_aum":   int(sub["aum"].mean()) if len(sub) else 0,
        }
    return JSONResponse(result)

@app.get("/api/history/{ticker}")
async def api_history(ticker: str, period: str = Query("3m")):
    ticker = ticker.upper()
    if period not in _YF_PERIOD:
        period = "3m"
    key = f"{ticker}_{period}"
    now = time.time()
    if key in _hist_cache:
        ts, data = _hist_cache[key]
        if now - ts < 3600:
            return JSONResponse(data)
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(_executor, _fetch_history, ticker, period)
    _hist_cache[key] = (now, data)
    return JSONResponse(data)

@app.get("/api/search")
def api_search(q: str = Query("")):
    """依代號/名稱模糊搜尋，回傳前 10 筆"""
    q = q.strip().upper()
    if not q:
        return JSONResponse([])
    df: pd.DataFrame | None = _cache.get("df")
    if df is None or df.empty:
        # 快取未就緒時改從內建清單比對代號
        hits = [{"ticker": t, "name": "", "aum": 0}
                for t in BUILTIN_ETFS if q in t][:10]
        return JSONResponse(hits)
    mask = (df["ticker"].str.contains(q, case=False, na=False) |
            df["name"].str.contains(q, case=False, na=False))
    sub = df[mask].head(10)[["ticker","name","aum"]].to_dict(orient="records")
    return JSONResponse(sub)

@app.get("/api/status")
def api_status():
    df = _cache.get("df")
    return {"loaded": df is not None, "count": len(df) if df is not None else 0,
            "loading": _cache["loading"], "loaded_at": _cache["loaded_at"]}

# ── 前端 HTML ─────────────────────────────────────────────────────────────────

OVERTAKE_HTML = r"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETF 規模超越事件追蹤</title>
<style>
:root{--bg:#070b10;--surface:#0d1117;--card:#111927;--border:#1e2738;
  --green:#10b981;--green-dim:#064e3b;--accent:#4f8ef7;
  --text:#eef2ff;--muted:#6b7280;--red:#ef4444;}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh;}

/* ── header ── */
header{background:#0a0f18;border-bottom:1px solid var(--border);
  padding:9px 22px;display:flex;align-items:center;gap:12px;flex-wrap:nowrap;}
.h-title{display:flex;align-items:center;gap:7px;font-weight:700;font-size:.95rem;
  color:var(--green);text-decoration:none;white-space:nowrap;}
.mode-tabs{display:flex;gap:2px;background:#131b28;border-radius:7px;padding:3px;}
.mode-tab{padding:4px 14px;border-radius:5px;font-size:.82rem;font-weight:600;
  cursor:pointer;color:var(--muted);border:none;background:transparent;transition:all .15s;}
.mode-tab.on{background:var(--accent);color:#fff;}
.date-nav{display:flex;align-items:center;gap:6px;margin-left:auto;flex-shrink:0;}
.nav-btn{width:28px;height:28px;border:1px solid var(--border);background:var(--card);
  color:var(--text);border-radius:6px;cursor:pointer;font-size:.9rem;flex-shrink:0;}
.nav-btn:hover{background:#1a2438;}
.date-sel{background:var(--card);border:1px solid var(--border);color:var(--text);
  padding:4px 10px;border-radius:7px;font-size:.8rem;cursor:pointer;max-width:200px;}
.today-btn{padding:4px 12px;background:transparent;border:1px solid var(--border);
  color:var(--muted);border-radius:6px;font-size:.78rem;cursor:pointer;white-space:nowrap;}
.today-btn:hover{border-color:var(--accent);color:var(--accent);}
.refresh-btn{padding:5px 14px;background:var(--green);color:#fff;border:none;
  border-radius:7px;font-weight:600;font-size:.8rem;cursor:pointer;white-space:nowrap;}
.refresh-btn:hover{background:#059669;}
.back-btn{padding:4px 12px;background:transparent;border:1px solid var(--border);
  color:var(--muted);border-radius:6px;font-size:.78rem;cursor:pointer;white-space:nowrap;}
.back-btn:hover{color:var(--text);}

/* ── content ── */
.wrap{max-width:1440px;margin:0 auto;padding:18px 22px;}
.date-title{font-size:1.05rem;font-weight:700;margin-bottom:14px;display:flex;align-items:center;gap:10px;}
.date-title small{font-weight:400;color:var(--muted);font-size:.82rem;}

/* ── summary chips ── */
.chips-grid{display:flex;flex-wrap:wrap;gap:7px;margin-bottom:26px;min-height:36px;}
.ov-chip{display:inline-flex;align-items:center;gap:6px;padding:5px 8px 5px 11px;
  border-radius:20px;background:#091a0f;border:1px solid #16331e;cursor:pointer;
  transition:border-color .12s;}
.ov-chip:hover{border-color:var(--green);}
.ov-chip-t{color:#e2f5ea;font-weight:700;font-size:.84rem;}
.ov-chip-n{background:var(--green);color:#fff;font-size:.67rem;font-weight:800;
  padding:1px 7px;border-radius:10px;}

/* ── tier sections ── */
.tier-sec{margin-bottom:12px;border:1px solid var(--border);border-radius:10px;overflow:hidden;}
.tier-hdr{display:flex;align-items:center;gap:9px;padding:9px 15px;
  background:var(--surface);cursor:pointer;user-select:none;}
.tier-hdr:hover{background:#0f1722;}
.tier-hdr-name{font-weight:700;font-size:.9rem;}
.tier-range{font-size:.7rem;color:var(--muted);background:#1a2438;
  padding:1px 7px;border-radius:4px;white-space:nowrap;}
.tier-cnt{font-size:.72rem;color:var(--muted);}
.tier-arrow{margin-left:auto;color:var(--muted);font-size:.75rem;
  transition:transform .2s;display:inline-block;}
.tier-sec.closed .tier-arrow{transform:rotate(180deg);}
.tier-sec.closed .tier-body{display:none;}
.tier-body{padding:11px 15px;background:var(--card);}
.tier-chips{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px;}
.tier-events{display:flex;flex-direction:column;gap:6px;}

/* ── event cards ── */
.ev-card{display:flex;align-items:center;gap:9px;padding:8px 13px;
  background:#0a0f18;border:1px solid #1a2438;border-radius:8px;
  font-size:.82rem;transition:border-color .12s;}
.ev-card:hover{border-color:#2d3a52;}
.ev-tier{padding:2px 8px;border-radius:5px;font-size:.68rem;font-weight:800;white-space:nowrap;}
.ev-a{font-weight:700;color:var(--green);font-size:.88rem;}
.ev-arrow{color:var(--green);font-size:.72rem;}
.ev-b{color:#9ca3af;font-size:.85rem;}
.ev-aum{margin-left:auto;color:var(--muted);font-size:.72rem;font-variant-numeric:tabular-nums;white-space:nowrap;}
.ev-diff{color:var(--green);font-size:.7rem;margin-left:6px;white-space:nowrap;}

/* ── states ── */
.empty{text-align:center;padding:50px 20px;color:var(--muted);}
.loading-dot{display:inline-block;animation:blink 1s infinite;}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
.err-msg{color:#f87171;background:#2d1010;border:1px solid #5a1a1a;
  border-radius:8px;padding:12px 16px;margin:16px 0;font-size:.85rem;}
</style>
</head>
<body>
<header>
  <a class="h-title" href="/">
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/>
      <polyline points="16 7 22 7 22 13"/>
    </svg>
    ETF 規模超越事件追蹤
  </a>
  <div class="mode-tabs">
    <button class="mode-tab on" data-m="day"   onclick="setMode('day')">日</button>
    <button class="mode-tab"    data-m="week"  onclick="setMode('week')">週</button>
    <button class="mode-tab"    data-m="month" onclick="setMode('month')">月</button>
  </div>
  <div class="date-nav">
    <button class="nav-btn" onclick="prevDate()">←</button>
    <select class="date-sel" id="dateSel" onchange="onDateSel()"></select>
    <button class="nav-btn" onclick="nextDate()">→</button>
    <button class="today-btn" onclick="gotoToday()">今日</button>
    <button class="refresh-btn" onclick="refresh()">⟳ 重新整理</button>
    <button class="back-btn" onclick="location.href='/'">← 篩選器</button>
  </div>
</header>

<div class="wrap" id="wrap">
  <div class="empty"><span class="loading-dot">計算中…</span></div>
</div>

<script>
const $ = id => document.getElementById(id);
const TIERS = {
  S:['千億','千億規模','≥ $100B','#f7c948'],
  A:['百億','百億規模','$10B – $100B','#4f8ef7'],
  B:['十億','十億規模','$1B – $10B','#3dd68c'],
  C:['億元','億元規模','$100M – $1B','#ff9f43'],
  D:['小型','小型規模','< $100M','#8892a4'],
};
const TIER_ORDER = ['S','A','B','C','D'];
const MODE_STEP  = {day:1, week:7, month:30};
let _mode    = 'day';
let _date    = todayStr();
let _closed  = {};

function todayStr(){ return new Date().toISOString().slice(0,10); }

function addDays(d,n){
  const dt=new Date(d); dt.setDate(dt.getDate()+n);
  return dt.toISOString().slice(0,10);
}

function fmtAum(v){
  if(!v||v<0) return '—';
  if(v>=1e12) return `$${(v/1e12).toFixed(2)}T`;
  if(v>=1e9)  return `$${(v/1e9).toFixed(2)}B`;
  if(v>=1e6)  return `$${(v/1e6).toFixed(1)}M`;
  return `$${v.toLocaleString()}`;
}

function modeLabel(m){ return {day:'日',week:'週',month:'月'}[m]||m; }

// ── Date select population ───────────────────────────────────────────────────
function buildDateSel(){
  const sel = $('dateSel'), today = todayStr();
  const opts = [];
  for(let i=0;i<=90;i++){
    const d = addDays(today,-i);
    const label = i===0 ? `${d} (今日)` : d;
    opts.push(`<option value="${d}"${d===_date?' selected':''}>${label}</option>`);
  }
  sel.innerHTML = opts.join('');
}

function onDateSel(){ _date=$('dateSel').value; loadEvents(); }
function prevDate(){ _date=addDays(_date,-(MODE_STEP[_mode]||1)); buildDateSel(); loadEvents(); }
function nextDate(){ const n=addDays(_date,MODE_STEP[_mode]||1); if(n>todayStr())return; _date=n; buildDateSel(); loadEvents(); }
function gotoToday(){ _date=todayStr(); buildDateSel(); loadEvents(); }
function setMode(m){
  _mode=m;
  document.querySelectorAll('.mode-tab').forEach(b=>b.classList.toggle('on',b.dataset.m===m));
  loadEvents();
}
function refresh(){ loadEvents(true); }

// ── Load & render ─────────────────────────────────────────────────────────────
async function loadEvents(force=false){
  $('wrap').innerHTML=`<div class="empty"><span class="loading-dot">批次抓取 ${_date} 附近價格歷史…</span></div>`;
  const url=`/api/overtake_events?date=${_date}&mode=${_mode}${force?'&force=1':''}`;
  const res=await fetch(url);
  if(!res.ok){ $('wrap').innerHTML=`<div class="err-msg">API 錯誤 ${res.status}</div>`; return; }
  const data=await res.json();
  if(data.error){ $('wrap').innerHTML=`<div class="err-msg">${data.error}</div>`; return; }
  render(data);
}

function render(data){
  const events   = data.events || [];
  const tcounts  = data.ticker_counts || {};
  const wrap     = $('wrap');

  const period = `${data.date} ${modeLabel(data.mode)}統計`;

  if(!events.length){
    wrap.innerHTML=`
      <div class="date-title">${period}</div>
      <div class="empty" style="padding:36px">此期間各分級內無規模超越事件<br>
      <small style="font-size:.78rem;margin-top:8px;display:block">
      （追蹤相同指數的 ETF 價格高度相關，排名不易改變；<br>主題/槓桿/商品類 ETF 效果最佳）</small></div>`;
    return;
  }

  // ── summary chips (sorted by count desc) ──
  const sorted = Object.entries(tcounts).sort((a,b)=>b[1]-a[1]);
  const chipsHtml = sorted.map(([tkr,cnt])=>`
    <span class="ov-chip" onclick="scrollTo('tier_${tierOf(tkr,events)}')" title="${tkr} 在此期間超越 ${cnt} 支 ETF">
      <span class="ov-chip-t">${tkr}</span>
      <span class="ov-chip-n">${cnt}次</span>
    </span>`).join('');

  // ── tier sections ──
  const tierHtml = TIER_ORDER.map(tier=>{
    const tevs = events.filter(e=>e.tier===tier);
    if(!tevs.length) return '';
    const [short,long,range,color] = TIERS[tier];
    const closed = _closed[tier]?'closed':'';

    // per-tier chips
    const tcLocal={};
    tevs.forEach(e=>{ tcLocal[e.overtaker]=(tcLocal[e.overtaker]||0)+1; });
    const tChips = Object.entries(tcLocal).sort((a,b)=>b[1]-a[1]).map(([t,n])=>`
      <span class="ov-chip" style="border-color:${color}44;background:${color}0d">
        <span class="ov-chip-t">${t}</span>
        <span class="ov-chip-n" style="background:${color}">${n}次</span>
      </span>`).join('');

    // event cards
    const cards = tevs.map(e=>`
      <div class="ev-card">
        <span class="ev-tier" style="background:${color}1a;color:${color};border:1px solid ${color}44">${short}</span>
        <span class="ev-a">${e.overtaker}</span>
        <span class="ev-arrow">▶</span>
        <span class="ev-b">超越</span>
        <span class="ev-b" style="font-weight:600;color:#c9d1e0">${e.overtaken}</span>
        <span class="ev-aum">${fmtAum(e.overtaker_aum)} vs ${fmtAum(e.overtaken_aum)}</span>
        <span class="ev-diff">+${fmtAum(e.aum_diff)}</span>
      </div>`).join('');

    return `
      <div class="tier-sec ${closed}" id="tier_${tier}">
        <div class="tier-hdr" onclick="toggleTier('${tier}')">
          <span class="tier-hdr-name" style="color:${color}">${long}</span>
          <span class="tier-range">${range}</span>
          <span class="tier-cnt">${tevs.length} 個事件</span>
          <span class="tier-arrow">∧</span>
        </div>
        <div class="tier-body">
          <div class="tier-chips">${tChips}</div>
          <div class="tier-events">${cards}</div>
        </div>
      </div>`;
  }).join('');

  wrap.innerHTML=`
    <div class="date-title">${period}<small>共 ${events.length} 個事件・${sorted.length} 支 ETF</small></div>
    <div class="chips-grid">${chipsHtml}</div>
    ${tierHtml}`;
}

function tierOf(ticker, events){
  const e=events.find(ev=>ev.overtaker===ticker);
  return e?e.tier:'S';
}

function scrollTo(id){
  const el=document.getElementById(id);
  if(el){ el.scrollIntoView({behavior:'smooth',block:'start'}); if(_closed[el.dataset?.tier]) toggleTier(el.dataset?.tier); }
}

function toggleTier(tier){
  _closed[tier]=!_closed[tier];
  const el=document.getElementById('tier_'+tier);
  if(el) el.classList.toggle('closed',_closed[tier]);
}

// ── init ──────────────────────────────────────────────────────────────────────
buildDateSel();
loadEvents();
</script>
</body>
</html>"""

HTML = r"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>美國 ETF 規模篩選器</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
:root {
  --bg:#0f1117; --surface:#1a1d27; --border:#2a2d3a;
  --accent:#4f8ef7; --accent2:#7c5ff7;
  --text:#e2e5f0; --muted:#666c85; --green:#3dd68c; --red:#f55;
}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;height:100vh;overflow:hidden;}

/* ── header ── */
header{background:linear-gradient(135deg,#1a1d27,#12141e);border-bottom:1px solid var(--border);
  padding:14px 24px;display:flex;align-items:center;gap:14px;height:54px;}
header h1{font-size:1.15rem;font-weight:700;}
header span{font-size:.78rem;color:var(--muted);}
.status-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--muted);margin-right:5px;vertical-align:middle;}
.status-dot.ok{background:var(--green);}

/* ── layout ── */
.layout{display:flex;height:calc(100vh - 54px);}

/* ── sidebar ── */
aside{width:268px;min-width:220px;background:var(--surface);border-right:1px solid var(--border);
  padding:16px 14px;display:flex;flex-direction:column;gap:12px;overflow-y:auto;}
.panel-title{font-size:.7rem;text-transform:uppercase;letter-spacing:.09em;color:var(--muted);margin-bottom:5px;}
label{font-size:.8rem;color:var(--text);display:block;margin-bottom:3px;}
input,select{width:100%;background:#12141e;border:1px solid var(--border);border-radius:7px;
  color:var(--text);padding:6px 9px;font-size:.83rem;outline:none;}
input:focus,select:focus{border-color:var(--accent);}
.row2{display:grid;grid-template-columns:1fr 1fr;gap:7px;}
button{width:100%;padding:8px;border:none;border-radius:8px;font-size:.88rem;font-weight:600;cursor:pointer;}
.btn-load{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;}
.btn-load:hover{opacity:.9;}
.btn-filter{background:var(--border);color:var(--text);margin-top:2px;}
.btn-filter:hover{background:#2f3348;}
.btn-export{background:transparent;border:1px solid var(--border);color:var(--muted);font-size:.78rem;margin-top:3px;}
.btn-export:hover{border-color:var(--accent);color:var(--accent);}
.prog-wrap{display:none;}
.prog-bar-bg{background:var(--border);border-radius:6px;height:5px;overflow:hidden;}
.prog-bar{height:100%;border-radius:6px;background:linear-gradient(90deg,var(--accent),var(--accent2));width:0%;transition:width .3s;}
.prog-text{font-size:.72rem;color:var(--muted);margin-top:3px;}
.sort-sel{display:flex;gap:5px;align-items:center;}
.sort-sel select{width:auto;padding:4px 7px;font-size:.78rem;}

/* ── main ── */
main{flex:1;overflow:hidden;display:flex;flex-direction:column;min-width:0;}
.toolbar{display:flex;align-items:center;gap:8px;padding:8px 16px;
  border-bottom:1px solid var(--border);font-size:.8rem;color:var(--muted);background:var(--surface);flex-shrink:0;}
.count-badge{background:var(--border);border-radius:12px;padding:2px 9px;font-size:.76rem;color:var(--text);}

/* ── table ── */
.table-wrap{flex:1;overflow:auto;min-height:100px;}
table{width:100%;border-collapse:collapse;font-size:.82rem;}
thead th{position:sticky;top:0;background:#12141e;padding:9px 10px;text-align:left;
  font-weight:600;color:var(--muted);font-size:.72rem;text-transform:uppercase;letter-spacing:.06em;
  border-bottom:1px solid var(--border);cursor:pointer;user-select:none;white-space:nowrap;}
thead th:hover{color:var(--accent);}
thead th .si{opacity:.35;margin-left:3px;}
thead th.active .si{opacity:1;color:var(--accent);}
tbody tr{border-bottom:1px solid #1e2130;transition:background .1s;}
tbody tr:hover{background:#1d2030;}
td{padding:8px 10px;}
.ticker{font-weight:700;color:var(--accent);font-size:.88rem;}
.name-cell{max-width:230px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--muted);}
.aum-cell{text-align:right;font-weight:600;font-variant-numeric:tabular-nums;}
.num{text-align:right;font-variant-numeric:tabular-nums;}
.green{color:var(--green);}
.red{color:var(--red);}
.cat{font-size:.72rem;background:#1e2130;border-radius:4px;padding:2px 5px;
  color:var(--muted);max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:inline-block;}
.empty{text-align:center;padding:50px;color:var(--muted);}

/* ── compare button in row ── */
.btn-cmp{width:26px;height:26px;padding:0;border-radius:6px;font-size:.95rem;line-height:1;
  background:#1e2130;color:var(--muted);border:1px solid var(--border);cursor:pointer;transition:all .15s;}
.btn-cmp:hover{background:var(--accent);color:#fff;border-color:var(--accent);}
.btn-cmp.active{background:var(--accent);color:#fff;border-color:var(--accent);}

/* ── resizer ── */
.resizer{height:5px;background:var(--border);cursor:row-resize;flex-shrink:0;
  display:flex;align-items:center;justify-content:center;}
.resizer:hover{background:var(--accent);}
.resizer::after{content:'';width:40px;height:2px;background:currentColor;border-radius:2px;opacity:.5;}

/* ── chart panel ── */
.chart-panel{flex-shrink:0;background:var(--surface);border-top:1px solid var(--border);
  display:none;flex-direction:column;min-height:160px;}
.chart-panel.open{display:flex;}
.chart-hdr1{display:flex;align-items:center;gap:8px;padding:7px 14px;
  border-bottom:1px solid var(--border);flex-shrink:0;}
.chart-hdr2{display:flex;align-items:center;gap:7px;padding:5px 14px;
  border-bottom:1px solid var(--border);flex-shrink:0;flex-wrap:wrap;}
.chart-title{font-size:.82rem;font-weight:600;white-space:nowrap;}
.chips-area{display:flex;gap:5px;flex-wrap:wrap;flex:1;}
.chip{display:inline-flex;align-items:center;gap:4px;padding:2px 8px 2px 10px;border-radius:20px;
  font-size:.74rem;font-weight:600;color:#fff;cursor:default;}
.chip-x{cursor:pointer;opacity:.7;margin-left:1px;}
.chip-x:hover{opacity:1;}

/* period selector */
.period-sel{display:flex;gap:3px;}
.period-sel button{width:auto;padding:3px 9px;font-size:.73rem;border-radius:5px;
  background:var(--border);color:var(--muted);border:1px solid transparent;}
.period-sel button.on{background:var(--accent2);color:#fff;}

/* view toggle */
.view-tog{display:flex;gap:3px;margin-left:4px;}
.view-tog button{width:auto;padding:3px 9px;font-size:.73rem;border-radius:5px;
  background:var(--border);color:var(--muted);border:1px solid transparent;}
.view-tog button.on{background:var(--accent);color:#fff;}

/* mode (abs/pct) */
.chart-ctrl{display:flex;gap:3px;}
.chart-ctrl button{width:auto;padding:3px 9px;font-size:.73rem;border-radius:5px;
  background:var(--border);color:var(--muted);border:1px solid transparent;}
.chart-ctrl button.on{background:#2f3d5c;color:var(--accent);border-color:var(--accent);}

.btn-close-chart{width:26px;height:26px;padding:0;font-size:1rem;border-radius:6px;
  background:transparent;color:var(--muted);border:1px solid var(--border);}
.btn-close-chart:hover{background:var(--border);color:var(--text);}
.chart-body{flex:1;padding:8px 14px 10px;min-height:0;position:relative;overflow:hidden;}
.chart-body canvas{width:100%!important;}
.chart-spin{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  color:var(--muted);font-size:.82rem;pointer-events:none;background:var(--surface);z-index:2;}

/* ── search autocomplete ── */
.search-wrap{position:relative;flex:1;max-width:300px;}
.search-wrap input{padding:5px 9px;font-size:.8rem;width:100%;}
.search-drop{position:absolute;top:calc(100% + 4px);left:0;right:0;background:#1a1d27;
  border:1px solid var(--border);border-radius:8px;z-index:999;overflow:hidden;display:none;
  box-shadow:0 8px 24px #00000080;}
.search-drop.open{display:block;}
.sdrop-item{padding:8px 12px;cursor:pointer;display:flex;align-items:center;gap:10px;font-size:.8rem;}
.sdrop-item:hover,.sdrop-item.focused{background:#232636;}
.sdrop-ticker{font-weight:700;color:var(--accent);min-width:52px;}
.sdrop-name{color:var(--muted);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.sdrop-aum{color:var(--text);font-size:.75rem;white-space:nowrap;}
.sdrop-empty{padding:10px 12px;color:var(--muted);font-size:.8rem;text-align:center;}

/* ── history table ── */
.hist-table-wrap{flex:1;overflow:auto;font-size:.78rem;}
.hist-table-wrap table{width:100%;border-collapse:collapse;}
.hist-table-wrap thead th{position:sticky;top:0;background:#12141e;padding:7px 10px;
  text-align:right;font-weight:600;color:var(--muted);font-size:.7rem;
  text-transform:uppercase;border-bottom:1px solid var(--border);white-space:nowrap;}
.hist-table-wrap thead th:first-child{text-align:left;}
.hist-table-wrap tbody tr{border-bottom:1px solid #1a1c28;}
.hist-table-wrap tbody tr:hover{background:#1d2030;}
.hist-table-wrap td{padding:6px 10px;text-align:right;font-variant-numeric:tabular-nums;}
.hist-table-wrap td:first-child{text-align:left;color:var(--muted);font-size:.75rem;}
.htd-aum{font-weight:600;}
.htd-chg-pos{color:var(--green);}
.htd-chg-neg{color:var(--red);}

/* ── tier 分級 ── */
.tier-badge{display:inline-flex;align-items:center;justify-content:center;
  width:22px;height:22px;border-radius:5px;font-size:.72rem;font-weight:800;flex-shrink:0;}
.tier-S{background:#f7c94822;color:#f7c948;border:1px solid #f7c94860;}
.tier-A{background:#4f8ef722;color:#4f8ef7;border:1px solid #4f8ef760;}
.tier-B{background:#3dd68c22;color:#3dd68c;border:1px solid #3dd68c60;}
.tier-C{background:#ff9f4322;color:#ff9f43;border:1px solid #ff9f4360;}
.tier-D{background:#8892a422;color:#8892a4;border:1px solid #8892a460;}

/* tier 篩選按鈕（側欄）*/
.tier-grid{display:grid;grid-template-columns:1fr 1fr;gap:5px;}
.tier-card{display:flex;flex-direction:column;padding:7px 9px;border-radius:8px;cursor:pointer;
  border:1px solid var(--border);background:#12141e;transition:all .15s;user-select:none;}
.tier-card:hover{border-color:var(--border);background:#1a1d2a;}
.tier-card.on{border-color:currentColor;}
.tier-card.tier-card-S{color:#f7c948;} .tier-card.on.tier-card-S{background:#f7c94812;}
.tier-card.tier-card-A{color:#4f8ef7;} .tier-card.on.tier-card-A{background:#4f8ef712;}
.tier-card.tier-card-B{color:#3dd68c;} .tier-card.on.tier-card-B{background:#3dd68c12;}
.tier-card.tier-card-C{color:#ff9f43;} .tier-card.on.tier-card-C{background:#ff9f4312;}
.tier-card.tier-card-D{color:#8892a4;} .tier-card.on.tier-card-D{background:#8892a412;}
.tier-card-top{display:flex;align-items:center;gap:6px;margin-bottom:3px;}
.tier-card-grade{font-size:.9rem;font-weight:800;}
.tier-card-label{font-size:.72rem;font-weight:600;}
.tier-card-count{margin-left:auto;font-size:.72rem;opacity:.7;}
.tier-card-range{font-size:.68rem;opacity:.55;color:var(--muted);}

/* tier 分布橫條（toolbar）*/
.tier-dist{display:flex;align-items:center;gap:2px;height:16px;border-radius:4px;overflow:hidden;
  width:140px;flex-shrink:0;}
.tier-dist-seg{height:100%;transition:width .4s;cursor:default;}
.tier-dist-seg:hover{filter:brightness(1.3);}

/* ── 超越提示 ── */
.ov-badge{display:inline-flex;align-items:center;gap:3px;padding:1px 6px;border-radius:4px;
  font-size:.64rem;font-weight:700;background:#3dd68c1a;border:1px solid #3dd68c50;
  color:#3dd68c;cursor:pointer;white-space:nowrap;vertical-align:middle;margin-left:4px;}
.ov-badge:hover{background:#3dd68c30;}
.ov-badge-down{background:#f554541a;border-color:#f5545450;color:#f55;}
.ov-badge-down:hover{background:#f5545430;}

.notif-btn{width:auto;padding:4px 11px;font-size:.75rem;border-radius:6px;
  background:var(--border);color:var(--muted);border:1px solid transparent;cursor:pointer;}
.notif-btn:hover{background:#2f3348;color:var(--text);}
.notif-btn.active{background:#3dd68c1a;color:#3dd68c;border-color:#3dd68c50;}

/* overtake panel - 橫向通知列 */
.ov-panel{border-bottom:1px solid var(--border);background:#0d1018;
  flex-shrink:0;overflow:hidden;max-height:0;transition:max-height .25s ease;}
.ov-panel.open{max-height:120px;}
.ov-scroll{overflow-x:auto;overflow-y:hidden;padding:6px 14px;display:flex;
  gap:6px;align-items:center;min-height:44px;flex-wrap:nowrap;}
.ov-event{display:inline-flex;align-items:center;gap:5px;padding:4px 9px;
  border-radius:7px;background:#1a1d27;border:1px solid var(--border);
  font-size:.76rem;flex-shrink:0;cursor:default;transition:border-color .15s;}
.ov-event:hover{border-color:#3dd68c50;}
.ov-tier-tag{font-weight:800;font-size:.68rem;padding:1px 5px;border-radius:4px;}
.ov-name-a{font-weight:700;color:var(--green);}
.ov-arrow{color:var(--green);font-size:.8rem;}
.ov-name-b{color:var(--muted);}
.ov-diff{font-size:.7rem;color:var(--green);opacity:.8;}
.ov-days-sel{width:auto;padding:3px 6px;font-size:.74rem;background:#12141e;
  border:1px solid var(--border);border-radius:5px;color:var(--muted);}
.ov-empty{color:var(--muted);font-size:.78rem;padding:4px 0;}
.ov-loading-txt{color:var(--muted);font-size:.76rem;animation:blink 1s infinite;}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.4}}

/* chart crossover marker */
.crossover-legend{display:flex;gap:8px;flex-wrap:wrap;padding:2px 14px 4px;flex-shrink:0;}
.cx-item{font-size:.7rem;color:var(--muted);display:flex;align-items:center;gap:3px;}
.cx-dot{width:6px;height:6px;border-radius:50%;background:#fff;opacity:.6;}
</style>
</head>
<body>
<header>
  <div>
    <h1>🇺🇸 美國 ETF 規模篩選器</h1>
    <span>依 AUM 篩選美國掛牌 ETF・點擊 + 比較 60 天規模曲線</span>
  </div>
  <div style="margin-left:auto;display:flex;align-items:center;gap:10px">
    <a href="/overtake" style="display:inline-flex;align-items:center;gap:5px;padding:5px 12px;
      background:#10b98118;border:1px solid #10b98150;border-radius:7px;color:#10b981;
      font-size:.78rem;font-weight:700;text-decoration:none;" title="超越事件追蹤">
      ↗ 超越事件
    </a>
    <span class="status-dot" id="statusDot"></span>
    <span id="statusText" style="font-size:.78rem;color:var(--muted)">未載入</span>
  </div>
</header>

<div class="layout">
<!-- ── sidebar ── -->
<aside>
  <div>
    <div class="panel-title">資料來源</div>
    <button class="btn-load" id="btnLoad" onclick="loadData()">載入 ETF 資料</button>
    <div class="prog-wrap" id="progWrap" style="margin-top:8px">
      <div class="prog-bar-bg"><div class="prog-bar" id="progBar"></div></div>
      <div class="prog-text" id="progText">0 / 0</div>
    </div>
  </div>
  <hr style="border-color:var(--border)">
  <div>
    <div class="panel-title">篩選條件</div>
    <div class="row2" style="margin-bottom:9px">
      <div><label>最小規模</label><input id="minAum" placeholder="1B, 500M"></div>
      <div><label>最大規模</label><input id="maxAum" placeholder="10B"></div>
    </div>
    <div style="margin-bottom:9px">
      <label>類別關鍵字</label>
      <input id="catKw" placeholder="equity, bond, leveraged…">
    </div>
    <div class="row2" style="margin-bottom:9px">
      <div><label>費用率上限 %</label><input id="maxEr" type="number" min="0" step="0.01" placeholder="0.5"></div>
      <div><label>前 N 支</label><input id="topN" type="number" min="1" placeholder="全部"></div>
    </div>
    <div class="sort-sel" style="margin-bottom:9px">
      <label style="white-space:nowrap;margin:0">排序</label>
      <select id="sortBy">
        <option value="aum">規模</option>
        <option value="expense_ratio">費用率</option>
        <option value="ytd_return">YTD</option>
        <option value="three_yr_avg">3Y 報酬</option>
        <option value="ticker">代號</option>
      </select>
      <select id="sortDir">
        <option value="0">↓ 高→低</option>
        <option value="1">↑ 低→高</option>
      </select>
    </div>
    <button class="btn-filter" onclick="applyFilter()">套用篩選</button>
    <button class="btn-export" onclick="exportCSV()">匯出 CSV</button>
  </div>

  <hr style="border-color:var(--border)">

  <!-- 規模分級篩選 -->
  <div>
    <div class="panel-title" style="display:flex;align-items:center;justify-content:space-between">
      <span>規模分級</span>
      <span style="font-size:.68rem;cursor:pointer;color:var(--accent)" onclick="clearTiers()">全選/全清</span>
    </div>
    <div class="tier-grid" id="tierGrid">
      <div class="tier-card tier-card-S" data-tier="S" onclick="toggleTier('S')">
        <div class="tier-card-top">
          <span class="tier-card-grade">S</span>
          <span class="tier-card-label">超大型</span>
          <span class="tier-card-count" id="tc_S">—</span>
        </div>
        <span class="tier-card-range">≥ $100B</span>
      </div>
      <div class="tier-card tier-card-A" data-tier="A" onclick="toggleTier('A')">
        <div class="tier-card-top">
          <span class="tier-card-grade">A</span>
          <span class="tier-card-label">大型</span>
          <span class="tier-card-count" id="tc_A">—</span>
        </div>
        <span class="tier-card-range">$10B – $100B</span>
      </div>
      <div class="tier-card tier-card-B" data-tier="B" onclick="toggleTier('B')">
        <div class="tier-card-top">
          <span class="tier-card-grade">B</span>
          <span class="tier-card-label">中型</span>
          <span class="tier-card-count" id="tc_B">—</span>
        </div>
        <span class="tier-card-range">$1B – $10B</span>
      </div>
      <div class="tier-card tier-card-C" data-tier="C" onclick="toggleTier('C')">
        <div class="tier-card-top">
          <span class="tier-card-grade">C</span>
          <span class="tier-card-label">小型</span>
          <span class="tier-card-count" id="tc_C">—</span>
        </div>
        <span class="tier-card-range">$100M – $1B</span>
      </div>
      <div class="tier-card tier-card-D" data-tier="D" onclick="toggleTier('D')" style="grid-column:span 2">
        <div class="tier-card-top">
          <span class="tier-card-grade">D</span>
          <span class="tier-card-label">微型</span>
          <span class="tier-card-count" id="tc_D">—</span>
        </div>
        <span class="tier-card-range">< $100M</span>
      </div>
    </div>
  </div>
</aside>

<!-- ── main ── -->
<main id="mainCol">
  <div class="toolbar">
    <span id="resultInfo" class="count-badge">—</span>
    <div class="tier-dist" id="tierDist" title="規模分級分布"></div>
    <button class="notif-btn" id="notifBtn" onclick="toggleOvPanel()" title="顯示超越事件">🔔 超越</button>
    <select class="ov-days-sel" id="ovDays" onchange="loadOvertakes()">
      <option value="15">15 天</option>
      <option value="30" selected>30 天</option>
      <option value="60">60 天</option>
    </select>
    <span style="font-size:.72rem;color:var(--muted)">點欄位排序・點 <b>+</b> 比較</span>
    <span style="margin-left:auto;font-size:.72rem;color:var(--muted)" id="cmpHint"></span>
  </div>
  <!-- 超越通知列 -->
  <div class="ov-panel" id="ovPanel">
    <div class="ov-scroll" id="ovScroll">
      <span class="ov-empty">—</span>
    </div>
  </div>

  <div class="table-wrap" id="tableWrap">
    <table>
      <thead><tr>
        <th style="width:32px"></th>
        <th style="width:28px;text-align:right">#</th>
        <th onclick="sortCol('tier')" style="width:42px;text-align:center">級 <span class="si">↕</span></th>
        <th onclick="sortCol('ticker')">代號 <span class="si">↕</span></th>
        <th>名稱</th>
        <th onclick="sortCol('aum')" id="th_aum">規模 (AUM) <span class="si">↓</span></th>
        <th onclick="sortCol('expense_ratio')" id="th_expense_ratio">費用率 <span class="si">↕</span></th>
        <th onclick="sortCol('ytd_return')" id="th_ytd_return">YTD <span class="si">↕</span></th>
        <th onclick="sortCol('three_yr_avg')" id="th_three_yr_avg">3Y 報酬 <span class="si">↕</span></th>
        <th>類別</th>
      </tr></thead>
      <tbody id="tbody">
        <tr><td colspan="9" class="empty">請先點擊「載入 ETF 資料」</td></tr>
      </tbody>
    </table>
  </div>

  <!-- resizer -->
  <div class="resizer" id="resizer"></div>

  <!-- chart panel -->
  <div class="chart-panel" id="chartPanel">
    <!-- row 1: 標題 + 搜尋 + 關閉 -->
    <div class="chart-hdr1">
      <span class="chart-title">📈 歷史規模查詢</span>
      <div class="search-wrap">
        <input id="etfSearch" placeholder="搜尋 ETF 代號或名稱…" autocomplete="off">
        <div class="search-drop" id="searchDrop"></div>
      </div>
      <span id="cmpCount" style="font-size:.72rem;color:var(--muted);white-space:nowrap"></span>
      <button class="btn-close-chart" onclick="closeChart()" title="收起">×</button>
    </div>
    <!-- row 2: chips + 時間段 + 顯示模式 + abs/pct -->
    <div class="chart-hdr2">
      <div class="chips-area" id="chips"></div>
      <div class="period-sel">
        <button onclick="setPeriod('1m')" data-p="1m">1M</button>
        <button onclick="setPeriod('3m')" data-p="3m" class="on">3M</button>
        <button onclick="setPeriod('6m')" data-p="6m">6M</button>
        <button onclick="setPeriod('1y')" data-p="1y">1Y</button>
        <button onclick="setPeriod('2y')" data-p="2y">2Y</button>
      </div>
      <div class="view-tog">
        <button onclick="setView('chart')" id="vChart" class="on">圖表</button>
        <button onclick="setView('table')" id="vTable">數據表</button>
      </div>
      <div class="chart-ctrl" id="modeCtrl">
        <button id="btnAbs" class="on" onclick="setMode('abs')">絕對值</button>
        <button id="btnPct" onclick="setMode('pct')">變化 %</button>
      </div>
    </div>
    <!-- body -->
    <div class="chart-body" id="chartBody">
      <div class="chart-spin" id="chartSpin" style="display:none">載入中…</div>
      <canvas id="chartCanvas"></canvas>
    </div>
    <div class="crossover-legend" id="cxLegend" style="display:none"></div>
    <div class="hist-table-wrap" id="histTableWrap" style="display:none"></div>
  </div>
</main>
</div>

<script>
const $ = id => document.getElementById(id);
let _data      = [];
let _sortCol   = 'aum';
let _sortAsc   = false;
let _tierSel   = new Set();   // 空 = 全選；有值 = 只顯示選中分級
let _cmpSet    = new Map();   // ticker -> {color, name, histData}
let _chartMode = 'abs';       // 'abs' | 'pct'
let _chartView = 'chart';     // 'chart' | 'table'
let _period    = '3m';
let _chart     = null;
let _chartH    = 320;

const PALETTE = [
  '#4f8ef7','#3dd68c','#f7c948','#f7734f',
  '#b57cf7','#4ecdc4','#ff9f43','#f56085',
];
const MAX_CMP = 8;

const TIER_COLOR = {S:'#f7c948',A:'#4f8ef7',B:'#3dd68c',C:'#ff9f43',D:'#8892a4'};
const TIER_ORDER = ['S','A','B','C','D'];

let _overtakes = [];       // 超越事件列表
let _ovMap     = {};       // ticker → [{overtaken, ...}] 超越別人
let _ovMapDown = {};       // ticker → [{overtaker, ...}] 被別人超越

// ── 格式化 ───────────────────────────────────────────────────────────────────
function parseAum(s) {
  s = s.trim().toUpperCase().replace(/[$,]/g,'');
  if (!s) return 0;
  if (s.endsWith('T')) return parseFloat(s)*1e12;
  if (s.endsWith('B')) return parseFloat(s)*1e9;
  if (s.endsWith('M')) return parseFloat(s)*1e6;
  return parseFloat(s)||0;
}
function fmtAum(v) {
  if (v>=1e12) return `$${(v/1e12).toFixed(2)}T`;
  if (v>=1e9)  return `$${(v/1e9).toFixed(2)}B`;
  if (v>=1e6)  return `$${(v/1e6).toFixed(1)}M`;
  return `$${v.toLocaleString()}`;
}
function fmtPct(v) {
  if (!v) return '—';
  const s = (v*100).toFixed(2)+'%';
  return v>0 ? `<span class="green">+${s}</span>` : `<span class="red">${s}</span>`;
}
function fmtEr(v) { return v ? (v*100).toFixed(4)+'%' : '—'; }

// ── 載入資料 ─────────────────────────────────────────────────────────────────
function loadData() {
  $('btnLoad').disabled = true;
  $('progWrap').style.display = 'block';
  const es = new EventSource('/api/load');
  es.onmessage = e => {
    const d = JSON.parse(e.data);
    $('progBar').style.width = d.pct+'%';
    $('progText').textContent = `${d.done} / ${d.total} (${d.pct}%)`;
    if (d.ready) {
      es.close();
      $('statusDot').className = 'status-dot ok';
      $('statusText').textContent = `已載入 ${d.count} 支`;
      $('btnLoad').disabled = false;
      $('btnLoad').textContent = '重新載入';
      applyFilter();
      loadTierStats();
    }
  };
  es.onerror = () => { es.close(); $('btnLoad').disabled = false; };
}

// ── 篩選 ─────────────────────────────────────────────────────────────────────
async function applyFilter() {
  const params = new URLSearchParams({
    min_aum:  parseAum($('minAum').value),
    max_aum:  parseAum($('maxAum').value),
    category: $('catKw').value.trim(),
    max_er:   parseFloat($('maxEr').value)||0,
    top_n:    parseInt($('topN').value)||0,
    sort_by:  $('sortBy').value,
    sort_asc: $('sortDir').value==='1'?'true':'false',
    tiers:    [..._tierSel].join(','),
  });
  const res = await fetch('/api/filter?'+params);
  if (!res.ok) { renderEmpty('尚未載入資料'); return; }
  _data = await res.json();
  renderTable(_data);
  updateTierDist(_data);
}

// ── 分級篩選 ─────────────────────────────────────────────────────────────────
function toggleTier(g) {
  if (_tierSel.has(g)) _tierSel.delete(g); else _tierSel.add(g);
  document.querySelectorAll('.tier-card').forEach(el => {
    el.classList.toggle('on', _tierSel.has(el.dataset.tier));
  });
  applyFilter();
}

function clearTiers() {
  if (_tierSel.size === 0) {
    // 全清（全選反而是全部都不選）→ 這裡改為切換行為：有選就清，沒選就全選
    TIER_ORDER.forEach(g => _tierSel.add(g));
  } else {
    _tierSel.clear();
  }
  document.querySelectorAll('.tier-card').forEach(el =>
    el.classList.toggle('on', _tierSel.has(el.dataset.tier))
  );
  applyFilter();
}

async function loadTierStats() {
  const res = await fetch('/api/tier_stats');
  if (!res.ok) return;
  const stats = await res.json();
  TIER_ORDER.forEach(g => {
    const el = $('tc_'+g);
    if (el && stats[g]) el.textContent = stats[g].count;
  });
}

function updateTierDist(rows) {
  const dist = $('tierDist');
  if (!dist || !rows.length) { dist && (dist.innerHTML=''); return; }
  const counts = {};
  TIER_ORDER.forEach(g => counts[g] = 0);
  rows.forEach(r => { if (counts[r.tier]!=null) counts[r.tier]++; });
  const total = rows.length || 1;
  dist.innerHTML = TIER_ORDER.filter(g => counts[g]>0).map(g =>
    `<div class="tier-dist-seg" style="width:${counts[g]/total*100}%;background:${TIER_COLOR[g]}"
      title="${g}級: ${counts[g]}支"></div>`
  ).join('');
}

function sortCol(col) {
  if (_sortCol===col) _sortAsc=!_sortAsc; else { _sortCol=col; _sortAsc=false; }
  $('sortBy').value = col;
  $('sortDir').value = _sortAsc?'1':'0';
  applyFilter();
}

// ── 超越事件 ─────────────────────────────────────────────────────────────────
async function loadOvertakes() {
  const days = $('ovDays').value;
  $('ovScroll').innerHTML = '<span class="ov-loading-txt">計算中，依各分級批次抓取歷史價格…</span>';
  const res = await fetch(`/api/overtake?days=${days}`);
  _overtakes = res.ok ? await res.json() : [];
  buildOvMaps();
  renderOvPanel();
  renderTable(_data);   // 重繪表格讓徽章刷新
}

function buildOvMaps() {
  _ovMap     = {};
  _ovMapDown = {};
  _overtakes.forEach(e => {
    (_ovMap[e.overtaker]     = _ovMap[e.overtaker]     || []).push(e);
    (_ovMapDown[e.overtaken] = _ovMapDown[e.overtaken] || []).push(e);
  });
}

function toggleOvPanel() {
  const panel = $('ovPanel');
  const isOpen = panel.classList.toggle('open');
  $('notifBtn').classList.toggle('active', isOpen);
  if (isOpen && !_overtakes.length) loadOvertakes();
}

function renderOvPanel() {
  const btn = $('notifBtn');
  const n   = _overtakes.length;
  btn.textContent = n ? `🔔 超越 (${n})` : '🔔 超越';
  btn.classList.toggle('active', $('ovPanel').classList.contains('open'));

  const scroll = $('ovScroll');
  if (!n) {
    scroll.innerHTML = `<span class="ov-empty">過去 ${$('ovDays').value} 天內，各分級無規模超越事件</span>`;
    return;
  }

  // 依分級顯示
  const html = TIER_ORDER.flatMap(tier => {
    const evs = _overtakes.filter(e => e.tier === tier);
    if (!evs.length) return [];
    return evs.map(e => {
      const tc = TIER_COLOR[tier];
      return `<span class="ov-event"
          title="${e.overtaker} 在過去 ${e.days} 天內規模超越 ${e.overtaken}&#10;差距 ${fmtAum(e.aum_diff)}">
        <span class="ov-tier-tag" style="background:${tc}22;color:${tc};border:1px solid ${tc}55">${tier}</span>
        <span class="ov-name-a">${e.overtaker}</span>
        <span class="ov-arrow">▶</span>
        <span class="ov-name-b">${e.overtaken}</span>
        <span class="ov-diff">+${fmtAum(e.aum_diff)}</span>
      </span>`;
    });
  }).join('');
  scroll.innerHTML = html;
}

// ── 渲染表格 ─────────────────────────────────────────────────────────────────
function renderEmpty(msg) {
  $('tbody').innerHTML = `<tr><td colspan="9" class="empty">${msg}</td></tr>`;
  $('resultInfo').textContent = '—';
}

function renderTable(rows) {
  if (!rows.length) { renderEmpty('找不到符合條件的 ETF'); return; }
  $('resultInfo').textContent = `共 ${rows.length} 支 ETF`;
  const html = rows.map((r,i) => {
    const inCmp  = _cmpSet.has(r.ticker);
    const color  = inCmp ? _cmpSet.get(r.ticker).color : '';
    const btnSt  = inCmp ? `style="background:${color};border-color:${color};color:#fff"` : '';
    const tier   = r.tier || 'D';
    const tcolor = TIER_COLOR[tier] || '#8892a4';
    return `<tr>
      <td style="padding:6px 6px 6px 10px">
        <button class="btn-cmp${inCmp?' active':''}" ${btnSt}
          onclick="toggleCmp('${r.ticker}','${escHtml(r.name)}',this)"
          title="${inCmp?'從比較移除':'加入比較'}">+</button>
      </td>
      <td style="color:var(--muted);text-align:right;font-size:.75rem">${i+1}</td>
      <td style="text-align:center;padding:6px 4px">
        <span class="tier-badge tier-${tier}" title="${tierLabel(tier)}">${tier}</span>
      </td>
      <td class="ticker">${r.ticker}</td>
      <td class="name-cell" title="${escHtml(r.name)}">${escHtml(r.name)}${buildOvBadge(r.ticker)}</td>
      <td class="aum-cell">${fmtAum(r.aum)}</td>
      <td class="num">${fmtEr(r.expense_ratio)}</td>
      <td class="num">${fmtPct(r.ytd_return)}</td>
      <td class="num">${fmtPct(r.three_yr_avg)}</td>
      <td><span class="cat" title="${escHtml(r.category)}">${r.category||'—'}</span></td>
    </tr>`;
  }).join('');
  $('tbody').innerHTML = html;
}

function tierLabel(g) {
  return {S:'超大型 ≥$100B',A:'大型 $10B–$100B',B:'中型 $1B–$10B',C:'小型 $100M–$1B',D:'微型 <$100M'}[g]||g;
}

function buildOvBadge(ticker) {
  const ups   = _ovMap[ticker]     || [];
  const downs = _ovMapDown[ticker] || [];
  let html = '';
  if (ups.length) {
    const tip = ups.map(e=>`超越 ${e.overtaken} (+${fmtAum(e.aum_diff)})`).join('&#10;');
    const label = ups.length===1 ? `▲ 超 ${ups[0].overtaken}` : `▲ 超越 ${ups.length} 支`;
    html += `<span class="ov-badge" title="${tip}" onclick="event.stopPropagation();openOvPanel()">${label}</span>`;
  }
  if (downs.length) {
    const tip = downs.map(e=>`被 ${e.overtaker} 超越 (-${fmtAum(e.aum_diff)})`).join('&#10;');
    const label = downs.length===1 ? `▼ 被 ${downs[0].overtaker}` : `▼ 被超越 ${downs.length} 次`;
    html += `<span class="ov-badge ov-badge-down" title="${tip}" onclick="event.stopPropagation();openOvPanel()">${label}</span>`;
  }
  return html;
}

function openOvPanel() {
  const panel = $('ovPanel');
  if (!panel.classList.contains('open')) {
    panel.classList.add('open');
    $('notifBtn').classList.add('active');
    if (!_overtakes.length) loadOvertakes();
  }
}

function escHtml(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── 比較功能 ─────────────────────────────────────────────────────────────────
async function toggleCmp(ticker, name, btnEl) {
  if (_cmpSet.has(ticker)) { removeCmp(ticker); return; }
  if (_cmpSet.size >= MAX_CMP) { alert(`最多同時比較 ${MAX_CMP} 支 ETF`); return; }
  const color = PALETTE[_cmpSet.size % PALETTE.length];
  _cmpSet.set(ticker, { color, name, histData: null });
  if (btnEl) {
    btnEl.classList.add('active');
    btnEl.style.background = color;
    btnEl.style.borderColor = color;
    btnEl.style.color = '#fff';
  }
  openChart();
  updateChips();
  showChartSpin(true);
  const res  = await fetch(`/api/history/${ticker}?period=${_period}`);
  const hist = res.ok ? await res.json() : [];
  const entry = _cmpSet.get(ticker);
  if (entry) {
    entry.histData = hist;
    showChartSpin(false);
    refreshView();
    updateCmpCount();
  }
}

function removeCmp(ticker) {
  _cmpSet.delete(ticker);
  updateChips();
  refreshView();
  updateCmpCount();
  if (_cmpSet.size === 0) closeChart();
  renderTable(_data);
}

function updateCmpCount() {
  const el = $('cmpCount');
  if (el) el.textContent = _cmpSet.size ? `${_cmpSet.size}/${MAX_CMP}` : '';
  const h = $('cmpHint');
  if (h) h.textContent = _cmpSet.size ? `已選 ${_cmpSet.size} / ${MAX_CMP} 支` : '';
}

function openChart() {
  const panel = $('chartPanel');
  if (!panel.classList.contains('open')) {
    panel.classList.add('open');
    panel.style.height = _chartH + 'px';
    initChart();
  }
}

function closeChart() {
  $('chartPanel').classList.remove('open');
  _cmpSet.clear();
  updateChips();
  updateCmpCount();
  renderTable(_data);
  closeSearchDrop();
}

// ── Chips ─────────────────────────────────────────────────────────────────────
function updateChips() {
  $('chips').innerHTML = [..._cmpSet.entries()].map(([ticker, {color}]) =>
    `<span class="chip" style="background:${color}">${ticker}
       <span class="chip-x" onclick="removeCmp('${ticker}')">×</span></span>`
  ).join('');
}

// ── Period selector ───────────────────────────────────────────────────────────
async function setPeriod(p) {
  if (_period === p) return;
  _period = p;
  document.querySelectorAll('.period-sel button').forEach(b =>
    b.classList.toggle('on', b.dataset.p === p)
  );
  // 清除所有 ETF 的歷史快取，重新抓取
  for (const [ticker, entry] of _cmpSet) {
    entry.histData = null;
  }
  if (_cmpSet.size === 0) return;
  showChartSpin(true);
  await Promise.all([..._cmpSet.keys()].map(async ticker => {
    const res  = await fetch(`/api/history/${ticker}?period=${_period}`);
    const hist = res.ok ? await res.json() : [];
    const entry = _cmpSet.get(ticker);
    if (entry) entry.histData = hist;
  }));
  showChartSpin(false);
  refreshView();
}

// ── View toggle ───────────────────────────────────────────────────────────────
function setView(v) {
  _chartView = v;
  $('vChart').classList.toggle('on', v==='chart');
  $('vTable').classList.toggle('on', v==='table');
  $('modeCtrl').style.display = v==='chart' ? '' : 'none';
  $('chartBody').style.display       = v==='chart' ? '' : 'none';
  $('histTableWrap').style.display   = v==='table'  ? '' : 'none';
  if (v==='chart') { initChart(); updateChart(); }
  else renderHistTable();
}

function refreshView() {
  if (_chartView==='chart') updateChart();
  else renderHistTable();
}

function showChartSpin(on) {
  $('chartSpin').style.display = on ? 'flex' : 'none';
}

// ── Chart ─────────────────────────────────────────────────────────────────────
function setMode(m) {
  _chartMode = m;
  $('btnAbs').classList.toggle('on', m==='abs');
  $('btnPct').classList.toggle('on', m==='pct');
  updateChart();
}

// 交叉偵測：找出兩條線的交叉點索引
function detectCrossovers(datasets) {
  const cxs = [];
  for (let a = 0; a < datasets.length - 1; a++) {
    for (let b = a + 1; b < datasets.length; b++) {
      const dA = datasets[a].data;
      const dB = datasets[b].data;
      for (let i = 1; i < dA.length; i++) {
        if (dA[i-1]==null||dA[i]==null||dB[i-1]==null||dB[i]==null) continue;
        const prev = dA[i-1] - dB[i-1];
        const curr = dA[i]   - dB[i];
        if (prev !== 0 && curr !== 0 && Math.sign(prev) !== Math.sign(curr)) {
          cxs.push({
            idx:    i,
            labelA: datasets[a].label,
            colorA: datasets[a].borderColor,
            labelB: datasets[b].label,
            colorB: datasets[b].borderColor,
            dir:    curr > 0 ? 'A>B' : 'B>A',
          });
        }
      }
    }
  }
  return cxs;
}

// Chart.js plugin：在交叉點畫垂直虛線
const _cxPlugin = {
  id: 'crossoverLines',
  _cxs: [],
  afterDraw(chart) {
    if (!this._cxs.length) return;
    const ctx   = chart.ctx;
    const xAxis = chart.scales.x;
    const yAxis = chart.scales.y;
    this._cxs.forEach(cx => {
      const x = xAxis.getPixelForTick(cx.idx);
      if (x == null) return;
      ctx.save();
      ctx.setLineDash([3, 4]);
      ctx.strokeStyle = '#ffffff28';
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(x, yAxis.top);
      ctx.lineTo(x, yAxis.bottom);
      ctx.stroke();
      // 小圓點
      ctx.setLineDash([]);
      ctx.fillStyle = '#ffffff55';
      ctx.beginPath();
      ctx.arc(x, yAxis.top + 8, 3, 0, Math.PI * 2);
      ctx.fill();
      ctx.restore();
    });
  }
};

function initChart() {
  if (_chart) return;
  const ctx = $('chartCanvas').getContext('2d');
  _chart = new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [] },
    plugins: [_cxPlugin],
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      animation: { duration: 250 },
      plugins: {
        legend: { labels: { color:'#9ca3b8', font:{size:11}, boxWidth:12, padding:14 } },
        tooltip: {
          backgroundColor:'#1a1d27', borderColor:'#2a2d3a', borderWidth:1,
          titleColor:'#e2e5f0', bodyColor:'#9ca3b8',
          callbacks: {
            label: ctx => {
              const v = ctx.parsed.y;
              return _chartMode==='pct'
                ? `  ${ctx.dataset.label}: ${v>=0?'+':''}${v.toFixed(2)}%`
                : `  ${ctx.dataset.label}: ${fmtAum(v)}`;
            }
          }
        }
      },
      scales: {
        x: {
          ticks: { color:'#666c85', font:{size:10}, maxTicksLimit:12, maxRotation:0,
            callback: (val, i, ticks) => {
              const lbl = _chart.data.labels[i] || '';
              return (_period==='1y'||_period==='2y') ? lbl.slice(0,7) : lbl.slice(5);
            }
          },
          grid: { color:'#1e2130' }
        },
        y: {
          ticks: {
            color:'#666c85', font:{size:10},
            callback: v => _chartMode==='abs' ? fmtAum(v) : v.toFixed(1)+'%'
          },
          grid: { color:'#1e2130' }
        }
      }
    }
  });
}

function updateChart() {
  if (!_chart) return;
  const ready = [..._cmpSet.values()].filter(e => e.histData?.length);
  if (!ready.length) { _chart.data.datasets=[]; _chart.update(); return; }

  const allDates = [...new Set(ready.flatMap(e => e.histData.map(d => d.date)))].sort();

  const datasets = [..._cmpSet.entries()]
    .filter(([,e]) => e.histData?.length)
    .map(([ticker, {color, histData}]) => {
      const map = Object.fromEntries(histData.map(d => [d.date, d.aum]));
      const first = histData[0]?.aum || 1;
      const values = allDates.map(d =>
        map[d] != null
          ? (_chartMode==='abs' ? map[d] : (map[d]/first-1)*100)
          : null
      );
      return {
        label: ticker, data: values,
        borderColor: color, backgroundColor: color+'18',
        borderWidth: 2, pointRadius: 0, pointHoverRadius: 4,
        tension: 0.3, fill: false, spanGaps: true,
      };
    });

  _chart.data.labels   = allDates;
  _chart.data.datasets = datasets;

  // 交叉偵測（僅絕對值模式才有意義）
  const cxs = _chartMode === 'abs' ? detectCrossovers(datasets) : [];
  _cxPlugin._cxs = cxs;

  // crossover legend
  const leg = $('cxLegend');
  if (cxs.length && _chartMode === 'abs') {
    leg.style.display = '';
    leg.innerHTML = '<span style="font-size:.7rem;color:var(--muted);margin-right:4px">交叉點：</span>' +
      cxs.map(cx => {
        const [winner, loser] = cx.dir === 'A>B'
          ? [cx.labelA, cx.labelB] : [cx.labelB, cx.labelA];
        const wColor = cx.dir === 'A>B' ? cx.colorA : cx.colorB;
        return `<span class="cx-item">
          <span class="cx-dot" style="background:${wColor}"></span>
          <span style="color:${wColor};font-weight:700">${winner}</span>
          <span>超越</span>
          <span style="color:var(--muted)">${loser}</span>
          <span style="color:var(--muted);font-size:.65rem">(${allDates[cx.idx]?.slice(5)||''})</span>
        </span>`;
      }).join('');
  } else {
    leg.style.display = 'none';
  }

  _chart.update();
}

// ── 歷史數據表 ────────────────────────────────────────────────────────────────
function renderHistTable() {
  const wrap = $('histTableWrap');
  const entries = [..._cmpSet.entries()].filter(([,e]) => e.histData?.length);
  if (!entries.length) { wrap.innerHTML = '<p style="padding:20px;color:var(--muted);text-align:center">尚無資料</p>'; return; }

  // 聯集日期（降序，最新在前）
  const allDates = [...new Set(entries.flatMap(([,e]) => e.histData.map(d => d.date)))].sort().reverse();

  // 每支 ETF 建立 date→row 對映
  const maps = entries.map(([ticker, {histData, color}]) => ({
    ticker, color,
    map: Object.fromEntries(histData.map(d => [d.date, d])),
  }));

  const thCols = maps.map(m =>
    `<th colspan="2" style="color:${m.color};text-align:center">${m.ticker}</th>`
  ).join('');
  const thSub = maps.map(() =>
    `<th>AUM</th><th>日變化</th>`
  ).join('');

  const bodyRows = allDates.map(date => {
    const cells = maps.map(({map}) => {
      const row = map[date];
      if (!row) return '<td>—</td><td>—</td>';
      const chg = row.day_chg;
      const chgCls = chg > 0 ? 'htd-chg-pos' : (chg < 0 ? 'htd-chg-neg' : '');
      const chgStr = chg !== 0 ? `${chg>0?'+':''}${chg.toFixed(2)}%` : '—';
      return `<td class="htd-aum">${fmtAum(row.aum)}</td><td class="${chgCls}">${chgStr}</td>`;
    }).join('');
    return `<tr><td>${date}</td>${cells}</tr>`;
  }).join('');

  wrap.innerHTML = `
    <table>
      <thead>
        <tr><th rowspan="2">日期</th>${thCols}</tr>
        <tr>${thSub}</tr>
      </thead>
      <tbody>${bodyRows}</tbody>
    </table>`;
}

// ── 搜尋自動補全 ──────────────────────────────────────────────────────────────
let _searchTimer = null;
let _dropFocus   = -1;

function initSearch() {
  const inp = $('etfSearch');
  const drop = $('searchDrop');
  inp.addEventListener('input', () => {
    clearTimeout(_searchTimer);
    _searchTimer = setTimeout(() => runSearch(inp.value), 280);
  });
  inp.addEventListener('keydown', e => {
    const items = drop.querySelectorAll('.sdrop-item');
    if (e.key==='ArrowDown') { _dropFocus=Math.min(_dropFocus+1,items.length-1); highlightDrop(items); e.preventDefault(); }
    else if (e.key==='ArrowUp') { _dropFocus=Math.max(_dropFocus-1,-1); highlightDrop(items); e.preventDefault(); }
    else if (e.key==='Enter' && _dropFocus>=0) { items[_dropFocus]?.click(); e.preventDefault(); }
    else if (e.key==='Escape') { closeSearchDrop(); }
  });
  inp.addEventListener('blur', () => setTimeout(closeSearchDrop, 180));
}

async function runSearch(q) {
  q = q.trim();
  const drop = $('searchDrop');
  if (!q) { closeSearchDrop(); return; }
  const res  = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
  const hits = res.ok ? await res.json() : [];
  _dropFocus = -1;
  if (!hits.length) {
    drop.innerHTML = `<div class="sdrop-empty">無符合結果</div>`;
  } else {
    drop.innerHTML = hits.map(h => {
      const inCmp = _cmpSet.has(h.ticker);
      const style = inCmp ? `style="opacity:.5;cursor:default"` : '';
      return `<div class="sdrop-item" ${style} data-ticker="${h.ticker}" data-name="${escHtml(h.name)}"
                   onclick="searchPick('${h.ticker}','${escHtml(h.name)}')">
        <span class="sdrop-ticker">${h.ticker}</span>
        <span class="sdrop-name" title="${escHtml(h.name)}">${escHtml(h.name)||'—'}</span>
        <span class="sdrop-aum">${h.aum?fmtAum(h.aum):''}</span>
        ${inCmp?'<span style="color:var(--accent);font-size:.7rem">已加入</span>':''}
      </div>`;
    }).join('');
  }
  drop.classList.add('open');
}

function highlightDrop(items) {
  items.forEach((el,i) => el.classList.toggle('focused', i===_dropFocus));
}

function closeSearchDrop() {
  $('searchDrop').classList.remove('open');
  _dropFocus = -1;
}

function searchPick(ticker, name) {
  closeSearchDrop();
  $('etfSearch').value = '';
  if (!_cmpSet.has(ticker)) toggleCmp(ticker, name, null);
}

// ── 拖曳調整圖表高度 ─────────────────────────────────────────────────────────
(function initResizer() {
  const resizer = $('resizer');
  let startY, startH;
  resizer.addEventListener('mousedown', e => {
    startY = e.clientY;
    startH = $('chartPanel').offsetHeight || _chartH;
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    e.preventDefault();
  });
  function onMove(e) {
    const delta = startY - e.clientY;
    const newH  = Math.max(160, Math.min(600, startH + delta));
    _chartH = newH;
    const panel = $('chartPanel');
    if (panel.classList.contains('open')) {
      panel.style.height = newH + 'px';
      if (_chart) _chart.resize();
    }
  }
  function onUp() {
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
  }
})();

// ── 匯出 CSV ─────────────────────────────────────────────────────────────────
function exportCSV() {
  if (!_data.length) return alert('請先載入並篩選資料');
  const cols = ['ticker','name','aum','expense_ratio','ytd_return','three_yr_avg','category'];
  const hdrs = ['代號','名稱','規模(USD)','費用率','YTD報酬','3Y均報酬','類別'];
  const rows = _data.map(r => cols.map(c => {
    const v = r[c];
    if (['expense_ratio','ytd_return','three_yr_avg'].includes(c))
      return ((v||0)*100).toFixed(4)+'%';
    return JSON.stringify(v??'');
  }).join(','));
  const csv = '﻿' + [hdrs.join(','), ...rows].join('\n');
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
  a.download = `etf_screener_${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
}

// ── 初始化 ───────────────────────────────────────────────────────────────────
(async () => {
  initSearch();
  const st = await fetch('/api/status').then(r=>r.json());
  if (st.loaded) {
    $('statusDot').className = 'status-dot ok';
    $('statusText').textContent = `已載入 ${st.count} 支`;
    $('btnLoad').textContent = '重新載入';
    await applyFilter();
    loadTierStats();
  }
  ['minAum','maxAum','catKw','maxEr','topN'].forEach(id =>
    $(id).addEventListener('keydown', e => { if(e.key==='Enter') applyFilter(); })
  );
})();
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML

@app.get("/overtake", response_class=HTMLResponse)
async def overtake_page():
    return OVERTAKE_HTML

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8000))
    host = "0.0.0.0" if os.environ.get("RENDER") else "127.0.0.1"
    uvicorn.run("etf_web:app", host=host, port=port, reload=False)
