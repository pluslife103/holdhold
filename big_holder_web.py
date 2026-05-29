"""
big_holder_web.py
=================
千張大戶分析網站

啟動：  python big_holder_web.py
瀏覽：  http://localhost:8001
"""

import io, json, os, sys, time, threading, logging, warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import uvicorn
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from scipy import stats

warnings.filterwarnings("ignore")
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── 設定 ──────────────────────────────────────────────────────────────────
_FM_HOST        = os.getenv("FINMIND_PROXY_HOST", "https://api.finmindtrade.com")
FINMIND_BASE    = f"{_FM_HOST}/api/v4/data"
FINMIND_BROKER_URL = f"{_FM_HOST}/api/v4/taiwan_stock_trading_daily_report"
BIG_BRACKET  = "more than 1,000,001"
TOKEN_PATH   = Path(__file__).parent / "finmind_chip_screener" / ".env"
CACHE_TTL_H  = 12
PORT         = 8001
DEFAULT_YEARS  = 2
MAX_LAG        = 8
GRADE_WORKERS  = 2      # concurrent API requests — keep low to avoid OOM on Railway

# ── 規模分層 & 波動等級 ────────────────────────────────────────────────────
# 市值單位：億 NTD
TIERS = [
    ("mega",  10_000, "超大型", "🏢"),   # > 1 兆
    ("large",  1_000, "大型",   "🏗"),   # 1000億–1兆
    ("mid",      100, "中型",   "🏬"),   # 100億–1000億
    ("small",     20, "小型",   "🏪"),   # 20億–100億
    ("micro",      0, "微型",   "🏠"),   # < 20億
]
TIER_ORDER = {t[0]: i for i, t in enumerate(TIERS)}

# 波動等級（同規模內 std(bpct_chg) 的百分位分布）
GRADES = [
    ("S", 0.90, "極高波動", "#ef4444"),  # top 10%
    ("A", 0.70, "高波動",   "#f97316"),  # 70-90%
    ("B", 0.30, "中波動",   "#eab308"),  # 30-70%
    ("C", 0.10, "低波動",   "#60a5fa"),  # 10-30%
    ("D", 0.00, "極低波動", "#6b7280"),  # bottom 10%
]
GRADE_COLORS = {g[0]: g[3] for g in GRADES}
GRADE_LABELS = {g[0]: g[2] for g in GRADES}

# ── Token ─────────────────────────────────────────────────────────────────
_TOKEN: str = ""

def _load_token() -> str:
    import os
    # 1. 直接讀環境變數（Render / Railway / Docker 部署）
    t = os.getenv("FINMIND_TOKEN", "")
    if t:
        return t
    # 2. 退回讀本地 .env 檔（開發環境）
    try:
        from dotenv import load_dotenv
        load_dotenv(TOKEN_PATH)
        t = os.getenv("FINMIND_TOKEN", "")
    except Exception:
        pass
    if not t:
        raise RuntimeError(
            "請設定 FINMIND_TOKEN 環境變數，或在 "
            f"{TOKEN_PATH} 建立 .env 檔"
        )
    return t


# ── 記憶體快取 ────────────────────────────────────────────────────────────
_CACHE: dict = {}
_CACHE_MAX = 600          # max entries; evict oldest when exceeded
_CLOCK = threading.Lock()


def _cset(key: str, val) -> None:
    with _CLOCK:
        _CACHE[key] = (val, time.time())
        if len(_CACHE) > _CACHE_MAX:
            oldest = min(_CACHE, key=lambda k: _CACHE[k][1])
            del _CACHE[oldest]


def _cget(key: str, ttl_h: float = CACHE_TTL_H):
    with _CLOCK:
        if key not in _CACHE:
            return None
        val, ts = _CACHE[key]
        if time.time() - ts > ttl_h * 3600:
            del _CACHE[key]
            return None
        return val


# ── FinMind API ────────────────────────────────────────────────────────────
def _fm(dataset: str, sid: str = "", start: str = "", end: str = "") -> pd.DataFrame:
    if not end:
        end = datetime.now().strftime("%Y-%m-%d")
    key = f"{dataset}|{sid}|{start}|{end}"
    cached = _cget(key)
    if cached is not None:
        return cached

    params: dict = {"dataset": dataset, "token": _TOKEN}
    if sid:    params["data_id"] = sid
    if start:  params["start_date"] = start
    if end:    params["end_date"] = end

    for attempt in range(3):
        try:
            r = requests.get(FINMIND_BASE, params=params, timeout=60)
            if r.status_code in (400, 403):
                # Permanent error (not subscribed / bad request) — cache empty to skip future calls
                _cset(key, pd.DataFrame())
                return pd.DataFrame()
            r.raise_for_status()
            body = r.json()
            if body.get("status") == 200:
                df = pd.DataFrame(body.get("data", []))
                _cset(key, df)
                return df
            print(f"  FinMind [{dataset}][{sid}]: {body.get('msg','')}")
            return pd.DataFrame()
        except Exception as exc:
            if attempt == 2:
                print(f"  FinMind error [{dataset}][{sid}]: {exc}")
            time.sleep(1.5 ** attempt)
    return pd.DataFrame()


# ── 股票清單 ──────────────────────────────────────────────────────────────
_STOCKS: list[dict] = []
_STOCK_MAP: dict[str, str] = {}      # id -> name
_STOCK_INDUSTRY: dict[str, str] = {} # id -> industry category
_STOCK_MCAP: dict[str, float] = {}   # id -> market_cap 億 NTD (快速分層用)


def _parse_cap_億(v_raw) -> "float | None":
    """將各種單位的市值原始值轉換成億 NTD。
    FinMind 可能用千元；TPEX 用億元；本函式自動偵測量級。"""
    try:
        v = float(str(v_raw).replace(",", "").replace("--", "").replace("N/A", "").strip())
        if v <= 0:
            return None
        # 自動偵測單位：> 1e10 → 千元, > 1e7 → 百萬元, > 1e4 → 億 (but still very large),
        # 合理的 億 值大約在 1~500,000 之間
        if v > 1e10:   # 千元 → 億
            return v / 1e5
        elif v > 1e7:  # 百萬元 → 億
            return v / 100
        elif v > 1e4:  # 可能已是億，但數值很大（> 10000 億），直接用
            return v
        elif v > 0:    # 小於 1e4 億，也直接用
            return v
        return None
    except Exception:
        return None


def _load_stocks() -> None:
    """取得上市+上櫃所有股票清單。三層 fallback：FinMind → TWSE OpenAPI → ISIN scraping"""
    global _STOCKS, _STOCK_MAP

    df = _fm("TaiwanStockInfo")
    if not df.empty:
        df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)].copy()
        if "stock_name" in df.columns and "type" in df.columns:
            cols = ["stock_id", "stock_name", "type"]
            if "industry_category" in df.columns:
                cols.append("industry_category")
            if "market_capitalization" in df.columns:
                cols.append("market_capitalization")
            df = df[cols].drop_duplicates("stock_id")
        else:
            df = pd.DataFrame(columns=["stock_id", "stock_name", "type"])
    print(f"  FinMind TaiwanStockInfo: {len(df)} 筆")

    # 第二層：TWSE/TPEX OpenAPI（JSON，穩定）
    if len(df) < 1000:
        print(f"  ⚠ FinMind 只回傳 {len(df)} 筆，改用 TWSE OpenAPI...")
        df = _load_stocks_from_twse_openapi()

    # 第三層：ISIN 網頁 scraping
    if len(df) < 1000:
        print(f"  ⚠ OpenAPI 只回傳 {len(df)} 筆，改用 ISIN scraping...")
        df = _load_stocks_from_twse()

    if len(df) == 0:
        print("  ✗ 所有股票清單來源均失敗！")
    _STOCKS = df.to_dict(orient="records")
    _STOCK_MAP = dict(zip(df["stock_id"], df["stock_name"]))
    global _STOCK_INDUSTRY, _STOCK_MCAP
    if "industry_category" in df.columns:
        _STOCK_INDUSTRY = {r["stock_id"]: (r.get("industry_category") or "") for r in _STOCKS}
    elif "industry" in df.columns:
        _STOCK_INDUSTRY = {r["stock_id"]: (r.get("industry") or "") for r in _STOCKS}
    # 從 FinMind market_capitalization 或 TPEX market_cap_億 預先建立市值 dict
    mcap: dict[str, float] = {}
    for r in _STOCKS:
        sid = r["stock_id"]
        raw = r.get("market_capitalization") or r.get("market_cap_億")
        if raw:
            cap = _parse_cap_億(raw)
            if cap and cap > 0:
                mcap[sid] = cap
    _STOCK_MCAP = mcap
    print(f"  股票清單：{len(_STOCKS)} 支，產業別已知：{sum(1 for v in _STOCK_INDUSTRY.values() if v)} 支，市值已知：{len(_STOCK_MCAP)} 支")


def _load_stocks_from_twse_openapi() -> pd.DataFrame:
    """第二備用：TWSE/TPEX OpenAPI（JSON，最穩定）"""
    import re
    rows = []
    sources = [
        ("https://openapi.twse.com.tw/v1/opendata/t187ap03_L", "twse"),
        ("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",  "tpex"),
    ]
    for url, market in sources:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            data = r.json()
            if not isinstance(data, list):
                data = data.get("data", [])
            for item in data:
                sid  = str(item.get("公司代號", item.get("SecuritiesCompanyCode", item.get("Code", "")))).strip()
                name = str(item.get("公司簡稱", item.get("CompanyAbbreviation", item.get("Name", "")))).strip()
                if re.match(r"^\d{4}$", sid) and name:
                    industry = str(item.get("產業別", item.get("industry_category", ""))).strip()
                    # TPEX quotes 有 市值(億元) 欄；TWSE t187ap03_L 沒有市值
                    cap_raw = (item.get("市值(億元)") or item.get("市值") or
                               item.get("MarketCapitalization") or item.get("市值(百萬元)") or "")
                    row: dict = {"stock_id": sid, "stock_name": name, "type": market, "industry": industry}
                    if cap_raw:
                        row["market_cap_億"] = cap_raw
                    rows.append(row)
            print(f"  OpenAPI {market}: {len(rows)} 筆")
        except Exception as e:
            print(f"  OpenAPI {market} 失敗：{e}")
    if rows:
        return pd.DataFrame(rows).drop_duplicates("stock_id")
    return pd.DataFrame(columns=["stock_id", "stock_name", "type"])


def _load_stocks_from_twse() -> pd.DataFrame:
    """備用：直接從 TWSE/TPEX ISIN API 取全股清單（公開，免 token）"""
    import re
    rows = []
    for mode, market in [("2", "twse"), ("4", "tpex")]:
        try:
            r = requests.get(
                "https://isin.twse.com.tw/isin/C_public.jsp",
                params={"strMode": mode},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=30,
            )
            r.encoding = "big5"
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")
            for tr in soup.select("tr"):
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue
                cell = tds[0].get_text(strip=True)
                m = re.match(r"^(\d{4,5})\s+(.+)", cell)
                if m and len(m.group(1)) == 4:
                    rows.append({"stock_id": m.group(1),
                                 "stock_name": m.group(2).strip(),
                                 "type": market})
        except Exception as e:
            print(f"  TWSE/TPEX ISIN {market} 失敗：{e}")

    if rows:
        df = pd.DataFrame(rows).drop_duplicates("stock_id")
        return df
    return pd.DataFrame(columns=["stock_id", "stock_name", "type"])


# ── 分級計算 ──────────────────────────────────────────────────────────────
_GRADING: dict[str, dict] = {}   # stock_id -> graded dict
_GRADE_PROG = {"done": 0, "total": 0, "running": False, "error": ""}


def _tier_of(cap_億: float) -> str:
    for name, min_cap, *_ in TIERS:
        if cap_億 >= min_cap:
            return name
    return "micro"


def _assign_grades_inplace(rows: list[dict]) -> None:
    """Assign grade S/A/B/C/D within each tier based on volatility percentile."""
    from collections import defaultdict
    tier_vols: dict[str, list] = defaultdict(list)
    for r in rows:
        tier_vols[r["tier"]].append(r["volatility"])

    tier_cuts: dict[str, dict] = {}
    for tier, vols in tier_vols.items():
        sv = sorted(vols)
        n  = len(sv)
        tier_cuts[tier] = {g: sv[min(n - 1, max(0, int(n * p) - 1))]
                           for g, p, *_ in GRADES}

    for r in rows:
        cuts = tier_cuts[r["tier"]]
        v    = r["volatility"]
        r["grade"] = next(
            (g for g, p, *_ in GRADES if v >= cuts[g]),
            "D"
        )


def _spark_arr(series: "pd.Series", n: int = 12) -> list:
    """取最近 n 筆、清除 NaN、四捨五入，回傳 list（給前端畫 sparkline）"""
    vals = series.dropna().tail(n).round(2).tolist()
    return vals if len(vals) >= 2 else []


def _fetch_one_grading(sid: str) -> dict | None:
    """Fetch 1-year holding + 3-month price for one stock; return grading metrics + sparklines."""
    start_1y = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    start_3m = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    # ── 持股分佈 ────────────────────────────────────────────────────────────
    hdf = _fm("TaiwanStockHoldingSharesPer", sid, start_1y)
    if hdf.empty:
        return None
    hdf["date"] = pd.to_datetime(hdf["date"])

    # 總股數（total bracket）
    tot = hdf[hdf["HoldingSharesLevel"] == "total"].sort_values("date")
    if tot.empty:
        return None
    total_shares = pd.to_numeric(tot["unit"].iloc[-1], errors="coerce")
    if pd.isna(total_shares) or total_shares <= 0:
        return None

    # 千張大戶波動度 + sparkline
    big = hdf[hdf["HoldingSharesLevel"] == BIG_BRACKET].copy()
    if len(big) < 4:
        return None
    big["pct"]     = pd.to_numeric(big["percent"], errors="coerce")
    big["people"]  = pd.to_numeric(big["people"],  errors="coerce")
    big            = big.sort_values("date")
    big["chg"]     = big["pct"].diff()
    volatility     = float(big["chg"].dropna().std())
    latest_bpct    = float(big["pct"].iloc[-1])
    latest_bp      = int(big["people"].iloc[-1])
    bpct_spark     = _spark_arr(big["pct"], 12)   # 近 12 週千張大戶% 走勢

    # ── 股價（3 個月，用於 sparkline + 市值）────────────────────────────────
    pdf = _fm("TaiwanStockPrice", sid, start_3m)
    if pdf.empty:
        return None
    pdf["date"]  = pd.to_datetime(pdf["date"])
    pdf["close"] = pd.to_numeric(pdf["close"], errors="coerce")
    pdf          = pdf.sort_values("date")
    # resample 成週收盤
    pw = pdf.set_index("date")["close"].resample("W-FRI").last().dropna()
    if pw.empty:
        return None
    latest_close = float(pw.iloc[-1])
    if latest_close <= 0:
        return None
    price_spark = _spark_arr(pw, 12)              # 近 12 週股價走勢

    market_cap_億 = float(latest_close * total_shares / 1e8)

    return {
        "stock_id":      sid,
        "stock_name":    _STOCK_MAP.get(sid, sid),
        "market_cap_億": round(market_cap_億, 1),
        "tier":          _tier_of(market_cap_億),
        "volatility":    round(volatility, 4),
        "latest_bpct":   round(latest_bpct, 2),
        "bpct_chg":      round(float(big["chg"].iloc[-1]) if not pd.isna(big["chg"].iloc[-1]) else 0, 3),
        "latest_bp":     latest_bp,
        "latest_price":  round(latest_close, 2),
        "n_weeks":       len(big),
        "industry":      _STOCK_INDUSTRY.get(sid, ""),
        "bpct_spark":    bpct_spark,
        "price_spark":   price_spark,
    }


def _run_grading() -> None:
    """Background thread: batch-fetch all stocks and compute tier+grade."""
    global _GRADING, _GRADE_PROG

    # Wait for stock list
    for _ in range(30):
        if _STOCKS:
            break
        time.sleep(1)
    if not _STOCKS:
        _GRADE_PROG["error"] = "股票清單未載入"
        return

    sids = [s["stock_id"] for s in _STOCKS]
    _GRADE_PROG.update({"done": 0, "total": len(sids), "running": True, "error": ""})
    print(f"  開始分級計算：{len(sids)} 支股票，{GRADE_WORKERS} 並發…")

    raw: list[dict] = []
    BATCH = 50   # 每累積 50 支就重算 grade 並更新 _GRADING
    with ThreadPoolExecutor(max_workers=GRADE_WORKERS) as ex:
        futs = {ex.submit(_fetch_one_grading, sid): sid for sid in sids}
        for fut in as_completed(futs):
            result = fut.result()
            if result:
                raw.append(result)
            with _CLOCK:
                _GRADE_PROG["done"] += 1
            # 批次更新：讓前端 grade badge 逐漸填入
            if len(raw) % BATCH == 0 and raw:
                snapshot = list(raw)
                _assign_grades_inplace(snapshot)
                with _CLOCK:
                    for r in snapshot:
                        _GRADING[r["stock_id"]] = r

    # 最終完整計算
    _assign_grades_inplace(raw)
    with _CLOCK:
        for r in raw:
            _GRADING[r["stock_id"]] = r   # 更新，不取代（保留預分層但無詳細資料的股票）

    _GRADE_PROG["running"] = False
    print(f"  分級完成：{len(raw)}/{len(sids)} 支股票有詳細資料，_GRADING 共 {len(_GRADING)} 支")
    import gc; gc.collect()  # release DataFrame memory held by grading workers


# ── 資料處理 ──────────────────────────────────────────────────────────────
def _process_stock(sid: str, years: int) -> dict:
    start = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")

    # 千張大戶
    hdf = _fm("TaiwanStockHoldingSharesPer", sid, start)
    if hdf.empty:
        return {"error": "無持股資料"}
    hdf["date"] = pd.to_datetime(hdf["date"])
    big = hdf[hdf["HoldingSharesLevel"] == BIG_BRACKET].copy()
    if big.empty:
        return {"error": "無千張大戶資料"}
    for c in ("people", "percent", "unit"):
        big[c] = pd.to_numeric(big[c], errors="coerce")
    big = (big.rename(columns={"people": "bp", "percent": "bpct", "unit": "bu"})
           [["date", "bp", "bpct", "bu"]].sort_values("date").reset_index(drop=True))
    big["bpct_chg"] = big["bpct"].diff().round(3)
    big["bp_chg"]   = big["bp"].diff()

    # 股價（週）
    pdf = _fm("TaiwanStockPrice", sid, start)
    if pdf.empty:
        return {"error": "無股價資料"}
    pdf["date"]  = pd.to_datetime(pdf["date"])
    pdf["close"] = pd.to_numeric(pdf["close"], errors="coerce")
    pw = (pdf.sort_values("date").set_index("date")["close"]
          .resample("W-FRI").last().dropna().reset_index())
    pw.columns = ["date", "close"]
    pw["ret"] = pw["close"].pct_change().round(5)

    # 合併
    df = pd.merge_asof(big.sort_values("date"), pw.sort_values("date"),
                       on="date", tolerance=pd.Timedelta("7d"), direction="nearest")
    df = df.dropna(subset=["close", "bpct"])
    if len(df) < 4:
        return {"error": "資料筆數不足"}

    # 未來報酬
    for lag in range(1, MAX_LAG + 1):
        df[f"f{lag}"] = df["ret"].shift(-lag)

    # Lead-Lag
    lags = []
    for lag in range(-MAX_LAG, MAX_LAG + 1):
        if lag < 0:
            x, y, lbl = df["bpct_chg"], df["ret"].shift(lag), f"股價先行{abs(lag)}週"
        elif lag == 0:
            x, y, lbl = df["bpct_chg"], df["ret"], "同期"
        else:
            col = f"f{lag}"
            x, y, lbl = df["bpct_chg"], df.get(col, pd.Series(dtype=float)), f"大戶先行{lag}週"
        valid = pd.concat([x, y], axis=1).dropna()
        if len(valid) < 8:
            continue
        r, p = stats.pearsonr(valid.iloc[:, 0], valid.iloc[:, 1])
        lags.append({"lag": lag, "label": lbl, "r": round(r, 4), "p": round(p, 4),
                     "sig": bool(p < 0.05), "n": int(len(valid))})

    # History
    history = []
    for _, row in df.iterrows():
        def _f(v): return None if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)
        def _i(v): return None if (v is None or (isinstance(v, float) and np.isnan(v))) else int(v)
        history.append({"date": row["date"].strftime("%Y-%m-%d"),
                        "bpct": _f(row["bpct"]),      "bpct_chg": _f(row["bpct_chg"]),
                        "bp":   _i(row["bp"]),         "bp_chg":   _i(row["bp_chg"]),
                        "close": _f(row["close"]),     "ret": _f(row["ret"])})

    latest = df.iloc[-1]
    def _safe(v, fn=float, dec=2):
        try: return round(fn(v), dec) if dec else fn(v)
        except: return None

    return {
        "stock_id":   sid,
        "stock_name": _STOCK_MAP.get(sid, sid),
        "weeks":      int(len(df)),
        "latest": {
            "date":        latest["date"].strftime("%Y-%m-%d"),
            "bpct":        _safe(latest["bpct"]),
            "bpct_chg":   _safe(latest["bpct_chg"], float, 3),
            "bp":          _safe(latest["bp"], int, 0),
            "bp_chg":     _safe(latest["bp_chg"], int, 0),
            "close":       _safe(latest["close"]),
        },
        "history":      history,
        "lag_analysis": lags,
    }


def _process_compare(sids: list[str], years: int) -> dict:
    start = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    result = []
    for sid in sids:
        hdf = _fm("TaiwanStockHoldingSharesPer", sid, start)
        if hdf.empty:
            continue
        hdf["date"] = pd.to_datetime(hdf["date"])
        big = hdf[hdf["HoldingSharesLevel"] == BIG_BRACKET].copy()
        if big.empty:
            continue
        big["percent"] = pd.to_numeric(big["percent"], errors="coerce")
        big["people"]  = pd.to_numeric(big["people"],  errors="coerce")
        big = (big[["date", "percent", "people"]].sort_values("date").reset_index(drop=True))
        big["pct_chg"] = big["percent"].diff().round(3)

        pdf = _fm("TaiwanStockPrice", sid, start)
        if pdf.empty:
            continue
        pdf["date"]  = pd.to_datetime(pdf["date"])
        pdf["close"] = pd.to_numeric(pdf["close"], errors="coerce")
        pw = (pdf.sort_values("date").set_index("date")["close"]
              .resample("W-FRI").last().dropna().reset_index())
        pw.columns = ["date", "close"]

        df = pd.merge_asof(big.sort_values("date"), pw.sort_values("date"),
                           on="date", tolerance=pd.Timedelta("7d"), direction="nearest")
        df = df.dropna(subset=["close", "percent"])
        if df.empty:
            continue

        first_close = df["close"].iloc[0]
        df["norm_price"] = (df["close"] / first_close * 100).round(2)

        rows = []
        for _, row in df.iterrows():
            def _f(v):
                return None if (v is None or (isinstance(v, float) and np.isnan(v))) else round(float(v), 3)
            def _i(v):
                return None if (v is None or (isinstance(v, float) and np.isnan(v))) else int(v)
            rows.append({"date": row["date"].strftime("%Y-%m-%d"),
                         "bpct": _f(row["percent"]), "pct_chg": _f(row["pct_chg"]),
                         "bp":   _i(row["people"]),  "close": _f(row["close"]),
                         "norm": _f(row["norm_price"])})

        latest = df.iloc[-1]
        result.append({
            "stock_id":   sid,
            "stock_name": _STOCK_MAP.get(sid, sid),
            "rows":       rows,
            "latest_bpct":  round(float(latest["percent"]), 2),
            "latest_close": round(float(latest["close"]), 2),
        })
    return {"stocks": result}


def _fill_mcap_from_twse() -> None:
    """Fallback: populate _STOCK_MCAP from TWSE (已發行股數×收盤價) + TPEX (市值(億元))."""
    import re as _re
    global _STOCK_MCAP
    mcap: dict[str, float] = {}

    # TPEX OTC: 市值(億元) is returned directly
    try:
        r = requests.get(
            "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=20,
        )
        for item in r.json():
            sid = str(item.get("SecuritiesCompanyCode", "")).strip()
            if not _re.match(r"^\d{4}$", sid):
                continue
            cap_raw = str(item.get("市值(億元)", "")).replace(",", "")
            try:
                cap = float(cap_raw)
                if cap > 0:
                    mcap[sid] = cap
            except Exception:
                pass
        print(f"  TPEX 市值：{len(mcap)} 支")
    except Exception as e:
        print(f"  TPEX 市值失敗：{e}")

    # TWSE listed: issued_shares × close_price / 1e8
    try:
        # Step 1: issued shares from t187ap03_L
        r1 = requests.get(
            "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=20,
        )
        shares: dict[str, float] = {}
        for item in r1.json():
            sid = str(item.get("公司代號", "")).strip()
            if not _re.match(r"^\d{4}$", sid):
                continue
            raw = str(
                item.get("已發行普通股數或TDR原股發行股數") or
                item.get("已發行普通股數") or ""
            ).replace(",", "")
            try:
                s = float(raw)
                if s > 0:
                    shares[sid] = s
            except Exception:
                pass
        print(f"  TWSE 已發行股數：{len(shares)} 支")

        # Step 2: closing prices from STOCK_DAY_ALL
        r2 = requests.get(
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL",
            params={"response": "json"}, timeout=20,
        )
        body = r2.json()
        fields = body.get("fields", [])
        code_idx  = next((i for i, f in enumerate(fields) if "代號" in f), 0)
        price_idx = next((i for i, f in enumerate(fields) if "收盤價" in f), -1)
        cnt = 0
        if price_idx >= 0:
            for row in body.get("data", []):
                sid = str(row[code_idx]).strip()
                if sid not in shares:
                    continue
                try:
                    price = float(str(row[price_idx]).replace(",", ""))
                    if price > 0:
                        mcap[sid] = round(shares[sid] * price / 1e8, 1)
                        cnt += 1
                except Exception:
                    pass
        print(f"  TWSE 市值計算：{cnt} 支")
    except Exception as e:
        print(f"  TWSE 市值失敗：{e}")

    if mcap:
        with _CLOCK:
            _STOCK_MCAP.update(mcap)
        print(f"  市值資料合計：{len(_STOCK_MCAP)} 支")
    else:
        print("  ⚠ TWSE/TPEX 市值補充均失敗")


def _init_tier_grading() -> None:
    """從 _STOCK_MCAP 預先將所有股票分層，填入 _GRADING 讓 tier 按鈕立即可用。
    grade 欄位標記為 '?' placeholder，等 _run_grading() 完成後再覆蓋。"""
    global _GRADING
    if not _STOCK_MCAP:
        print("  _STOCK_MCAP 空，嘗試從 TWSE/TPEX 補充市值…")
        _fill_mcap_from_twse()
    if not _STOCK_MCAP:
        print("  ⚠ _STOCK_MCAP 仍空，跳過快速分層")
        return
    pre: dict[str, dict] = {}
    for s in _STOCKS:
        sid = s["stock_id"]
        cap_億 = _STOCK_MCAP.get(sid)
        if cap_億 is None:
            continue
        pre[sid] = {
            "stock_id":      sid,
            "stock_name":    _STOCK_MAP.get(sid, sid),
            "market_cap_億": round(cap_億, 1),
            "tier":          _tier_of(cap_億),
            "industry":      _STOCK_INDUSTRY.get(sid, ""),
            "volatility":    0.0,
            "grade":         "?",
            "latest_bpct":   0.0,
            "bpct_chg":      0.0,
            "latest_bp":     0,
            "latest_price":  0.0,
            "n_weeks":       0,
            "bpct_spark":    [],
            "price_spark":   [],
        }
    if pre:
        with _CLOCK:
            if not _GRADING:
                _GRADING.update(pre)
        tier_counts = {}
        for v in pre.values():
            t = v["tier"]
            tier_counts[t] = tier_counts.get(t, 0) + 1
        print(f"  快速市值分層：{len(pre)} 支，分布：{tier_counts}")


# ── Lifespan ──────────────────────────────────────────────────────────────
def _startup_tasks():
    """Run sequentially in a background thread: load stocks then start grading."""
    _load_stocks()
    _init_tier_grading()   # 立即從市值預分層，讓 tier 按鈕可用
    _run_grading()         # 背景逐支抓詳細資料（更新 grade / sparkline）


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _TOKEN
    _TOKEN = _load_token()
    threading.Thread(target=_startup_tasks, daemon=True).start()
    yield


app = FastAPI(title="千張大戶分析", lifespan=_lifespan)


# ── API ────────────────────────────────────────────────────────────────────
@app.get("/api/stocks")
def api_stocks(q: str = Query("", description="搜尋關鍵字")):
    stocks = _STOCKS
    if q:
        q = q.upper()
        stocks = [s for s in stocks if q in s["stock_id"] or q in s["stock_name"]]
    return {"stocks": stocks, "total": len(stocks)}


@app.get("/api/stock/{stock_id}")
def api_stock(stock_id: str, years: int = Query(DEFAULT_YEARS)):
    ckey = f"stock|{stock_id}|{years}"
    cached = _cget(ckey, CACHE_TTL_H)
    if cached is not None:
        return cached
    data = _process_stock(stock_id, years)
    if "error" not in data:
        _cset(ckey, data)
    return data


@app.post("/api/compare")
async def api_compare(request_body: dict):
    sids  = request_body.get("stocks", [])[:8]
    years = int(request_body.get("years", DEFAULT_YEARS))
    if not sids:
        raise HTTPException(400, "stocks 不可為空")
    ckey = f"compare|{'_'.join(sorted(sids))}|{years}"
    cached = _cget(ckey, CACHE_TTL_H)
    if cached is not None:
        return cached
    data = _process_compare(sids, years)
    _cset(ckey, data)
    return data


@app.get("/api/grading/status")
def api_grading_status():
    with _CLOCK:
        prog = dict(_GRADE_PROG)
        ready = len(_GRADING)
    return {**prog, "ready": ready,
            "pct": round(prog["done"] / prog["total"] * 100, 1) if prog.get("total") else 0}


@app.get("/api/grading")
def api_grading(
    tier:  str = Query("", description="mega|large|mid|small|micro"),
    grade: str = Query("", description="S|A|B|C|D"),
    q:     str = Query("", description="Search code/name"),
):
    with _CLOCK:
        stocks = list(_GRADING.values())

    if tier:
        stocks = [s for s in stocks if s.get("tier") == tier]
    if grade:
        stocks = [s for s in stocks if s.get("grade") == grade]
    if q:
        qu = q.upper()
        stocks = [s for s in stocks
                  if qu in s["stock_id"] or qu in (s.get("stock_name") or "")]

    stocks.sort(key=lambda s: (
        TIER_ORDER.get(s.get("tier", "micro"), 99),
        -s.get("volatility", 0)
    ))

    # Group by tier
    grouped: dict[str, list] = {}
    for s in stocks:
        t = s.get("tier", "micro")
        grouped.setdefault(t, []).append(s)

    # Tier metadata
    tier_meta = []
    for name, min_cap, label, icon in TIERS:
        items = grouped.get(name, [])
        tier_meta.append({
            "key": name, "label": label, "icon": icon,
            "count": len(items),
            "stocks": items,
        })

    return {
        "tiers":  [t for t in tier_meta if t["count"] > 0],
        "total":  len(stocks),
        "ready":  len(_GRADING),
        "grades": [{"key": g, "label": GRADE_LABELS[g], "color": GRADE_COLORS[g]}
                   for g, *_ in GRADES],
    }


@app.post("/api/grading/refresh")
def api_grading_refresh():
    if _GRADE_PROG.get("running"):
        return {"ok": False, "message": "計算中，請稍候"}
    threading.Thread(target=_run_grading, daemon=True).start()
    return {"ok": True, "message": "已開始重新計算"}


@app.get("/api/broker")
def api_broker(
    stock_id: str = Query(""),
    date:     str = Query(""),
    force:    int = Query(0),
):
    import os
    from datetime import date as dt_date
    stock_id = stock_id.strip().upper()
    if not stock_id:
        raise HTTPException(400, "請輸入股票代號")
    if not date:
        date = dt_date.today().isoformat()
    ckey = f"broker|{stock_id}|{date}"
    if not force:
        cached = _cget(ckey, ttl_h=6)
        if cached is not None:
            return cached

    token = _TOKEN or os.getenv("FINMIND_TOKEN", "")
    if not token:
        raise HTTPException(500, "未設定 FINMIND_TOKEN")

    try:
        r = requests.get(
            FINMIND_BASE,
            params={
                "dataset":    "TaiwanStockTradingDailyReport",
                "data_id":    stock_id,
                "start_date": date,
                "end_date":   date,
                "token":      token,
            },
            timeout=25,
        )
    except requests.exceptions.RequestException as e:
        raise HTTPException(502, f"無法連線 FinMind API: {type(e).__name__}")

    # 先嘗試解析 JSON，以取得 FinMind 的錯誤訊息（不暴露 token URL）
    try:
        j = r.json()
    except Exception:
        raise HTTPException(502, f"FinMind 回應無法解析 (HTTP {r.status_code}): {r.text[:200]}")

    if r.status_code != 200 or j.get("status") not in (200, None):
        msg = j.get("msg") or j.get("message") or f"HTTP {r.status_code}"
        # 回傳完整 JSON 供除錯（不含 token）
        raise HTTPException(502, f"FinMind error: {msg} | raw: {json.dumps(j, ensure_ascii=False)[:300]}")

    rows_raw = j.get("data", [])
    if not rows_raw:
        result = {"stock_id": stock_id, "date": date, "rows": [], "summary": {}}
        _cset(ckey, result)
        return result

    INST_THRESHOLD = 8_000_000  # 買進+賣出金額合計 > 800萬 → 主力

    # 序號連號且分點名稱相同 → 合併為同一筆
    processed = []
    i = 0
    while i < len(rows_raw):
        name = rows_raw[i].get("securities_trader", "")
        bid  = rows_raw[i].get("securities_trader_id", "")
        g_buy_s = g_sell_s = g_buy_amt = g_sell_amt = 0.0
        j = i
        while j < len(rows_raw) and rows_raw[j].get("securities_trader", "") == name:
            r  = rows_raw[j]
            bs = float(r.get("buy",   0) or 0)
            ss = float(r.get("sell",  0) or 0)
            px = float(r.get("price", 0) or 0)
            g_buy_s    += bs
            g_sell_s   += ss
            g_buy_amt  += bs * px
            g_sell_amt += ss * px
            j += 1
        buy_lots  = g_buy_s  / 1000
        sell_lots = g_sell_s / 1000
        avg_buy_px  = round(g_buy_amt  / g_buy_s,  2) if g_buy_s  else 0.0
        avg_sell_px = round(g_sell_amt / g_sell_s, 2) if g_sell_s else 0.0
        total_amt   = g_buy_amt + g_sell_amt
        processed.append({
            "broker_id":   bid,
            "broker_name": name,
            "buy":         int(buy_lots),
            "sell":        int(sell_lots),
            "net":         int(buy_lots - sell_lots),
            "buy_price":   avg_buy_px,
            "sell_price":  avg_sell_px,
            "buy_amount":  round(g_buy_amt),
            "sell_amount": round(g_sell_amt),
            "is_retail":   g_buy_amt <= INST_THRESHOLD and g_sell_amt <= INST_THRESHOLD,
        })
        i = j

    processed.sort(key=lambda x: x["net"], reverse=True)
    retail = [r for r in processed if r["is_retail"]]
    inst   = [r for r in processed if not r["is_retail"]]
    result = {
        "stock_id":   stock_id,
        "stock_name": rows_raw[0].get("stock_name", stock_id),
        "date":       date,
        "rows":       processed,
        "summary": {
            "total":        len(processed),
            "retail_count": len(retail),
            "inst_count":   len(inst),
            "retail_buy":   sum(r["buy"]  for r in retail),
            "retail_sell":  sum(r["sell"] for r in retail),
            "retail_net":   sum(r["net"]  for r in retail),
            "inst_buy":     sum(r["buy"]  for r in inst),
            "inst_sell":    sum(r["sell"] for r in inst),
            "inst_net":     sum(r["net"]  for r in inst),
        },
    }
    _cset(ckey, result)
    return result


@app.get("/api/broker_trader")
def api_broker_trader(
    trader_id: str = Query(""),
    date:      str = Query(""),
    force:     int = Query(0),
):
    import os
    from datetime import date as dt_date
    trader_id = trader_id.strip()
    if not trader_id:
        raise HTTPException(400, "請輸入券商代碼")
    if not date:
        date = dt_date.today().isoformat()
    ckey = f"broker_trader|{trader_id}|{date}"
    if not force:
        cached = _cget(ckey, ttl_h=6)
        if cached is not None:
            return cached
    token = _TOKEN or os.getenv("FINMIND_TOKEN", "")
    if not token:
        raise HTTPException(500, "未設定 FINMIND_TOKEN")
    try:
        r = requests.get(
            FINMIND_BROKER_URL,
            headers={"Authorization": f"Bearer {token}"},
            params={
                "securities_trader_id": trader_id,
                "date":                 date,
            },
            timeout=25,
        )
    except requests.exceptions.RequestException as e:
        raise HTTPException(502, f"無法連線 FinMind API: {type(e).__name__}")
    try:
        j = r.json()
    except Exception:
        raise HTTPException(502, f"FinMind 回應無法解析 (HTTP {r.status_code}): {r.text[:200]}")
    if r.status_code != 200 or j.get("status") not in (200, None):
        msg = j.get("msg") or j.get("message") or f"HTTP {r.status_code}"
        raise HTTPException(502, f"FinMind error: {msg} | raw: {json.dumps(j, ensure_ascii=False)[:300]}")
    rows_raw = j.get("data", [])
    if not rows_raw:
        result = {"trader_id": trader_id, "date": date, "rows": [], "summary": {}}
        _cset(ckey, result)
        return result

    INST_THRESHOLD = 8_000_000
    processed = []
    i = 0
    while i < len(rows_raw):
        sid  = rows_raw[i].get("stock_id", "")
        g_buy_s = g_sell_s = g_buy_amt = g_sell_amt = 0.0
        j2 = i
        while j2 < len(rows_raw) and rows_raw[j2].get("stock_id", "") == sid:
            row = rows_raw[j2]
            bs  = float(row.get("buy",   0) or 0)
            ss  = float(row.get("sell",  0) or 0)
            px  = float(row.get("price", 0) or 0)
            g_buy_s    += bs
            g_sell_s   += ss
            g_buy_amt  += bs * px
            g_sell_amt += ss * px
            j2 += 1
        buy_lots    = g_buy_s  / 1000
        sell_lots   = g_sell_s / 1000
        avg_buy_px  = round(g_buy_amt  / g_buy_s,  2) if g_buy_s  else 0.0
        avg_sell_px = round(g_sell_amt / g_sell_s, 2) if g_sell_s else 0.0
        processed.append({
            "stock_id":    sid,
            "buy":         int(buy_lots),
            "sell":        int(sell_lots),
            "net":         int(buy_lots - sell_lots),
            "buy_price":   avg_buy_px,
            "sell_price":  avg_sell_px,
            "buy_amount":  round(g_buy_amt),
            "sell_amount": round(g_sell_amt),
            "is_retail":   g_buy_amt <= INST_THRESHOLD and g_sell_amt <= INST_THRESHOLD,
        })
        i = j2

    processed.sort(key=lambda x: x["net"], reverse=True)
    retail = [r for r in processed if r["is_retail"]]
    inst   = [r for r in processed if not r["is_retail"]]
    result = {
        "trader_id":   trader_id,
        "trader_name": rows_raw[0].get("securities_trader", trader_id),
        "date":        date,
        "rows":        processed,
        "summary": {
            "total":        len(processed),
            "retail_count": len(retail),
            "inst_count":   len(inst),
            "retail_buy":   sum(r["buy"]  for r in retail),
            "retail_sell":  sum(r["sell"] for r in retail),
            "retail_net":   sum(r["net"]  for r in retail),
            "inst_buy":     sum(r["buy"]  for r in inst),
            "inst_sell":    sum(r["sell"] for r in inst),
            "inst_net":     sum(r["net"]  for r in inst),
        },
    }
    _cset(ckey, result)
    return result


class _FinMindRateLimit(Exception):
    pass


def _fetch_broker_day(token: str, stock_id: str, date: str):
    """Fetch+process one stock's broker data for one date. Cached. Returns rows list or []."""
    ckey = f"broker|{stock_id}|{date}"
    cached = _cget(ckey, ttl_h=6)
    if cached is not None:
        return cached.get("rows", [])
    try:
        r = requests.get(
            FINMIND_BASE,
            params={"dataset": "TaiwanStockTradingDailyReport", "data_id": stock_id,
                    "start_date": date, "end_date": date, "token": token},
            timeout=20,
        )
        j = r.json()
    except Exception:
        return []
    fm_status = j.get("status")
    if r.status_code == 402 or fm_status == 402:
        raise _FinMindRateLimit(j.get("msg", "FinMind 402: 超過請求頻率限制"))
    if r.status_code != 200 or fm_status not in (200, None):
        return []
    rows_raw = j.get("data", [])
    if not rows_raw:
        _cset(ckey, {"stock_id": stock_id, "date": date, "rows": [], "summary": {}})
        return []
    THOLD = 8_000_000
    processed, i = [], 0
    while i < len(rows_raw):
        name = rows_raw[i].get("securities_trader", "")
        bid  = rows_raw[i].get("securities_trader_id", "")
        g_buy_s = g_sell_s = g_buy_amt = g_sell_amt = 0.0
        j2 = i
        while j2 < len(rows_raw) and rows_raw[j2].get("securities_trader", "") == name:
            row = rows_raw[j2]
            bs  = float(row.get("buy",   0) or 0)
            ss  = float(row.get("sell",  0) or 0)
            px  = float(row.get("price", 0) or 0)
            g_buy_s += bs; g_sell_s += ss
            g_buy_amt += bs * px; g_sell_amt += ss * px
            j2 += 1
        bl = g_buy_s / 1000; sl = g_sell_s / 1000
        processed.append({
            "broker_id":   bid,
            "broker_name": name,
            "buy":         int(bl),
            "sell":        int(sl),
            "net":         int(bl - sl),
            "buy_price":   round(g_buy_amt  / g_buy_s,  2) if g_buy_s  else 0.0,
            "sell_price":  round(g_sell_amt / g_sell_s, 2) if g_sell_s else 0.0,
            "buy_amount":  round(g_buy_amt),
            "sell_amount": round(g_sell_amt),
            "is_retail":   g_buy_amt <= THOLD and g_sell_amt <= THOLD,
        })
        i = j2
    retail = [r for r in processed if r["is_retail"]]
    inst   = [r for r in processed if not r["is_retail"]]
    _cset(ckey, {
        "stock_id": stock_id, "date": date, "rows": processed,
        "summary": {
            "total": len(processed),
            "retail_count": len(retail), "inst_count": len(inst),
            "retail_buy":  sum(r["buy"]  for r in retail),
            "retail_sell": sum(r["sell"] for r in retail),
            "retail_net":  sum(r["net"]  for r in retail),
            "inst_buy":    sum(r["buy"]  for r in inst),
            "inst_sell":   sum(r["sell"] for r in inst),
            "inst_net":    sum(r["net"]  for r in inst),
        },
    })
    return processed


# ── 大戶總覽後台掃描 ──────────────────────────────────────────────────────
_OV_SCAN: dict = {
    "running": False, "done": 0, "total": 0,
    "results": {}, "error": "", "days": 15, "started": "", "tier": "",
    "industry": "", "skip": 0, "last_err": "",
}
_OV_SCAN_LOCK = threading.Lock()


def _fetch_broker_day(token: str, stock_id: str, date_str: str) -> list:
    """Fetch + aggregate broker data for ONE trading day.
    Returns list of processed broker rows; result cached 12h per day."""
    from collections import defaultdict
    ckey = f"broker_day|{stock_id}|{date_str}"
    cached = _cget(ckey, ttl_h=12)
    if cached is not None:
        return cached
    try:
        r = requests.get(
            FINMIND_BASE,
            # Use start_date=end_date to pin to exactly one day (same as 分點籌碼 tab)
            params={"dataset": "TaiwanStockTradingDailyReport", "data_id": stock_id,
                    "start_date": date_str, "end_date": date_str, "token": token},
            timeout=20,
        )
        j = r.json()
    except Exception as exc:
        print(f"  [broker_day] {stock_id} {date_str}: exception {exc}")
        return []
    fm_status = j.get("status")
    rows_all = j.get("data", [])
    print(f"  [broker_day] {stock_id} {date_str}: http={r.status_code} fm={fm_status} rows={len(rows_all)}")
    if r.status_code == 402 or fm_status == 402:
        raise _FinMindRateLimit(j.get("msg", "FinMind 402"))
    if r.status_code != 200 or fm_status not in (200, None):
        print(f"  [broker_day] {stock_id} {date_str}: error msg={j.get('msg','')}")
        return []
    rows_raw = rows_all  # end_date=date_str guarantees all rows are for this date
    if not rows_raw:
        _cset(ckey, [])
        return []
    THOLD = 8_000_000
    agg: dict = defaultdict(
        lambda: {"name": "", "buy_s": 0.0, "sell_s": 0.0, "buy_amt": 0.0, "sell_amt": 0.0}
    )
    for row in rows_raw:
        bid   = row.get("securities_trader_id", "")
        buy_s = float(row.get("buy",   0) or 0)
        sel_s = float(row.get("sell",  0) or 0)
        px    = float(row.get("price", 0) or 0)
        agg[bid]["name"]     = row.get("securities_trader", "")
        agg[bid]["buy_s"]   += buy_s
        agg[bid]["sell_s"]  += sel_s
        agg[bid]["buy_amt"] += buy_s * px
        agg[bid]["sell_amt"]+= sel_s * px
    processed = []
    for bid, v in agg.items():
        is_retail = v["buy_amt"] <= THOLD and v["sell_amt"] <= THOLD
        processed.append({
            "name": v["name"], "id": bid,
            "buy":  round(v["buy_s"] / 1000),
            "sell": round(v["sell_s"] / 1000),
            "net":  round((v["buy_s"] - v["sell_s"]) / 1000),
            "buy_amount": v["buy_amt"], "sell_amount": v["sell_amt"],
            "is_retail": is_retail,
        })
    _cset(ckey, processed)
    return processed


def _fetch_broker_range(token: str, stock_id: str, dates: list, force: bool = False) -> dict:
    """Fetch broker data one day at a time using start_date=end_date per call.
    Returns {date_str: [processed_rows]}."""
    if not dates:
        return {}

    result: dict = {}
    for date_str in dates:
        day_ckey = f"broker_day|{stock_id}|{date_str}"
        if force:
            # Clear stale per-day cache so we re-fetch from API
            with _CLOCK:
                _CACHE.pop(day_ckey, None)
        cached_day = _cget(day_ckey, ttl_h=12)
        if cached_day is None:
            time.sleep(0.13)  # ~7 API calls/sec = 420/min, safely under 600/min
            rows = _fetch_broker_day(token, stock_id, date_str)  # raises _FinMindRateLimit on 402
        else:
            rows = cached_day
        if rows:
            result[date_str] = rows

    return result


def _scan_one_stock(token: str, stock_id: str, dates: list, start: str, end: str,
                    force: bool = False) -> dict:
    """Fetch + compute overview item for one stock."""
    broker_data = _fetch_broker_range(token, stock_id, dates, force=force)
    price_map   = _fetch_price_range(stock_id, start, end)
    THOLD = 8_000_000
    timeline = []
    for dt in sorted(dates):
        rows  = broker_data.get(dt, [])
        close = price_map.get(dt, {}).get("close", 0)
        bs = ss = rb = rs = 0
        for row in rows:
            buy_lots  = row.get("buy",  0)
            sell_lots = row.get("sell", 0)
            # FinMind TaiwanStockTradingDailyReport has no price field;
            # use closing price to estimate NTD amount for institutional classification.
            buy_amt  = buy_lots  * 1000 * close if close > 0 else row.get("buy_amount",  0)
            sell_amt = sell_lots * 1000 * close if close > 0 else row.get("sell_amount", 0)
            if buy_amt > THOLD or sell_amt > THOLD:
                bs += int(buy_amt  // THOLD)
                ss += int(sell_amt // THOLD)
            else:
                rb += buy_lots
                rs += sell_lots
        pm = price_map.get(dt, {})
        timeline.append({"date": dt, "buy_score": bs, "sell_score": ss,
                         "net_score": bs - ss, "retail_buy": rb, "retail_sell": rs,
                         "close": pm.get("close", 0), "volume": pm.get("volume", 0)})
    while timeline and not any([
        timeline[-1]["buy_score"], timeline[-1]["sell_score"],
        timeline[-1]["retail_buy"], timeline[-1]["retail_sell"], timeline[-1]["close"],
    ]):
        timeline.pop()
    total_buy  = sum(t["buy_score"]  for t in timeline)
    total_sell = sum(t["sell_score"] for t in timeline)
    closes = [t["close"] for t in timeline if t["close"] > 0]
    latest_close  = closes[-1]               if closes          else 0
    prev_close    = closes[-2]               if len(closes) > 1 else 0
    price_chg_pct = round((latest_close - prev_close) / prev_close * 100, 2) if prev_close else 0
    g = _GRADING.get(stock_id, {})
    return {
        "stock_id": stock_id, "timeline": timeline,
        "total_buy": total_buy, "total_sell": total_sell,
        "total_net": total_buy - total_sell,
        "total_retail_buy":  sum(t["retail_buy"]  for t in timeline),
        "total_retail_sell": sum(t["retail_sell"] for t in timeline),
        "latest_close": latest_close, "price_chg_pct": price_chg_pct,
        "tier": g.get("tier", "micro"), "market_cap_億": g.get("market_cap_億", 0),
    }


def _ov_scan_worker(token: str, stock_ids: list, days: int, force: bool = False):
    from datetime import date as dt_date, timedelta as td
    end_d   = dt_date.today()
    start_d = end_d - td(days=round(days * 1.5))
    dates: list = []
    d = start_d
    while d <= end_d:
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d += td(days=1)
    start, end = start_d.isoformat(), end_d.isoformat()
    print(f"[OV] scan {len(stock_ids)} stocks, dates {dates[0]}~{dates[-1]} ({len(dates)} days), force={force}")

    for stock_id in stock_ids:
        ckey = f"ov_item|{stock_id}|{days}"
        if not force:
            cached = _cget(ckey, ttl_h=6)
            if cached is not None:
                with _OV_SCAN_LOCK:
                    _OV_SCAN["results"][stock_id] = cached
                    _OV_SCAN["done"] += 1
                continue

        for attempt in range(3):
            try:
                item = _scan_one_stock(token, stock_id, dates, start, end, force=force)
                _cset(ckey, item)
                with _OV_SCAN_LOCK:
                    _OV_SCAN["results"][stock_id] = item
                    _OV_SCAN["done"] += 1
                break
            except _FinMindRateLimit as rl_exc:
                if attempt < 2:
                    time.sleep(60)
                else:
                    with _OV_SCAN_LOCK:
                        _OV_SCAN["done"] += 1
                        _OV_SCAN["skip"] += 1
                        _OV_SCAN["last_err"] = f"FinMind 402: {rl_exc or '每日API次數超限'}"
            except Exception as exc:
                import traceback
                with _OV_SCAN_LOCK:
                    _OV_SCAN["done"] += 1
                    _OV_SCAN["skip"] += 1
                    _OV_SCAN["last_err"] = f"{stock_id}: {exc}"
                print(f"  [OV] {stock_id} error: {traceback.format_exc()[:300]}")
                break

        time.sleep(0.5)   # extra buffer between stocks on top of per-day throttle inside _fetch_broker_range

    with _OV_SCAN_LOCK:
        _OV_SCAN["running"] = False


@app.post("/api/overview_scan/start")
def api_ov_scan_start(
    days: int = Query(15), force: bool = Query(False),
    tier: str = Query(""), industry: str = Query(""),
):
    import os
    token = _TOKEN or os.getenv("FINMIND_TOKEN", "")
    if not token:
        raise HTTPException(500, "未設定 FINMIND_TOKEN")
    tier = tier.strip().lower()
    if tier not in ({t[0] for t in TIERS} | {""}):
        tier = ""
    industry = industry.strip()
    with _OV_SCAN_LOCK:
        if _OV_SCAN["running"]:
            return {"status": "already_running", "done": _OV_SCAN["done"],
                    "total": _OV_SCAN["total"]}
        if (force or _OV_SCAN["days"] != days or _OV_SCAN["tier"] != tier
                or _OV_SCAN.get("industry", "") != industry):
            _OV_SCAN["results"] = {}
        _OV_SCAN["tier"] = tier
        _OV_SCAN["industry"] = industry
    if tier:
        with _CLOCK:
            tier_stocks = [sid for sid, g in _GRADING.items() if g.get("tier") == tier]
        if not tier_stocks:
            return {"status": "grading_not_ready",
                    "total": 0, "tier": tier,
                    "running": _GRADE_PROG.get("running", False),
                    "ready": len(_GRADING)}
        if industry:
            stock_ids = [sid for sid in tier_stocks
                         if _GRADING.get(sid, {}).get("industry") == industry]
        else:
            stock_ids = tier_stocks
    else:
        stock_ids = [s["stock_id"] for s in _STOCKS]
    with _OV_SCAN_LOCK:
        _OV_SCAN.update({"running": True, "done": 0, "total": len(stock_ids),
                         "error": "", "days": days, "skip": 0, "last_err": "",
                         "started": datetime.now().strftime("%H:%M")})
    threading.Thread(target=_ov_scan_worker, args=(token, stock_ids, days, force),
                     daemon=True).start()
    return {"status": "started", "total": len(stock_ids), "tier": tier, "industry": industry}


@app.get("/api/overview_scan/status")
def api_ov_scan_status():
    with _OV_SCAN_LOCK:
        results = [dict(v) for v in _OV_SCAN["results"].values()]
    # Refresh tier/market_cap from latest _GRADING so stale cache doesn't
    # misclassify stocks scanned before grading completed
    with _CLOCK:
        grading_snap = dict(_GRADING)
    for item in results:
        g = grading_snap.get(item["stock_id"])
        if g:
            item["tier"] = g["tier"]
            item["market_cap_億"] = g["market_cap_億"]
    results.sort(key=lambda x: (
        TIER_ORDER.get(x.get("tier", "micro"), 99),
        -(x["total_buy"] + x["total_sell"])
    ))
    return {
        "running": _OV_SCAN["running"], "done": _OV_SCAN["done"],
        "total":   _OV_SCAN["total"],   "days":  _OV_SCAN["days"],
        "started": _OV_SCAN["started"], "error": _OV_SCAN["error"],
        "skip":    _OV_SCAN.get("skip", 0), "last_err": _OV_SCAN.get("last_err", ""),
        "tier":     _OV_SCAN["tier"],
        "industry": _OV_SCAN.get("industry", ""),
        "results": results,
    }


@app.get("/api/debug_ov")
def api_debug_ov():
    """Debug endpoint: show raw scan state without timeline data."""
    with _OV_SCAN_LOCK:
        snap = {k: v for k, v in _OV_SCAN.items() if k != "results"}
        snap["results_count"] = len(_OV_SCAN["results"])
        snap["result_tiers"] = {sid: item.get("tier") for sid, item in _OV_SCAN["results"].items()}
        snap["result_totals"] = {sid: (item.get("total_buy", 0), item.get("total_sell", 0))
                                 for sid, item in _OV_SCAN["results"].items()}
    return snap


@app.get("/api/industries")
def api_industries(tier: str = Query("")):
    """Return industry categories with counts for a given tier (from _GRADING)."""
    with _CLOCK:
        stocks = list(_GRADING.values())
    if tier:
        stocks = [s for s in stocks if s.get("tier") == tier]
    counts: dict[str, int] = {}
    for s in stocks:
        ind = s.get("industry", "") or ""
        if ind:
            counts[ind] = counts.get(ind, 0) + 1
    result = sorted(counts.items(), key=lambda x: -x[1])
    return {
        "tier": tier,
        "total": len(stocks),
        "industries": [{"name": n, "count": c} for n, c in result],
    }


@app.get("/api/debug")
def api_debug():
    """診斷：stocks/grading 狀態 + 測試各資料來源。"""
    import os, traceback as tb
    token = _TOKEN or os.getenv("FINMIND_TOKEN", "")

    # Test TaiwanStockInfo
    stock_info_result = {}
    try:
        r = requests.get(FINMIND_BASE,
                         params={"dataset": "TaiwanStockInfo", "token": token},
                         timeout=20)
        j = r.json()
        rows = j.get("data", [])
        stock_info_result = {"status": j.get("status"), "row_count": len(rows),
                             "msg": j.get("msg", ""), "sample": rows[:2]}
    except Exception as exc:
        stock_info_result = {"error": str(exc)}

    # Test TWSE OpenAPI
    twse_api_result = {}
    try:
        r = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        data = r.json() if isinstance(r.json(), list) else []
        twse_api_result = {"row_count": len(data), "sample": data[:2] if data else []}
    except Exception as exc:
        twse_api_result = {"error": str(exc)}

    # Test TaiwanStockTradingDailyReport (single day = today)
    broker_result = {}
    try:
        from datetime import date as dt_date, timedelta as td
        today = dt_date.today().isoformat()
        r2 = requests.get(FINMIND_BASE,
                          params={"dataset": "TaiwanStockTradingDailyReport",
                                  "data_id": "2330", "start_date": today, "token": token},
                          timeout=15)
        j2 = r2.json()
        rows2 = j2.get("data", [])
        broker_result = {"status": j2.get("status"), "row_count": len(rows2),
                         "msg": j2.get("msg", ""), "sample": rows2[:2]}
    except Exception as exc:
        broker_result = {"error": str(exc)}

    # Test TaiwanStockTradingDailyReport (range = last 5 trading days)
    broker_range_result = {}
    try:
        end_d   = dt_date.today()
        start_d = end_d - td(days=10)
        r3 = requests.get(FINMIND_BASE,
                          params={"dataset": "TaiwanStockTradingDailyReport",
                                  "data_id": "2330",
                                  "start_date": start_d.isoformat(),
                                  "end_date":   end_d.isoformat(),
                                  "token": token},
                          timeout=20)
        j3 = r3.json()
        rows3 = j3.get("data", [])
        dates_in_resp = sorted(set(str(r.get("date",""))[:10] for r in rows3)) if rows3 else []
        broker_range_result = {"status": j3.get("status"), "row_count": len(rows3),
                               "msg": j3.get("msg", ""),
                               "dates_covered": dates_in_resp,
                               "sample": rows3[:2]}
    except Exception as exc:
        broker_range_result = {"error": str(exc)}

    with _OV_SCAN_LOCK:
        scan_snap = {k: v for k, v in _OV_SCAN.items() if k != "results"}
        scan_snap["results_count"] = len(_OV_SCAN["results"])

    return {
        "stocks_count":    len(_STOCKS),
        "grading_count":   len(_GRADING),
        "grade_prog":      _GRADE_PROG,
        "scan":            scan_snap,
        "test_stock_info": stock_info_result,
        "test_twse_api":   twse_api_result,
        "test_broker_1day": broker_result,
        "test_broker_range": broker_range_result,
    }


@app.get("/api/debug_broker")
def api_debug_broker(stock_id: str = Query("2330"), days: int = Query(5)):
    """直接測試 _fetch_broker_range，回傳 raw API 結果供診斷。"""
    import os
    from datetime import date as dt_date, timedelta as td
    token = _TOKEN or os.getenv("FINMIND_TOKEN", "")
    if not token:
        return {"error": "no token"}
    end_d   = dt_date.today()
    start_d = end_d - td(days=round(days * 1.5))
    dates: list = []
    d = start_d
    while d <= end_d:
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d += td(days=1)
    # raw call
    try:
        r = requests.get(
            FINMIND_BASE,
            params={"dataset": "TaiwanStockTradingDailyReport", "data_id": stock_id,
                    "start_date": dates[0], "end_date": dates[-1], "token": token},
            timeout=30,
        )
        j = r.json()
    except Exception as exc:
        return {"error": str(exc)}
    rows = j.get("data", [])
    dates_in_resp = sorted(set(str(rw.get("date",""))[:10] for rw in rows)) if rows else []
    processed = _fetch_broker_range(token, stock_id, dates)
    return {
        "stock_id": stock_id,
        "dates_requested": [dates[0], dates[-1]],
        "http_status":   r.status_code,
        "fm_status":     j.get("status"),
        "fm_msg":        j.get("msg", ""),
        "raw_row_count": len(rows),
        "dates_in_response": dates_in_resp,
        "sample_row":    rows[0] if rows else None,
        "processed_dates": sorted(processed.keys()),
        "processed_rows_per_date": {k: len(v) for k, v in processed.items()},
    }


@app.get("/api/kline_data")
def api_kline_data(stock_id: str = Query("2330"), days: int = Query(90)):
    """Return OHLCV + 三大法人 + 融資融券 for K-line chart."""
    end_dt  = datetime.now()
    start_dt = end_dt - timedelta(days=days + 30)
    start   = start_dt.strftime("%Y-%m-%d")
    end     = end_dt.strftime("%Y-%m-%d")
    cutoff  = (end_dt - timedelta(days=days)).strftime("%Y-%m-%d")

    # ── OHLCV ────────────────────────────────────────────────────────────
    price_df = _fm("TaiwanStockPrice", stock_id, start, end)
    prices: list = []
    if not price_df.empty:
        for _, row in price_df.iterrows():
            d = str(row.get("date", ""))[:10]
            if d < cutoff:
                continue
            o = float(row.get("open",  0) or 0)
            h = float(row.get("max",   0) or 0)   # FinMind field name
            l = float(row.get("min",   0) or 0)
            c = float(row.get("close", 0) or 0)
            v = int(row.get("Trading_Volume", 0) or 0)
            if o > 0 and c > 0:
                prices.append({"date": d, "open": o, "high": h, "low": l, "close": c,
                                "volume": round(v / 1000)})

    # ── 三大法人 ──────────────────────────────────────────────────────────
    inst_df = _fm("TaiwanStockInstitutionalInvestors", stock_id, start, end)
    inst_map: dict = {}
    if not inst_df.empty:
        for _, row in inst_df.iterrows():
            d = str(row.get("date", ""))[:10]
            if d < cutoff:
                continue
            name = str(row.get("name", ""))
            net  = int(row.get("net_buy", 0) or 0)
            if d not in inst_map:
                inst_map[d] = {"foreign": 0, "trust": 0, "dealer": 0}
            if "外資" in name:
                inst_map[d]["foreign"] = net
            elif "投信" in name:
                inst_map[d]["trust"]   = net
            elif "自營" in name:
                inst_map[d]["dealer"]  = net
    institutional = [{"date": d, **v} for d, v in sorted(inst_map.items())]

    # ── 融資融券 ──────────────────────────────────────────────────────────
    margin_df = _fm("TaiwanStockMarginPurchaseShortSale", stock_id, start, end)
    margins: list = []
    if not margin_df.empty:
        for _, row in margin_df.iterrows():
            d = str(row.get("date", ""))[:10]
            if d < cutoff:
                continue
            margins.append({
                "date":           d,
                "margin_balance": int(row.get("MarginPurchaseTodayBalance", 0) or 0),
                "short_balance":  int(row.get("ShortSaleTodayBalance",     0) or 0),
            })

    name_info  = next((s for s in _STOCKS if s["stock_id"] == stock_id), {})
    return {
        "stock_id":       stock_id,
        "stock_name":     name_info.get("stock_name", ""),
        "prices":         prices,
        "institutional":  institutional,
        "margins":        margins,
    }


@app.get("/api/industry_chain")
def api_industry_chain():
    """Taiwan stock industry chain (TaiwanStockIndustryChain) via FinMind DataLoader."""
    ckey = "industry_chain"
    cached = _cget(ckey, ttl_h=12)
    if cached is not None:
        return cached

    token = _TOKEN or os.getenv("FINMIND_TOKEN", "")
    try:
        from FinMind.data import DataLoader
        dl = DataLoader()
        if token:
            dl.login_by_token(api_token=token)
        df = dl.taiwan_stock_industry_chain()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FinMind DataLoader error: {exc}")

    name_map = {s["stock_id"]: s.get("stock_name", "") for s in _STOCKS}

    from collections import defaultdict
    tree: dict = defaultdict(lambda: defaultdict(list))
    seen: set = set()
    for _, row in df.iterrows():
        sid = str(row["stock_id"])
        ind = str(row["industry"])
        sub = str(row["sub_industry"])
        key = (sid, ind, sub)
        if key in seen:
            continue
        seen.add(key)
        tree[ind][sub].append({"id": sid, "name": name_map.get(sid, "")})

    industry_counts = {ind: sum(len(v) for v in subs.values()) for ind, subs in tree.items()}
    industry_list = sorted(tree.keys(), key=lambda x: -industry_counts[x])
    industries = {ind: dict(subs) for ind, subs in tree.items()}

    result = {
        "total": int(len(df)),
        "industries": industries,
        "industry_list": industry_list,
        "industry_counts": industry_counts,
    }
    _cset(ckey, result)
    return result


@app.post("/api/reload_stocks")
def api_reload_stocks():
    """強制重新載入股票清單並重新計算分級。"""
    def _do():
        _load_stocks()
        if _STOCKS:
            _run_grading()
    threading.Thread(target=_do, daemon=True).start()
    return {"status": "started", "msg": "重新載入中，請等 1 分鐘後查看 /api/debug"}


def _fetch_price_range(stock_id: str, start: str, end: str) -> dict:
    """Fetch daily close+volume for one stock over a date range. Returns {date_str: {close, volume}}."""
    df = _fm("TaiwanStockPrice", stock_id, start, end)
    price_map: dict = {}
    if not df.empty:
        for _, pr in df.iterrows():
            d_str = str(pr.get("date", ""))[:10]
            price_map[d_str] = {
                "close":  float(pr.get("close", 0) or 0),
                "volume": round(float(pr.get("Trading_Volume", 0) or 0) / 1000),
            }
    return price_map


@app.get("/api/multi_timeline")
def api_multi_timeline(
    stocks: str = Query(""),
    days:   int = Query(15),
):
    from datetime import date as dt_date, timedelta as td
    import os

    stock_list = [s.strip().upper() for s in stocks.split(",") if s.strip()]
    if not stock_list:
        raise HTTPException(400, "請輸入股票代號")
    stock_list = stock_list[:50]

    token = _TOKEN or os.getenv("FINMIND_TOKEN", "")
    if not token:
        raise HTTPException(500, "未設定 FINMIND_TOKEN")

    today   = dt_date.today()
    end_d   = today
    start_d = end_d - td(days=round(days * 1.5))
    dates   = []
    d = start_d
    while d <= end_d:
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d += td(days=1)

    THOLD = 8_000_000
    raw: dict       = {s: {} for s in stock_list}
    price_raw: dict = {s: {} for s in stock_list}
    rate_err: list  = []

    # FinMind free tier: ~3 req/s sustained. Use semaphore to throttle.
    _sem = threading.Semaphore(3)

    def _broker_throttled(tok, sid, dt):
        with _sem:
            result = _fetch_broker_day(tok, sid, dt)
            time.sleep(0.35)
            return result

    with ThreadPoolExecutor(max_workers=4) as ex:
        broker_futs = {ex.submit(_broker_throttled, token, s, dt): (s, dt)
                       for s in stock_list for dt in dates}
        price_futs  = {ex.submit(_fetch_price_range, s, start_d.isoformat(), end_d.isoformat()): s
                       for s in stock_list}
        for fut in as_completed(broker_futs):
            sid, dt = broker_futs[fut]
            try:
                raw[sid][dt] = fut.result()
            except _FinMindRateLimit as e:
                rate_err.append(str(e))
                raw[sid][dt] = []
        for fut in as_completed(price_futs):
            sid = price_futs[fut]
            try:
                price_raw[sid] = fut.result()
            except Exception:
                price_raw[sid] = {}

    if rate_err and not any(any(v for v in raw[s].values()) for s in stock_list):
        raise HTTPException(429, f"FinMind 速率限制（402）：{rate_err[0]}。請稍後再試，或改用付費 token。")

    output = []
    for stock_id in stock_list:
        timeline = []
        for dt in sorted(dates):
            rows  = raw[stock_id].get(dt, [])
            close = price_raw[stock_id].get(dt, {}).get("close", 0)
            bs = ss = rb = rs = 0
            for row in rows:
                buy_lots  = row.get("buy",  0)
                sell_lots = row.get("sell", 0)
                buy_amt  = buy_lots  * 1000 * close if close > 0 else row.get("buy_amount",  0)
                sell_amt = sell_lots * 1000 * close if close > 0 else row.get("sell_amount", 0)
                if buy_amt > THOLD or sell_amt > THOLD:
                    bs += int(buy_amt  // THOLD)
                    ss += int(sell_amt // THOLD)
                else:
                    rb += buy_lots
                    rs += sell_lots
            pm = price_raw[stock_id].get(dt, {})
            timeline.append({
                "date":        dt,
                "buy_score":   bs,
                "sell_score":  ss,
                "net_score":   bs - ss,
                "retail_buy":  rb,
                "retail_sell": rs,
                "close":       pm.get("close",  0),
                "volume":      pm.get("volume", 0),
            })

        # Drop trailing all-zero dates
        while timeline and not any([
            timeline[-1]["buy_score"], timeline[-1]["sell_score"],
            timeline[-1]["retail_buy"], timeline[-1]["retail_sell"],
            timeline[-1]["close"],
        ]):
            timeline.pop()

        total_buy  = sum(t["buy_score"]  for t in timeline)
        total_sell = sum(t["sell_score"] for t in timeline)

        closes = [t["close"] for t in timeline if t["close"] > 0]
        latest_close  = closes[-1]               if closes          else 0
        prev_close    = closes[-2]               if len(closes) > 1 else 0
        price_chg_pct = round((latest_close - prev_close) / prev_close * 100, 2) if prev_close else 0

        g = _GRADING.get(stock_id, {})
        output.append({
            "stock_id":          stock_id,
            "timeline":          timeline,
            "total_buy":         total_buy,
            "total_sell":        total_sell,
            "total_net":         total_buy - total_sell,
            "total_retail_buy":  sum(t["retail_buy"]  for t in timeline),
            "total_retail_sell": sum(t["retail_sell"] for t in timeline),
            "latest_close":      latest_close,
            "price_chg_pct":     price_chg_pct,
            "tier":              g.get("tier", "micro"),
            "market_cap_億":     g.get("market_cap_億", 0),
        })

    output.sort(key=lambda x: (
        TIER_ORDER.get(x["tier"], 99),
        -(x["total_buy"] + x["total_sell"])
    ))
    return {"results": output, "dates": sorted(dates)}


@app.get("/api/big_player_timeline")
def api_big_player_timeline(
    stock_id:   str = Query(""),
    start_date: str = Query(""),
    end_date:   str = Query(""),
):
    import os
    from datetime import date as dt_date, timedelta as td

    stock_id = stock_id.strip().upper()
    if not stock_id:
        raise HTTPException(400, "請輸入股票代號")
    token = _TOKEN or os.getenv("FINMIND_TOKEN", "")
    if not token:
        raise HTTPException(500, "未設定 FINMIND_TOKEN")

    today = dt_date.today()
    end_d   = dt_date.fromisoformat(end_date)   if end_date   else today
    start_d = dt_date.fromisoformat(start_date) if start_date else end_d - td(days=41)

    dates = []
    d = start_d
    while d <= end_d:
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d += td(days=1)

    THRESHOLD = 8_000_000

    def _fetch_one(date: str):
        ckey = f"broker|{stock_id}|{date}"
        cached = _cget(ckey, ttl_h=6)
        if cached is not None:
            return date, cached.get("rows", [])
        try:
            r = requests.get(
                FINMIND_BASE,
                params={"dataset": "TaiwanStockTradingDailyReport", "data_id": stock_id,
                        "start_date": date, "end_date": date, "token": token},
                timeout=20,
            )
            j = r.json()
        except Exception:
            return date, None
        if r.status_code != 200 or j.get("status") not in (200, None):
            return date, None
        rows_raw = j.get("data", [])
        if not rows_raw:
            _cset(ckey, {"stock_id": stock_id, "date": date, "rows": [], "summary": {}})
            return date, []
        processed = []
        i = 0
        while i < len(rows_raw):
            name = rows_raw[i].get("securities_trader", "")
            bid  = rows_raw[i].get("securities_trader_id", "")
            g_buy_s = g_sell_s = g_buy_amt = g_sell_amt = 0.0
            j2 = i
            while j2 < len(rows_raw) and rows_raw[j2].get("securities_trader", "") == name:
                row = rows_raw[j2]
                bs  = float(row.get("buy",   0) or 0)
                ss  = float(row.get("sell",  0) or 0)
                px  = float(row.get("price", 0) or 0)
                g_buy_s += bs; g_sell_s += ss
                g_buy_amt += bs * px; g_sell_amt += ss * px
                j2 += 1
            buy_lots  = g_buy_s  / 1000
            sell_lots = g_sell_s / 1000
            avg_buy_px  = round(g_buy_amt  / g_buy_s,  2) if g_buy_s  else 0.0
            avg_sell_px = round(g_sell_amt / g_sell_s, 2) if g_sell_s else 0.0
            processed.append({
                "broker_id":   bid,
                "broker_name": name,
                "buy":         int(buy_lots),
                "sell":        int(sell_lots),
                "net":         int(buy_lots - sell_lots),
                "buy_price":   avg_buy_px,
                "sell_price":  avg_sell_px,
                "buy_amount":  round(g_buy_amt),
                "sell_amount": round(g_sell_amt),
                "is_retail":   g_buy_amt <= THRESHOLD and g_sell_amt <= THRESHOLD,
            })
            i = j2
        retail = [r for r in processed if r["is_retail"]]
        inst   = [r for r in processed if not r["is_retail"]]
        result = {
            "stock_id": stock_id,
            "date":     date,
            "rows":     processed,
            "summary": {
                "total":        len(processed),
                "retail_count": len(retail),
                "inst_count":   len(inst),
                "retail_buy":   sum(r["buy"]  for r in retail),
                "retail_sell":  sum(r["sell"] for r in retail),
                "retail_net":   sum(r["net"]  for r in retail),
                "inst_buy":     sum(r["buy"]  for r in inst),
                "inst_sell":    sum(r["sell"] for r in inst),
                "inst_net":     sum(r["net"]  for r in inst),
            },
        }
        _cset(ckey, result)
        return date, processed

    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(_fetch_one, d): d for d in dates}
        day_map = {}
        for fut in as_completed(futs):
            date, rows = fut.result()
            day_map[date] = rows

    # ── fetch price + volume first so we can use close for broker scoring ──
    pdf = _fm("TaiwanStockPrice", stock_id, start_d.isoformat(), end_d.isoformat())
    price_map: dict = {}
    if not pdf.empty:
        for _, pr in pdf.iterrows():
            d_str = str(pr.get("date", ""))[:10]
            price_map[d_str] = {
                "open":   float(pr.get("open",  0) or 0),
                "high":   float(pr.get("max",   0) or 0),
                "low":    float(pr.get("min",   0) or 0),
                "close":  float(pr.get("close", 0) or 0),
                "volume": round(float(pr.get("Trading_Volume", 0) or 0) / 1000),
            }

    # ── compute broker scores per day using close price ───────────────────
    # FinMind TaiwanStockTradingDailyReport has no price field, so buy_amount
    # in the cached rows is 0. Recalculate using closing price.
    all_dates = sorted(set(price_map.keys()) | set(day_map.keys()))
    timeline = []
    for date in all_dates:
        rows  = day_map.get(date) or []
        pr    = price_map.get(date, {"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0})
        close = pr["close"]
        bs = ss = rb = rs = 0
        for row in rows:
            buy_lots  = row.get("buy",  0)
            sell_lots = row.get("sell", 0)
            buy_amt  = buy_lots  * 1000 * close if close > 0 else row.get("buy_amount",  0)
            sell_amt = sell_lots * 1000 * close if close > 0 else row.get("sell_amount", 0)
            if buy_amt > THRESHOLD or sell_amt > THRESHOLD:
                bs += int(buy_amt  // THRESHOLD)
                ss += int(sell_amt // THRESHOLD)
            else:
                rb += buy_lots
                rs += sell_lots
        timeline.append({
            "date":        date,
            "buy_score":   bs,
            "sell_score":  ss,
            "net_score":   bs - ss,
            "retail_buy":  rb,
            "retail_sell": rs,
            "open":        pr["open"],
            "high":        pr["high"],
            "low":         pr["low"],
            "close":       pr["close"],
            "volume":      pr["volume"],
        })

    return {"stock_id": stock_id, "timeline": timeline}


@app.get("/", response_class=HTMLResponse)
def index():
    return _HTML


# ── HTML ──────────────────────────────────────────────────────────────────
_HTML = r"""<!DOCTYPE html>
<html lang="zh-Hant-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>千張大戶分析</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<script src="https://unpkg.com/lightweight-charts@3.8.0/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{
  --bg:#0d1117;--sur:#161b22;--sur2:#21262d;--bor:#30363d;
  --txt:#e6edf3;--mut:#8b949e;--acc:#3fb950;--red:#f85149;
  --blu:#58a6ff;--yel:#d29922;--pur:#c084fc;--rad:8px;
  --topbar-h:52px;--botnav-h:60px;
}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html{height:100%}
body{background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;font-size:13px;height:100%;display:flex;flex-direction:column;overflow:hidden}

/* ── Top bar ── */
.topbar{display:flex;align-items:center;gap:12px;padding:10px 16px;border-bottom:1px solid var(--bor);flex-shrink:0;background:var(--sur)}
.topbar h1{font-size:1.1rem;font-weight:700;white-space:nowrap}
.topbar .badge{font-size:10px;padding:2px 7px;border-radius:12px;background:var(--sur2);color:var(--mut)}

/* ── Layout ── */
.layout{display:flex;flex:1;overflow:hidden}

/* ── Sidebar ── */
.sidebar{width:240px;flex-shrink:0;border-right:1px solid var(--bor);display:flex;flex-direction:column;background:var(--sur)}
.sidebar-head{padding:10px;border-bottom:1px solid var(--bor);flex-shrink:0}
.search{width:100%;background:var(--sur2);border:1px solid var(--bor);color:var(--txt);border-radius:var(--rad);padding:6px 10px;font-size:12px}
.search:focus{outline:none;border-color:var(--acc)}
.sort-bar{display:flex;gap:4px;margin-top:6px;flex-wrap:wrap}
.sort-btn{font-size:10px;padding:2px 7px;border-radius:12px;border:1px solid var(--bor);background:var(--sur2);color:var(--mut);cursor:pointer}
.sort-btn.active{background:var(--acc);border-color:var(--acc);color:#000;font-weight:700}
.stock-count{font-size:10px;color:var(--mut);margin-top:4px}
.stock-list{flex:1;overflow-y:auto;padding:4px}
.stock-item{display:flex;align-items:center;padding:7px 8px;border-radius:6px;cursor:pointer;gap:8px;position:relative;border:1px solid transparent;margin-bottom:2px}
.stock-item:hover{background:var(--sur2)}
.stock-item.active{background:rgba(63,185,80,.1);border-color:rgba(63,185,80,.3)}
.stock-item.compare-sel{background:rgba(88,166,255,.08);border-color:rgba(88,166,255,.25)}
.stock-info{flex:1;min-width:0}
.stock-code{font-weight:700;font-size:12px;font-family:monospace}
.stock-name{font-size:11px;color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.stock-metrics{text-align:right;flex-shrink:0;font-size:11px}
.pct-val{font-weight:700}
.pct-up{color:var(--acc)}
.pct-dn{color:var(--red)}
.pct-chg{font-size:10px;color:var(--mut)}
.add-compare{width:18px;height:18px;border-radius:50%;border:1px solid var(--bor);background:var(--sur2);color:var(--mut);display:flex;align-items:center;justify-content:center;font-size:13px;line-height:1;flex-shrink:0;opacity:0;transition:.15s}
.stock-item:hover .add-compare{opacity:1}
.add-compare.added{opacity:1;background:var(--blu);border-color:var(--blu);color:#fff}

/* ── Main ── */
.main{flex:1;display:flex;flex-direction:column;overflow:hidden}
.tabs{display:flex;gap:0;padding:0 16px;border-bottom:1px solid var(--bor);flex-shrink:0;background:var(--sur)}
.tab{padding:10px 18px;cursor:pointer;border-bottom:2px solid transparent;color:var(--mut);font-size:13px;transition:.15s}
.tab:hover{color:var(--txt)}
.tab.active{color:var(--acc);border-bottom-color:var(--acc)}
.pane{display:none;flex:1;overflow:hidden;flex-direction:column}
.pane.active{display:flex}

/* ── 大戶總覽 cards ── */
.ov-card{background:var(--sur);border:1px solid var(--bor);border-radius:var(--rad);padding:10px;cursor:pointer;transition:.15s}
.ov-card:hover{border-color:var(--acc);background:var(--sur2)}

/* ── Single stock pane ── */
.single-wrap{display:flex;flex-direction:column;flex:1;overflow:hidden;padding:12px 16px;gap:10px}
.stat-row{display:flex;gap:10px;flex-shrink:0;flex-wrap:wrap}
.stat-card{background:var(--sur);border:1px solid var(--bor);border-radius:var(--rad);padding:12px 16px;flex:1;min-width:120px}
.stat-label{font-size:10px;color:var(--mut);text-transform:uppercase;letter-spacing:.05em}
.stat-val{font-size:1.4rem;font-weight:700;margin-top:4px}
.stat-sub{font-size:11px;color:var(--mut);margin-top:2px}
.charts-row{display:flex;gap:10px;flex:1;overflow:hidden;min-height:0}
.chart-box{background:var(--sur);border:1px solid var(--bor);border-radius:var(--rad);flex:1;overflow:hidden;min-width:0}
.chart-box.lag-box{flex:0 0 320px}
.chart-title{font-size:11px;color:var(--mut);padding:8px 12px;border-bottom:1px solid var(--bor);font-weight:600}

/* ── Compare pane ── */
.compare-wrap{display:flex;flex-direction:column;flex:1;overflow:hidden;padding:12px 16px;gap:10px}
.compare-head{display:flex;align-items:center;gap:8px;flex-wrap:wrap;flex-shrink:0}
.compare-chips{display:flex;gap:6px;flex-wrap:wrap;flex:1}
.chip{display:flex;align-items:center;gap:5px;padding:4px 10px;background:var(--sur2);border:1px solid var(--bor);border-radius:16px;font-size:11px}
.chip-rm{cursor:pointer;color:var(--mut);font-size:14px;line-height:1}
.chip-rm:hover{color:var(--red)}
.chain-ind-item{padding:6px 8px;border-radius:6px;cursor:pointer;font-size:12px;display:flex;justify-content:space-between;align-items:center;gap:4px;border:1px solid transparent;transition:.12s}
.chain-ind-item:hover{background:var(--sur2);border-color:var(--bor)}
.chain-ind-item.active{background:var(--acc)22;border-color:var(--acc);color:var(--acc);font-weight:700}
.chain-chip{padding:3px 8px;border-radius:12px;border:1px solid var(--bor);background:var(--sur2);color:var(--txt);font-size:11px;cursor:pointer;transition:.12s}
.chain-chip:hover{border-color:var(--acc);color:var(--acc)}
.chain-chip.active{background:var(--acc);border-color:var(--acc);color:#000;font-weight:700}
.years-sel{background:var(--sur2);border:1px solid var(--bor);color:var(--txt);border-radius:var(--rad);padding:4px 8px;font-size:12px}
.compare-charts{display:flex;gap:10px;flex:1;overflow:hidden;min-height:0;flex-direction:column}
.compare-charts-row{display:flex;gap:10px;flex:1;min-height:0}
.compare-table-wrap{flex-shrink:0;overflow-x:auto}
.ctable{width:100%;border-collapse:collapse;font-size:12px}
.ctable th{padding:6px 12px;text-align:right;color:var(--mut);font-size:10px;text-transform:uppercase;border-bottom:1px solid var(--bor);white-space:nowrap}
.ctable th:first-child{text-align:left}
.ctable td{padding:7px 12px;text-align:right;border-bottom:1px solid var(--bor)}
.ctable td:first-child{text-align:left;font-weight:700;font-family:monospace}
.ctable tr:last-child td{border-bottom:none}
.ctable tr:hover td{background:var(--sur2)}

/* ── Empty state ── */
.empty{display:flex;align-items:center;justify-content:center;flex-direction:column;flex:1;color:var(--mut);gap:10px}
.empty svg{opacity:.3}
.loading{display:flex;align-items:center;justify-content:center;flex:1;flex-direction:column;gap:12px;color:var(--mut)}
.spinner{width:32px;height:32px;border:3px solid var(--bor);border-top-color:var(--acc);border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

/* ─── TOOLTIP ─── */
.tip{position:relative;display:inline-flex;align-items:center;gap:3px;cursor:help}
.tip::after{content:attr(data-tip);position:absolute;bottom:calc(100% + 8px);left:50%;transform:translateX(-50%);background:#1c2030;color:#e6edf3;border:1px solid #444c6b;border-radius:8px;padding:10px 14px;font-size:11px;line-height:1.7;white-space:pre-line;max-width:280px;text-align:left;z-index:2000;pointer-events:none;opacity:0;transition:opacity .15s;box-shadow:0 6px 24px rgba(0,0,0,.7);font-weight:400}
.tip:hover::after,.tip.tipped::after{opacity:1}
.tip-i{width:15px;height:15px;border-radius:50%;background:var(--sur2);color:var(--mut);font-size:9px;font-weight:700;display:inline-flex;align-items:center;justify-content:center;flex-shrink:0;border:1px solid var(--bor)}
/* ─── QUICK SORT BUTTONS ─── */
.qs-btn{font-size:11px;padding:3px 10px;border-radius:14px;border:1px solid var(--bor);background:var(--sur2);color:var(--mut);cursor:pointer;white-space:nowrap;transition:.15s}
.qs-btn:hover{background:var(--bor);color:var(--txt)}
.qs-btn.active{background:var(--blu);border-color:var(--blu);color:#fff;font-weight:700}
/* ─── GRADE LEGEND ─── */
.grade-legend{background:var(--sur);border:1px solid var(--bor);border-radius:var(--rad);padding:12px 14px;font-size:11px;color:var(--mut);flex-shrink:0}
.grade-legend b{color:var(--txt)}
.legend-row{display:flex;align-items:center;gap:8px;padding:3px 0;flex-wrap:wrap}
.legend-sep{border:none;border-top:1px solid var(--bor);margin:6px 0}
/* ─── LAG LEGEND ─── */
.lag-note{font-size:10px;color:var(--mut);padding:6px 10px;line-height:1.7;flex-shrink:0}
/* Plotly dark override */
.js-plotly-plot .plotly .bg{fill:transparent!important}
.nsewdrag{fill:transparent!important}

/* scrollbar */
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:var(--sur)}
::-webkit-scrollbar-thumb{background:var(--bor);border-radius:2px}
::-webkit-scrollbar-thumb:hover{background:var(--mut)}

/* toast */
.toast{position:fixed;bottom:80px;right:16px;background:var(--sur2);border:1px solid var(--bor);color:var(--txt);padding:10px 16px;border-radius:var(--rad);transform:translateY(60px);opacity:0;transition:.3s;z-index:999;font-size:12px;max-width:280px}
.toast.show{transform:none;opacity:1}
.toast.err{border-color:var(--red);color:var(--red)}

/* ─── GRADING ─── */
.grade-wrap{display:flex;flex-direction:column;flex:1;overflow:hidden;padding:10px 14px;gap:8px}
.grade-controls{flex-shrink:0;background:var(--sur);border:1px solid var(--bor);border-radius:var(--rad);padding:10px 12px}
.grade-filter-row{display:flex;align-items:center;gap:5px;flex-wrap:wrap}
.gf-btn{font-size:11px;padding:3px 9px;border-radius:14px;border:1px solid var(--bor);background:var(--sur2);color:var(--mut);cursor:pointer;white-space:nowrap;transition:.15s}
.gf-btn:hover{background:var(--bor);color:var(--txt)}
.gf-btn.active{background:var(--acc);border-color:var(--acc);color:#000;font-weight:700}
.grade-s.active{background:#ef4444;border-color:#ef4444;color:#fff}
.grade-a.active{background:#f97316;border-color:#f97316;color:#fff}
.grade-b.active{background:#eab308;border-color:#eab308;color:#000}
.grade-c.active{background:#60a5fa;border-color:#60a5fa;color:#000}
.grade-d.active{background:#6b7280;border-color:#6b7280;color:#fff}
.grade-body{flex:1;overflow-y:auto}
.tier-section{margin-bottom:14px}
.tier-header{display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--sur2);border-radius:6px;cursor:pointer;user-select:none;margin-bottom:6px;border:1px solid var(--bor)}
.tier-header-title{font-weight:700;font-size:13px}
.tier-cnt{font-size:11px;color:var(--mut);margin-left:4px}
.tier-toggle{margin-left:auto;color:var(--mut);transition:.2s}
.tier-section.collapsed .tier-toggle{transform:rotate(-90deg)}
.tier-section.collapsed .grade-table-wrap{display:none}
/* grade table */
.grade-table-wrap{overflow-x:auto;border:1px solid var(--bor);border-radius:var(--rad)}
.gtable{width:100%;border-collapse:collapse;min-width:580px;font-size:12px}
.gtable th{padding:7px 10px;text-align:right;color:var(--mut);font-size:10px;text-transform:uppercase;border-bottom:1px solid var(--bor);white-space:nowrap;background:var(--sur2);cursor:pointer;user-select:none}
.gtable th:first-child,.gtable th:nth-child(2){text-align:left}
.gtable th:hover{color:var(--txt)}
.gtable td{padding:8px 10px;text-align:right;border-bottom:1px solid var(--bor)}
.gtable td:first-child,.gtable td:nth-child(2){text-align:left}
.gtable tr:last-child td{border-bottom:none}
.gtable tr:hover td{background:var(--sur2)}
/* grade badge */
.gbadge{display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border-radius:50%;font-size:12px;font-weight:800;color:#fff;flex-shrink:0}
.gb-S{background:#ef4444}
.gb-A{background:#f97316}
.gb-B{background:#ca8a04;color:#000}
.gb-C{background:#3b82f6}
.gb-D{background:#6b7280}
/* tier badge */
.tier-badge{font-size:10px;padding:1px 7px;border-radius:10px;font-weight:600}
.tb-mega{background:rgba(168,85,247,.2);color:#c084fc}
.tb-large{background:rgba(59,130,246,.2);color:#60a5fa}
.tb-mid{background:rgba(63,185,80,.2);color:#4ade80}
.tb-small{background:rgba(234,179,8,.2);color:#ca8a04}
.tb-micro{background:rgba(107,114,128,.2);color:#9ca3af}
/* sparkline for volatility */
.vol-bar{display:inline-block;height:6px;border-radius:3px;vertical-align:middle;margin-right:5px;min-width:2px}
@media(max-width:767px){
  .grade-wrap{padding:6px}
  .gtable{min-width:480px}
  .grade-controls{padding:7px 8px}
}
/* ─── DRAWER (mobile stock list) ─── */
.drawer-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:200;backdrop-filter:blur(2px)}
.drawer-overlay.open{display:block}
.drawer{position:fixed;top:0;left:-100%;width:min(300px,88vw);height:100%;background:var(--sur);border-right:1px solid var(--bor);z-index:201;display:flex;flex-direction:column;transition:left .28s cubic-bezier(.4,0,.2,1);box-shadow:4px 0 24px rgba(0,0,0,.5)}
.drawer.open{left:0}
.drawer-header{display:flex;align-items:center;justify-content:space-between;padding:14px 12px;border-bottom:1px solid var(--bor);flex-shrink:0}
.drawer-close{background:none;border:none;color:var(--mut);font-size:22px;cursor:pointer;padding:4px 8px;border-radius:6px}
.drawer-close:hover{color:var(--txt);background:var(--sur2)}
.drawer-content{display:flex;flex-direction:column;flex:1;overflow:hidden}

/* ─── BOTTOM NAV (mobile only, hidden on desktop) ─── */
.bottom-nav{display:none;position:fixed;bottom:0;left:0;right:0;height:var(--botnav-h);background:var(--sur);border-top:1px solid var(--bor);z-index:100;align-items:stretch}
.bnav-btn{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:3px;background:none;border:none;color:var(--mut);cursor:pointer;font-size:10px;padding:6px;transition:.15s;position:relative}
.bnav-btn svg{flex-shrink:0}
.bnav-btn.active{color:var(--acc)}
.bnav-btn.active svg{stroke:var(--acc)}
.bnav-badge{position:absolute;top:6px;right:calc(50% - 18px);background:var(--red);color:#fff;font-size:9px;font-weight:700;border-radius:8px;padding:1px 4px;min-width:14px;text-align:center}

/* ─── ICON BUTTON (topbar) ─── */
.icon-btn{background:none;border:none;color:var(--mut);cursor:pointer;display:flex;align-items:center;justify-content:center;width:36px;height:36px;border-radius:8px;flex-shrink:0}
.icon-btn:hover,.icon-btn:active{background:var(--sur2);color:var(--txt)}

/* ─── MOBILE RESPONSIVE ─── */
@media (max-width:767px){
  /* hide desktop sidebar */
  .sidebar{display:none}
  /* show drawer + bottom nav */
  .bottom-nav{display:flex}
  /* main fills screen minus topbar & botnav */
  .main{padding-bottom:var(--botnav-h)}
  /* topbar: remove years selector label on mobile */
  .years-label{display:none}
  /* stat cards: 2 columns */
  .stat-row{display:grid;grid-template-columns:1fr 1fr;gap:8px}
  .stat-val{font-size:1.2rem}
  /* charts stack vertically */
  .charts-row{flex-direction:column;gap:8px}
  .chart-box{min-height:260px}
  .chart-box.lag-box{flex:none!important;min-height:220px}
  /* compare charts vertical */
  .compare-charts-row{flex-direction:column}
  /* tabs hidden on mobile (use bottom nav instead) */
  .tabs{display:none}
  /* pane padding smaller */
  .single-wrap,.compare-wrap{padding:8px}
  /* toast above botnav */
  .toast{bottom:calc(var(--botnav-h) + 12px)}
  /* topbar compact */
  .topbar{padding:8px 12px}
  .topbar h1{font-size:1rem}
}
@media (min-width:768px){
  /* desktop: show sidebar, hide drawer+botnav */
  .drawer-overlay,.drawer,.bottom-nav{display:none!important}
}
</style>
</head>
<body>

<!-- Drawer overlay -->
<div class="drawer-overlay" id="drawer-overlay" onclick="closeDrawer()"></div>

<!-- Drawer (mobile stock list) -->
<div class="drawer" id="drawer">
  <div class="drawer-header">
    <span style="font-weight:700;font-size:14px">股票清單 <span id="d-badge" style="font-size:11px;color:var(--mut)"></span></span>
    <button class="drawer-close" onclick="closeDrawer()">✕</button>
  </div>
  <div class="drawer-content">
    <div style="padding:8px">
      <input class="search" id="search-drawer" placeholder="搜尋代號或名稱..." oninput="filterStocks('drawer')">
      <div class="sort-bar" style="margin-top:6px">
        <button class="sort-btn active" onclick="setSort('code',this)">代號</button>
        <button class="sort-btn" onclick="setSort('bpct',this)">千張%</button>
        <button class="sort-btn" onclick="setSort('chg',this)">週增減</button>
        <button class="sort-btn" onclick="setSort('bp',this)">人數</button>
      </div>
    </div>
    <div class="stock-count" id="stock-count-drawer" style="padding:0 8px 4px;font-size:10px;color:var(--mut)"></div>
    <div class="stock-list" id="stock-list-drawer" style="flex:1;overflow-y:auto;padding:4px"></div>
  </div>
</div>

<!-- Top bar -->
<div class="topbar">
  <!-- hamburger (mobile only) -->
  <button class="icon-btn" id="drawer-btn" onclick="openDrawer()" style="margin-right:4px" aria-label="股票清單">
    <svg width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
      <path d="M3 12h18M3 6h18M3 18h18"/>
    </svg>
  </button>
  <h1 style="font-size:clamp(.9rem,3vw,1.1rem)">千張大戶</h1>
  <span class="badge" id="stock-badge" style="white-space:nowrap">載入中...</span>
  <div style="margin-left:auto;display:flex;align-items:center;gap:6px;font-size:12px;color:var(--mut)">
    <span class="years-label">分析年數</span>
    <select id="years-sel" class="years-sel" onchange="onYearsChange()">
      <option value="1">1年</option>
      <option value="2" selected>2年</option>
      <option value="3">3年</option>
      <option value="5">5年</option>
    </select>
  </div>
</div>

<!-- Layout -->
<div class="layout">

  <!-- Sidebar (desktop only) -->
  <aside class="sidebar">
    <div class="sidebar-head">
      <input class="search" id="search" placeholder="搜尋代號或名稱..." oninput="filterStocks('sidebar')">
      <div class="sort-bar">
        <button class="sort-btn active" onclick="setSort('code',this)">代號</button>
        <button class="sort-btn" onclick="setSort('bpct',this)">千張%</button>
        <button class="sort-btn" onclick="setSort('chg',this)">週增減</button>
        <button class="sort-btn" onclick="setSort('bp',this)">人數</button>
      </div>
      <div class="stock-count" id="stock-count"></div>
    </div>
    <div class="stock-list" id="stock-list">
      <div class="loading"><div class="spinner"></div><span>載入股票清單...</span></div>
    </div>
  </aside>

  <!-- Main -->
  <main class="main">
    <div class="tabs">
      <div class="tab active" onclick="switchTab('single')">個股分析</div>
      <div class="tab" onclick="switchTab('compare')">對比分析 <span id="compare-count" style="font-size:10px;padding:1px 5px;background:var(--sur2);border-radius:8px;margin-left:4px"></span></div>
      <div class="tab" onclick="switchTab('grade')">分級排行</div>
      <div class="tab" onclick="switchTab('broker')">🏦 分點籌碼</div>
      <div class="tab" onclick="switchTab('overview')">📊 大戶總覽</div>
      <div class="tab" onclick="switchTab('kline')">📈 K線分析</div>
      <div class="tab" onclick="switchTab('chain')">🏭 產業鏈</div>
    </div>

    <!-- 個股 pane -->
    <div class="pane active" id="pane-single">
      <div class="single-wrap" id="single-wrap">
        <div class="empty">
          <svg width="48" height="48" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24">
            <path d="M3 3v18h18"/><path d="M7 16l4-4 4 4 4-7"/>
          </svg>
          <span>從左側清單選擇股票</span>
        </div>
      </div>
    </div>

    <!-- 對比 pane -->
    <div class="pane" id="pane-compare">
      <div class="compare-wrap">
        <div class="compare-head">
          <span style="color:var(--mut);font-size:12px;white-space:nowrap">已選股票：</span>
          <div class="compare-chips" id="compare-chips">
            <span style="color:var(--mut);font-size:11px;font-style:italic">點選股票右側 + 加入比較</span>
          </div>
          <select class="years-sel" id="compare-years" onchange="updateCompare()">
            <option value="1">1 年</option>
            <option value="2" selected>2 年</option>
            <option value="3">3 年</option>
          </select>
          <button class="sort-btn" onclick="updateCompare()" style="padding:4px 12px">更新</button>
        </div>
        <div id="compare-body" style="flex:1;overflow:hidden;display:flex;flex-direction:column;gap:10px;min-height:0">
          <div class="empty">
            <svg width="40" height="40" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24">
              <rect x="3" y="3" width="18" height="18" rx="2"/><path d="M8 12h8M12 8v8"/>
            </svg>
            <span>尚無選擇股票</span>
          </div>
        </div>
      </div>
    </div>

    <!-- 分級 pane -->
    <div class="pane" id="pane-grade">
      <div class="grade-wrap">

        <!-- 進度條 -->
        <div id="grade-progress-bar" style="display:none">
          <div style="font-size:11px;color:var(--mut);margin-bottom:4px">
            正在計算分級資料… <span id="gp-label"></span>
          </div>
          <div style="height:4px;background:var(--bor);border-radius:2px;overflow:hidden">
            <div id="gp-fill" style="height:100%;background:var(--acc);width:0%;transition:width .5s;border-radius:2px"></div>
          </div>
        </div>

        <!-- 控制列 -->
        <div class="grade-controls">
          <div class="grade-filter-row">
            <span class="tip" style="font-size:11px;color:var(--mut)"
              data-tip="依市值（股價×總股數）分層&#10;超大型 > 1兆&#10;大型 1000億–1兆&#10;中型 100億–1000億&#10;小型 20億–100億&#10;微型 &lt; 20億">
              規模 <span class="tip-i">?</span>：</span>
            <button class="gf-btn active" onclick="setGradeFilter('tier','',this)">全部</button>
            <button class="gf-btn" onclick="setGradeFilter('tier','mega',this)">🏢 超大型<small style="opacity:.6;margin-left:3px">&gt;1兆</small></button>
            <button class="gf-btn" onclick="setGradeFilter('tier','large',this)">🏗 大型<small style="opacity:.6;margin-left:3px">&gt;1000億</small></button>
            <button class="gf-btn" onclick="setGradeFilter('tier','mid',this)">🏬 中型<small style="opacity:.6;margin-left:3px">&gt;100億</small></button>
            <button class="gf-btn" onclick="setGradeFilter('tier','small',this)">🏪 小型<small style="opacity:.6;margin-left:3px">&gt;20億</small></button>
            <button class="gf-btn" onclick="setGradeFilter('tier','micro',this)">🏠 微型<small style="opacity:.6;margin-left:3px">&lt;20億</small></button>
          </div>
          <div class="grade-filter-row" style="margin-top:4px">
            <span class="tip" style="font-size:11px;color:var(--mut)"
              data-tip="同規模內，大戶波動度的百分位排名&#10;波動度 = 每週千張大戶持股%變化的標準差&#10;&#10;S 前10%  → 大戶每週大幅進出（最活躍）&#10;A 前10-30% → 大戶頻繁異動&#10;B 中間30-70% → 多數股票的正常水準&#10;C 後10-30% → 大戶較穩定&#10;D 後10%  → 大戶幾乎鎖倉不動（最穩定）">
              等級 <span class="tip-i">?</span>：</span>
            <button class="gf-btn active" onclick="setGradeFilter('grade','',this)">全部</button>
            <button class="gf-btn grade-s" onclick="setGradeFilter('grade','S',this)" title="同規模前10%，大戶每週大幅進出">S 極高波動</button>
            <button class="gf-btn grade-a" onclick="setGradeFilter('grade','A',this)" title="同規模前10–30%">A 高波動</button>
            <button class="gf-btn grade-b" onclick="setGradeFilter('grade','B',this)" title="同規模中間30–70%，大多數股票">B 中波動</button>
            <button class="gf-btn grade-c" onclick="setGradeFilter('grade','C',this)" title="同規模後10–30%">C 低波動</button>
            <button class="gf-btn grade-d" onclick="setGradeFilter('grade','D',this)" title="同規模後10%，大戶幾乎鎖倉">D 極低波動</button>
          </div>
          <!-- 快速排序 -->
          <div class="grade-filter-row" style="margin-top:6px;gap:4px">
            <span style="font-size:11px;color:var(--mut);flex-shrink:0">排序：</span>
            <button class="qs-btn" id="qs-bpct"    onclick="quickSort('latest_bpct',  false, this)">千張% 高→低</button>
            <button class="qs-btn" id="qs-buy"     onclick="quickSort('bpct_chg',     false, this)">📈 週大買（增減↓）</button>
            <button class="qs-btn" id="qs-sell"    onclick="quickSort('bpct_chg',     true,  this)">📉 週大賣（增減↑）</button>
            <button class="qs-btn" id="qs-people"  onclick="quickSort('latest_bp',    false, this)">人數 高→低</button>
            <button class="qs-btn" id="qs-vol"     onclick="quickSort('volatility',   false, this)">波動度 高→低</button>
            <button class="qs-btn" id="qs-mktcap"  onclick="quickSort('market_cap_億',false, this)">市值 高→低</button>
          </div>
          <!-- 搜尋 + 重算 -->
          <div style="display:flex;align-items:center;gap:8px;margin-top:6px;flex-wrap:wrap">
            <input id="grade-search" class="search" placeholder="搜尋代號或名稱…"
                   style="flex:1;min-width:120px;max-width:260px" oninput="loadGrading()">
            <span id="grade-count" style="font-size:11px;color:var(--mut)"></span>
            <button class="sort-btn" onclick="refreshGrading()" style="padding:4px 10px;margin-left:auto">
              ↺ 重新計算
            </button>
          </div>
        </div>

        <!-- 名詞說明 legend -->
        <div class="grade-legend">
          <div class="legend-row">
            <b>📖 名詞說明</b>
          </div>
          <hr class="legend-sep">
          <div class="legend-row">
            <span class="gbadge gb-S" style="width:20px;height:20px;font-size:10px">S</span>
            <span><b style="color:#ef4444">極高波動</b> — 同規模內前10%，千張大戶每週持股%大幅變動，大戶積極進出</span>
          </div>
          <div class="legend-row">
            <span class="gbadge gb-A" style="width:20px;height:20px;font-size:10px">A</span>
            <span><b style="color:#f97316">高波動</b> — 同規模內前10–30%，大戶頻繁異動</span>
          </div>
          <div class="legend-row">
            <span class="gbadge gb-B" style="width:20px;height:20px;font-size:10px">B</span>
            <span><b style="color:#ca8a04">中波動</b> — 同規模內30–70%，大多數股票的正常水準</span>
          </div>
          <div class="legend-row">
            <span class="gbadge gb-C" style="width:20px;height:20px;font-size:10px">C</span>
            <span><b style="color:#60a5fa">低波動</b> — 同規模內後10–30%，大戶相對穩定持有</span>
          </div>
          <div class="legend-row">
            <span class="gbadge gb-D" style="width:20px;height:20px;font-size:10px">D</span>
            <span><b style="color:#9ca3af">極低波動</b> — 同規模內後10%，大戶幾乎鎖倉不動</span>
          </div>
          <hr class="legend-sep">
          <div class="legend-row" style="gap:16px;flex-wrap:wrap">
            <span>📊 <b>大戶波動度</b> = 每週千張大戶持股%變化量的標準差（越高=進出越頻繁）</span>
            <span>🏦 <b>規模層</b> = 股價 × 總股數 = 市值</span>
            <span>👥 <b>千張</b> = 持股≥1000張（=100萬股）的股東</span>
          </div>
        </div>

        <!-- 分級結果 -->
        <div id="grade-body" class="grade-body">
          <div class="loading"><div class="spinner"></div><span>等待計算…</span></div>
        </div>
      </div>
    </div>

    <!-- 分點籌碼 pane -->
    <div class="pane" id="pane-broker">
      <div style="padding:12px 16px;display:flex;flex-direction:column;gap:10px;height:100%;overflow-y:auto">

        <!-- 模式切換 -->
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          <button class="gf-btn active" id="bmode-stock"    onclick="setBrokerMode('stock')"    style="font-size:12px;padding:4px 14px">個股查詢</button>
          <button class="gf-btn"        id="bmode-trader"   onclick="setBrokerMode('trader')"   style="font-size:12px;padding:4px 14px">券商統計</button>
          <button class="gf-btn"        id="bmode-timeline" onclick="setBrokerMode('timeline')" style="font-size:12px;padding:4px 14px">大戶時間軸</button>
        </div>

        <!-- 控制列：個股模式 -->
        <div id="broker-ctrl-stock" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <input id="broker-stock" class="search" placeholder="股票代號 e.g. 0050" style="width:130px"
                 onkeydown="if(event.key==='Enter')loadBroker()">
          <button class="sort-btn" onclick="setPreset('0050')" style="font-size:11px;padding:3px 8px">0050</button>
          <button class="sort-btn" onclick="setPreset('2330')" style="font-size:11px;padding:3px 8px">2330</button>
          <button class="sort-btn" onclick="setPreset('006208')" style="font-size:11px;padding:3px 8px">006208</button>
          <button class="sort-btn" onclick="setPreset('00878')" style="font-size:11px;padding:3px 8px">00878</button>
          <button class="sort-btn" onclick="brokerPrevDay()" style="padding:3px 8px">←</button>
          <input id="broker-date" type="date" class="search" style="width:140px"
                 onchange="loadBroker()">
          <button class="sort-btn" onclick="brokerNextDay()" style="padding:3px 8px">→</button>
          <button class="sort-btn" onclick="loadBroker()" style="background:var(--acc);color:#000;font-weight:700">查詢</button>
          <div style="display:flex;gap:3px;margin-left:auto">
            <button class="gf-btn active" id="bf-all"    onclick="setBrokerFilter('all')">全部</button>
            <button class="gf-btn"        id="bf-inst"   onclick="setBrokerFilter('inst')">主力</button>
            <button class="gf-btn"        id="bf-retail" onclick="setBrokerFilter('retail')">散戶</button>
          </div>
        </div>

        <!-- 控制列：券商模式 -->
        <div id="broker-ctrl-trader" style="display:none;align-items:center;gap:8px;flex-wrap:wrap">
          <input id="broker-trader-id" class="search" placeholder="券商代碼 e.g. 1020" style="width:130px"
                 onkeydown="if(event.key==='Enter')loadBrokerTrader()">
          <button class="sort-btn" onclick="setTraderPreset('1020')" style="font-size:11px;padding:3px 8px">1020 合庫</button>
          <button class="sort-btn" onclick="setTraderPreset('1440')" style="font-size:11px;padding:3px 8px">1440 元大</button>
          <button class="sort-btn" onclick="setTraderPreset('9200')" style="font-size:11px;padding:3px 8px">9200 富邦</button>
          <button class="sort-btn" onclick="setTraderPreset('6460')" style="font-size:11px;padding:3px 8px">6460 永豐金</button>
          <button class="sort-btn" onclick="brokerPrevDay()" style="padding:3px 8px">←</button>
          <input id="broker-date-trader" type="date" class="search" style="width:140px"
                 onchange="loadBrokerTrader()">
          <button class="sort-btn" onclick="brokerNextDay()" style="padding:3px 8px">→</button>
          <button class="sort-btn" onclick="loadBrokerTrader()" style="background:var(--acc);color:#000;font-weight:700">查詢</button>
          <div style="display:flex;gap:3px;margin-left:auto">
            <button class="gf-btn active" id="btf-all"    onclick="setBrokerFilter('all')">全部</button>
            <button class="gf-btn"        id="btf-inst"   onclick="setBrokerFilter('inst')">主力</button>
            <button class="gf-btn"        id="btf-retail" onclick="setBrokerFilter('retail')">散戶</button>
          </div>
        </div>

        <!-- 控制列：時間軸模式 -->
        <div id="broker-ctrl-timeline" style="display:none;align-items:center;gap:8px;flex-wrap:wrap">
          <input id="tl-stock" class="search" placeholder="股票代號 e.g. 2330" style="width:130px"
                 onkeydown="if(event.key==='Enter')loadTimeline()">
          <button class="sort-btn" onclick="tlSetPreset('2330')" style="font-size:11px;padding:3px 8px">2330</button>
          <button class="sort-btn" onclick="tlSetPreset('2454')" style="font-size:11px;padding:3px 8px">2454</button>
          <button class="sort-btn" onclick="tlSetPreset('2317')" style="font-size:11px;padding:3px 8px">2317</button>
          <button class="sort-btn" onclick="tlSetPreset('0050')" style="font-size:11px;padding:3px 8px">0050</button>
          <input id="tl-start" type="date" class="search" style="width:140px">
          <span style="color:var(--mut);font-size:12px">～</span>
          <input id="tl-end"   type="date" class="search" style="width:140px">
          <button class="sort-btn" onclick="loadTimeline()" style="background:var(--acc);color:#000;font-weight:700">查詢</button>
          <select id="tl-range" class="search" style="width:110px" onchange="tlSetRange(this.value)">
            <option value="20">近20交易日</option>
            <option value="40" selected>近40交易日</option>
            <option value="60">近60交易日</option>
          </select>
        </div>

        <!-- 時間軸圖表區 -->
        <div id="broker-timeline-wrap" style="display:none;flex-direction:column;gap:6px"></div>

        <!-- 摘要卡 -->
        <div id="broker-summary" style="display:none;gap:8px;flex-wrap:wrap"></div>
        <!-- 佔比橫條 -->
        <div id="broker-flow-wrap" style="display:none">
          <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--mut);margin-bottom:3px">
            <span id="bf-inst-lbl"></span><span id="bf-retail-lbl"></span>
          </div>
          <div style="height:6px;border-radius:3px;overflow:hidden;background:var(--bor);display:flex">
            <div id="bf-inst-bar"   style="background:#d29922;height:100%;transition:width .4s"></div>
            <div id="bf-retail-bar" style="background:#58a6ff;height:100%;transition:width .4s"></div>
          </div>
        </div>

        <!-- 表格 -->
        <div id="broker-table-wrap" style="overflow-x:auto;flex:1"></div>
      </div>
    </div>

    <!-- 大戶總覽 pane -->
    <div class="pane" id="pane-overview">
      <div style="padding:8px 16px;border-bottom:1px solid var(--bor);flex-shrink:0">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <span style="font-weight:700;font-size:13px">大戶總覽</span>
          <span id="ov-stock-count" style="font-size:10px;color:var(--mut)"></span>
          <select id="ov-days" class="search" style="width:130px" onchange="ovReload()">
            <option value="10">近10交易日</option>
            <option value="15" selected>近15交易日</option>
            <option value="20">近20交易日</option>
            <option value="30">近30交易日</option>
          </select>
          <select id="ov-sort" class="search" style="width:120px" onchange="ovSort()">
            <option value="active">最活躍</option>
            <option value="buy">主力淨買多</option>
            <option value="sell">主力淨賣多</option>
          </select>
          <button class="sort-btn" onclick="ovReload(true)"
            style="background:var(--acc);color:#000;font-weight:700;padding:4px 14px;font-size:12px">重新掃描</button>
          <button class="sort-btn" onclick="ovToggleCustom()"
            style="font-size:11px;padding:3px 10px" id="ov-custom-btn">自訂股票</button>
        </div>
        <!-- 規模篩選 -->
        <div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:6px;align-items:center">
          <button class="gf-btn active" id="ovt-all"   onclick="ovSetTier('')">全部</button>
          <button class="gf-btn" id="ovt-mega"  onclick="ovSetTier('mega')" >🏢 超大型<small id="ovt-cnt-mega"  style="opacity:.6;margin-left:3px"></small><span style="font-size:9px;opacity:.5;margin-left:4px;font-weight:400">≥1兆</span></button>
          <button class="gf-btn" id="ovt-large" onclick="ovSetTier('large')">🏗 大型<small   id="ovt-cnt-large" style="opacity:.6;margin-left:3px"></small><span style="font-size:9px;opacity:.5;margin-left:4px;font-weight:400">1000–10000億</span></button>
          <button class="gf-btn" id="ovt-mid"   onclick="ovSetTier('mid')"  >🏬 中型<small   id="ovt-cnt-mid"   style="opacity:.6;margin-left:3px"></small><span style="font-size:9px;opacity:.5;margin-left:4px;font-weight:400">100–1000億</span></button>
          <button class="gf-btn" id="ovt-small" onclick="ovSetTier('small')">🏪 小型<small   id="ovt-cnt-small" style="opacity:.6;margin-left:3px"></small><span style="font-size:9px;opacity:.5;margin-left:4px;font-weight:400">20–100億</span></button>
          <button class="gf-btn" id="ovt-micro" onclick="ovSetTier('micro')">🏠 微型<small   id="ovt-cnt-micro" style="opacity:.6;margin-left:3px"></small><span style="font-size:9px;opacity:.5;margin-left:4px;font-weight:400">&lt;20億</span></button>
        </div>
        <!-- 產業類別篩選（規模選定後動態出現） -->
        <div id="ov-industry-row" style="display:none;gap:4px;flex-wrap:wrap;margin-top:6px;align-items:center">
        </div>
        <div id="ov-custom-area" style="display:none;margin-top:6px">
          <textarea id="ov-stocks" class="search"
            style="width:100%;height:52px;resize:vertical;font-family:monospace;font-size:11px;line-height:1.5"
            placeholder="輸入股票代號，逗號或換行分隔（留空=全部股票）"></textarea>
          <button class="sort-btn" onclick="loadOverview()" style="margin-top:4px;font-size:11px">套用</button>
        </div>
      </div>
      <div id="overview-grid"
        style="flex:1;overflow-y:auto;padding:12px;
               display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));
               gap:10px;align-content:start">
        <div class="empty" style="grid-column:1/-1;color:var(--mut)">← 點左側規模按鈕開始掃描</div>
      </div>
    </div>

    <!-- K線分析 pane -->
    <div class="pane" id="pane-kline">
      <div style="padding:10px 14px;display:flex;flex-direction:column;gap:8px;height:100%;overflow-y:auto">
        <!-- Controls -->
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;flex-shrink:0">
          <input id="kline-input" class="search" style="width:110px;font-size:13px" placeholder="代號 e.g. 2330"
            onkeydown="if(event.key==='Enter')klineLoad()"
            oninput="klineAutocomplete(this.value)" autocomplete="off"/>
          <div id="kline-ac" style="display:none;position:absolute;top:42px;left:14px;z-index:200;background:var(--sur2);border:1px solid var(--bor);border-radius:6px;min-width:160px;max-height:180px;overflow-y:auto;box-shadow:0 4px 16px #0006"></div>
          <span id="kline-name" style="font-size:12px;color:var(--mut);min-width:60px"></span>
          <div style="display:flex;gap:4px;margin-left:auto;flex-wrap:wrap">
            <button class="sort-btn kl-range active" id="klr-90"  onclick="klineSetRange(90)">3M</button>
            <button class="sort-btn kl-range"        id="klr-180" onclick="klineSetRange(180)">6M</button>
            <button class="sort-btn kl-range"        id="klr-365" onclick="klineSetRange(365)">1Y</button>
            <button class="sort-btn kl-range"        id="klr-730" onclick="klineSetRange(730)">2Y</button>
          </div>
          <button class="sort-btn" style="background:var(--acc);color:#000;font-weight:700" onclick="klineLoad()">查詢</button>
        </div>
        <!-- Status -->
        <div id="kline-status" style="font-size:11px;color:var(--mut);flex-shrink:0"></div>
        <!-- Candlestick + Volume chart -->
        <div style="flex-shrink:0">
          <div style="font-size:11px;color:var(--mut);margin-bottom:4px;font-weight:600">K線 &amp; 成交量</div>
          <div id="kline-chart-wrap" style="height:320px;border-radius:6px;overflow:hidden;background:#0d1117;position:relative">
            <div id="kline-chart" style="width:100%;height:100%"></div>
          </div>
        </div>
        <!-- Institutional investors chart -->
        <div style="flex-shrink:0">
          <div style="font-size:11px;color:var(--mut);margin-bottom:4px;font-weight:600">三大法人 淨買超（張）
            <span style="margin-left:8px">
              <span style="color:#3b82f6">■</span><span style="font-size:10px">外資</span>
              <span style="margin-left:6px;color:#a855f7">■</span><span style="font-size:10px">投信</span>
              <span style="margin-left:6px;color:#f59e0b">■</span><span style="font-size:10px">自營</span>
            </span>
          </div>
          <div id="kline-inst-wrap" style="height:120px;border-radius:6px;overflow:hidden;background:#0d1117">
            <div id="kline-inst" style="width:100%;height:100%"></div>
          </div>
        </div>
        <!-- Margin chart -->
        <div style="flex-shrink:0;margin-bottom:12px">
          <div style="font-size:11px;color:var(--mut);margin-bottom:4px;font-weight:600">融資融券 餘額（張）
            <span style="margin-left:8px">
              <span style="color:#f87171">■</span><span style="font-size:10px">融資</span>
              <span style="margin-left:6px;color:#34d399">■</span><span style="font-size:10px">融券</span>
            </span>
          </div>
          <div id="kline-margin-wrap" style="height:100px;border-radius:6px;overflow:hidden;background:#0d1117">
            <div id="kline-margin" style="width:100%;height:100%"></div>
          </div>
        </div>
      </div>
    </div>

    <!-- 產業鏈 pane -->
    <div class="pane" id="pane-chain">
      <div style="padding:10px 14px;display:flex;flex-direction:column;gap:8px;height:100%;overflow:hidden">
        <!-- Controls -->
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;flex-shrink:0;position:relative">
          <input id="chain-stock-input" class="search" style="width:110px;font-size:13px" placeholder="代號查產業鏈"
            oninput="chainStockInput(this.value)" onkeydown="if(event.key==='Enter')chainStockCommit()"
            autocomplete="off"/>
          <span id="chain-stock-name" style="font-size:12px;color:var(--mut);min-width:50px"></span>
          <button class="sort-btn" onclick="chainClearStock()">清除</button>
          <span id="chain-status" style="font-size:11px;color:var(--mut);margin-left:auto"></span>
          <div id="chain-ac" style="display:none;position:absolute;top:34px;left:0;z-index:200;background:var(--sur2);border:1px solid var(--bor);border-radius:6px;min-width:160px;max-height:180px;overflow-y:auto;box-shadow:0 4px 16px #0006"></div>
        </div>
        <!-- Stock chain result -->
        <div id="chain-stock-result" style="display:none;flex-shrink:0;overflow-y:auto;max-height:220px;padding:4px 0"></div>
        <!-- Industry browser (two-column) -->
        <div id="chain-browser" style="display:flex;gap:0;flex:1;min-height:0;overflow:hidden;border:1px solid var(--bor);border-radius:8px">
          <div id="chain-ind-list" style="width:130px;flex-shrink:0;overflow-y:auto;border-right:1px solid var(--bor);padding:4px"></div>
          <div id="chain-sub-panel" style="flex:1;overflow-y:auto;padding:8px 10px">
            <div style="color:var(--mut);font-size:12px;padding:20px;text-align:center">← 選擇左側產業</div>
          </div>
        </div>
      </div>
    </div>

  </main>
</div>

<div class="toast" id="toast"></div>

<!-- Bottom navigation (mobile only) -->
<nav class="bottom-nav" id="bottom-nav">
  <button class="bnav-btn" id="bnav-list" onclick="openDrawer()">
    <svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24">
      <path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"/>
    </svg>
    <span>清單</span>
  </button>
  <button class="bnav-btn active" id="bnav-single" onclick="switchTab('single')">
    <svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24">
      <path d="M3 3v18h18"/><path d="M7 16l4-4 4 4 4-7"/>
    </svg>
    <span>個股</span>
  </button>
  <button class="bnav-btn" id="bnav-compare" onclick="switchTab('compare')">
    <svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24">
      <rect x="3" y="3" width="8" height="18" rx="1"/><rect x="13" y="8" width="8" height="13" rx="1"/>
    </svg>
    <span>對比</span>
    <span class="bnav-badge" id="bnav-badge" style="display:none"></span>
  </button>
  <button class="bnav-btn" id="bnav-grade" onclick="switchTab('grade')">
    <svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24">
      <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>
    </svg>
    <span>分級</span>
  </button>
  <button class="bnav-btn" id="bnav-broker" onclick="switchTab('broker')">
    <svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24">
      <path d="M3 21h18M3 10h18M5 6l7-3 7 3M4 10v11M20 10v11M8 14v3M12 14v3M16 14v3"/>
    </svg>
    <span>分點</span>
  </button>
  <button class="bnav-btn" id="bnav-overview" onclick="switchTab('overview')">
    <svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24">
      <rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/>
      <rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>
    </svg>
    <span>總覽</span>
  </button>
  <button class="bnav-btn" id="bnav-kline" onclick="switchTab('kline')">
    <svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24">
      <path d="M9 3v4M9 10v11M15 3v11M15 17v4M6 7h6M12 14h6"/>
    </svg>
    <span>K線</span>
  </button>
  <button class="bnav-btn" id="bnav-chain" onclick="switchTab('chain')">
    <svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24">
      <rect x="2" y="3" width="6" height="5" rx="1"/><rect x="9" y="3" width="6" height="5" rx="1"/><rect x="16" y="3" width="6" height="5" rx="1"/>
      <path d="M5 8v3M5 11h14M19 11V8M12 8v3"/><rect x="9" y="14" width="6" height="5" rx="1"/>
    </svg>
    <span>產業鏈</span>
  </button>
</nav>

<script>
// ── State ──────────────────────────────────────────────────────────────
let allStocks = [];       // [{stock_id, stock_name, type}]
let stockCache = {};      // id -> processed data
let compareSel = [];      // list of stock_ids in compare
let sortKey = 'code';
let activeId = null;
let activeYears = 2;

const COLORS = ['#58a6ff','#3fb950','#f85149','#d29922','#c084fc','#79c0ff','#56d364','#ff7b72'];

// ── Drawer (mobile) ──────────────────────────────────────────────────
function openDrawer() {
  document.getElementById('drawer').classList.add('open');
  document.getElementById('drawer-overlay').classList.add('open');
  document.getElementById('search-drawer').focus();
}
function closeDrawer() {
  document.getElementById('drawer').classList.remove('open');
  document.getElementById('drawer-overlay').classList.remove('open');
}

// ── Init ──────────────────────────────────────────────────────────────
(async () => {
  await loadStockList();
})();

// ── Stock list ────────────────────────────────────────────────────────
async function loadStockList() {
  const r = await fetch('/api/stocks');
  const j = await r.json();
  allStocks = j.stocks;
  const total = `${allStocks.length} 支`;
  document.getElementById('stock-badge').textContent = total;
  document.getElementById('d-badge').textContent = total;
  renderList();
  document.getElementById('ov-stock-count').textContent = '點左側規模按鈕開始掃描';
}

function filterStocks(src) {
  renderList(src);
}

function setSort(key, btn) {
  sortKey = key;
  // sync all sort button groups
  document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
  if (btn) {
    // activate same key in both sidebar and drawer
    document.querySelectorAll('.sort-btn').forEach(b => {
      if (b.textContent.trim() === btn.textContent.trim()) b.classList.add('active');
    });
  }
  renderList();
}

function renderList(src) {
  // gather query from whichever panel triggered
  const qSidebar = (document.getElementById('search')?.value ?? '').trim().toUpperCase();
  const qDrawer  = (document.getElementById('search-drawer')?.value ?? '').trim().toUpperCase();
  const q = src === 'drawer' ? qDrawer : qSidebar;

  // sync the other input
  if (src === 'drawer' && document.getElementById('search'))
    document.getElementById('search').value = document.getElementById('search-drawer').value;
  if (src === 'sidebar' && document.getElementById('search-drawer'))
    document.getElementById('search-drawer').value = document.getElementById('search').value;

  const cnt = document.getElementById('stock-count');
  const cntD = document.getElementById('stock-count-drawer');
  const el  = document.getElementById('stock-list');
  const elD = document.getElementById('stock-list-drawer');

  let list = allStocks;
  if (q) {
    list = list.filter(s => s.stock_id.includes(q) || s.stock_name.includes(q));
  }

  // sort
  if (sortKey === 'bpct') {
    list = [...list].sort((a, b) => {
      const av = stockCache[a.stock_id]?.latest?.bpct ?? -Infinity;
      const bv = stockCache[b.stock_id]?.latest?.bpct ?? -Infinity;
      return bv - av;
    });
  } else if (sortKey === 'chg') {
    list = [...list].sort((a, b) => {
      const av = stockCache[a.stock_id]?.latest?.bpct_chg ?? -Infinity;
      const bv = stockCache[b.stock_id]?.latest?.bpct_chg ?? -Infinity;
      return bv - av;
    });
  } else if (sortKey === 'bp') {
    list = [...list].sort((a, b) => {
      const av = stockCache[a.stock_id]?.latest?.bp ?? -Infinity;
      const bv = stockCache[b.stock_id]?.latest?.bp ?? -Infinity;
      return bv - av;
    });
  } else {
    list = [...list].sort((a, b) => a.stock_id.localeCompare(b.stock_id));
  }

  const label = `${list.length} / ${allStocks.length} 支`;
  if (cnt)  cnt.textContent  = `顯示 ${label}`;
  if (cntD) cntD.textContent = `顯示 ${label}`;

  const html = list.map(s => {
    const d = stockCache[s.stock_id];
    const inCmp = compareSel.includes(s.stock_id);
    const isAct = s.stock_id === activeId;
    let metricsHtml = '';
    if (d && d.latest) {
      const chg = d.latest.bpct_chg ?? 0;
      const cls = chg > 0 ? 'pct-up' : chg < 0 ? 'pct-dn' : '';
      const sign = chg > 0 ? '+' : '';
      metricsHtml = `
        <div class="stock-metrics">
          <div class="pct-val ${cls}">${d.latest.bpct?.toFixed(1) ?? '—'}%</div>
          <div class="pct-chg ${cls}">${sign}${chg.toFixed(2)}</div>
        </div>`;
    }
    return `<div class="stock-item ${isAct ? 'active' : ''} ${inCmp && !isAct ? 'compare-sel' : ''}"
              onclick="selectStock('${s.stock_id}')" id="si-${s.stock_id}">
      <div class="stock-info">
        <div class="stock-code">${s.stock_id}</div>
        <div class="stock-name">${s.stock_name}</div>
      </div>
      ${metricsHtml}
      <div class="add-compare ${inCmp ? 'added' : ''}"
           onclick="toggleCompare(event,'${s.stock_id}')"
           title="${inCmp ? '移出比較' : '加入比較'}">
        ${inCmp ? '✓' : '+'}
      </div>
    </div>`;
  }).join('');

  if (el)  el.innerHTML  = html;
  if (elD) elD.innerHTML = html;
}

// ── Select single stock ────────────────────────────────────────────────
async function selectStock(sid) {
  activeId = sid;
  closeDrawer();           // close on mobile
  switchTab('single');
  renderList();

  const wrap = document.getElementById('single-wrap');
  wrap.innerHTML = `<div class="loading"><div class="spinner"></div><span>載入 ${sid} 資料中...</span></div>`;

  let data = stockCache[sid];
  if (!data) {
    const years = document.getElementById('years-sel').value;
    const r = await fetch(`/api/stock/${sid}?years=${years}`);
    data = await r.json();
    if (data.error) {
      wrap.innerHTML = `<div class="empty">${data.error}</div>`;
      return;
    }
    stockCache[sid] = data;
    renderList();
  }
  renderSingle(data);
}

function onYearsChange() {
  activeYears = parseInt(document.getElementById('years-sel').value);
  if (activeId) {
    delete stockCache[activeId];
    selectStock(activeId);
  }
}

// ── Render single stock ─────────────────────────────────────────────────
function renderSingle(d) {
  const l = d.latest;
  const chg = l.bpct_chg ?? 0;
  const chgCls = chg > 0 ? 'pct-up' : chg < 0 ? 'pct-dn' : '';
  const sign = chg > 0 ? '+' : '';
  const bpChg = l.bp_chg ?? 0;

  document.getElementById('single-wrap').innerHTML = `
    <div class="stat-row">
      <div class="stat-card">
        <div class="stat-label tip" data-tip="持股≥1000張（=100萬股）的投資人\n合計持有的股份佔全公司比例\n比例越高表示籌碼越集中在大戶手中">
          千張大戶持股% <span class="tip-i">?</span></div>
        <div class="stat-val ${chgCls}">${l.bpct?.toFixed(2) ?? '—'}%</div>
        <div class="stat-sub ${chgCls} tip" data-tip="與上一週相比的變化量（百分點 pp）\n正值=大戶本週增持，負值=大戶減持">${sign}${chg.toFixed(3)} pp 週增減 <span class="tip-i">?</span></div>
      </div>
      <div class="stat-card">
        <div class="stat-label tip" data-tip="持有≥1000張的股東人數\n人數增加=新大戶進場，減少=大戶出場\n搭配持股%一起看才準確">
          千張大戶人數 <span class="tip-i">?</span></div>
        <div class="stat-val">${l.bp?.toLocaleString() ?? '—'}</div>
        <div class="stat-sub ${bpChg > 0 ? 'pct-up' : bpChg < 0 ? 'pct-dn' : ''}">${bpChg > 0 ? '+' : ''}${bpChg ?? 0} 人/週</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">週收盤價</div>
        <div class="stat-val" style="color:var(--blu)">${l.close?.toFixed(0) ?? '—'}</div>
        <div class="stat-sub">${d.stock_id} ${d.stock_name}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label tip" data-tip="FinMind 取得的週收盤價歷史筆數\n資料來源：台灣證交所 每週四公布">
          資料期間 <span class="tip-i">?</span></div>
        <div class="stat-val" style="font-size:1rem">${d.weeks} 週</div>
        <div class="stat-sub">截至 ${l.date}</div>
      </div>
    </div>
    <div class="charts-row">
      <div class="chart-box" style="flex:2">
        <div class="chart-title">${d.stock_id} ${d.stock_name}｜千張大戶持股% + 收盤價</div>
        <div id="chart-main" style="height:calc(100% - 32px)"></div>
      </div>
      <div class="chart-box lag-box" style="display:flex;flex-direction:column">
        <div class="chart-title tip" data-tip="Lead-Lag（領先滯後）分析：\n計算千張大戶持股%週增減\n與不同時間點股價報酬的相關係數(r)\n\n大戶先行N週 → r>0 代表大戶增持後\n  N週股價傾向上漲（大戶是領先指標）\n同期 → 大戶增減與同週股價的相關\n股價先行N週 → 股價先漲跌 大戶才跟進\n\n★ = 統計顯著(p<0.05) 較可信
">Lead-Lag 分析 <span class="tip-i">?</span></div>
        <div id="chart-lag" style="flex:1;min-height:0"></div>
        <div class="lag-note">
          <b style="color:var(--acc)">大戶先行</b> → 大戶增持後N週股價是否上漲 ｜
          <b style="color:var(--txt)">同期</b> → 同週相關 ｜
          <b style="color:var(--blu)">股價先行</b> → 股價先漲 大戶才跟進 ｜
          <b style="color:var(--yel)">★</b> 統計顯著 p&lt;0.05
        </div>
      </div>
    </div>`;

  plotMain(d);
  plotLag(d);
}

// ── Plotly: main dual-axis ─────────────────────────────────────────────
const PLY = {paper_bgcolor:'transparent', plot_bgcolor:'transparent',
             font:{color:'#8b949e',size:11}, margin:{t:10,r:60,b:40,l:50},
             xaxis:{gridcolor:'#21262d',zeroline:false},
             showlegend:true, legend:{bgcolor:'transparent',font:{size:10}}};

function plotMain(d) {
  const dates = d.history.map(r => r.date);
  const bpct  = d.history.map(r => r.bpct);
  const close = d.history.map(r => r.close);
  const bpchg = d.history.map(r => r.bpct_chg ?? 0);

  const t1 = {x:dates, y:bpct,  name:'千張大戶%', type:'scatter', mode:'lines',
              line:{color:'#3fb950',width:2}, yaxis:'y', fill:'tozeroy',
              fillcolor:'rgba(63,185,80,.1)'};
  const t2 = {x:dates, y:close, name:'收盤價',    type:'scatter', mode:'lines',
              line:{color:'#58a6ff',width:1.5}, yaxis:'y2'};
  const t3 = {x:dates, y:bpchg, name:'週增減', type:'bar',
              marker:{color:bpchg.map(v => v >= 0 ? 'rgba(63,185,80,.6)' : 'rgba(248,81,73,.6)')},
              yaxis:'y3', visible:'legendonly'};

  const layout = {
    ...PLY,
    yaxis: {title:'千張大戶%', gridcolor:'#21262d', zeroline:false, titlefont:{color:'#3fb950'}},
    yaxis2:{title:'收盤價', overlaying:'y', side:'right', gridcolor:'transparent',
            zeroline:false, titlefont:{color:'#58a6ff'}},
    yaxis3:{overlaying:'y', side:'right', showgrid:false, showticklabels:false, zeroline:true,
            zerolinecolor:'#30363d'},
    hovermode:'x unified',
  };
  Plotly.newPlot('chart-main', [t1, t2, t3], layout, {responsive:true, displayModeBar:false});
}

// ── Plotly: lead-lag bar ───────────────────────────────────────────────
function plotLag(d) {
  const lags  = d.lag_analysis;
  const x     = lags.map(l => l.label);
  const y     = lags.map(l => l.r);
  const clrs  = y.map((v, i) => lags[i].sig ? (v > 0 ? '#3fb950' : '#f85149') : '#30363d');
  const text  = lags.map(l => l.sig ? '★' : '');

  Plotly.newPlot('chart-lag', [{
    type:'bar', x, y, text, textposition:'outside',
    marker:{color:clrs},
    hovertemplate:'%{x}<br>r = %{y:.3f}<extra></extra>',
  }], {
    ...PLY, margin:{t:10,r:20,b:70,l:50},
    xaxis:{tickangle:-40, gridcolor:'transparent', tickfont:{size:9}},
    yaxis:{range:[-0.7,0.7], gridcolor:'#21262d', zeroline:true, zerolinecolor:'#30363d'},
    shapes:[{type:'line', x0:-0.5, x1:lags.length-0.5, y0:0, y1:0,
             line:{color:'#30363d',width:1}}],
    annotations:[{text:'★ = p<0.05', xref:'paper', yref:'paper',
                  x:0.98, y:0.98, xanchor:'right', showarrow:false,
                  font:{color:'#d29922',size:10}}],
    showlegend:false,
  }, {responsive:true, displayModeBar:false});
}

// ── Compare ────────────────────────────────────────────────────────────
function toggleCompare(e, sid) {
  e.stopPropagation();
  const idx = compareSel.indexOf(sid);
  if (idx >= 0) {
    compareSel.splice(idx, 1);
  } else {
    if (compareSel.length >= 8) { showToast('最多比較 8 支股票', true); return; }
    compareSel.push(sid);
  }
  updateCompareCount();
  renderList();
  if (document.getElementById('pane-compare').classList.contains('active')) {
    updateCompare();
  }
}

function updateCompareCount() {
  const n = compareSel.length;
  const el = document.getElementById('compare-count');
  if (el) el.textContent = n > 0 ? n : '';
  // bottom nav badge
  const badge = document.getElementById('bnav-badge');
  if (badge) {
    badge.textContent = n;
    badge.style.display = n > 0 ? '' : 'none';
  }
}

function removeFromCompare(sid) {
  const idx = compareSel.indexOf(sid);
  if (idx >= 0) compareSel.splice(idx, 1);
  updateCompareCount();
  renderList();
  renderCompareChips();
  updateCompare();
}

function renderCompareChips() {
  const el = document.getElementById('compare-chips');
  if (compareSel.length === 0) {
    el.innerHTML = '<span style="color:var(--mut);font-size:11px;font-style:italic">點選股票右側 + 加入比較</span>';
    return;
  }
  el.innerHTML = compareSel.map((sid, i) => {
    const s = allStocks.find(x => x.stock_id === sid);
    const name = s ? s.stock_name : sid;
    const color = COLORS[i % COLORS.length];
    return `<span class="chip" style="border-color:${color}40">
      <span style="color:${color};font-weight:700">${sid}</span>
      <span style="color:var(--mut)">${name}</span>
      <span class="chip-rm" onclick="removeFromCompare('${sid}')">×</span>
    </span>`;
  }).join('');
}

async function updateCompare() {
  renderCompareChips();
  const body = document.getElementById('compare-body');
  if (compareSel.length === 0) {
    body.innerHTML = `<div class="empty">
      <svg width="40" height="40" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24">
        <rect x="3" y="3" width="18" height="18" rx="2"/><path d="M8 12h8M12 8v8"/>
      </svg><span>尚無選擇股票</span></div>`;
    return;
  }

  body.innerHTML = '<div class="loading"><div class="spinner"></div><span>載入比較資料...</span></div>';
  const years = document.getElementById('compare-years').value;

  const r = await fetch('/api/compare', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({stocks: compareSel, years: parseInt(years)})
  });
  const j = await r.json();

  if (!j.stocks || j.stocks.length === 0) {
    body.innerHTML = '<div class="empty">無法取得比較資料</div>';
    return;
  }

  body.innerHTML = `
    <div class="compare-charts-row" style="flex:1;min-height:0">
      <div class="chart-box" style="flex:1">
        <div class="chart-title">標準化股價（= 100 at 起始）</div>
        <div id="cmp-price" style="height:calc(100% - 32px)"></div>
      </div>
      <div class="chart-box" style="flex:1">
        <div class="chart-title">千張大戶持股%</div>
        <div id="cmp-bpct" style="height:calc(100% - 32px)"></div>
      </div>
    </div>
    <div class="compare-table-wrap">
      <table class="ctable" id="cmp-table"></table>
    </div>`;

  plotCompare(j.stocks);
}

function plotCompare(stocks) {
  const traces_price = [], traces_bpct = [];
  stocks.forEach((s, i) => {
    const color = COLORS[i % COLORS.length];
    const dates = s.rows.map(r => r.date);
    traces_price.push({
      x: dates, y: s.rows.map(r => r.norm), name: `${s.stock_id} ${s.stock_name}`,
      type:'scatter', mode:'lines', line:{color, width:2},
      hovertemplate:'%{x}<br>%{y:.1f}<extra>' + s.stock_id + '</extra>',
    });
    traces_bpct.push({
      x: dates, y: s.rows.map(r => r.bpct), name: `${s.stock_id} ${s.stock_name}`,
      type:'scatter', mode:'lines', line:{color, width:2},
      hovertemplate:'%{x}<br>%{y:.2f}%<extra>' + s.stock_id + '</extra>',
    });
  });

  const commonLayout = {...PLY, hovermode:'x unified',
                        legend:{bgcolor:'transparent',font:{size:10},orientation:'h',y:-0.15}};
  Plotly.newPlot('cmp-price', traces_price, {
    ...commonLayout,
    yaxis:{gridcolor:'#21262d', zeroline:false, title:'標準化價格'},
    shapes:[{type:'line',x0:0,x1:1,xref:'paper',y0:100,y1:100,
             line:{color:'#30363d',width:1,dash:'dot'}}],
  }, {responsive:true, displayModeBar:false});

  Plotly.newPlot('cmp-bpct', traces_bpct, {
    ...commonLayout,
    yaxis:{gridcolor:'#21262d', zeroline:false, title:'千張大戶%'},
  }, {responsive:true, displayModeBar:false});

  // Table
  const tbl = document.getElementById('cmp-table');
  tbl.innerHTML = `<tr>
    <th style="text-align:left">代號</th><th>名稱</th><th>千張大戶%</th>
    <th>週增減pp</th><th>人數</th><th>收盤價</th><th>期間漲跌%</th>
  </tr>` + stocks.map((s, i) => {
    const color = COLORS[i % COLORS.length];
    const first = s.rows[0]?.close ?? 0;
    const last  = s.rows.at(-1)?.close ?? 0;
    const perf  = first > 0 ? ((last - first) / first * 100).toFixed(1) : '—';
    const perfCls = parseFloat(perf) > 0 ? 'pct-up' : parseFloat(perf) < 0 ? 'pct-dn' : '';
    const lastR = s.rows.at(-1) ?? {};
    const chg = lastR.pct_chg ?? 0;
    const chgCls = chg > 0 ? 'pct-up' : chg < 0 ? 'pct-dn' : '';
    return `<tr>
      <td><span style="color:${color};font-weight:700">${s.stock_id}</span></td>
      <td style="text-align:left;color:var(--mut)">${s.stock_name}</td>
      <td>${s.latest_bpct?.toFixed(2) ?? '—'}%</td>
      <td class="${chgCls}">${chg > 0 ? '+' : ''}${chg.toFixed(3)}</td>
      <td>${lastR.bp?.toLocaleString() ?? '—'}</td>
      <td style="color:var(--blu)">${s.latest_close?.toFixed(0) ?? '—'}</td>
      <td class="${perfCls}">${chg >= 0 ? '+' : ''}${perf}%</td>
    </tr>`;
  }).join('');
}

// ── Tab switch ─────────────────────────────────────────────────────────
function switchTab(name) {
  // desktop tabs
  const tabNames = ['single','compare','grade','broker','overview','kline','chain'];
  document.querySelectorAll('.tab').forEach((t, i) => {
    t.classList.toggle('active', tabNames[i] === name);
  });
  // panes
  document.querySelectorAll('.pane').forEach(p => p.classList.remove('active'));
  document.getElementById(`pane-${name}`).classList.add('active');
  // bottom nav
  document.querySelectorAll('.bnav-btn').forEach(b => b.classList.remove('active'));
  const navBtn = document.getElementById(`bnav-${name}`);
  if (navBtn) navBtn.classList.add('active');

  if (name === 'compare') {
    renderCompareChips();
    if (compareSel.length > 0) updateCompare();
  }
  if (name === 'grade') {
    loadGrading();
    fetch('/api/grading/status').then(r => r.json()).then(s => {
      if (s.running && !gradePolling) {
        gradePolling = setInterval(pollGradingProgress, 3000);
      }
    });
  }
  if (name === 'broker') {
    // set today's date if empty
    const inp = document.getElementById('broker-date');
    if (!inp.value) inp.value = new Date().toISOString().slice(0,10);
  }
  if (name === 'overview' && allStocks.length) {
    _ovLoadTierCounts();
    if (_ovData !== null) _ovStartPoll();  // resume polling if scan is still running
    // no auto-scan: user picks tier first
  }
  if (name === 'chain') chainLoad();
  setTimeout(() => Plotly.Plots.resize(), 80);
}

// ── 分點籌碼 ──────────────────────────────────────────────────────────
let _brokerData = null;
let _brokerFilter = 'all';
let _brokerSort = { col: 'net', asc: false };
let _brokerMode = 'stock';

function setBrokerMode(mode) {
  _brokerMode = mode;
  ['stock','trader','timeline'].forEach(m => {
    document.getElementById('bmode-'+m).classList.toggle('active', m === mode);
    document.getElementById('broker-ctrl-'+m).style.display = m === mode ? 'flex' : 'none';
  });
  document.getElementById('broker-summary').style.display = 'none';
  document.getElementById('broker-flow-wrap').style.display = 'none';
  document.getElementById('broker-table-wrap').innerHTML = '';
  document.getElementById('broker-timeline-wrap').style.display = 'none';
  document.getElementById('broker-timeline-wrap').innerHTML = '';
  if (mode === 'trader') {
    const d = document.getElementById('broker-date-trader');
    if (!d.value) d.value = document.getElementById('broker-date').value;
  }
  if (mode === 'timeline') {
    const end = new Date(); end.setDate(end.getDate() - 1);
    const start = new Date(end); start.setDate(start.getDate() - 55);
    document.getElementById('tl-end').value   = end.toISOString().slice(0,10);
    document.getElementById('tl-start').value = start.toISOString().slice(0,10);
  }
}

function tlSetPreset(id) {
  document.getElementById('tl-stock').value = id;
  loadTimeline();
}

function tlSetRange(days) {
  const end = new Date(); end.setDate(end.getDate() - 1);
  const start = new Date(end); start.setDate(start.getDate() - Math.round(days * 1.45));
  document.getElementById('tl-end').value   = end.toISOString().slice(0,10);
  document.getElementById('tl-start').value = start.toISOString().slice(0,10);
}

async function loadTimeline() {
  const stock = document.getElementById('tl-stock').value.trim().toUpperCase();
  const start = document.getElementById('tl-start').value;
  const end   = document.getElementById('tl-end').value;
  if (!stock) { showToast('請輸入股票代號'); return; }
  const wrap = document.getElementById('broker-timeline-wrap');
  wrap.style.display = 'flex';
  wrap.innerHTML = '<div class="empty" style="padding:40px">抓取多日分點資料中，請稍候…</div>';
  try {
    const res = await fetch(`/api/big_player_timeline?stock_id=${encodeURIComponent(stock)}&start_date=${start}&end_date=${end}`);
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      wrap.innerHTML = `<div class="empty" style="color:var(--red)">${err.detail || '載入失敗'}</div>`;
      return;
    }
    const d = await res.json();
    if (!d.timeline || !d.timeline.length) {
      wrap.innerHTML = '<div class="empty">此區間無大戶活動資料</div>';
      return;
    }
    renderTimeline(d.timeline, stock);
  } catch(e) {
    wrap.innerHTML = `<div class="empty" style="color:var(--red)">錯誤：${e.message}</div>`;
  }
}

function renderTimeline(tl, stock) {
  const wrap = document.getElementById('broker-timeline-wrap');

  const total_buy  = tl.reduce((s,d) => s+d.buy_score,  0);
  const total_sell = tl.reduce((s,d) => s+d.sell_score, 0);
  const total_net  = total_buy - total_sell;
  const netColor   = total_net > 0 ? '#f85149' : total_net < 0 ? '#3fb950' : '#8b949e';

  wrap.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:4px">
      <span style="font-weight:700;font-size:13px">${stock} 大戶時間軸</span>
      <div style="display:flex;gap:12px;font-size:12px">
        <span style="color:#f85149">主力買 +${total_buy}</span>
        <span style="color:#3fb950">主力賣 -${total_sell}</span>
        <span style="color:${netColor};font-weight:700">淨 ${total_net>0?'+':''}${total_net}</span>
      </div>
    </div>
    <div style="font-size:10px;color:#8b949e;margin-bottom:6px">每格 = 800萬 NTD｜紅漲綠跌（台股慣例）</div>
    <div id="tl-chart" style="width:100%;height:620px"></div>`;

  const dates       = tl.map(d => d.date);
  const closes      = tl.map(d => d.close  || null);
  const opens       = tl.map(d => d.open   || null);
  const highs       = tl.map(d => d.high   || null);
  const lows        = tl.map(d => d.low    || null);
  const volumes     = tl.map(d => d.volume || 0);
  const buyScores   = tl.map(d =>  d.buy_score);
  const sellScores  = tl.map(d => -d.sell_score);
  const retailBuys  = tl.map(d =>  (d.retail_buy  || 0));
  const retailSells = tl.map(d => -(d.retail_sell || 0));
  const hasPrice    = closes.some(c => c && c > 0);
  const hasVolume   = volumes.some(v => v > 0);

  const traces = [];

  // 成交量 (overlay on price panel, right axis, scaled to bottom 20%)
  if (hasVolume) {
    traces.push({
      type: 'bar', x: dates, y: volumes,
      name: '成交量(張)', marker: { color: 'rgba(88,166,255,0.22)' },
      xaxis: 'x', yaxis: 'y4',
      hovertemplate: '%{x}<br>成交量 %{y}張<extra></extra>',
    });
  }

  // K線
  if (hasPrice) {
    traces.push({
      type: 'candlestick',
      x: dates, open: opens, high: highs, low: lows, close: closes,
      name: '股價',
      increasing: { line: { color: '#f85149', width: 1 }, fillcolor: '#f85149' },
      decreasing: { line: { color: '#3fb950', width: 1 }, fillcolor: '#3fb950' },
      xaxis: 'x', yaxis: 'y',
      showlegend: false,
    });
  }

  // 主力強度
  traces.push({
    type: 'bar', x: dates, y: buyScores,
    name: '主力買', marker: { color: 'rgba(248,81,73,0.80)' },
    xaxis: 'x', yaxis: 'y2',
    hovertemplate: '%{x}<br>主力買 +%{y}<extra></extra>',
  });
  traces.push({
    type: 'bar', x: dates, y: sellScores,
    name: '主力賣', marker: { color: 'rgba(63,185,80,0.80)' },
    xaxis: 'x', yaxis: 'y2',
    hovertemplate: '%{x}<br>主力賣 %{y}<extra></extra>',
  });

  // 散戶
  traces.push({
    type: 'bar', x: dates, y: retailBuys,
    name: '散戶買', marker: { color: 'rgba(248,81,73,0.45)' },
    xaxis: 'x', yaxis: 'y3',
    hovertemplate: '%{x}<br>散戶買 +%{y}張<extra></extra>',
  });
  traces.push({
    type: 'bar', x: dates, y: retailSells,
    name: '散戶賣', marker: { color: 'rgba(63,185,80,0.45)' },
    xaxis: 'x', yaxis: 'y3',
    hovertemplate: '%{x}<br>散戶賣 %{y}張<extra></extra>',
  });

  // domain: 價格[0.54,1.0]  主力[0.30,0.51]  散戶[0,0.26]
  const pD = [0.54, 1.00];
  const iD = [0.30, 0.51];
  const rD = [0.00, 0.26];
  const maxVol = hasVolume ? Math.max(...volumes) : 1;

  const layout = {
    paper_bgcolor: '#0d1117',
    plot_bgcolor:  '#161b22',
    font:   { color: '#e6edf3', size: 11, family: 'system-ui,sans-serif' },
    margin: { l: 55, r: 55, t: 8, b: 40 },
    xaxis: {
      type: 'category',
      tickfont:    { size: 10 },
      gridcolor:   '#30363d',
      linecolor:   '#30363d',
      rangeslider: { visible: false },
      nticks: Math.min(tl.length, 20),
      anchor: 'y3',
    },
    yaxis: {
      title:     { text: '股價', font: { size: 10 } },
      gridcolor: '#30363d', linecolor: '#30363d',
      domain: pD, tickfont: { size: 10 },
      visible: hasPrice,
    },
    yaxis2: {
      title:     { text: '主力強度', font: { size: 10 } },
      gridcolor: '#30363d', linecolor: '#30363d',
      domain: iD,
      zeroline: true, zerolinecolor: '#8b949e', zerolinewidth: 1,
      tickfont: { size: 10 },
    },
    yaxis3: {
      title:     { text: '散戶(張)', font: { size: 10 } },
      gridcolor: '#30363d', linecolor: '#30363d',
      domain: rD,
      zeroline: true, zerolinecolor: '#8b949e', zerolinewidth: 1,
      tickfont: { size: 10 },
    },
    yaxis4: {
      overlaying: 'y',
      side: 'right',
      showgrid: false, showticklabels: false,
      domain: pD,
      range: [0, maxVol * 5],
      visible: hasVolume,
    },
    barmode: 'relative',
    legend: { orientation: 'h', x: 0, y: -0.06, font: { size: 10 } },
    hovermode: 'x unified',
    dragmode:  'pan',
  };

  Plotly.newPlot('tl-chart', traces, layout, {
    displayModeBar: true,
    modeBarButtonsToRemove: ['select2d','lasso2d','autoScale2d'],
    responsive: true,
    scrollZoom: true,
  });
}

// ── 大戶總覽 ──────────────────────────────────────────────────────────────
let _ovData         = null;
let _ovPollTimer    = null;
let _ovTier         = '';
let _ovIndustry     = '';

async function ovSetTier(tier) {
  _ovTier = tier;
  _ovIndustry = '';
  document.querySelectorAll('[id^="ovt-"]').forEach(b => b.classList.remove('active'));
  document.getElementById('ovt-' + (tier || 'all'))?.classList.add('active');
  document.getElementById('ov-industry-row').style.display = 'none';

  if (tier) {
    const countEl = document.getElementById('ov-stock-count');
    try {
      const gr = await fetch('/api/grading/status').then(r => r.json());
      if ((gr.ready || 0) === 0) {
        // 完全沒有分層資料（連市值預分層都沒有）才阻擋
        if (countEl) countEl.textContent = '分級資料尚未載入，請稍後再試';
        return;
      }
      // gr.ready > 0：預分層已就緒，可以掃描；若 gr.running 還在跑則只顯示提示
      if (gr.running && countEl) {
        const pct = gr.total ? Math.round(gr.done / gr.total * 100) : 0;
        countEl.textContent = `分層就緒（波動度計算中 ${pct}%）`;
      }
    } catch(e) {}
    _ovLoadIndustries(tier);  // async, shows industry buttons after load
  }
  ovReload(true);
}

async function _ovLoadTierCounts() {
  try {
    const r = await fetch('/api/grading?');
    if (!r.ok) return;
    const d = await r.json();
    for (const t of (d.tiers || [])) {
      const el = document.getElementById(`ovt-cnt-${t.key}`);
      if (el) el.textContent = t.count;
    }
  } catch(e) {}
}

async function _ovLoadIndustries(tier) {
  const row = document.getElementById('ov-industry-row');
  row.style.display = 'none';
  if (!tier) return;
  try {
    const r = await fetch(`/api/industries?tier=${tier}`);
    if (!r.ok) return;
    const d = await r.json();
    const inds = (d.industries || []);
    if (!inds.length) return;
    row.innerHTML = '<span style="font-size:11px;color:var(--mut);flex-shrink:0;margin-right:2px">產業：</span>'
      + '<button class="gf-btn active" id="ovi-all" onclick="ovSetIndustry(\'\')">全部</button>'
      + inds.map(({name, count}) => {
          const safe = name.replace(/\\/g,'\\\\').replace(/'/g,"\\'");
          return `<button class="gf-btn ovi-btn" data-ind="${name}"
            onclick="ovSetIndustry('${safe}')">${name}<small style="opacity:.6;margin-left:3px">${count}</small></button>`;
        }).join('');
    row.style.display = 'flex';
  } catch(e) {}
}

function ovSetIndustry(industry) {
  _ovIndustry = industry;
  const row = document.getElementById('ov-industry-row');
  row.querySelectorAll('.gf-btn').forEach(b => b.classList.remove('active'));
  if (!industry) {
    document.getElementById('ovi-all')?.classList.add('active');
  } else {
    row.querySelectorAll('.ovi-btn').forEach(b => {
      if (b.dataset.ind === industry) b.classList.add('active');
    });
  }
  ovReload(true);
}

function ovToggleCustom() {
  const area = document.getElementById('ov-custom-area');
  const btn  = document.getElementById('ov-custom-btn');
  const open = area.style.display === 'none';
  area.style.display = open ? 'block' : 'none';
  btn.style.background = open ? 'var(--acc)' : '';
  btn.style.color      = open ? '#000' : '';
}

function _ovStopPoll() {
  if (_ovPollTimer) { clearInterval(_ovPollTimer); _ovPollTimer = null; }
}

async function ovReload(force = true) {
  _ovStopPoll();
  const days = document.getElementById('ov-days').value;
  const countEl = document.getElementById('ov-stock-count');
  if (countEl) countEl.textContent = '啟動掃描…';
  document.getElementById('overview-grid').innerHTML =
    '<div class="empty" style="grid-column:1/-1;padding:30px">後台掃描中，每完成一批即更新…</div>';
  const resp = await fetch(`/api/overview_scan/start?days=${days}&force=${force}&tier=${_ovTier}&industry=${encodeURIComponent(_ovIndustry)}`, { method: 'POST' });
  const data = await resp.json();
  if (data.status === 'grading_not_ready') {
    const pct = data.ready && _ovTier ? `（已有 ${data.ready} 支，分級計算中）` : '';
    if (countEl) countEl.textContent = `分級資料尚未準備好${pct}，請稍後再試`;
    document.getElementById('overview-grid').innerHTML =
      '<div class="empty" style="grid-column:1/-1;color:var(--mut)">← 點左側規模按鈕開始掃描</div>';
    return;
  }
  _ovStartPoll();
}

// For custom stock list, fall back to the old batched approach
async function loadOverview() {
  const raw    = document.getElementById('ov-stocks').value;
  const allIds = raw.split(/[\s,\n]+/).map(s => s.trim().toUpperCase()).filter(Boolean);
  if (!allIds.length) { ovReload(false); return; }
  _ovStopPoll();
  const days    = document.getElementById('ov-days').value;
  const grid    = document.getElementById('overview-grid');
  const countEl = document.getElementById('ov-stock-count');
  const BATCH   = 50;
  const batches = [];
  for (let i = 0; i < allIds.length; i += BATCH) batches.push(allIds.slice(i, i + BATCH));
  _ovData = { results: [], dates: [] };
  let loaded = 0;
  grid.innerHTML = '';
  const progDiv = Object.assign(document.createElement('div'),
    { id: 'ov-progress', className: 'empty',
      style: 'grid-column:1/-1;padding:20px;font-size:12px' });
  grid.appendChild(progDiv);
  for (const batch of batches) {
    progDiv.textContent = `載入中 ${loaded} / ${allIds.length} 支…`;
    if (countEl) countEl.textContent = progDiv.textContent;
    try {
      const res = await fetch(`/api/multi_timeline?stocks=${batch.join(',')}&days=${days}`);
      if (res.ok) {
        const bd = await res.json();
        _ovData.results.push(...bd.results);
        if ((bd.dates || []).length > _ovData.dates.length) _ovData.dates = bd.dates;
      }
    } catch(e) { /* skip */ }
    loaded += batch.length;
    progDiv.remove();
    renderOverviewGrid();
    if (loaded < allIds.length) grid.appendChild(progDiv);
  }
  if (countEl) countEl.textContent = `共 ${_ovData.results.length} 支`;
}

function _ovStartPoll() {
  _ovPollTimer = setInterval(_ovPollTick, 3000);
  _ovPollTick();
}

async function _ovPollTick() {
  try {
    const r  = await fetch('/api/overview_scan/status');
    const st = await r.json();
    const countEl = document.getElementById('ov-stock-count');

    _ovData = { results: st.results, dates: [], skip: st.skip || 0, last_err: st.last_err || '' };
    renderOverviewGrid();

    const pct  = st.total ? Math.round(st.done / st.total * 100) : 0;
    const skip = st.skip || 0;
    const rateErr = skip > 0 && (st.last_err || '').includes('402');
    const skipTxt = skip ? (rateErr ? ` ⚠ API限制跳過${skip}支` : ` 跳過${skip}`) : '';
    const info = st.running
      ? `掃描中 ${st.done}/${st.total}（${pct}%）${skipTxt}`
      : `共 ${st.results.length} 支・${st.started} 完成${skipTxt}`;
    if (countEl) countEl.textContent = info;
    if (skip > 0 && rateErr && countEl) countEl.title = `FinMind API 每日呼叫次數已達上限，請明天再試或升級方案。\n最後錯誤：${st.last_err}`;
    else if (countEl) countEl.title = st.last_err || '';
    if (st.last_err) console.warn('[OV scan error]', st.last_err);

    if (!st.running) _ovStopPoll();
  } catch(e) { /* ignore transient errors */ }
}

function ovSort() { if (_ovData) renderOverviewGrid(); }

const OV_TIERS = [
  { key: 'mega',  label: '超大型', icon: '🏢', range: '> 1兆'   },
  { key: 'large', label: '大型',   icon: '🏗',  range: '> 1000億' },
  { key: 'mid',   label: '中型',   icon: '🏬', range: '> 100億'  },
  { key: 'small', label: '小型',   icon: '🏪', range: '> 20億'   },
  { key: 'micro', label: '微型',   icon: '🏠', range: '< 20億'   },
];

function _ovCard(item) {
  const info     = allStocks.find(s => s.stock_id === item.stock_id);
  const name     = info?.stock_name || '';
  const net      = item.total_net;
  const netColor = net > 0 ? '#f85149' : net < 0 ? '#3fb950' : '#8b949e';
  const netStr   = net > 0 ? '+'+net : ''+net;
  const closeStr = item.latest_close > 0 ? item.latest_close.toFixed(1) : '—';
  const chgPct   = item.price_chg_pct || 0;
  const chgColor = chgPct > 0 ? '#f85149' : chgPct < 0 ? '#3fb950' : '#8b949e';
  const chgSign  = chgPct > 0 ? '+' : '';
  const rBuy     = item.total_retail_buy  || 0;
  const rSell    = item.total_retail_sell || 0;
  const cap      = item.market_cap_億 >= 10000
    ? `${(item.market_cap_億/10000).toFixed(1)}兆`
    : item.market_cap_億 > 0 ? `${Math.round(item.market_cap_億)}億` : '';
  const spark    = _ovSpark(item.timeline, 188);
  return `<div class="ov-card" onclick="jumpToTimeline('${item.stock_id}')">
    <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:3px">
      <div>
        <span style="font-weight:700;font-size:13px">${item.stock_id}</span>
        <span style="font-size:10px;color:var(--mut);margin-left:4px">${name}</span>
      </div>
      <span style="font-size:12px;font-weight:700;color:${netColor}">${netStr}</span>
    </div>
    <div style="display:flex;justify-content:space-between;font-size:10px;margin-bottom:4px">
      <span><span style="color:var(--blu)">${closeStr}</span><span style="color:${chgColor};margin-left:4px">${chgSign}${chgPct}%</span></span>
      <span style="color:var(--mut)">${cap}</span>
    </div>
    ${spark}
    <div style="display:flex;justify-content:space-between;font-size:10px;margin-top:4px">
      <span style="color:var(--mut)">主 <span style="color:#f85149">+${item.total_buy}</span><span style="color:#3fb950"> -${item.total_sell}</span></span>
      <span style="color:var(--mut)">散 <span style="color:#f85149">+${rBuy}</span><span style="color:#3fb950"> -${rSell}</span></span>
    </div>
  </div>`;
}

function renderOverviewGrid() {
  const sort = document.getElementById('ov-sort').value;
  const results = [...(_ovData?.results || [])];

  function sortGroup(arr) {
    if      (sort === 'buy')  arr.sort((a,b) => b.total_net  - a.total_net);
    else if (sort === 'sell') arr.sort((a,b) => a.total_net  - b.total_net);
    else                      arr.sort((a,b) => (b.total_buy+b.total_sell) - (a.total_buy+a.total_sell));
    return arr;
  }

  // Group by tier
  const byTier = {};
  for (const item of results) byTier[item.tier || 'micro'] = (byTier[item.tier || 'micro'] || []).concat(item);

  let html = '';
  try {
    for (const t of OV_TIERS) {
      const group = byTier[t.key];
      if (!group || !group.length) continue;
      sortGroup(group);
      html += `<div style="grid-column:1/-1;display:flex;align-items:center;gap:8px;padding:6px 2px;border-bottom:1px solid var(--bor);margin-bottom:2px">
        <span style="font-size:15px">${t.icon}</span>
        <span style="font-weight:700;font-size:13px">${t.label}</span>
        <span style="font-size:10px;color:var(--mut)">${t.range}</span>
        <span style="font-size:11px;color:var(--acc);margin-left:4px">${group.length} 支</span>
      </div>`;
      html += group.map(_ovCard).join('');
    }
  } catch(renderErr) {
    console.error('[大戶總覽 render error]', renderErr);
    document.getElementById('overview-grid').innerHTML =
      `<div class="empty" style="grid-column:1/-1;padding:20px;color:var(--red)">
        渲染錯誤（${results.length} 筆資料）：${renderErr.message}</div>`;
    return;
  }

  if (!html) {
    const skip = _ovData?.skip || 0;
    const lastErr = _ovData?.last_err || '';
    const isRate = lastErr.includes('402');
    const cnt = results.length;
    const msg = cnt > 0
      ? `有 ${cnt} 筆資料但分級不符，請重新掃描`
      : skip > 0 && isRate
        ? `⚠ FinMind API 每日呼叫次數已達上限（${skip} 支無法取得），請明天再試或升級方案`
        : skip > 0
          ? `無大戶活動資料（${skip} 支跳過：${lastErr}）`
          : '無大戶活動資料';
    html = `<div class="empty" style="grid-column:1/-1;padding:20px;line-height:1.7">${msg}</div>`;
  }
  document.getElementById('overview-grid').innerHTML = html;
}

function _ovSpark(timeline, w) {
  const pH = 34, iH = 48, rH = 28, gap = 2;
  const h  = pH + gap + iH + gap + rH;
  if (!timeline || !timeline.length) {
    return `<svg width="${w}" height="${h}" style="display:block;border-radius:3px">
      <rect width="${w}" height="${h}" fill="#161b22"/></svg>`;
  }
  const n    = timeline.length;
  const barW = Math.max(2, Math.floor((w - n) / n));
  const iY0  = pH + gap;
  const rY0  = iY0 + iH + gap;
  const iMid = iY0 + Math.floor(iH / 2);
  const rMid = rY0 + Math.floor(rH / 2);

  const closes  = timeline.map(d => d.close  || 0);
  const volumes = timeline.map(d => d.volume || 0);
  const hasPx   = closes.some(c => c > 0);
  const hasVol  = volumes.some(v => v > 0);
  const maxI    = Math.max(...timeline.map(d => Math.max(d.buy_score || 0, d.sell_score || 0)), 1);
  const maxR    = Math.max(...timeline.map(d => Math.max(d.retail_buy || 0, d.retail_sell || 0)), 1);
  const hasR    = timeline.some(d => (d.retail_buy || 0) + (d.retail_sell || 0) > 0);

  let s = '';

  // Volume bars (bottom of price panel)
  if (hasVol) {
    const maxV = Math.max(...volumes, 1);
    timeline.forEach((d, i) => {
      const x  = i * (barW + 1);
      const vH = Math.round((d.volume || 0) / maxV * (pH * 0.55));
      if (vH > 0) s += `<rect x="${x}" y="${pH - vH}" width="${barW}" height="${vH}" fill="rgba(88,166,255,0.22)"/>`;
    });
  }

  // Price line
  if (hasPx) {
    const vv  = closes.filter(c => c > 0);
    const mn  = Math.min(...vv), mx = Math.max(...vv);
    const rng = mx - mn || 1;
    const pts = timeline.map((d, i) => {
      const x = (i * (barW + 1) + barW / 2).toFixed(1);
      const y = (pH - 3 - ((d.close || mn) - mn) / rng * (pH - 8)).toFixed(1);
      return `${x},${y}`;
    }).join(' ');
    s += `<polyline points="${pts}" fill="none" stroke="#58a6ff" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>`;
  }

  // Big player bars
  timeline.forEach((d, i) => {
    const x  = i * (barW + 1);
    const bH = Math.round((d.buy_score  || 0) / maxI * (iH / 2 - 3));
    const sH = Math.round((d.sell_score || 0) / maxI * (iH / 2 - 3));
    if (bH > 0) s += `<rect x="${x}" y="${iMid - bH}" width="${barW}" height="${bH}" fill="rgba(248,81,73,0.85)"/>`;
    if (sH > 0) s += `<rect x="${x}" y="${iMid}" width="${barW}" height="${sH}" fill="rgba(63,185,80,0.85)"/>`;
  });

  // Retail bars
  if (hasR) {
    timeline.forEach((d, i) => {
      const x  = i * (barW + 1);
      const bH = Math.round((d.retail_buy  || 0) / maxR * (rH / 2 - 2));
      const sH = Math.round((d.retail_sell || 0) / maxR * (rH / 2 - 2));
      if (bH > 0) s += `<rect x="${x}" y="${rMid - bH}" width="${barW}" height="${bH}" fill="rgba(248,81,73,0.45)"/>`;
      if (sH > 0) s += `<rect x="${x}" y="${rMid}" width="${barW}" height="${sH}" fill="rgba(63,185,80,0.45)"/>`;
    });
  }

  const hasI = timeline.some(d => (d.buy_score || 0) + (d.sell_score || 0) > 0);
  return `<svg width="${w}" height="${h}" style="display:block;border-radius:3px">
    <rect width="${w}" height="${h}" fill="#161b22"/>
    <rect x="0" y="${iY0}" width="${w}" height="${iH}" fill="${hasI ? 'rgba(248,81,73,0.03)' : 'rgba(255,255,255,0.01)'}"/>
    <rect x="0" y="${rY0}" width="${w}" height="${rH}" fill="${hasR ? 'rgba(63,185,80,0.03)' : 'rgba(255,255,255,0.01)'}"/>
    <line x1="0" y1="${iY0}" x2="${w}" y2="${iY0}" stroke="#30363d" stroke-width="1"/>
    <line x1="0" y1="${iMid}" x2="${w}" y2="${iMid}" stroke="#30363d" stroke-width="0.5"/>
    <line x1="0" y1="${rY0}" x2="${w}" y2="${rY0}" stroke="#30363d" stroke-width="1"/>
    <line x1="0" y1="${rMid}" x2="${w}" y2="${rMid}" stroke="#30363d" stroke-width="0.5"/>
    ${s}
    <text x="2" y="${pH - 2}" font-size="7" fill="#6e7681">量</text>
    <text x="2" y="${iY0 + 9}" font-size="7" fill="${hasI ? '#f85149' : '#6e7681'}">主力</text>
    <text x="2" y="${rY0 + 9}" font-size="7" fill="${hasR ? '#3fb950' : '#6e7681'}">散戶</text>
  </svg>`;
}

function _miniSpark(timeline, w, h) {
  if (!timeline || !timeline.length) {
    return `<svg width="${w}" height="${h}" style="display:block;border-radius:3px">
      <rect width="${w}" height="${h}" fill="#161b22"/>
      <line x1="0" y1="${h/2}" x2="${w}" y2="${h/2}" stroke="#30363d" stroke-width="1"/>
    </svg>`;
  }
  const n      = timeline.length;
  const barW   = Math.max(2, Math.floor((w - n + 1) / n));
  const midY   = Math.floor(h / 2);
  const maxAbs = Math.max(...timeline.map(d => Math.max(d.buy_score, d.sell_score)), 1);
  const rects  = timeline.map((d, i) => {
    const x  = i * (barW + 1);
    const bH = Math.round(d.buy_score  / maxAbs * (midY - 2));
    const sH = Math.round(d.sell_score / maxAbs * (midY - 2));
    return `<rect x="${x}" y="${midY-bH}" width="${barW}" height="${bH}" fill="#f85149" opacity="0.85"/>` +
           `<rect x="${x}" y="${midY}"    width="${barW}" height="${sH}" fill="#3fb950" opacity="0.85"/>`;
  }).join('');
  return `<svg width="${w}" height="${h}" style="display:block;border-radius:3px">
    <rect width="${w}" height="${h}" fill="#161b22"/>
    <line x1="0" y1="${midY}" x2="${w}" y2="${midY}" stroke="#30363d" stroke-width="1"/>
    ${rects}
  </svg>`;
}

function jumpToTimeline(stockId) {
  switchTab('broker');
  setBrokerMode('timeline');
  document.getElementById('tl-stock').value = stockId;
  loadTimeline();
}

function setTraderPreset(id) {
  document.getElementById('broker-trader-id').value = id;
  loadBrokerTrader();
}

async function loadBrokerTrader() {
  const tid  = document.getElementById('broker-trader-id').value.trim();
  const date = document.getElementById('broker-date-trader').value;
  if (!tid) { document.getElementById('broker-table-wrap').innerHTML = '<div class="empty">請輸入券商代碼</div>'; return; }
  document.getElementById('broker-summary').style.display = 'none';
  document.getElementById('broker-flow-wrap').style.display = 'none';
  document.getElementById('broker-table-wrap').innerHTML = '<div class="empty" style="padding:40px">抓取券商資料中…</div>';
  try {
    const res = await fetch(`/api/broker_trader?trader_id=${encodeURIComponent(tid)}&date=${date||''}`);
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      document.getElementById('broker-table-wrap').innerHTML = `<div class="empty" style="color:var(--red)">${err.detail || '載入失敗'}</div>`;
      return;
    }
    const d = await res.json();
    if (!d.rows || !d.rows.length) {
      document.getElementById('broker-table-wrap').innerHTML = `<div class="empty">${date||'今日'} 無資料（非交易日或查無資料）</div>`;
      return;
    }
    _brokerData = d;
    renderBrokerSummary(d.summary, d.trader_name || tid, d.date || date);
    renderTraderTable(d.rows);
  } catch(e) {
    document.getElementById('broker-table-wrap').innerHTML = `<div class="empty" style="color:var(--red)">錯誤：${e.message}</div>`;
  }
}

function renderTraderTable(rows) {
  const filtered = rows.filter(r => {
    if (_brokerFilter === 'inst')   return !r.is_retail;
    if (_brokerFilter === 'retail') return  r.is_retail;
    return true;
  });
  const { col, asc } = _brokerSort;
  const sorted = [...filtered].sort((a, b) => {
    const av = a[col] ?? 0, bv = b[col] ?? 0;
    return asc ? av - bv : bv - av;
  });
  if (!sorted.length) {
    document.getElementById('broker-table-wrap').innerHTML = '<div class="empty">此分類無資料</div>';
    return;
  }
  const si = c => c === col ? (asc ? '↑' : '↓') : '<span style="opacity:.3">↕</span>';
  const th = (c, label) => `<th onclick="brokerSortBy('${c}')" style="cursor:pointer;white-space:nowrap">${label} ${si(c)}</th>`;
  const rows_html = sorted.map((r, i) => `
    <tr>
      <td style="color:var(--mut);font-size:11px">${i+1}</td>
      <td style="font-weight:700;color:var(--acc)">${r.stock_id}</td>
      <td><span style="display:inline-block;padding:1px 7px;border-radius:10px;font-size:11px;font-weight:700;${r.is_retail ? 'background:#58a6ff22;color:#58a6ff;border:1px solid #58a6ff44' : 'background:#d2992222;color:#d29922;border:1px solid #d2992244'}">${r.is_retail ? '散戶' : '主力'}</span></td>
      <td style="text-align:right;color:var(--acc)">${r.buy ? r.buy.toLocaleString() : '—'}</td>
      <td style="text-align:right;color:var(--red)">${r.sell ? r.sell.toLocaleString() : '—'}</td>
      <td style="text-align:right">${fmtNet(r.net)}</td>
      <td style="text-align:right;color:var(--mut);font-size:11px">${r.buy_price || '—'}</td>
      <td style="text-align:right;color:var(--mut);font-size:11px">${r.sell_price || '—'}</td>
      <td style="text-align:right;font-size:11px;color:var(--mut)">${fmtAmt(r.buy_amount)}</td>
      <td style="text-align:right;font-size:11px;color:var(--mut)">${fmtAmt(r.sell_amount)}</td>
    </tr>`).join('');
  document.getElementById('broker-table-wrap').innerHTML = `
    <table style="width:100%;border-collapse:collapse;font-size:12px">
      <thead style="position:sticky;top:0;background:var(--sur)">
        <tr style="border-bottom:1px solid var(--bor)">
          <th style="text-align:left;padding:7px 8px;color:var(--mut);font-size:10px">#</th>
          <th style="text-align:left;padding:7px 8px;color:var(--mut);font-size:10px">股票代號</th>
          <th style="text-align:left;padding:7px 8px;color:var(--mut);font-size:10px">類型</th>
          ${th('buy','買進(張)')}
          ${th('sell','賣出(張)')}
          ${th('net','淨買超')}
          ${th('buy_price','買均價')}
          ${th('sell_price','賣均價')}
          ${th('buy_amount','買進金額')}
          ${th('sell_amount','賣出金額')}
        </tr>
      </thead>
      <tbody>${rows_html}</tbody>
    </table>`;
}

function setPreset(s) {
  document.getElementById('broker-stock').value = s;
  loadBroker();
}

function brokerPrevDay() {
  const inp = document.getElementById('broker-date');
  if (!inp.value) inp.value = new Date().toISOString().slice(0,10);
  const d = new Date(inp.value); d.setDate(d.getDate() - 1);
  inp.value = d.toISOString().slice(0,10);
  loadBroker();
}
function brokerNextDay() {
  const inp = document.getElementById('broker-date');
  if (!inp.value) inp.value = new Date().toISOString().slice(0,10);
  const d = new Date(inp.value); d.setDate(d.getDate() + 1);
  const today = new Date().toISOString().slice(0,10);
  if (d.toISOString().slice(0,10) > today) return;
  inp.value = d.toISOString().slice(0,10);
  loadBroker();
}

function setBrokerFilter(f) {
  _brokerFilter = f;
  ['all','inst','retail'].forEach(x => {
    const s = document.getElementById('bf-'+x);
    const t = document.getElementById('btf-'+x);
    if (s) s.classList.toggle('active', x === f);
    if (t) t.classList.toggle('active', x === f);
  });
  if (_brokerData) {
    if (_brokerMode === 'trader') renderTraderTable(_brokerData.rows || []);
    else renderBrokerTable(_brokerData.rows || []);
  }
}

function fmtAmt(v) {
  if (!v) return '—';
  if (v >= 1e8) return (v/1e8).toFixed(2) + '億';
  return (v/1e4).toFixed(0) + '萬';
}
function fmtNet(v) {
  if (!v) return '<span style="color:var(--mut)">0</span>';
  const s = v.toLocaleString();
  return v > 0 ? `<span style="color:var(--acc);font-weight:700">+${s}</span>`
               : `<span style="color:var(--red);font-weight:700">${s}</span>`;
}

async function loadBroker() {
  const stock = document.getElementById('broker-stock').value.trim().toUpperCase();
  const date  = document.getElementById('broker-date').value;
  if (!stock) { document.getElementById('broker-table-wrap').innerHTML = '<div class="empty">請輸入股票代號</div>'; return; }
  document.getElementById('broker-summary').style.display = 'none';
  document.getElementById('broker-flow-wrap').style.display = 'none';
  document.getElementById('broker-table-wrap').innerHTML = '<div class="empty" style="padding:40px">抓取分點資料中…</div>';
  try {
    const res = await fetch(`/api/broker?stock_id=${encodeURIComponent(stock)}&date=${date||''}`);
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      document.getElementById('broker-table-wrap').innerHTML = `<div class="empty" style="color:var(--red)">${err.detail || '載入失敗'}</div>`;
      return;
    }
    const d = await res.json();
    if (!d.rows || !d.rows.length) {
      document.getElementById('broker-table-wrap').innerHTML = `<div class="empty">${date||'今日'} 無分點資料（非交易日或查無資料）</div>`;
      return;
    }
    _brokerData = d;
    renderBrokerSummary(d.summary, d.stock_name || stock, d.date || date);
    renderBrokerTable(d.rows);
  } catch(e) {
    document.getElementById('broker-table-wrap').innerHTML = `<div class="empty" style="color:var(--red)">錯誤：${e.message}</div>`;
  }
}

function renderBrokerSummary(s, name, date) {
  if (!s || !s.total) return;
  const instNet   = s.inst_net   || 0;
  const retailNet = s.retail_net || 0;
  const totalVol  = (s.inst_buy + s.inst_sell + s.retail_buy + s.retail_sell) || 1;
  const instPct   = Math.round((s.inst_buy + s.inst_sell) / totalVol * 100);
  const retailPct = 100 - instPct;
  const card = (label, color, count, buy, sell, net) => `
    <div style="flex:1;min-width:150px;background:var(--sur2);border-radius:8px;padding:10px 13px;border:1px solid var(--bor)">
      <div style="font-size:10px;color:${color};font-weight:700;margin-bottom:6px;text-transform:uppercase">${label} (${count} 分點)</div>
      <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px">
        <span style="color:var(--mut)">買進</span><span style="color:var(--acc);font-weight:700">${buy.toLocaleString()}張</span>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px">
        <span style="color:var(--mut)">賣出</span><span style="color:var(--red);font-weight:700">${sell.toLocaleString()}張</span>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:12px">
        <span style="color:var(--mut)">淨</span>${fmtNet(net)}
      </div>
    </div>`;
  const sumEl = document.getElementById('broker-summary');
  sumEl.style.display = 'flex';
  sumEl.innerHTML =
    card('🏦 主力', '#d29922', s.inst_count, s.inst_buy||0, s.inst_sell||0, instNet) +
    card('👥 散戶', '#58a6ff', s.retail_count, s.retail_buy||0, s.retail_sell||0, retailNet) +
    `<div style="flex:1;min-width:150px;background:var(--sur2);border-radius:8px;padding:10px 13px;border:1px solid var(--bor)">
      <div style="font-size:10px;color:var(--mut);font-weight:700;margin-bottom:6px">${name} · ${date} · ${s.total} 分點</div>
      <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px">
        <span style="color:var(--mut)">主力佔比</span><span style="color:#d29922;font-weight:700">${instPct}%</span>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:12px">
        <span style="color:var(--mut)">散戶佔比</span><span style="color:#58a6ff;font-weight:700">${retailPct}%</span>
      </div>
    </div>`;
  document.getElementById('broker-flow-wrap').style.display = 'block';
  document.getElementById('bf-inst-lbl').textContent   = `主力 ${instPct}%`;
  document.getElementById('bf-retail-lbl').textContent = `散戶 ${retailPct}%`;
  document.getElementById('bf-inst-bar').style.width   = instPct   + '%';
  document.getElementById('bf-retail-bar').style.width = retailPct + '%';
}

function renderBrokerTable(rows) {
  const filtered = rows.filter(r => {
    if (_brokerFilter === 'inst')   return !r.is_retail;
    if (_brokerFilter === 'retail') return  r.is_retail;
    return true;
  });
  const { col, asc } = _brokerSort;
  const sorted = [...filtered].sort((a, b) => {
    const av = a[col] ?? 0, bv = b[col] ?? 0;
    return asc ? av - bv : bv - av;
  });
  if (!sorted.length) {
    document.getElementById('broker-table-wrap').innerHTML = '<div class="empty">此分類無資料</div>';
    return;
  }
  const si = c => c === col ? (asc ? '↑' : '↓') : '<span style="opacity:.3">↕</span>';
  const th = (c, label) => `<th onclick="brokerSortBy('${c}')" style="cursor:pointer;white-space:nowrap">${label} ${si(c)}</th>`;
  const rows_html = sorted.map((r, i) => `
    <tr>
      <td style="color:var(--mut);font-size:11px">${i+1}</td>
      <td style="font-weight:600;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${r.broker_name}">${r.broker_name || r.broker_id}</td>
      <td><span style="display:inline-block;padding:1px 7px;border-radius:10px;font-size:11px;font-weight:700;${r.is_retail ? 'background:#58a6ff22;color:#58a6ff;border:1px solid #58a6ff44' : 'background:#d2992222;color:#d29922;border:1px solid #d2992244'}">${r.is_retail ? '散戶' : '主力'}</span></td>
      <td style="text-align:right;color:var(--acc)">${r.buy ? r.buy.toLocaleString() : '—'}</td>
      <td style="text-align:right;color:var(--red)">${r.sell ? r.sell.toLocaleString() : '—'}</td>
      <td style="text-align:right">${fmtNet(r.net)}</td>
      <td style="text-align:right;color:var(--mut);font-size:11px">${r.buy_price || '—'}</td>
      <td style="text-align:right;color:var(--mut);font-size:11px">${r.sell_price || '—'}</td>
      <td style="text-align:right;font-size:11px;color:var(--mut)">${fmtAmt(r.buy_amount)}</td>
      <td style="text-align:right;font-size:11px;color:var(--mut)">${fmtAmt(r.sell_amount)}</td>
    </tr>`).join('');
  document.getElementById('broker-table-wrap').innerHTML = `
    <table style="width:100%;border-collapse:collapse;font-size:12px">
      <thead style="position:sticky;top:0;background:var(--sur)">
        <tr style="border-bottom:1px solid var(--bor)">
          <th style="text-align:left;padding:7px 8px;color:var(--mut);font-size:10px">#</th>
          <th style="text-align:left;padding:7px 8px;color:var(--mut);font-size:10px">分點名稱</th>
          <th style="text-align:left;padding:7px 8px;color:var(--mut);font-size:10px">類型</th>
          ${th('buy','買進(張)')}
          ${th('sell','賣出(張)')}
          ${th('net','淨買超')}
          ${th('buy_price','買均價')}
          ${th('sell_price','賣均價')}
          ${th('buy_amount','買進金額')}
          ${th('sell_amount','賣出金額')}
        </tr>
      </thead>
      <tbody>${rows_html}</tbody>
    </table>`;
}

function brokerSortBy(col) {
  if (_brokerSort.col === col) _brokerSort.asc = !_brokerSort.asc;
  else { _brokerSort.col = col; _brokerSort.asc = false; }
  if (_brokerData) {
    if (_brokerMode === 'trader') renderTraderTable(_brokerData.rows || []);
    else renderBrokerTable(_brokerData.rows || []);
  }
}

// ── Toast ──────────────────────────────────────────────────────────────
// ─── TOOLTIP: mobile tap toggle ──────────────────────────────────────
document.addEventListener('click', e => {
  const tip = e.target.closest('.tip');
  if (tip) {
    e.stopPropagation();
    const wasActive = tip.classList.contains('tipped');
    document.querySelectorAll('.tip.tipped').forEach(t => t.classList.remove('tipped'));
    if (!wasActive) tip.classList.add('tipped');
  } else {
    document.querySelectorAll('.tip.tipped').forEach(t => t.classList.remove('tipped'));
  }
});

function showToast(msg, err=false) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast' + (err ? ' err' : '') + ' show';
  setTimeout(() => { t.className = 'toast' + (err ? ' err' : ''); }, 3000);
}

// ─── GRADING ────────────────────────────────────────────────────────────
let gradeFilterTier  = '';
let gradeFilterGrade = '';
let gradeSortKey     = 'volatility';
let gradeSortAsc     = false;
let gradePolling     = null;

function setGradeFilter(type, val, btn) {
  if (type === 'tier')  gradeFilterTier  = val;
  if (type === 'grade') gradeFilterGrade = val;
  // activate button in the right group
  btn.closest('.grade-filter-row').querySelectorAll('.gf-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  loadGrading();
}

async function loadGrading() {
  const q = document.getElementById('grade-search')?.value ?? '';
  const url = `/api/grading?tier=${gradeFilterTier}&grade=${gradeFilterGrade}&q=${encodeURIComponent(q)}`;
  const r   = await fetch(url);
  const j   = await r.json();
  renderGrading(j);
}

// ── Sparkline SVG ────────────────────────────────────────────────────────
function sparkline(vals, w=52, h=20, clr=null) {
  if (!vals || vals.length < 2) return '<svg width="'+w+'" height="'+h+'"></svg>';
  const f = vals.filter(v => v != null && !isNaN(v));
  if (f.length < 2) return '<svg width="'+w+'" height="'+h+'"></svg>';
  const mn = Math.min(...f), mx = Math.max(...f);
  const rng = mx - mn || 1;
  const n = f.length;
  const pts = f.map((v, i) => {
    const x = (i / (n - 1) * w).toFixed(1);
    const y = ((1 - (v - mn) / rng) * (h - 3) + 1.5).toFixed(1);
    return x + ',' + y;
  }).join(' ');
  const c = clr || (f[f.length-1] >= f[0] ? '#3fb950' : '#f85149');
  // filled area under curve
  const first = f[0], last = f[f.length-1];
  const fx = '0', fy = ((1-(first-mn)/rng)*(h-3)+1.5).toFixed(1);
  const lx = w.toFixed(1), ly = ((1-(last-mn)/rng)*(h-3)+1.5).toFixed(1);
  const area = pts + ' ' + lx + ',' + h + ' ' + fx + ',' + h;
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" style="flex-shrink:0;display:block;overflow:visible">
    <polygon points="${area}" fill="${c}" opacity="0.12"/>
    <polyline points="${pts}" fill="none" stroke="${c}" stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"/>
    <circle cx="${lx}" cy="${ly}" r="2" fill="${c}"/>
  </svg>`;
}

function renderGrading(j) {
  const body = document.getElementById('grade-body');
  document.getElementById('grade-count').textContent =
    `${j.total.toLocaleString()} / ${j.ready.toLocaleString()} 支`;
  _syncQsBtns();

  if (!j.tiers || j.tiers.length === 0) {
    body.innerHTML = '<div class="empty" style="padding:40px"><span>尚無分級資料，請稍候…</span></div>';
    return;
  }

  const tierClsMap = {mega:'tb-mega',large:'tb-large',mid:'tb-mid',small:'tb-small',micro:'tb-micro'};
  const gradeClr   = {S:'#ef4444',A:'#f97316',B:'#eab308',C:'#60a5fa',D:'#6b7280'};
  const gradeLbl   = {S:'極高波動',A:'高波動',B:'中波動',C:'低波動',D:'極低波動'};

  // compute max volatility for bar scaling
  let maxVol = 0;
  j.tiers.forEach(t => t.stocks.forEach(s => { if (s.volatility > maxVol) maxVol = s.volatility; }));

  body.innerHTML = j.tiers.map(tier => {
    const rows = [...tier.stocks].sort((a, b) =>
      gradeSortAsc ? (a[gradeSortKey]??0)-(b[gradeSortKey]??0)
                   : (b[gradeSortKey]??0)-(a[gradeSortKey]??0)
    );

    const tableRows = rows.map(s => {
      const barW = maxVol > 0 ? Math.min(60, s.volatility / maxVol * 60) : 2;
      const bpChg = s.bpct_chg ?? 0;
      const chgCls = bpChg > 0 ? 'pct-up' : bpChg < 0 ? 'pct-dn' : '';
      const cap = s.market_cap_億 >= 10000
        ? `${(s.market_cap_億/10000).toFixed(1)}兆`
        : `${Math.round(s.market_cap_億).toLocaleString()}億`;
      return `<tr onclick="selectStock('${s.stock_id}')" style="cursor:pointer">
        <td><a style="color:var(--blu);font-weight:700;font-family:monospace">${s.stock_id}</a></td>
        <td style="max-width:90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.stock_name}</td>
        <td style="text-align:center">
          <span class="gbadge gb-${s.grade}" title="${gradeLbl[s.grade]}">${s.grade}</span>
        </td>
        <td>
          <span style="display:inline-block;width:${barW}px;height:6px;border-radius:3px;background:${gradeClr[s.grade]??'#888'};opacity:.8;vertical-align:middle;margin-right:4px"></span>
          ${s.volatility.toFixed(3)}
        </td>
        <td>${cap}</td>
        <td>
          <div style="display:flex;align-items:center;gap:5px;justify-content:flex-end">
            ${sparkline(s.bpct_spark, 52, 20)}
            <span style="color:var(--acc);font-weight:600">${s.latest_bpct?.toFixed(1)??'—'}%</span>
          </div>
        </td>
        <td class="${chgCls}">${bpChg>=0?'+':''}${(bpChg??0).toFixed(3)}</td>
        <td>${s.latest_bp?.toLocaleString()??'—'}</td>
        <td>
          <div style="display:flex;align-items:center;gap:5px;justify-content:flex-end">
            ${sparkline(s.price_spark, 52, 20)}
            <span style="color:var(--blu);font-weight:600">${s.latest_price?.toFixed(0)??'—'}</span>
          </div>
        </td>
      </tr>`;
    }).join('');

    const thSort = (key, label, tip='') => {
      const active = gradeSortKey === key;
      const arrow  = active ? (gradeSortAsc ? '↑' : '↓') : '';
      const tipAttr = tip ? ` class="tip" data-tip="${tip}"` : '';
      return `<th onclick="gradeSort('${key}')" style="${active?'color:var(--acc)':''}"><span${tipAttr}>${label} ${arrow}</span></th>`;
    };

    return `<div class="tier-section" id="ts-${tier.key}">
      <div class="tier-header" onclick="toggleTier('${tier.key}')">
        <span>${tier.icon}</span>
        <span class="tier-header-title">${tier.label}</span>
        <span class="tier-cnt">${tier.count} 支</span>
        <span class="tier-toggle">▾</span>
      </div>
      <div class="grade-table-wrap">
        <table class="gtable">
          <thead><tr>
            ${thSort('stock_id','代號')}
            <th>名稱</th>
            ${thSort('grade','等級')}
            ${thSort('volatility','大戶波動度 ⓘ','每週千張大戶持股%變動量的標準差\n數字越大=大戶進出越頻繁\n等級S/A/B/C/D以此在同規模內排名')}
            ${thSort('market_cap_億','市值 ⓘ','股價 × 總股數（億元）\n用來決定規模層（超大/大/中/小/微型）')}
            ${thSort('latest_bpct','千張大戶%')}
            ${thSort('bpct_chg','週增減')}
            ${thSort('latest_bp','人數')}
            ${thSort('latest_price','股價')}
          </tr></thead>
          <tbody>${tableRows}</tbody>
        </table>
      </div>
    </div>`;
  }).join('');
}

function gradeSort(key) {
  if (gradeSortKey === key) gradeSortAsc = !gradeSortAsc;
  else { gradeSortKey = key; gradeSortAsc = false; }
  // sync quick-sort button active state
  _syncQsBtns();
  loadGrading();
}

function quickSort(key, asc, btn) {
  gradeSortKey = key;
  gradeSortAsc = asc;
  _syncQsBtns();
  loadGrading();
}

// Quick-sort → 大買/大賣 特殊判定
const _QS_MAP = {
  'qs-bpct':   { key:'latest_bpct',   asc:false },
  'qs-buy':    { key:'bpct_chg',      asc:false },
  'qs-sell':   { key:'bpct_chg',      asc:true  },
  'qs-people': { key:'latest_bp',     asc:false },
  'qs-vol':    { key:'volatility',    asc:false },
  'qs-mktcap': { key:'market_cap_億', asc:false },
};

function _syncQsBtns() {
  Object.entries(_QS_MAP).forEach(([id, cfg]) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.toggle('active',
      gradeSortKey === cfg.key && gradeSortAsc === cfg.asc);
  });
}

function toggleTier(key) {
  document.getElementById(`ts-${key}`)?.classList.toggle('collapsed');
}

async function pollGradingProgress() {
  const r = await fetch('/api/grading/status');
  const s = await r.json();
  const bar  = document.getElementById('grade-progress-bar');
  const fill = document.getElementById('gp-fill');
  const lbl  = document.getElementById('gp-label');

  if (s.running) {
    if (bar) bar.style.display = '';
    if (fill) fill.style.width = `${s.pct}%`;
    if (lbl)  lbl.textContent  = `${s.done} / ${s.total} (${s.pct}%)`;
    loadGrading();
  } else {
    if (bar) bar.style.display = 'none';
    if (s.ready > 0) loadGrading();
    clearInterval(gradePolling);
    gradePolling = null;
  }
}

async function refreshGrading() {
  const r = await fetch('/api/grading/refresh', {method:'POST'});
  const j = await r.json();
  showToast(j.message);
  if (j.ok && !gradePolling) {
    gradePolling = setInterval(pollGradingProgress, 3000);
  }
}

// ── K線分析 ─────────────────────────────────────────────────────────────
let _klRange  = 90;
let _klChart  = null;  // main chart
let _klInstC  = null;  // institutional chart
let _klMargC  = null;  // margin chart

function klineSetRange(days) {
  _klRange = days;
  document.querySelectorAll('.kl-range').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById(`klr-${days}`);
  if (btn) btn.classList.add('active');
}

function klineAutocomplete(q) {
  const ac = document.getElementById('kline-ac');
  if (!q || q.length < 1) { ac.style.display = 'none'; return; }
  const hits = allStocks.filter(s =>
    s.stock_id.startsWith(q) ||
    (s.stock_name || '').includes(q)
  ).slice(0, 8);
  if (!hits.length) { ac.style.display = 'none'; return; }
  ac.innerHTML = hits.map(s =>
    `<div onclick="klinePickStock('${s.stock_id}','${s.stock_name||''}')"
       style="padding:7px 12px;cursor:pointer;font-size:12px;border-bottom:1px solid var(--bor)"
       onmouseover="this.style.background='var(--sur)'" onmouseout="this.style.background=''">
      <b>${s.stock_id}</b> ${s.stock_name||''}
    </div>`
  ).join('');
  ac.style.display = 'block';
}

function klinePickStock(id, name) {
  document.getElementById('kline-input').value = id;
  document.getElementById('kline-name').textContent = name;
  document.getElementById('kline-ac').style.display = 'none';
  klineLoad();
}

const _klChartOpts = {
  layout: { background: { color: '#0d1117' }, textColor: '#8b949e' },
  grid:   { vertLines: { color: '#21262d' }, horzLines: { color: '#21262d' } },
  crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
  rightPriceScale: { borderColor: '#30363d' },
  timeScale: { borderColor: '#30363d', timeVisible: false },
  handleScroll: true, handleScale: true,
};

function _klDestroyAll() {
  if (_klChart)  { try { _klChart.remove();  } catch(e){} _klChart  = null; }
  if (_klInstC)  { try { _klInstC.remove();  } catch(e){} _klInstC  = null; }
  if (_klMargC)  { try { _klMargC.remove();  } catch(e){} _klMargC  = null; }
}

function _klCreateChart(containerId, height) {
  const el = document.getElementById(containerId);
  if (!el) return null;
  el.innerHTML = '';
  return LightweightCharts.createChart(el, {
    ..._klChartOpts,
    width:  el.clientWidth,
    height: height,
  });
}

async function klineLoad() {
  const stockId = (document.getElementById('kline-input').value || '').trim();
  if (!stockId) { showToast('請輸入股票代號'); return; }
  document.getElementById('kline-ac').style.display = 'none';
  document.getElementById('kline-status').textContent = '載入中…';
  _klDestroyAll();

  let data;
  try {
    const r = await fetch(`/api/kline_data?stock_id=${encodeURIComponent(stockId)}&days=${_klRange}`);
    data = await r.json();
  } catch(e) {
    document.getElementById('kline-status').textContent = '載入失敗：' + e;
    return;
  }
  if (data.error) {
    document.getElementById('kline-status').textContent = data.error;
    return;
  }

  const name = data.stock_name || '';
  document.getElementById('kline-name').textContent = name;
  document.getElementById('kline-status').textContent =
    `${data.stock_id} ${name} · ${data.prices.length} 交易日`;

  _klRenderMain(data.prices);
  _klRenderInst(data.institutional);
  _klRenderMargin(data.margins);

  // Sync time scales across all charts
  _klSyncScales();
}

function _klRenderMain(prices) {
  if (!prices.length) return;
  const wrap = document.getElementById('kline-chart-wrap');
  _klChart = _klCreateChart('kline-chart', wrap.clientHeight || 320);

  // Candlestick series
  const candle = _klChart.addCandlestickSeries({
    upColor:        '#f85149', downColor:        '#3fb950',
    borderUpColor:  '#f85149', borderDownColor:  '#3fb950',
    wickUpColor:    '#f85149', wickDownColor:    '#3fb950',
  });
  candle.setData(prices.map(p => ({
    time: p.date, open: p.open, high: p.high, low: p.low, close: p.close,
  })));

  // Volume histogram (secondary scale)
  const vol = _klChart.addHistogramSeries({
    color: '#3b82f680',
    priceFormat: { type: 'volume' },
    priceScaleId: 'vol',
    scaleMargins: { top: 0.82, bottom: 0 },
  });
  vol.setData(prices.map(p => ({
    time: p.date, value: p.volume,
    color: p.close >= p.open ? '#f8514940' : '#3fb95040',
  })));

  _klChart.timeScale().fitContent();
}

function _klRenderInst(inst) {
  if (!inst.length) return;
  const wrap = document.getElementById('kline-inst-wrap');
  _klInstC = _klCreateChart('kline-inst', wrap.clientHeight || 120);

  const addInstSeries = (field, color) => {
    const s = _klInstC.addHistogramSeries({
      color, priceScaleId: 'right', base: 0,
    });
    s.setData(inst.map(d => ({
      time: d.date, value: d[field] || 0,
      color: (d[field] || 0) >= 0 ? color : color.replace(/[^,]+\)/, '0.4)').replace('rgb','rgba'),
    })));
    return s;
  };

  // Stack: foreign (blue), trust (purple), dealer (amber)
  _klInstC.addHistogramSeries({ color: '#3b82f6', priceScaleId: 'right', base: 0 })
    .setData(inst.map(d => ({ time: d.date, value: d.foreign||0, color: (d.foreign||0)>=0?'#3b82f6':'#3b82f680' })));
  _klInstC.addHistogramSeries({ color: '#a855f7', priceScaleId: 'right', base: 0 })
    .setData(inst.map(d => ({ time: d.date, value: d.trust||0,   color: (d.trust||0)>=0?'#a855f7':'#a855f780'   })));
  _klInstC.addHistogramSeries({ color: '#f59e0b', priceScaleId: 'right', base: 0 })
    .setData(inst.map(d => ({ time: d.date, value: d.dealer||0,  color: (d.dealer||0)>=0?'#f59e0b':'#f59e0b80'  })));

  _klInstC.timeScale().fitContent();
}

function _klRenderMargin(margins) {
  if (!margins.length) return;
  const wrap = document.getElementById('kline-margin-wrap');
  _klMargC = _klCreateChart('kline-margin', wrap.clientHeight || 100);

  const mSeries = _klMargC.addLineSeries({ color: '#f87171', lineWidth: 1.5, priceScaleId: 'right' });
  mSeries.setData(margins.map(m => ({ time: m.date, value: m.margin_balance })));

  const sSeries = _klMargC.addLineSeries({ color: '#34d399', lineWidth: 1.5, priceScaleId: 'left' });
  sSeries.setData(margins.map(m => ({ time: m.date, value: m.short_balance })));

  _klMargC.timeScale().fitContent();
}

function _klSyncScales() {
  // Sync scroll/zoom of sub-charts to main chart
  if (!_klChart) return;
  const sub = [_klInstC, _klMargC].filter(Boolean);
  _klChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
    if (!range) return;
    sub.forEach(c => c.timeScale().setVisibleLogicalRange(range));
  });
  sub.forEach(c => c.timeScale().subscribeVisibleLogicalRangeChange(range => {
    if (!range || !_klChart) return;
    _klChart.timeScale().setVisibleLogicalRange(range);
  }));
}

// Resize all K線 charts when window resizes
window.addEventListener('resize', () => {
  const charts = [
    ['kline-chart',  _klChart],
    ['kline-inst',   _klInstC],
    ['kline-margin', _klMargC],
  ];
  charts.forEach(([id, c]) => {
    if (!c) return;
    const el = document.getElementById(id);
    if (el) c.resize(el.clientWidth, el.clientHeight);
  });
});

// ── 產業鏈 ─────────────────────────────────────────────────────────────
let _chainData = null;

async function chainLoad() {
  if (_chainData) return;
  const statusEl = document.getElementById('chain-status');
  statusEl.textContent = '載入中…';
  try {
    const r = await fetch('/api/industry_chain');
    if (!r.ok) { const t = await r.text(); throw new Error(t); }
    _chainData = await r.json();
    chainRenderIndList();
    statusEl.textContent = `${_chainData.industry_list.length} 產業｜${_chainData.total} 條目`;
  } catch(e) {
    statusEl.textContent = '載入失敗: ' + e.message;
  }
}

function chainRenderIndList() {
  if (!_chainData) return;
  const el = document.getElementById('chain-ind-list');
  el.innerHTML = _chainData.industry_list.map(ind => {
    const cnt = _chainData.industry_counts[ind] || 0;
    return `<div class="chain-ind-item" data-ind="${ind}" onclick="chainSelectInd(this,'${ind.replace(/'/g,"\\'")}')">`
         + `<span style="flex:1;line-height:1.3">${ind}</span>`
         + `<span style="font-size:9px;color:var(--mut)">${cnt}</span>`
         + `</div>`;
  }).join('');
}

function chainSelectInd(itemEl, ind) {
  document.querySelectorAll('.chain-ind-item').forEach(el => el.classList.remove('active'));
  itemEl.classList.add('active');
  const subs = _chainData.industries[ind] || {};
  const panel = document.getElementById('chain-sub-panel');
  const sorted = Object.entries(subs).sort((a,b) => b[1].length - a[1].length);
  panel.innerHTML = `<div style="font-size:13px;font-weight:700;margin-bottom:10px;color:var(--txt)">${ind}</div>`
    + sorted.map(([sub, stocks]) =>
        `<div style="margin-bottom:12px">`
      + `<div style="font-size:10px;font-weight:700;color:var(--mut);margin-bottom:5px;padding-bottom:3px;border-bottom:1px solid var(--bor)">`
      + `${sub} <span style="font-weight:400">(${stocks.length})</span></div>`
      + `<div style="display:flex;flex-wrap:wrap;gap:4px">`
      + stocks.map(s => `<button class="chain-chip" onclick="chainGoStock('${s.id}')">${s.id}${s.name ? ' '+s.name : ''}</button>`).join('')
      + `</div></div>`
    ).join('');
}

function chainStockInput(q) {
  q = q.trim();
  if (!q) { document.getElementById('chain-ac').style.display='none'; return; }
  const matches = allStocks.filter(s => s.stock_id.startsWith(q) || s.stock_name.includes(q)).slice(0,8);
  const ac = document.getElementById('chain-ac');
  if (matches.length) {
    ac.style.display = 'block';
    ac.innerHTML = matches.map(s =>
      `<div class="ac-item" onclick="chainPickStock('${s.stock_id}','${s.stock_name.replace(/'/g,"\\'")}')">
        ${s.stock_id} ${s.stock_name}</div>`
    ).join('');
  } else {
    ac.style.display = 'none';
  }
}

function chainStockCommit() {
  const id = document.getElementById('chain-stock-input').value.trim();
  if (!id) return;
  const stock = allStocks.find(s => s.stock_id === id);
  chainPickStock(id, stock ? stock.stock_name : '');
}

function chainPickStock(id, name) {
  document.getElementById('chain-stock-input').value = id;
  document.getElementById('chain-stock-name').textContent = name;
  document.getElementById('chain-ac').style.display = 'none';
  if (_chainData) chainShowStockChains(id, name);
}

function chainShowStockChains(id, name) {
  const result = document.getElementById('chain-stock-result');
  const found = [];
  for (const [ind, subs] of Object.entries(_chainData.industries)) {
    for (const [sub, stocks] of Object.entries(subs)) {
      if (stocks.some(s => s.id === id)) found.push({ ind, sub, peers: stocks });
    }
  }
  if (!found.length) {
    result.innerHTML = `<div style="color:var(--mut);font-size:12px;padding:6px">${id} 無產業鏈資料</div>`;
    result.style.display = 'block';
    return;
  }
  result.innerHTML = `<div style="font-size:12px;font-weight:700;margin-bottom:6px">${id} ${name} 所屬產業鏈</div>`
    + found.map(f =>
        `<div style="margin-bottom:8px;padding:7px 10px;background:var(--sur2);border-radius:6px;border:1px solid var(--bor)">`
      + `<div style="font-size:10px;color:var(--mut);margin-bottom:5px"><span style="color:var(--acc);font-weight:700">${f.ind}</span> › ${f.sub} (${f.peers.length}家)</div>`
      + `<div style="display:flex;flex-wrap:wrap;gap:4px">`
      + f.peers.map(s => `<button class="chain-chip${s.id===id?' active':''}" onclick="chainGoStock('${s.id}')">${s.id}${s.name?' '+s.name:''}</button>`).join('')
      + `</div></div>`
    ).join('');
  result.style.display = 'block';
}

function chainClearStock() {
  document.getElementById('chain-stock-input').value = '';
  document.getElementById('chain-stock-name').textContent = '';
  document.getElementById('chain-stock-result').style.display = 'none';
  document.getElementById('chain-ac').style.display = 'none';
}

function chainGoStock(id) {
  document.getElementById('chain-ac').style.display = 'none';
  const stock = allStocks.find(s => s.stock_id === id);
  if (stock) {
    switchTab('single');
    loadStock(id, stock.stock_name);
    showToast(`${id} ${stock.stock_name}`);
  } else {
    showToast('找不到 ' + id);
  }
}
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import os
    port = int(os.getenv("PORT", PORT))
    uvicorn.run("big_holder_web:app", host="0.0.0.0", port=port, reload=False)
