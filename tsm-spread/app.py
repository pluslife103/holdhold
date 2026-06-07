from flask import Flask, jsonify, render_template, render_template_string, request
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os, json, re
import requests as http_req

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))

ADR_RATIO = 5  # 1 TSM ADR = 5 shares of 2330.TW


def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/bom")
def bom():
    return render_template("bom.html")


# ── TSM endpoints ──────────────────────────────────────────────────────────────

@app.route("/api/current")
def current_data():
    try:
        tsm_price = safe_float(yf.Ticker("TSM").fast_info.last_price)
        twse_price = safe_float(yf.Ticker("2330.TW").fast_info.last_price)
        usdtwd = safe_float(yf.Ticker("TWD=X").fast_info.last_price)

        if not all([tsm_price, twse_price, usdtwd]):
            return jsonify({"error": "無法取得即時報價"}), 500

        implied_twse = tsm_price * usdtwd / ADR_RATIO
        premium_pct = (implied_twse - twse_price) / twse_price * 100

        return jsonify({
            "tsm_price": round(tsm_price, 2),
            "twse_price": round(twse_price, 2),
            "usdtwd": round(usdtwd, 4),
            "implied_twse": round(implied_twse, 2),
            "premium_pct": round(premium_pct, 4),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/history")
def history_data():
    try:
        end = datetime.now()
        start = end - timedelta(days=90)

        raw = yf.download(
            ["TSM", "2330.TW", "TWD=X"],
            start=start, end=end,
            auto_adjust=True, progress=False,
            group_by="ticker",
        )

        combined = pd.DataFrame({
            "tsm":    raw["TSM"]["Close"],
            "twse":   raw["2330.TW"]["Close"],
            "usdtwd": raw["TWD=X"]["Close"],
        }).dropna()

        combined["implied_twse"] = combined["tsm"] * combined["usdtwd"] / ADR_RATIO
        combined["premium_pct"] = (
            (combined["implied_twse"] - combined["twse"]) / combined["twse"] * 100
        )

        return jsonify({
            "dates":        combined.index.strftime("%Y-%m-%d").tolist(),
            "tsm":          [round(float(x), 2) for x in combined["tsm"]],
            "twse":         [round(float(x), 2) for x in combined["twse"]],
            "usdtwd":       [round(float(x), 4) for x in combined["usdtwd"]],
            "implied_twse": [round(float(x), 2) for x in combined["implied_twse"]],
            "premium_pct":  [round(float(x), 4) for x in combined["premium_pct"]],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── EWT endpoints ──────────────────────────────────────────────────────────────

@app.route("/api/ewt/current")
def ewt_current():
    try:
        ewt_ticker = yf.Ticker("EWT")
        ewt_price = safe_float(ewt_ticker.fast_info.last_price)
        nav = safe_float(ewt_ticker.info.get("navPrice"))
        zero50_price = safe_float(yf.Ticker("0050.TW").fast_info.last_price)
        usdtwd = safe_float(yf.Ticker("TWD=X").fast_info.last_price)

        if not all([ewt_price, nav, zero50_price, usdtwd]):
            return jsonify({"error": "無法取得 EWT 即時報價"}), 500

        # Official NAV-based premium/discount
        premium_pct = (ewt_price - nav) / nav * 100
        zero50_usd = zero50_price / usdtwd

        return jsonify({
            "ewt_price":   round(ewt_price, 2),
            "nav":         round(nav, 2),
            "zero50_price": round(zero50_price, 2),
            "zero50_usd":  round(zero50_usd, 4),
            "usdtwd":      round(usdtwd, 4),
            "premium_pct": round(premium_pct, 4),
            "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ewt/history")
def ewt_history():
    try:
        end = datetime.now()
        start = end - timedelta(days=90)

        raw = yf.download(
            ["EWT", "0050.TW", "TWD=X"],
            start=start, end=end,
            auto_adjust=True, progress=False,
            group_by="ticker",
        )

        combined = pd.DataFrame({
            "ewt":    raw["EWT"]["Close"],
            "zero50": raw["0050.TW"]["Close"],
            "usdtwd": raw["TWD=X"]["Close"],
        }).dropna()

        # Convert 0050.TW to USD so both series are in the same currency
        combined["zero50_usd"] = combined["zero50"] / combined["usdtwd"]

        # Index both to 100 at start of period; spread = divergence from Taiwan market
        combined["ewt_idx"]    = combined["ewt"]       / combined["ewt"].iloc[0]       * 100
        combined["zero50_idx"] = combined["zero50_usd"] / combined["zero50_usd"].iloc[0] * 100
        # Positive spread = EWT outperformed (at premium to Taiwan market)
        combined["spread"] = combined["ewt_idx"] - combined["zero50_idx"]

        return jsonify({
            "dates":      combined.index.strftime("%Y-%m-%d").tolist(),
            "ewt":        [round(float(x), 2)  for x in combined["ewt"]],
            "zero50":     [round(float(x), 2)  for x in combined["zero50"]],
            "zero50_usd": [round(float(x), 4)  for x in combined["zero50_usd"]],
            "ewt_idx":    [round(float(x), 3)  for x in combined["ewt_idx"]],
            "zero50_idx": [round(float(x), 3)  for x in combined["zero50_idx"]],
            "spread":     [round(float(x), 4)  for x in combined["spread"]],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Positions snapshot ────────────────────────────────────────────────────────

_POS_FILE = os.path.join(os.path.dirname(__file__), "positions.json")

@app.route("/api/positions")
def positions_data():
    if not os.path.exists(_POS_FILE):
        return jsonify({"error": "positions.json 尚未建立，請先執行 deploy_positions.py"}), 404
    try:
        with open(_POS_FILE, encoding="utf-8") as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── AI Analysis ───────────────────────────────────────────────────
_STATEMENTDOG_SESSION = os.environ.get("STATEMENTDOG_SESSION", "")
_UPSTASH_URL          = os.environ.get("UPSTASH_REDIS_REST_URL", "")
_UPSTASH_TOKEN        = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
_HISTORY_KEY          = "ai_qa_history"

def _redis_cmd(*args):
    """Execute a single Redis command via Upstash pipeline API."""
    if not _UPSTASH_URL:
        return None
    try:
        r = http_req.post(
            f"{_UPSTASH_URL}/pipeline",
            headers={"Authorization": f"Bearer {_UPSTASH_TOKEN}", "Content-Type": "application/json"},
            json=[list(args)],
            timeout=5,
        )
        results = r.json()
        return results[0].get("result") if results else None
    except Exception:
        return None

def _redis_get(key):
    return _redis_cmd("GET", key)

def _redis_set(key, value_str):
    """Store value_str (plain string) in Redis."""
    _redis_cmd("SET", key, value_str)

_AI_DATA_FILE = os.path.join(os.path.dirname(__file__), "ai_data.json")

@app.route("/ai")
def ai_analysis():
    data = {}
    if os.path.exists(_AI_DATA_FILE):
        with open(_AI_DATA_FILE, encoding="utf-8") as f:
            data = json.load(f)
    return render_template("ai.html", data=data)


@app.route("/api/ai-data")
def ai_data_api():
    stock_id = request.args.get("stock_id", "2330").strip()
    if not os.path.exists(_AI_DATA_FILE):
        return jsonify({"error": "no data", "qa_pairs": []})
    with open(_AI_DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)
    # filter by stock_id if the file is for a different stock
    info = data.get("stock_info", {})
    if info.get("id", stock_id) != stock_id:
        return jsonify({"error": "stock mismatch", "qa_pairs": []})
    return jsonify(data)


@app.route("/api/history-load")
def history_load():
    raw = _redis_get(_HISTORY_KEY)
    if not raw:
        return jsonify({})
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(data, dict):   # 拒絕 array 或其他非 dict
            return jsonify({})
        return jsonify(data)
    except Exception:
        return jsonify({})


@app.route("/api/history-save", methods=["POST"])
def history_save():
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        return jsonify({"ok": False, "error": "invalid data"}), 400
    # _redis_set expects a plain string; serialize dict once here
    _redis_set(_HISTORY_KEY, json.dumps(data, ensure_ascii=False))
    return jsonify({"ok": True})


_REMOTE_URL_KEY = "remote_url"

@app.route("/api/remote-url", methods=["GET"])
def remote_url_get():
    raw = _redis_get(_REMOTE_URL_KEY)
    if not raw:
        return jsonify({"url": None}), 404
    try:
        data = json.loads(raw) if isinstance(raw, str) and raw.startswith("{") else {"url": raw}
        return jsonify(data)
    except Exception:
        return jsonify({"url": raw})

@app.route("/api/remote-url", methods=["POST"])
def remote_url_set():
    data = request.get_json(force=True, silent=True)
    if not data or not data.get("url") or data.get("pwd") != "remote2024":
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    payload = json.dumps({"url": data["url"], "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, ensure_ascii=False)
    _redis_set(_REMOTE_URL_KEY, payload)
    return jsonify({"ok": True, "url": data["url"]})

@app.route("/remote")
def remote_page():
    return render_template_string("""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>遠端桌面</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#05070f;color:#e2e8f0;font-family:system-ui,sans-serif;
  display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;gap:20px;padding:24px}
h1{font-size:1.4rem;font-weight:700}
.url-box{background:#0e1425;border:1px solid #1e2d50;border-radius:12px;padding:16px 20px;
  word-break:break-all;font-size:.9rem;color:#60a5fa;max-width:480px;width:100%;text-align:center}
.btn{background:#3b82f6;color:#fff;border:none;border-radius:10px;padding:14px 32px;
  font-size:1rem;font-weight:600;cursor:pointer;text-decoration:none;display:inline-block;margin-top:4px}
.btn:hover{opacity:.85}
.muted{color:#7a8ba8;font-size:.8rem}
.offline{color:#ef4444}
</style>
</head>
<body>
<div style="font-size:2rem">🖥️</div>
<h1>遠端桌面</h1>
<div id="status" class="muted">載入中…</div>
<div id="url-box" class="url-box" style="display:none"></div>
<a id="open-btn" class="btn" style="display:none" href="#">開啟遠端桌面</a>
<div id="time" class="muted"></div>
<script>
fetch('/api/remote-url')
  .then(r => r.ok ? r.json() : null)
  .then(d => {
    if (d && d.url) {
      document.getElementById('status').textContent = '遠端已就緒';
      document.getElementById('url-box').style.display = 'block';
      document.getElementById('url-box').textContent = d.url;
      const btn = document.getElementById('open-btn');
      btn.href = d.url;
      btn.style.display = 'inline-block';
      if (d.updated_at) document.getElementById('time').textContent = '更新時間：' + d.updated_at;
    } else {
      document.getElementById('status').className = 'offline';
      document.getElementById('status').textContent = '遠端目前離線，請先啟動電腦上的遠端桌面';
    }
  })
  .catch(() => {
    document.getElementById('status').className = 'offline';
    document.getElementById('status').textContent = '無法連線，請稍後再試';
  });
</script>
</body>
</html>""")

@app.route("/api/ai-proxy")
def ai_proxy():
    ticker   = request.args.get("ticker", "2330").strip()
    question = request.args.get("question", "").strip()
    if not question:
        return jsonify({"error": "缺少 question 參數"}), 400

    session_cookie = _STATEMENTDOG_SESSION
    if not session_cookie:
        return jsonify({"error": "未設定 STATEMENTDOG_SESSION 環境變數"}), 500

    ua           = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    cookie_hdr   = f"_statementdog_session_v2={session_cookie}"
    analysis_url = f"https://statementdog.com/analysis/{ticker}"

    # 取頁面 HTML（用於 page_context）
    try:
        page_r = http_req.get(
            analysis_url,
            headers={"Cookie": cookie_hdr, "User-Agent": ua},
            timeout=15,
        )
        page_html = page_r.text
    except Exception as e:
        return jsonify({"error": f"無法取得頁面: {e}"}), 500

    # 從 title 抽股票名稱
    m = re.search(r"<title>(\d{4,6})([一-鿿]+)股票", page_html)
    stock_name = m.group(2) if m else ticker

    # 注入 analysis-app-meta-data（讓 AI 識別正確股票）
    meta_attrs = (
        f' data-route="/{ticker}"'
        f' data-ticker="{ticker}"'
        f' data-stock-name="{stock_name}"'
        f' data-stock-company-name="{stock_name}"'
        f' data-stock-country="tw"'
        f' data-ticker-name="{ticker} {stock_name}"'
    )
    if 'id="analysis-app-meta-data"' in page_html:
        page_html = re.sub(
            r'<div id="analysis-app-meta-data"[^>]*>',
            f'<div id="analysis-app-meta-data"{meta_attrs}>',
            page_html, count=1,
        )
    else:
        page_html += f'<div id="analysis-app-meta-data"{meta_attrs}></div>'

    # 問題加入股票代號讓 AI 定位正確
    user_input = f"{ticker} {stock_name} {question}"

    payload = {
        "user_input": user_input,
        "metadata": {
            "page_context":     page_html,
            "selected_context": "",
            "current_url":      analysis_url,
            "labels":           [f"{ticker} {stock_name}"],
            "question_from":    "page_default_question",
        },
    }
    api_hdrs = {
        "Cookie":       cookie_hdr,
        "User-Agent":   ua,
        "Content-Type": "application/json",
        "Referer":      analysis_url,
    }

    try:
        r = http_req.post(
            "https://statementdog.com/api/v1/ai_chat",
            json=payload, headers=api_hdrs,
            stream=True, timeout=90,
        )
        r.encoding = "utf-8"

        full_text = ""
        buf = b""

        def parse_event(evt_bytes):
            try:
                text = evt_bytes.decode("utf-8")
            except UnicodeDecodeError:
                text = evt_bytes.decode("utf-8", errors="replace")
            parts = []
            for ln in text.splitlines():
                if ln.startswith("data:"):
                    parts.append(ln[5:].lstrip(" "))
            return "".join(parts)

        for chunk in r.iter_content(chunk_size=None):
            if not chunk:
                continue
            buf += chunk
            while b"\n\n" in buf:
                evt, buf = buf.split(b"\n\n", 1)
                data_str = parse_event(evt)
                if not data_str or data_str.strip() == "[DONE]":
                    continue
                try:
                    d = json.loads(data_str)
                    t = d.get("type", "")
                    if t == "delta":
                        full_text += d.get("content", "")
                    elif t == "done":
                        content = d.get("content", "")
                        if content and not full_text:
                            full_text = content
                except json.JSONDecodeError:
                    pass

        return jsonify({
            "answer":     full_text,
            "question":   question,
            "ticker":     ticker,
            "stock_name": stock_name,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5100)
