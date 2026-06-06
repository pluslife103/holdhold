from flask import Flask, jsonify, render_template
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os, json

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


if __name__ == "__main__":
    app.run(debug=True, port=5100)
