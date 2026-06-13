#!/usr/bin/env python3
"""
TWSE 買賣日報表爬蟲 (bsr.twse.com.tw/bshtm/)
爬取個股每日各券商分點的買進/賣出/買賣超股數
"""

import io
import re
import sys
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from PIL import Image, ImageOps, ImageFilter, ImageEnhance

try:
    import ddddocr
    _ocr = ddddocr.DdddOcr(show_ad=False)
except ImportError:
    _ocr = None

BASE_URL = "https://bsr.twse.com.tw/bshtm/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


# ---------------------------------------------------------------------------
# Captcha helpers
# ---------------------------------------------------------------------------

def _preprocess_captcha(img_bytes: bytes) -> bytes:
    """Invert + contrast-boost + threshold for cleaner OCR on TWSE dark-bg captcha."""
    img = Image.open(io.BytesIO(img_bytes)).convert("L")
    img = ImageOps.invert(img)
    img = ImageEnhance.Contrast(img).enhance(2.5)
    img = img.point(lambda p: 255 if p > 110 else 0, "1").convert("L")
    img = img.filter(ImageFilter.SHARPEN)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _solve_captcha(session: requests.Session, guid: str) -> str:
    url = BASE_URL + f"CaptchaImage.aspx?guid={guid}"
    resp = session.get(url, timeout=10)
    resp.raise_for_status()

    if _ocr:
        processed = _preprocess_captcha(resp.content)
        text = _ocr.classification(processed)
        return re.sub(r"[^A-Za-z0-9]", "", text).upper()

    # Manual fallback
    import os, tempfile
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(resp.content)
        tmp = f.name
    print(f"[captcha] 圖片儲存至: {tmp}")
    answer = input("請輸入驗證碼（5碼英數字）: ").strip().upper()
    os.unlink(tmp)
    return answer


# ---------------------------------------------------------------------------
# POST / form helpers
# ---------------------------------------------------------------------------

def _parse_form(html: str):
    """Return (hidden_fields, captcha_guid)."""
    soup = BeautifulSoup(html, "html.parser")
    hidden = {}
    for inp in soup.find_all("input", type="hidden"):
        name = inp.get("name")
        if name:
            hidden[name] = inp.get("value", "")
    guid = None
    img = soup.find("img", src=re.compile(r"CaptchaImage\.aspx", re.I))
    if img:
        m = re.search(r"[Gg][Uu][Ii][Dd]=([^&\"'\s]+)", img["src"])
        if m:
            guid = m.group(1)
    return hidden, guid


def _extract_csv_url(html: str, trade_type: str = "normal") -> tuple[str, str] | tuple[None, None]:
    """
    After POST success, the response contains:
      JS redirect: top.page2.location.href='bsContent.aspx?v=t&StkNo=2330&RecCount=49'
      CSV link:    href="bsContent.aspx?StkNo=2330&RecCount=49"
    Return (stock_id, rec_count) for the CSV download URL.
    """
    m = re.search(r"bsContent\.aspx\?[^\"']*StkNo=(\w+)[^\"']*RecCount=(\d+)", html)
    if m:
        return m.group(1), m.group(2)
    return None, None


# ---------------------------------------------------------------------------
# CSV parser
# ---------------------------------------------------------------------------

def _parse_csv(raw: bytes) -> pd.DataFrame | None:
    """
    Parse TWSE bsContent CSV (BIG5 encoded, two-column layout).

    CSV structure (big5):
      Row 0: title
      Row 1: 股票代碼,="XXXX"
      Row 2: 序號,券商,價格,買進股數,賣出股數,,序號,券商,價格,買進股數,賣出股數
      Row 3+: left_seq,left_broker,left_price,left_buy,left_sell,,right_seq,...

    Returns aggregated DataFrame: 券商代號, 券商名稱, 買進股數, 賣出股數, 買賣超股數
    """
    text = raw.decode("big5", errors="replace")
    lines = text.splitlines()

    # Find data start: first line that starts with a digit
    data_start = 0
    for i, line in enumerate(lines):
        if re.match(r"^\d", line.strip()):
            data_start = i
            break

    if data_start == 0:
        return None

    records: list[dict] = []
    for line in lines[data_start:]:
        line = line.strip()
        if not line:
            continue
        # Two-column layout: left,,right (separated by ",,")
        halves = re.split(r",,", line)
        for half in halves:
            half = half.strip().rstrip()
            parts = half.split(",")
            if len(parts) < 5:
                continue
            # parts: [seq, broker, price, buy, sell]
            broker_raw = parts[1].strip()
            if not broker_raw or not re.match(r"^[A-Za-z0-9]{4}", broker_raw):
                continue
            code = broker_raw[:4]  # case-sensitive: 9A9s ≠ 9A9S
            name = broker_raw[4:].strip()
            try:
                buy = int(parts[3].strip().replace(",", "")) if parts[3].strip() else 0
                sell = int(parts[4].strip().replace(",", "")) if parts[4].strip() else 0
            except ValueError:
                continue
            records.append({"券商代號": code, "券商名稱": name, "買進股數": buy, "賣出股數": sell})

    if not records:
        return None

    df = pd.DataFrame(records)
    agg = (
        df.groupby(["券商代號", "券商名稱"], as_index=False)
        .agg({"買進股數": "sum", "賣出股數": "sum"})
        .assign(買賣超股數=lambda x: x["買進股數"] - x["賣出股數"])
        .sort_values("買賣超股數", ascending=False)
        .reset_index(drop=True)
    )
    return agg


# ---------------------------------------------------------------------------
# Main public API
# ---------------------------------------------------------------------------

def query_stock(
    stock_id: str,
    trade_type: str = "normal",
    max_retry: int = 8,
    delay: float = 1.2,
    verbose: bool = True,
) -> pd.DataFrame | None:
    """
    查詢個股當日券商分點買賣紀錄。

    Args:
        stock_id  : 股票代碼，例如 '2330'
        trade_type: 'normal'（一般交易）或 'block'（鉅額交易）
        max_retry : 驗證碼失敗最大重試次數
        delay     : 每次重試間隔秒數
        verbose   : 是否印出進度

    Returns:
        DataFrame 含 券商代號/券商名稱/買進股數/賣出股數/買賣超股數（按買賣超降序），
        查無資料回傳空 DataFrame，失敗回傳 None。
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    for attempt in range(1, max_retry + 1):
        try:
            # Step 1: GET page
            resp = session.get(BASE_URL + "bsMenu.aspx", timeout=15)
            resp.encoding = "utf-8"
            hidden, guid = _parse_form(resp.text)

            if not guid:
                if verbose:
                    print(f"  [{attempt}] 找不到驗證碼 GUID")
                time.sleep(delay)
                continue

            # Step 2: Solve captcha
            captcha = _solve_captcha(session, guid)
            if verbose:
                print(f"  [{attempt}] captcha={captcha}", flush=True)

            # Step 3: Build POST payload
            payload = dict(hidden)
            if trade_type == "block":
                payload["RadioButton_Excd"] = "RadioButton_Excd"
            else:
                payload["RadioButton_Normal"] = "RadioButton_Normal"
            payload["TextBox_Stkno"] = stock_id.strip()
            payload["CaptchaControl1"] = captcha
            payload["btnOK"] = "查詢"

            # Step 4: POST to get the RecCount
            resp2 = session.post(
                BASE_URL + "bsMenu.aspx",
                data=payload,
                headers={"Referer": BASE_URL + "bsMenu.aspx"},
                timeout=20,
            )
            resp2.encoding = "utf-8"
            html = resp2.text

            if "驗證碼錯誤" in html:
                if verbose:
                    print(f"  [{attempt}] 驗證碼錯誤，重試…", flush=True)
                time.sleep(delay)
                continue

            if "查無" in html or "no data" in html.lower():
                if verbose:
                    print(f"  [{stock_id}] 查無資料", flush=True)
                return pd.DataFrame()

            stkno, rec_count = _extract_csv_url(html, trade_type)
            if not stkno:
                if verbose:
                    print(f"  [{attempt}] 找不到 CSV URL，重試…", flush=True)
                time.sleep(delay)
                continue

            # Step 5: Download CSV (BIG5, two-column layout)
            csv_url = BASE_URL + "bsContent.aspx?StkNo=" + stkno + "&RecCount=" + rec_count
            resp3 = session.get(csv_url, timeout=20)

            df = _parse_csv(resp3.content)
            if df is not None and not df.empty:
                if verbose:
                    print(f"  → {len(df)} 個分點", flush=True)
                return df

            if verbose:
                print(f"  [{attempt}] CSV 解析失敗，重試…", flush=True)
            time.sleep(delay)

        except requests.RequestException as exc:
            if verbose:
                print(f"  [{attempt}] 網路錯誤: {exc}", flush=True)
            time.sleep(delay * 2)

    if verbose:
        print(f"  [失敗] {stock_id} 超過最大重試次數 ({max_retry})", flush=True)
    return None


def query_batch(
    stock_ids: list[str],
    trade_type: str = "normal",
    delay_between: float = 3.0,
    save_csv: bool = True,
    out_dir: str = ".",
) -> dict[str, pd.DataFrame]:
    """批次查詢多支個股，回傳 {stock_id: DataFrame}。"""
    today = date.today().strftime("%Y%m%d")
    import os
    results: dict[str, pd.DataFrame] = {}

    for sid in stock_ids:
        print(f"\n[{sid}] 查詢中…", flush=True)
        df = query_stock(sid, trade_type=trade_type)
        results[sid] = df

        if df is not None and not df.empty:
            print(f"  → {len(df)} 個分點")
            if save_csv:
                fname = os.path.join(out_dir, f"bshtm_{sid}_{today}.csv")
                df.to_csv(fname, index=False, encoding="utf-8-sig")
                print(f"  → 已存 {fname}")
        elif df is not None:
            print(f"  → 查無資料")
        else:
            print(f"  → 查詢失敗")

        if sid != stock_ids[-1]:
            time.sleep(delay_between)

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")

    ids = sys.argv[1:] if len(sys.argv) > 1 else []
    if not ids:
        raw = input("請輸入股票代碼（多筆用空格分隔，例如 2330 2317）: ").strip()
        ids = raw.split() if raw else ["2330"]

    if len(ids) == 1:
        df = query_stock(ids[0])
        if df is not None and not df.empty:
            print(f"\n=== {ids[0]} 今日券商分點 ===")
            print(df.to_string(index=False))
            out = f"bshtm_{ids[0]}_{date.today().strftime('%Y%m%d')}.csv"
            df.to_csv(out, index=False, encoding="utf-8-sig")
            print(f"\n已存 {out}")
        elif df is not None:
            print("今日查無資料")
        else:
            print("查詢失敗")
    else:
        results = query_batch(ids, save_csv=True)
        print(f"\n[完成] 共查詢 {len(results)} 支股票")
