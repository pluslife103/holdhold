import { NextResponse } from "next/server";
import { getClient } from "@/lib/db";

export interface MAStock {
  ticker:     string;
  name:       string;
  sector:     string;
  currentCap: number;
  ma3:        number;
  ma10:       number;
  ma20:       number | null;
  ma30:       number;
  ma40:       number | null;
  ma50:       number | null;
  ma60:       number;
  ma120:      number | null;
  strength:   number;   // (ma3/ma60 - 1) × 100
  price:      number | null;
  changePct:  number | null;
  volume:     number | null;
  sparkline:  number[]; // last 30 market-cap values
}

export interface MASector {
  sector: string;
  stocks: MAStock[];
}

let cache: { data: MASector[]; ts: number } | null = null;
const CACHE_TTL = 30 * 60 * 1000;

function sma(caps: number[], period: number): number | null {
  if (caps.length < period) return null;
  const slice = caps.slice(caps.length - period);
  return slice.reduce((a, b) => a + b, 0) / period;
}

// date → cache entry (for historical queries)
const histCache = new Map<string, { data: MASector[]; ts: number }>();

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const targetDate = searchParams.get("date") ?? null; // null = latest

  // Use simple cache for "latest" (no date param)
  if (!targetDate && cache && Date.now() - cache.ts < CACHE_TTL) {
    return NextResponse.json(cache.data);
  }
  if (targetDate) {
    const h = histCache.get(targetDate);
    if (h && Date.now() - h.ts < CACHE_TTL * 2) {
      return NextResponse.json(h.data);
    }
  }

  const db = getClient();

  // Take 125 trading days UP TO (and including) targetDate
  const datesRs = targetDate
    ? await db.execute({
        sql: "SELECT DISTINCT date FROM snapshots WHERE date <= ? ORDER BY date DESC LIMIT 125",
        args: [targetDate],
      })
    : await db.execute(
        "SELECT DISTINCT date FROM snapshots ORDER BY date DESC LIMIT 125"
      );

  const dates = (datesRs.rows.map((r: { [key: number]: unknown }) => r[0] as string)).sort();
  if (dates.length < 60) {
    return NextResponse.json({ error: "歷史資料不足 60 天" }, { status: 422 });
  }

  // The "as-of" date for sparkline / price data is the last date in the window
  const asOfDate = dates[dates.length - 1];

  const ph = dates.map(() => "?").join(",");

  // 同時抓 market_cap 和 price（計算股價 60MA）
  const rs = await db.execute({
    sql: `SELECT date, ticker, name, sector, market_cap, price
          FROM snapshots
          WHERE date IN (${ph}) AND market_cap >= 300000000 AND sector != '' AND sector != 'Unknown'
          ORDER BY ticker ASC, date ASC`,
    args: dates,
  });

  const tickerMap = new Map<string, {
    name: string; sector: string; caps: number[]; prices: number[];
  }>();
  for (const r of rs.rows) {
    const ticker = r[1] as string;
    const name   = r[2] as string;
    const sector = r[3] as string;
    const cap    = r[4] as number;
    const price  = r[5] as number;
    if (!tickerMap.has(ticker)) tickerMap.set(ticker, { name, sector, caps: [], prices: [] });
    const entry = tickerMap.get(ticker)!;
    entry.caps.push(cap);
    if (price > 0) entry.prices.push(price);
  }

  const qualifying: MAStock[] = [];

  for (const [ticker, { name, sector, caps, prices }] of tickerMap) {
    if (caps.length < 60) continue;

    // ── 條件 1: 市值 10MA > 30MA > 60MA ────────────────────────────────────
    const ma10 = sma(caps, 10);
    const ma30 = sma(caps, 30);
    const ma60 = sma(caps, 60);
    if (!ma10 || !ma30 || !ma60) continue;
    if (!(ma10 > ma30 && ma30 > ma60)) continue;

    // ── 條件 2: 股價 > 60日均價 ──────────────────────────────────────────────
    const price60ma = sma(prices, 60);
    const currentPrice = prices[prices.length - 1] ?? 0;
    if (!price60ma || currentPrice <= price60ma) continue;

    // ── 條件 3: 過去 7 天有小回檔（跌幅 1%–8%，且已回升）────────────────────
    const last7 = caps.slice(-7);
    if (last7.length < 4) continue;
    const high7    = Math.max(...last7);
    const low7     = Math.min(...last7);
    const current  = caps[caps.length - 1];
    const pullback = (high7 - low7) / high7; // 最大回檔幅度
    const recovering = current > low7;        // 現在高於最低點（回升中）
    const hasSmallPullback = pullback >= 0.01 && pullback <= 0.08 && recovering;
    if (!hasSmallPullback) continue;

    const ma3 = sma(caps, 3)!;
    qualifying.push({
      ticker,
      name,
      sector,
      currentCap: current,
      ma3,
      ma10,
      ma20:  sma(caps, 20),
      ma30,
      ma40:  sma(caps, 40),
      ma50:  sma(caps, 50),
      ma60,
      ma120: sma(caps, 120),
      strength: (ma10 / ma60 - 1) * 100,  // 以 10MA/60MA 衡量強度
      price:     currentPrice,
      changePct: null,
      volume:    null,
      sparkline: caps.slice(-30),
    });
  }

  // 補充即時股價 / 漲跌幅 / 成交量（使用 asOfDate 當天的資料）
  const latestDate = asOfDate;
  const latestRs = await db.execute({
    sql: "SELECT ticker, price, change_pct, volume FROM snapshots WHERE date = ?",
    args: [latestDate],
  });
  const latestMap = new Map<string, { price: number; changePct: number; volume: number }>();
  for (const r of latestRs.rows) {
    latestMap.set(r[0] as string, {
      price:     r[1] as number,
      changePct: r[2] as number,
      volume:    r[3] as number,
    });
  }
  for (const s of qualifying) {
    const l = latestMap.get(s.ticker);
    if (l) { s.price = l.price; s.changePct = l.changePct; s.volume = l.volume; }
  }

  // 依板塊分組
  const sectorMap = new Map<string, MAStock[]>();
  for (const s of qualifying) {
    if (!sectorMap.has(s.sector)) sectorMap.set(s.sector, []);
    sectorMap.get(s.sector)!.push(s);
  }

  const result: MASector[] = [...sectorMap.entries()]
    .map(([sector, stocks]) => ({
      sector,
      stocks: stocks.sort((a, b) => b.strength - a.strength),
    }))
    .sort((a, b) => b.stocks.length - a.stocks.length);

  if (targetDate) {
    histCache.set(targetDate, { data: result, ts: Date.now() });
  } else {
    cache = { data: result, ts: Date.now() };
  }
  return NextResponse.json(result);
}
