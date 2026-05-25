import { NextResponse } from "next/server";
import { getClient } from "@/lib/db";
import type { TierKey, CombinedStock, CombinedTier } from "@/types/combined";

const TIER_ORDER: TierKey[] = ["超大型", "巨型", "大型", "中大型", "中型", "小型"];

function capTier(cap: number): TierKey {
  if (cap >= 800e9) return "超大型";
  if (cap >= 200e9) return "巨型";
  if (cap >= 70e9)  return "大型";
  if (cap >= 10e9)  return "中大型";
  if (cap >= 2e9)   return "中型";
  return "小型";
}

function capTierTop(c: number) {
  if (c >= 800e9) return "超大型";
  if (c >= 200e9) return "巨型";
  if (c >= 70e9)  return "大型";
  return null;
}

function sma(caps: number[], period: number): number | null {
  if (caps.length < period) return null;
  const s = caps.slice(caps.length - period);
  return s.reduce((a, b) => a + b, 0) / period;
}

let cache: { data: CombinedTier[]; ts: number } | null = null;
const CACHE_TTL = 15 * 60 * 1000;

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const lookback = Math.min(60, Math.max(7, Number(searchParams.get("days") ?? 30)));

  if (cache && Date.now() - cache.ts < CACHE_TTL) {
    return NextResponse.json(cache.data);
  }

  const db = getClient();

  // ── 1. MA 多頭排列 ──────────────────────────────────────────────────────────
  const datesRs = await db.execute(
    "SELECT DISTINCT date FROM snapshots ORDER BY date DESC LIMIT 65"
  );
  const dates = datesRs.rows.map((r) => r[0] as string).sort();

  if (dates.length < 60) return NextResponse.json([]);

  const ph = dates.map(() => "?").join(",");
  const priceRs = await db.execute({
    sql: `SELECT date, ticker, name, sector, market_cap FROM snapshots
          WHERE date IN (${ph}) AND market_cap >= 2000000000 AND sector != '' AND sector != 'Unknown'
          ORDER BY ticker ASC, date ASC`,
    args: dates,
  });

  const tickerMap = new Map<string, { name: string; sector: string; caps: number[] }>();
  for (const r of priceRs.rows) {
    const ticker = r[1] as string;
    const name   = r[2] as string;
    const sector = r[3] as string;
    const cap    = r[4] as number;
    if (!tickerMap.has(ticker)) tickerMap.set(ticker, { name, sector, caps: [] });
    tickerMap.get(ticker)!.caps.push(cap);
  }

  const maStocks = new Map<string, {
    name: string; sector: string; currentCap: number;
    maStrength: number; ma3: number; ma10: number; ma30: number; ma60: number;
  }>();

  for (const [ticker, { name, sector, caps }] of tickerMap) {
    if (caps.length < 60) continue;
    const ma3  = sma(caps, 3)!;
    const ma10 = sma(caps, 10);
    const ma30 = sma(caps, 30);
    const ma60 = sma(caps, 60);
    if (!ma10 || !ma30 || !ma60) continue;
    if (!(ma3 > ma10 && ma10 > ma30 && ma30 > ma60)) continue;
    maStocks.set(ticker, {
      name, sector,
      currentCap: caps[caps.length - 1],
      maStrength: (ma3 / ma60 - 1) * 100,
      ma3, ma10, ma30, ma60,
    });
  }

  // ── 2. 超越榜 ───────────────────────────────────────────────────────────────
  const lDatesRs = await db.execute({
    sql: "SELECT DISTINCT date FROM snapshots ORDER BY date DESC LIMIT ?",
    args: [lookback + 1],
  });
  const lDates = lDatesRs.rows.map((r) => r[0] as string).sort();

  const ph2 = lDates.map(() => "?").join(",");
  const crossRs = await db.execute({
    sql: `SELECT date, ticker, market_cap FROM snapshots
          WHERE date IN (${ph2}) AND market_cap >= 70000000000
          ORDER BY date ASC, market_cap DESC`,
    args: lDates,
  });

  const byDate = new Map<string, Map<string, number>>();
  for (const r of crossRs.rows) {
    const date   = r[0] as string;
    const ticker = r[1] as string;
    const cap    = r[2] as number;
    if (!byDate.has(date)) byDate.set(date, new Map());
    byDate.get(date)!.set(ticker, cap);
  }

  const crossMap = new Map<string, { count: number; losers: Set<string>; lastDate: string }>();

  for (let i = 1; i < lDates.length; i++) {
    const today = byDate.get(lDates[i]);
    const prev  = byDate.get(lDates[i - 1]);
    if (!today || !prev) continue;
    const common = [...today.keys()].filter((t) => prev.has(t));

    for (let j = 0; j < common.length; j++) {
      for (let k = j + 1; k < common.length; k++) {
        const a = common[j], b = common[k];
        const tA = today.get(a)!, tB = today.get(b)!;
        const pA = prev.get(a)!,  pB = prev.get(b)!;
        const tierA = capTierTop(tA), tierB = capTierTop(tB);
        if (!tierA || tierA !== tierB) continue;

        let winner: string | null = null, loser: string | null = null;
        if (tA > tB && pA <= pB) { winner = a; loser = b; }
        else if (tB > tA && pB <= pA) { winner = b; loser = a; }

        if (winner && loser) {
          if (!crossMap.has(winner)) crossMap.set(winner, { count: 0, losers: new Set(), lastDate: "" });
          const e = crossMap.get(winner)!;
          e.count++;
          e.losers.add(loser);
          e.lastDate = lDates[i];
        }
      }
    }
  }

  // ── 3. 交集 ─────────────────────────────────────────────────────────────────
  const combined: CombinedStock[] = [];
  for (const [ticker, crossInfo] of crossMap) {
    const ma = maStocks.get(ticker);
    if (!ma) continue;
    combined.push({
      ticker,
      name:              ma.name,
      sector:            ma.sector,
      tier:              capTier(ma.currentCap),
      currentCap:        ma.currentCap,
      maStrength:        ma.maStrength,
      ma3:               ma.ma3,
      ma10:              ma.ma10,
      ma30:              ma.ma30,
      ma60:              ma.ma60,
      crossoverCount:    crossInfo.count,
      uniqueLosers:      crossInfo.losers.size,
      lastCrossoverDate: crossInfo.lastDate,
    });
  }

  // ── 4. 依規模分組 ───────────────────────────────────────────────────────────
  const score = (s: CombinedStock) => s.maStrength * 0.6 + Math.log1p(s.crossoverCount) * 10;
  const tierMap = new Map<TierKey, CombinedStock[]>();
  for (const s of combined) {
    if (!tierMap.has(s.tier)) tierMap.set(s.tier, []);
    tierMap.get(s.tier)!.push(s);
  }

  const result: CombinedTier[] = TIER_ORDER
    .filter((t) => tierMap.has(t))
    .map((tier) => ({
      tier,
      stocks: tierMap.get(tier)!.sort((a, b) => score(b) - score(a)),
    }));

  cache = { data: result, ts: Date.now() };
  return NextResponse.json(result);
}
