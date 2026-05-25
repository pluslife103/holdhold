import { NextResponse } from "next/server";
import { hasSnapshotForDate, saveSnapshot } from "@/lib/db";

interface NasdaqRow {
  symbol: string;
  name: string;
  lastsale: string;
  netchange: string;
  pctchange: string;
  marketCap: string;
  country: string;
  ipoyear: string;
  volume: string;
  sector: string;
  industry: string;
}

function parsePrice(s: string): number | null {
  if (!s) return null;
  const n = parseFloat(s.replace(/[$,]/g, ""));
  return isNaN(n) ? null : n;
}

function parseNum(s: string): number | null {
  if (!s) return null;
  const n = parseFloat(s.replace(/,/g, ""));
  return isNaN(n) || n === 0 ? null : n;
}

function parsePct(s: string): number | null {
  if (!s) return null;
  const n = parseFloat(s.replace(/%/g, ""));
  return isNaN(n) ? null : n;
}

export async function POST(req: Request) {
  const body = await req.json().catch(() => ({}));
  const today = new Date().toISOString().slice(0, 10);
  const date: string = body.date ?? today;
  const force: boolean = body.force ?? false;

  if (!force && await hasSnapshotForDate(date)) {
    return NextResponse.json({ ok: true, skipped: true, date, message: "已有該日期的快照" });
  }

  const url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&download=true";
  const res = await fetch(url, {
    headers: {
      Accept: "application/json",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    },
    cache: "no-store",
  });

  if (!res.ok) {
    return NextResponse.json({ ok: false, error: `NASDAQ fetch failed: ${res.status}` }, { status: 502 });
  }

  const json = await res.json();
  const rows: NasdaqRow[] = json?.data?.rows ?? [];

  const records = rows.map((row) => ({
    ticker: row.symbol,
    name: row.name?.trim() ?? "",
    sector: row.sector || "Unknown",
    industry: row.industry || "",
    country: row.country || "",
    price: parsePrice(row.lastsale),
    market_cap: parseNum(row.marketCap),
    change_pct: parsePct(row.pctchange),
    volume: parseNum(row.volume),
  }));

  const saved = await saveSnapshot(date, records);
  return NextResponse.json({ ok: true, skipped: false, date, saved });
}
