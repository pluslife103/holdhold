import { NextResponse } from "next/server";

export interface StockQuote {
  ticker: string;
  name: string;
  sector: string;
  industry: string;
  country: string;
  price: number | null;
  marketCap: number | null;
  changePercent: number | null;
  netChange: number | null;
  volume: number | null;
  ipoYear: string;
}

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

let cache: { data: StockQuote[]; ts: number } | null = null;
const CACHE_TTL = 15 * 60 * 1000;

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

export async function GET() {
  if (cache && Date.now() - cache.ts < CACHE_TTL) {
    return NextResponse.json(cache.data);
  }

  const url =
    "https://api.nasdaq.com/api/screener/stocks?tableonly=true&download=true";

  const res = await fetch(url, {
    headers: {
      Accept: "application/json",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    },
    next: { revalidate: 0 },
  });

  if (!res.ok) {
    return NextResponse.json(
      { error: `NASDAQ API error: ${res.status}` },
      { status: 502 }
    );
  }

  const json = await res.json();
  const rows: NasdaqRow[] = json?.data?.rows ?? [];

  const result: StockQuote[] = rows.map((row) => ({
    ticker: row.symbol,
    name: row.name?.trim() ?? "",
    sector: row.sector || "Unknown",
    industry: row.industry || "",
    country: row.country || "",
    price: parsePrice(row.lastsale),
    marketCap: parseNum(row.marketCap),
    changePercent: parsePct(row.pctchange),
    netChange: parseNum(row.netchange),
    volume: parseNum(row.volume),
    ipoYear: row.ipoyear || "",
  }));

  cache = { data: result, ts: Date.now() };
  return NextResponse.json(result);
}
