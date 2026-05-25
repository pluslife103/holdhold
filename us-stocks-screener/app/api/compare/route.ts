import { NextResponse } from "next/server";
import { getTickerHistory, searchTickers } from "@/lib/db";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const tickersParam = searchParams.get("tickers") ?? "";
  const search = searchParams.get("search") ?? "";

  // Ticker search mode
  if (search) {
    const results = await searchTickers(search, 15);
    return NextResponse.json(results);
  }

  const tickers = tickersParam
    .split(",")
    .map((t) => t.trim().toUpperCase())
    .filter(Boolean)
    .slice(0, 10); // max 10 tickers

  if (tickers.length === 0) {
    return NextResponse.json({ error: "no tickers" }, { status: 400 });
  }

  const rows = await getTickerHistory(tickers);

  // Group by ticker, build series
  const byTicker = new Map<string, { date: string; marketCap: number; price: number; changePct: number | null }[]>();
  const names = new Map<string, string>();

  for (const r of rows) {
    if (!byTicker.has(r.ticker)) byTicker.set(r.ticker, []);
    byTicker.get(r.ticker)!.push({
      date: r.date,
      marketCap: r.market_cap,
      price: r.price,
      changePct: r.change_pct,
    });
    if (!names.has(r.ticker)) names.set(r.ticker, r.name);
  }

  const series = tickers
    .filter((t) => byTicker.has(t))
    .map((ticker) => ({
      ticker,
      name: names.get(ticker) ?? ticker,
      data: byTicker.get(ticker)!,
    }));

  return NextResponse.json(series);
}
