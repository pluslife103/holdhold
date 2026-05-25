import { NextResponse } from "next/server";
import { getSnapshotForDate } from "@/lib/db";
import { StockQuote } from "@/app/api/stocks/route";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ date: string }> }
) {
  const { date } = await params;

  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return NextResponse.json({ error: "invalid date format" }, { status: 400 });
  }

  const rows = await getSnapshotForDate(date);
  if (rows.length === 0) {
    return NextResponse.json({ error: "no data for this date" }, { status: 404 });
  }

  const result: StockQuote[] = rows.map((r) => ({
    ticker: r.ticker,
    name: r.name,
    sector: r.sector,
    industry: r.industry,
    country: r.country,
    price: r.price,
    marketCap: r.market_cap,
    changePercent: r.change_pct,
    netChange: null,
    volume: r.volume,
    ipoYear: "",
  }));

  return NextResponse.json(result);
}
