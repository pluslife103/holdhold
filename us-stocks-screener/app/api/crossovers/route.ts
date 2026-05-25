import { NextResponse } from "next/server";
import { getCrossovers } from "@/lib/db";

let cache: { data: Awaited<ReturnType<typeof getCrossovers>>; ts: number } | null = null;
const CACHE_TTL = 10 * 60 * 1000;

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const days = Math.min(60, Math.max(1, Number(searchParams.get("days") ?? 30)));

  if (cache && Date.now() - cache.ts < CACHE_TTL) {
    return NextResponse.json(cache.data);
  }

  const events = await getCrossovers(days);
  cache = { data: events, ts: Date.now() };
  return NextResponse.json(events);
}
