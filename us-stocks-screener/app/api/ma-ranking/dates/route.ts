import { NextResponse } from "next/server";
import { getClient } from "@/lib/db";

// Returns all available dates sampled every 3 trading days (newest first)
export async function GET() {
  const db = getClient();
  const rs = await db.execute(
    "SELECT DISTINCT date FROM snapshots ORDER BY date DESC LIMIT 365"
  );
  const all = rs.rows.map((r) => r[0] as string); // newest first

  // Every 3rd trading day: index 0 (latest), 3, 6, 9 ...
  const sampled = all.filter((_, i) => i % 3 === 0);

  return NextResponse.json({ dates: sampled });
}
