import { NextResponse } from "next/server";
import { getAvailableDates } from "@/lib/db";

export async function GET() {
  const dates = await getAvailableDates();
  return NextResponse.json({ dates });
}
