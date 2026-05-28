import { createClient, type Client } from "@libsql/client";
import path from "path";

// ── Client (singleton) ────────────────────────────────────────────────────────
// Local dev  : TURSO_DATABASE_URL=file:./data/history.db  (or unset → default path)
// Production : TURSO_DATABASE_URL=libsql://xxx.turso.io   TURSO_AUTH_TOKEN=...

let _client: Client | null = null;

export function getClient(): Client {
  if (_client) return _client;

  const url =
    process.env.TURSO_DATABASE_URL ||
    `file:${path.join(process.cwd(), "data", "history.db")}`;

  _client = createClient({
    url,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });
  return _client;
}

// ── Schema init ───────────────────────────────────────────────────────────────

let _initialized = false;

async function ensureSchema() {
  if (_initialized) return;
  const db = getClient();
  await db.executeMultiple(`
    CREATE TABLE IF NOT EXISTS snapshots (
      date       TEXT NOT NULL,
      ticker     TEXT NOT NULL,
      name       TEXT,
      sector     TEXT,
      industry   TEXT,
      country    TEXT,
      price      REAL,
      market_cap REAL,
      change_pct REAL,
      volume     REAL,
      PRIMARY KEY (date, ticker)
    );
    CREATE INDEX IF NOT EXISTS idx_snap_date   ON snapshots(date);
    CREATE INDEX IF NOT EXISTS idx_snap_ticker ON snapshots(ticker);
  `);
  _initialized = true;
}

// ── Types ─────────────────────────────────────────────────────────────────────

export interface SnapshotRow {
  date: string;
  ticker: string;
  name: string;
  sector: string;
  industry: string;
  country: string;
  price: number | null;
  market_cap: number | null;
  change_pct: number | null;
  volume: number | null;
}

// ── Snapshot queries ──────────────────────────────────────────────────────────

export async function getAvailableDates(): Promise<string[]> {
  await ensureSchema();
  const rs = await getClient().execute(
    "SELECT DISTINCT date FROM snapshots ORDER BY date DESC LIMIT 365"
  );
  return rs.rows.map((r) => r[0] as string);
}

export async function hasSnapshotForDate(date: string): Promise<boolean> {
  await ensureSchema();
  const rs = await getClient().execute({
    sql: "SELECT COUNT(*) FROM snapshots WHERE date = ?",
    args: [date],
  });
  return (rs.rows[0][0] as number) > 0;
}

export async function getSnapshotForDate(date: string): Promise<SnapshotRow[]> {
  await ensureSchema();
  const rs = await getClient().execute({
    sql: `SELECT date, ticker, name, sector, industry, country,
                 price, market_cap, change_pct, volume
          FROM snapshots WHERE date = ?
          ORDER BY market_cap DESC`,
    args: [date],
  });
  return rs.rows.map((r) => ({
    date:       r[0] as string,
    ticker:     r[1] as string,
    name:       r[2] as string,
    sector:     r[3] as string,
    industry:   r[4] as string,
    country:    r[5] as string,
    price:      r[6] as number | null,
    market_cap: r[7] as number | null,
    change_pct: r[8] as number | null,
    volume:     r[9] as number | null,
  }));
}

export async function saveSnapshot(
  date: string,
  rows: Omit<SnapshotRow, "date">[]
): Promise<number> {
  await ensureSchema();
  const db = getClient();
  // Batch in chunks of 500 to stay under libsql limits
  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const stmts = chunk.map((r) => ({
      sql: `INSERT OR REPLACE INTO snapshots
              (date,ticker,name,sector,industry,country,price,market_cap,change_pct,volume)
            VALUES (?,?,?,?,?,?,?,?,?,?)`,
      args: [date, r.ticker, r.name, r.sector, r.industry, r.country,
             r.price, r.market_cap, r.change_pct, r.volume],
    }));
    await db.batch(stmts, "write");
  }
  return rows.length;
}

// ── Ticker history & search ───────────────────────────────────────────────────

export interface TickerHistoryRow {
  date: string;
  ticker: string;
  name: string;
  market_cap: number;
  price: number;
  change_pct: number | null;
}

export async function getTickerHistory(tickers: string[]): Promise<TickerHistoryRow[]> {
  if (tickers.length === 0) return [];
  await ensureSchema();
  const ph = tickers.map(() => "?").join(",");
  const rs = await getClient().execute({
    sql: `SELECT date, ticker, name, market_cap, price, change_pct
          FROM snapshots
          WHERE ticker IN (${ph}) AND market_cap > 0
          ORDER BY ticker, date ASC`,
    args: tickers,
  });
  return rs.rows.map((r) => ({
    date:       r[0] as string,
    ticker:     r[1] as string,
    name:       r[2] as string,
    market_cap: r[3] as number,
    price:      r[4] as number,
    change_pct: r[5] as number | null,
  }));
}

export async function searchTickers(
  query: string, limit = 20
): Promise<{ ticker: string; name: string; sector: string }[]> {
  await ensureSchema();
  const q = `%${query.toUpperCase()}%`;
  const ql = `%${query}%`;
  const rs = await getClient().execute({
    sql: `SELECT DISTINCT ticker, name, sector
          FROM snapshots
          WHERE (UPPER(ticker) LIKE ? OR name LIKE ?)
            AND market_cap > 0
          ORDER BY ticker LIMIT ?`,
    args: [q, ql, limit],
  });
  return rs.rows.map((r) => ({
    ticker: r[0] as string,
    name:   r[1] as string,
    sector: r[2] as string,
  }));
}

// ── Crossover detection ───────────────────────────────────────────────────────

export type TierLabel = "超大型" | "巨型" | "大型";

function capTier(cap: number): TierLabel | null {
  if (cap >= 800e9) return "超大型";
  if (cap >= 200e9) return "巨型";
  if (cap >= 70e9)  return "大型";
  return null;
}

export interface CrossoverEvent {
  date:       string;
  tier:       TierLabel;
  winner:     string;
  winnerName: string;
  winnerCap:  number;
  winnerPrev: number;
  loser:      string;
  loserName:  string;
  loserCap:   number;
  loserPrev:  number;
}

export async function getCrossovers(lookbackDays = 30): Promise<CrossoverEvent[]> {
  await ensureSchema();
  const db = getClient();

  const datesRs = await db.execute({
    sql: "SELECT DISTINCT date FROM snapshots ORDER BY date DESC LIMIT ?",
    args: [lookbackDays + 1],
  });
  if (datesRs.rows.length < 2) return [];
  const dates = (datesRs.rows.map((r) => r[0] as string)).sort();

  const ph = dates.map(() => "?").join(",");
  const rs = await db.execute({
    sql: `SELECT date, ticker, name, market_cap FROM snapshots
          WHERE date IN (${ph}) AND market_cap >= 70000000000
          ORDER BY date ASC, market_cap DESC`,
    args: dates,
  });

  const byDate = new Map<string, Map<string, { name: string; cap: number }>>();
  for (const r of rs.rows) {
    const date   = r[0] as string;
    const ticker = r[1] as string;
    const name   = r[2] as string;
    const cap    = r[3] as number;
    if (!byDate.has(date)) byDate.set(date, new Map());
    byDate.get(date)!.set(ticker, { name, cap });
  }

  const events: CrossoverEvent[] = [];

  for (let i = 1; i < dates.length; i++) {
    const today = byDate.get(dates[i]);
    const prev  = byDate.get(dates[i - 1]);
    if (!today || !prev) continue;

    const common = [...today.keys()].filter((t) => prev.has(t));

    for (let j = 0; j < common.length; j++) {
      for (let k = j + 1; k < common.length; k++) {
        const a = common[j], b = common[k];
        const tA = today.get(a)!, tB = today.get(b)!;
        const pA = prev.get(a)!,  pB = prev.get(b)!;
        const tierA = capTier(tA.cap), tierB = capTier(tB.cap);
        if (!tierA || tierA !== tierB) continue;

        if (tA.cap > tB.cap && pA.cap <= pB.cap) {
          events.push({ date: dates[i], tier: tierA, winner: a, winnerName: tA.name, winnerCap: tA.cap, winnerPrev: pA.cap, loser: b, loserName: tB.name, loserCap: tB.cap, loserPrev: pB.cap });
        } else if (tB.cap > tA.cap && pB.cap <= pA.cap) {
          events.push({ date: dates[i], tier: tierB, winner: b, winnerName: tB.name, winnerCap: tB.cap, winnerPrev: pB.cap, loser: a, loserName: tA.name, loserCap: tA.cap, loserPrev: pA.cap });
        }
      }
    }
  }

  return events.sort((a, b) => b.date.localeCompare(a.date) || b.winnerCap - a.winnerCap);
}
