/**
 * 將本機 SQLite 資料遷移至 Turso 雲端資料庫
 * 使用前先設定環境變數：
 *   TURSO_DATABASE_URL=libsql://your-db.turso.io
 *   TURSO_AUTH_TOKEN=your-token
 *
 * 執行：node scripts/migrate-to-turso.mjs
 */

import { createClient } from "@libsql/client";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LOCAL_DB  = path.join(__dirname, "..", "data", "history.db");

async function main() {
  const remoteUrl   = process.env.TURSO_DATABASE_URL;
  const remoteToken = process.env.TURSO_AUTH_TOKEN;

  if (!remoteUrl || !remoteUrl.startsWith("libsql://")) {
    console.error("❌  請設定 TURSO_DATABASE_URL=libsql://your-db.turso.io");
    process.exit(1);
  }

  console.log("📂  讀取本機 SQLite:", LOCAL_DB);
  const local = createClient({ url: `file:${LOCAL_DB}` });

  console.log("☁️   連接 Turso:", remoteUrl);
  const remote = createClient({ url: remoteUrl, authToken: remoteToken });

  // 建立 schema
  await remote.executeMultiple(`
    CREATE TABLE IF NOT EXISTS snapshots (
      date TEXT NOT NULL, ticker TEXT NOT NULL, name TEXT,
      sector TEXT, industry TEXT, country TEXT,
      price REAL, market_cap REAL, change_pct REAL, volume REAL,
      PRIMARY KEY (date, ticker)
    );
    CREATE INDEX IF NOT EXISTS idx_snap_date   ON snapshots(date);
    CREATE INDEX IF NOT EXISTS idx_snap_ticker ON snapshots(ticker);
  `);

  // 取得已有日期（增量遷移）
  const existingRs = await remote.execute(
    "SELECT DISTINCT date FROM snapshots"
  );
  const existing = new Set(existingRs.rows.map((r) => r[0]));

  const datesRs = await local.execute(
    "SELECT DISTINCT date FROM snapshots ORDER BY date"
  );
  const dates = datesRs.rows.map((r) => r[0]);
  console.log(`📅  本機共 ${dates.length} 個日期，Turso 已有 ${existing.size} 個`);

  let totalRows = 0;
  const CHUNK = 200;

  for (const date of dates) {
    if (existing.has(date)) {
      console.log(`  ⏭️  ${date} 已存在，跳過`);
      continue;
    }

    const rs = await local.execute({
      sql: "SELECT ticker,name,sector,industry,country,price,market_cap,change_pct,volume FROM snapshots WHERE date=?",
      args: [date],
    });

    for (let i = 0; i < rs.rows.length; i += CHUNK) {
      const batch = rs.rows.slice(i, i + CHUNK).map((r) => ({
        sql: `INSERT OR REPLACE INTO snapshots
              (date,ticker,name,sector,industry,country,price,market_cap,change_pct,volume)
              VALUES (?,?,?,?,?,?,?,?,?,?)`,
        args: [date, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]],
      }));
      await remote.batch(batch, "write");
    }

    totalRows += rs.rows.length;
    console.log(`  ✅  ${date}: ${rs.rows.length} 筆`);
  }

  console.log(`\n🎉  遷移完成！共寫入 ${totalRows.toLocaleString()} 筆`);
}

main().catch((e) => { console.error(e); process.exit(1); });
