"use client";

import { useEffect, useState, useMemo, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { MASector, MAStock } from "@/app/api/ma-ranking/route";

// ── Tier ─────────────────────────────────────────────────────────────────────

type TierKey = "超大型" | "巨型" | "大型" | "中大型" | "中型" | "小型";
const TIER_ORDER: TierKey[] = ["超大型", "巨型", "大型", "中大型", "中型", "小型"];

const TC: Record<TierKey, { bg: string; bd: string; tx: string }> = {
  超大型: { bg: "rgba(109,40,217,.4)",  bd: "#7c3aed", tx: "#e9d5ff" },
  巨型:   { bg: "rgba(126,34,206,.4)",  bd: "#9333ea", tx: "#e879f9" },
  大型:   { bg: "rgba(29,78,216,.4)",   bd: "#3b82f6", tx: "#93c5fd" },
  中大型: { bg: "rgba(14,116,144,.4)",  bd: "#06b6d4", tx: "#a5f3fc" },
  中型:   { bg: "rgba(21,128,61,.4)",   bd: "#22c55e", tx: "#bbf7d0" },
  小型:   { bg: "rgba(75,85,99,.4)",    bd: "#6b7280", tx: "#d1d5db" },
};

function capTier(c: number): TierKey {
  if (c >= 800e9) return "超大型";
  if (c >= 200e9) return "巨型";
  if (c >= 70e9)  return "大型";
  if (c >= 10e9)  return "中大型";
  if (c >= 2e9)   return "中型";
  return "小型";
}

// ── Formatters ────────────────────────────────────────────────────────────────

// 轉億（1億 = 1e8），保留一位小數
function f億(v: number | null | undefined): string {
  if (v == null) return "—";
  const yi = v / 1e8;
  if (yi >= 10000) return `${(yi / 10000).toFixed(1)}萬`;
  return yi.toLocaleString("en", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

// ── Mini MA bar chart ─────────────────────────────────────────────────────────

const MA_PERIODS = [3, 10, 20, 30, 40, 50, 60, 120] as const;
const BAR_COLORS = [
  "#22c55e", "#4ade80", "#86efac", // green shades (shorter MA)
  "#34d399", "#6ee7b7",             // teal
  "#60a5fa", "#93c5fd",             // blue (longer MA)
  "#6b7280",                        // gray (120MA)
];

function MAMiniBar({ stock }: { stock: MAStock }) {
  const values: (number | null)[] = [
    stock.ma3, stock.ma10, stock.ma20, stock.ma30,
    stock.ma40, stock.ma50, stock.ma60, stock.ma120,
  ];

  const valid = values.filter((v): v is number => v != null);
  if (valid.length < 4) return <span className="text-gray-700 text-xs">—</span>;

  const base = stock.ma3; // tallest bar (3MA is the highest in bull alignment)
  const W = 52, H = 22, GAP = 1, barW = (W - GAP * 7) / 8;

  return (
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`}>
      {values.map((v, i) => {
        if (v == null) return null;
        const ratio = Math.min(1, v / base);
        const bh = Math.max(2, ratio * (H - 2));
        const x = i * (barW + GAP);
        const y = H - bh;
        return (
          <rect key={i} x={x} y={y} width={barW} height={bh}
            fill={BAR_COLORS[i]} rx="1" opacity="0.9" />
        );
      })}
    </svg>
  );
}

// ── Sort ──────────────────────────────────────────────────────────────────────

type SortKey = "currentCap" | "strength" | "ma3" | "ma10" | "ma20"
             | "ma30" | "ma40" | "ma50" | "ma60" | "ma120";
type SortDir = "asc" | "desc";

// grid columns: # | 股票 | 規模 | 市值億 | 3MA | 10MA | 20MA | 30MA | 40MA | 50MA | 60MA | 120MA | 強度
const GRID_COLS = "2rem minmax(130px,1fr) 5rem 7.5rem 7rem 7rem 7rem 7rem 7rem 7rem 7rem 7rem 58px";

const COL_DEFS: { key: SortKey; label: string; color: string }[] = [
  { key: "currentCap", label: "市值(億)", color: "#e2e8f0" },
  { key: "ma3",        label: "3MA",      color: "#22c55e" },
  { key: "ma10",       label: "10MA",     color: "#4ade80" },
  { key: "ma20",       label: "20MA",     color: "#86efac" },
  { key: "ma30",       label: "30MA",     color: "#34d399" },
  { key: "ma40",       label: "40MA",     color: "#6ee7b7" },
  { key: "ma50",       label: "50MA",     color: "#60a5fa" },
  { key: "ma60",       label: "60MA",     color: "#93c5fd" },
  { key: "ma120",      label: "120MA",    color: "#9ca3af" },
  { key: "strength",   label: "強度",     color: "#a3e635" },
];

const ROW_H = 44;

// ── Virtual table ─────────────────────────────────────────────────────────────

const MIN_W = 950;

function VTable({ stocks, sortKey, sortDir, onSort }: {
  stocks: MAStock[]; sortKey: SortKey; sortDir: SortDir; onSort: (k: SortKey) => void;
}) {
  const parentRef  = useRef<HTMLDivElement>(null);
  const topBarRef  = useRef<HTMLDivElement>(null);
  const wrapRef    = useRef<HTMLDivElement>(null);
  const syncing    = useRef(false);

  const virt = useVirtualizer({
    count: stocks.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_H,
    overscan: 20,
  });

  // Sync horizontal scroll: top-bar ↔ wrapper
  const onTopScroll = () => {
    if (syncing.current) return;
    syncing.current = true;
    if (wrapRef.current && topBarRef.current)
      wrapRef.current.scrollLeft = topBarRef.current.scrollLeft;
    syncing.current = false;
  };
  const onWrapScroll = () => {
    if (syncing.current) return;
    syncing.current = true;
    if (wrapRef.current && topBarRef.current)
      topBarRef.current.scrollLeft = wrapRef.current.scrollLeft;
    syncing.current = false;
  };

  return (
    <div>
      {/* ── Top scrollbar mirror ── */}
      <div ref={topBarRef} onScroll={onTopScroll}
        className="overflow-x-auto border-b border-gray-700/40"
        style={{ height: 10 }}>
        <div style={{ width: MIN_W, height: 1 }} />
      </div>

      {/* ── Main table (header + rows) ── */}
      <div ref={wrapRef} onScroll={onWrapScroll} className="overflow-x-auto">
        {/* Header */}
        <div className="grid sticky top-0 z-10 border-b border-gray-700/80"
          style={{ gridTemplateColumns: GRID_COLS, minWidth: MIN_W, background: "#0f172a" }}>
        <div className="px-2 py-2.5 text-xs text-gray-500">#</div>
        <div className="px-2 py-2.5 text-xs text-gray-500">股票</div>
        <div className="px-2 py-2.5 text-xs text-gray-500">規模</div>
        {COL_DEFS.map(({ key, label, color }) => (
          <button key={key} onClick={() => onSort(key)}
            className="px-2 py-2.5 text-right text-xs font-semibold hover:opacity-80 transition-opacity select-none flex items-center justify-end gap-0.5 relative group"
            style={{ color }}>
            {label}
            {/* 強度欄：加 ⓘ 說明 */}
            {key === "strength" && (
              <span className="ml-0.5 text-gray-600 hover:text-gray-400 cursor-help" style={{ fontSize: 10 }}>ⓘ
                <span className="absolute right-0 top-full mt-1 w-56 text-left font-normal leading-snug px-3 py-2 rounded-lg shadow-xl z-50 pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity"
                  style={{ background: "#1e293b", border: "1px solid #334155", color: "#cbd5e1" }}>
                  <span className="font-bold text-white block mb-1">MA 強度</span>
                  (10MA ÷ 60MA − 1) × 100%
                  <span className="block mt-1 text-gray-400">
                    10日均線相對60日均線高出的百分比。數值越高代表中期市值成長動能越強。
                  </span>
                </span>
              </span>
            )}
            {sortKey === key
              ? <span style={{ color: "#60a5fa" }}>{sortDir === "desc" ? "↓" : "↑"}</span>
              : <span style={{ color: "#374151" }}>↕</span>}
          </button>
        ))}
      </div>

        {/* Rows */}
        <div ref={parentRef} className="overflow-y-auto"
          style={{ height: Math.min(stocks.length * ROW_H, 520), minWidth: MIN_W }}>
        <div style={{ height: `${virt.getTotalSize()}px`, position: "relative" }}>
          {virt.getVirtualItems().map((vrow) => {
            const s = stocks[vrow.index];
            const tier = capTier(s.currentCap);
            const tc = TC[tier];
            const even = vrow.index % 2 === 0;

            return (
              <div key={vrow.key}
                style={{
                  position: "absolute", top: `${vrow.start}px`, left: 0, right: 0,
                  height: `${ROW_H}px`, display: "grid",
                  gridTemplateColumns: GRID_COLS,
                  background: even ? "rgba(15,23,42,0.8)" : "rgba(30,41,59,0.4)",
                }}
                className="items-center hover:bg-blue-950/40 transition-colors border-b border-gray-800/30"
              >
                {/* # */}
                <span className="px-2 text-xs" style={{ color: "#4b5563" }}>{vrow.index + 1}</span>

                {/* Ticker */}
                <div className="px-2 flex flex-col min-w-0">
                  <span className="font-bold text-white text-sm leading-tight">{s.ticker}</span>
                  <span className="text-xs truncate leading-tight" style={{ color: "#6b7280" }}>
                    {s.name.split(" ").slice(0, 3).join(" ")}
                  </span>
                </div>

                {/* Tier */}
                <div className="px-1">
                  <span className="text-xs px-1.5 py-0.5 rounded font-medium whitespace-nowrap"
                    style={{ backgroundColor: tc.bg, border: `1px solid ${tc.bd}`, color: tc.tx }}>
                    {tier}
                  </span>
                </div>

                {/* 市值 */}
                <span className="px-2 text-right font-mono font-semibold text-sm"
                  style={{ color: "#e2e8f0" }}>{f億(s.currentCap)}</span>

                {/* 3MA */}
                <span className="px-2 text-right font-mono text-sm" style={{ color: "#22c55e" }}>{f億(s.ma3)}</span>

                {/* 10MA */}
                <span className="px-2 text-right font-mono text-sm" style={{ color: "#4ade80" }}>{f億(s.ma10)}</span>

                {/* 20MA */}
                <span className="px-2 text-right font-mono text-sm" style={{ color: "#86efac" }}>{f億(s.ma20)}</span>

                {/* 30MA */}
                <span className="px-2 text-right font-mono text-sm" style={{ color: "#34d399" }}>{f億(s.ma30)}</span>

                {/* 40MA */}
                <span className="px-2 text-right font-mono text-sm" style={{ color: "#6ee7b7" }}>{f億(s.ma40)}</span>

                {/* 50MA */}
                <span className="px-2 text-right font-mono text-sm" style={{ color: "#60a5fa" }}>{f億(s.ma50)}</span>

                {/* 60MA */}
                <span className="px-2 text-right font-mono text-sm" style={{ color: "#93c5fd" }}>{f億(s.ma60)}</span>

                {/* 120MA */}
                <span className="px-2 text-right font-mono text-sm" style={{ color: "#9ca3af" }}>{f億(s.ma120)}</span>

                {/* Mini bar */}
                <div className="px-1 flex justify-center items-center">
                  <MAMiniBar stock={s} />
                </div>
              </div>
            );
          })}
        </div>
      </div>   {/* end overflow-y rows */}
      </div>   {/* end overflow-x wrapper */}
    </div>
  );
}

// ── Main ──────────────────────────────────────────────────────────────────────

export default function MAScreener() {
  const [rawSectors, setRawSectors] = useState<MASector[]>([]);
  const [loading, setLoading]   = useState(true);
  const [open, setOpen]         = useState(true);
  const [search, setSearch]     = useState("");
  const [sectorFilter, setSectorFilter] = useState("全部");
  const [activeTier, setActiveTier] = useState<TierKey | "全部">("全部");
  const [sortKey, setSortKey]   = useState<SortKey>("strength");
  const [sortDir, setSortDir]   = useState<SortDir>("desc");
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  // Date navigation
  const [availDates, setAvailDates] = useState<string[]>([]); // every-3-day list, newest first
  const [selDate, setSelDate]       = useState<string | null>(null); // null = latest

  // Load date list once
  useEffect(() => {
    fetch("/api/ma-ranking/dates")
      .then((r) => r.json())
      .then((d: { dates: string[] }) => setAvailDates(d.dates ?? []));
  }, []);

  // Fetch MA ranking for selected date
  useEffect(() => {
    setLoading(true);
    setRawSectors([]);
    const url = selDate ? `/api/ma-ranking?date=${selDate}` : "/api/ma-ranking";
    fetch(url)
      .then((r) => r.json())
      .then((data: MASector[]) => { setRawSectors(data); setLastUpdated(new Date()); })
      .finally(() => setLoading(false));
  }, [selDate]);

  const allStocks = useMemo(() => rawSectors.flatMap((s) => s.stocks), [rawSectors]);

  const sectors = useMemo(
    () => ["全部", ...Array.from(new Set(allStocks.map((s) => s.sector))).sort()],
    [allStocks]
  );

  const tierCounts = useMemo(() => {
    const m: Partial<Record<TierKey, number>> = {};
    for (const s of allStocks) { const t = capTier(s.currentCap); m[t] = (m[t] ?? 0) + 1; }
    return m;
  }, [allStocks]);

  const handleSort = (key: SortKey) => {
    setSortDir((p) => (sortKey === key ? (p === "desc" ? "asc" : "desc") : "desc"));
    setSortKey(key);
  };

  const sorted = useMemo(() => {
    const q = search.toLowerCase();
    return allStocks
      .filter((s) => {
        if (activeTier !== "全部" && capTier(s.currentCap) !== activeTier) return false;
        if (sectorFilter !== "全部" && s.sector !== sectorFilter) return false;
        if (q && !s.ticker.toLowerCase().includes(q) && !s.name.toLowerCase().includes(q)) return false;
        return true;
      })
      .sort((a, b) => {
        const av = (a[sortKey] ?? (sortDir === "desc" ? -Infinity : Infinity)) as number;
        const bv = (b[sortKey] ?? (sortDir === "desc" ? -Infinity : Infinity)) as number;
        return sortDir === "desc" ? bv - av : av - bv;
      });
  }, [allStocks, activeTier, sectorFilter, search, sortKey, sortDir]);

  return (
    <div className="rounded-xl border border-gray-700/50 overflow-hidden"
      style={{ background: "rgba(10,15,28,0.85)" }}>

      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800 flex-wrap gap-2">
        <button onClick={() => setOpen((v) => !v)}
          className="flex items-center gap-2 text-sm font-semibold text-white hover:text-gray-200">
          <span>{open ? "▾" : "▸"}</span>
          📈 市值成長篩選器
          <span className="text-xs font-normal" style={{ color: "#4b5563" }}>
            10MA &gt; 30MA &gt; 60MA ｜ 股價 &gt; 60MA ｜ 7日小回檔
          </span>
          {!loading && <span className="text-xs font-normal text-gray-500">{sorted.length} 支</span>}
        </button>
        {lastUpdated && open && (
          <span className="text-xs" style={{ color: "#4b5563" }}>{lastUpdated.toLocaleTimeString("zh-TW")}</span>
        )}
      </div>

      {open && (
        <>
          {/* Date navigation */}
          {availDates.length > 0 && (
            <div className="px-4 py-2.5 border-b border-gray-800/60 flex items-center gap-2 flex-wrap"
              style={{ background: "rgba(15,23,42,0.7)" }}>
              {/* Latest button */}
              <button onClick={() => setSelDate(null)}
                className="flex items-center gap-1.5 px-3 py-1 rounded-lg text-xs font-medium transition-colors"
                style={selDate === null
                  ? { background: "#16a34a", color: "#fff" }
                  : { background: "#1e293b", color: "#6b7280", border: "1px solid #334155" }}>
                <span className={`w-1.5 h-1.5 rounded-full ${selDate === null ? "bg-green-300 animate-pulse" : "bg-gray-600"}`} />
                最新
              </button>

              {/* Prev */}
              <button
                onClick={() => {
                  const idx = selDate ? availDates.indexOf(selDate) : -1;
                  const next = idx < availDates.length - 1 ? availDates[idx + 1] : availDates[availDates.length - 1];
                  setSelDate(next);
                }}
                disabled={selDate === availDates[availDates.length - 1]}
                className="px-2.5 py-1 rounded-lg text-xs transition-colors disabled:opacity-30"
                style={{ background: "#1e293b", color: "#9ca3af", border: "1px solid #334155" }}>
                ← 往前
              </button>

              {/* Date selector */}
              <select
                value={selDate ?? ""}
                onChange={(e) => setSelDate(e.target.value || null)}
                className="border rounded-lg px-2 py-1 text-xs text-white focus:outline-none cursor-pointer"
                style={{ background: "#1e293b", borderColor: "#334155" }}>
                <option value="">— 最新 —</option>
                {availDates.map((d) => <option key={d} value={d}>{d}</option>)}
              </select>

              {/* Next */}
              <button
                onClick={() => {
                  const idx = selDate ? availDates.indexOf(selDate) : 0;
                  setSelDate(idx > 0 ? availDates[idx - 1] : null);
                }}
                disabled={selDate === null}
                className="px-2.5 py-1 rounded-lg text-xs transition-colors disabled:opacity-30"
                style={{ background: "#1e293b", color: "#9ca3af", border: "1px solid #334155" }}>
                往後 →
              </button>

              <span className="text-xs ml-auto" style={{ color: "#4b5563" }}>
                每 3 個交易日一筆 · 共 {availDates.length} 個紀錄
              </span>
            </div>
          )}

          {/* Filters */}
          <div className="px-4 py-3 border-b border-gray-800/60 space-y-2.5"
            style={{ background: "rgba(15,23,42,0.6)" }}>
            <div className="flex gap-3 flex-wrap items-end">
              <div className="flex-1 min-w-[140px]">
                <label className="block text-xs mb-1" style={{ color: "#6b7280" }}>搜尋</label>
                <input value={search} onChange={(e) => setSearch(e.target.value)}
                  placeholder="代號或公司名稱…"
                  className="w-full border rounded-lg px-3 py-1.5 text-sm text-white placeholder-gray-600 focus:outline-none focus:border-blue-500"
                  style={{ background: "#1e293b", borderColor: "#334155" }} />
              </div>
              <div className="min-w-[150px]">
                <label className="block text-xs mb-1" style={{ color: "#6b7280" }}>行業板塊</label>
                <select value={sectorFilter} onChange={(e) => setSectorFilter(e.target.value)}
                  className="w-full border rounded-lg px-3 py-1.5 text-sm text-white focus:outline-none cursor-pointer"
                  style={{ background: "#1e293b", borderColor: "#334155" }}>
                  {sectors.map((s) => <option key={s}>{s}</option>)}
                </select>
              </div>
            </div>

            {/* Tier pills */}
            <div className="flex flex-wrap gap-1.5">
              <button onClick={() => setActiveTier("全部")}
                className="text-xs px-3 py-1 rounded-full font-medium transition-colors"
                style={activeTier === "全部"
                  ? { background: "#2563eb", color: "#fff" }
                  : { background: "#1e293b", color: "#9ca3af", border: "1px solid #334155" }}>
                全部 ({allStocks.length})
              </button>
              {TIER_ORDER.map((t) => {
                const c = TC[t]; const cnt = tierCounts[t] ?? 0; const active = activeTier === t;
                return (
                  <button key={t} onClick={() => setActiveTier(active ? "全部" : t)}
                    className="text-xs px-3 py-1 rounded-full font-medium"
                    style={{
                      backgroundColor: active ? c.bg : "rgba(30,41,59,0.8)",
                      border: `1px solid ${active ? c.bd : "#334155"}`,
                      color: active ? c.tx : "#9ca3af",
                    }}>
                    {t} ({cnt})
                  </button>
                );
              })}
            </div>
          </div>

          {/* Table */}
          {loading ? (
            <div className="flex items-center justify-center py-12 gap-3 text-sm" style={{ color: "#6b7280" }}>
              <span className="w-4 h-4 border-2 border-t-green-400 rounded-full animate-spin"
                style={{ borderColor: "#374151 #374151 #374151 #4ade80" }} />
              計算移動平均線…
            </div>
          ) : sorted.length === 0 ? (
            <p className="text-center py-10 text-sm" style={{ color: "#4b5563" }}>無符合條件的股票</p>
          ) : (
            <VTable stocks={sorted} sortKey={sortKey} sortDir={sortDir} onSort={handleSort} />
          )}
        </>
      )}
    </div>
  );
}
