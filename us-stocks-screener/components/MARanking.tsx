"use client";

import { useEffect, useState, useMemo } from "react";
import { MASector, MAStock } from "@/app/api/ma-ranking/route";

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatCap(v: number): string {
  if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9)  return `$${(v / 1e9).toFixed(1)}B`;
  return `$${(v / 1e6).toFixed(0)}M`;
}

function strengthBar(pct: number): string {
  return `${Math.min(100, (pct / 20) * 100).toFixed(0)}%`;
}

const TIER_ORDER = ["超大型", "巨型", "大型", "中大型", "中型", "小型"] as const;
type TierKey = typeof TIER_ORDER[number];

const TIER_RANGES: Record<TierKey, [number, number]> = {
  超大型: [800e9,  Infinity],
  巨型:   [200e9,  800e9],
  大型:   [70e9,   200e9],
  中大型: [10e9,   70e9],
  中型:   [2e9,    10e9],
  小型:   [0,      2e9],
};

const TIER_COLORS: Record<TierKey, { bg: string; border: string; text: string }> = {
  超大型: { bg: "rgba(109,40,217,0.5)",  border: "#7c3aed", text: "#e9d5ff" },
  巨型:   { bg: "rgba(126,34,206,0.5)",  border: "#9333ea", text: "#e879f9" },
  大型:   { bg: "rgba(29,78,216,0.5)",   border: "#3b82f6", text: "#93c5fd" },
  中大型: { bg: "rgba(14,116,144,0.5)",  border: "#06b6d4", text: "#a5f3fc" },
  中型:   { bg: "rgba(21,128,61,0.5)",   border: "#22c55e", text: "#bbf7d0" },
  小型:   { bg: "rgba(75,85,99,0.5)",    border: "#6b7280", text: "#d1d5db" },
};

const SECTOR_COLORS: Record<string, { border: string; text: string }> = {
  Technology:               { border: "#3b82f6", text: "#60a5fa" },
  "Communication Services": { border: "#7c3aed", text: "#a78bfa" },
  "Consumer Discretionary": { border: "#ea580c", text: "#fb923c" },
  "Consumer Staples":       { border: "#059669", text: "#34d399" },
  Financials:               { border: "#ca8a04", text: "#facc15" },
  Healthcare:               { border: "#db2777", text: "#f472b6" },
  Industrials:              { border: "#0891b2", text: "#22d3ee" },
  Energy:                   { border: "#d97706", text: "#fbbf24" },
  Materials:                { border: "#65a30d", text: "#a3e635" },
  "Real Estate":            { border: "#0d9488", text: "#2dd4bf" },
  Utilities:                { border: "#0284c7", text: "#38bdf8" },
};

function capTier(cap: number): TierKey {
  if (cap >= 800e9) return "超大型";
  if (cap >= 200e9) return "巨型";
  if (cap >= 70e9)  return "大型";
  if (cap >= 10e9)  return "中大型";
  if (cap >= 2e9)   return "中型";
  return "小型";
}

// ── Sparkline ─────────────────────────────────────────────────────────────────

function Sparkline({ data }: { data: number[] }) {
  if (!data || data.length < 2) return <span className="text-gray-700 text-xs">—</span>;
  const W = 80, H = 28;
  const min = Math.min(...data), max = Math.max(...data);
  const range = max - min || 1;
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * W;
    const y = H - ((v - min) / range) * (H - 4) - 2;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const up = data[data.length - 1] >= data[0];
  const color = up ? "#34d399" : "#f87171";
  const lx = W, ly = H - ((data[data.length - 1] - min) / range) * (H - 4) - 2;
  return (
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} className="shrink-0">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" strokeLinejoin="round" />
      <circle cx={lx} cy={ly} r="2.5" fill={color} />
    </svg>
  );
}

// ── MA pill ───────────────────────────────────────────────────────────────────

function MAPill({ label, value, color }: { label: string; value: number | null; color: string }) {
  return (
    <span className="flex items-center gap-0.5 text-xs font-mono">
      <span style={{ color: "#6b7280" }}>{label}</span>
      <span style={{ color }}>{value != null ? formatCap(value) : "—"}</span>
    </span>
  );
}

// ── Stock row ─────────────────────────────────────────────────────────────────

function StockRow({ stock, rank, showSector }: { stock: MAStock; rank: number; showSector?: boolean }) {
  const tier = capTier(stock.currentCap);
  const tc = TIER_COLORS[tier];

  return (
    <div className="flex items-start gap-2 py-2.5 px-3 hover:bg-gray-800/30 transition-colors border-b border-gray-800/40 last:border-0">
      <span className="text-gray-600 text-xs w-5 shrink-0 mt-0.5 text-right">{rank}</span>

      {/* Main info */}
      <div className="flex-1 min-w-0">
        {/* Row 1: ticker + tier + name + cap + strength */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="font-bold text-white text-sm">{stock.ticker}</span>
          <span
            className="text-xs px-1.5 py-0.5 rounded-full border font-medium shrink-0 whitespace-nowrap"
            style={{ backgroundColor: tc.bg, borderColor: tc.border, color: tc.text }}
          >
            {tier}
          </span>
          {showSector && (
            <span className="text-xs text-gray-500 hidden sm:inline">{stock.sector}</span>
          )}
          <span className="text-gray-500 text-xs truncate max-w-[120px] hidden md:block">
            {stock.name.split(" ").slice(0, 3).join(" ")}
          </span>
          <span className="font-mono text-gray-300 text-xs ml-auto">{formatCap(stock.currentCap)}</span>
          <span className="text-green-400 font-bold text-sm">+{stock.strength.toFixed(1)}%</span>
        </div>

        {/* Row 2: core MAs */}
        <div className="flex gap-2.5 mt-1 flex-wrap">
          <MAPill label="3MA"  value={stock.ma3}  color="#4ade80" />
          <MAPill label="10MA" value={stock.ma10} color="#60a5fa" />
          <MAPill label="20MA" value={stock.ma20 ?? null} color="#93c5fd" />
          <MAPill label="30MA" value={stock.ma30} color="#a78bfa" />
          <MAPill label="40MA" value={stock.ma40 ?? null} color="#c4b5fd" />
          <MAPill label="50MA" value={stock.ma50 ?? null} color="#d8b4fe" />
          <MAPill label="60MA" value={stock.ma60} color="#9ca3af" />
          <MAPill label="120MA" value={stock.ma120 ?? null} color="#6b7280" />
        </div>

        {/* Row 3: strength bar */}
        <div className="mt-1.5 h-1 bg-gray-800 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full bg-gradient-to-r from-green-500 to-emerald-400"
            style={{ width: strengthBar(stock.strength) }}
          />
        </div>
      </div>

      {/* Sparkline */}
      <div className="shrink-0 self-center ml-1 hidden sm:block">
        <Sparkline data={stock.sparkline ?? []} />
      </div>
    </div>
  );
}

// ── Section header (shared) ───────────────────────────────────────────────────

function SectionHeader({
  label, borderColor, textColor, count, avgStrength, topTicker, isOpen, onClick,
}: {
  label: string; borderColor: string; textColor: string; count: number;
  avgStrength: number; topTicker: string;
  isOpen: boolean; onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-gray-800/20 transition-colors border-l-2"
      style={{ borderLeftColor: borderColor, color: textColor }}
    >
      <span className="text-sm font-semibold flex-1 text-left">{label}</span>
      <div className="flex items-center gap-3">
        <span className="text-xs text-gray-500">{count} 支</span>
        <span className="text-xs font-mono text-green-400">均 +{avgStrength.toFixed(1)}%</span>
        <span className="text-xs text-gray-400 hidden sm:inline">領 {topTicker}</span>
        <span className="text-gray-600 text-xs">{isOpen ? "▲" : "▼"}</span>
      </div>
    </button>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

type ViewMode = "板塊" | "規模";

export default function MARanking() {
  const [sectors, setSectors]   = useState<MASector[]>([]);
  const [loading, setLoading]   = useState(true);
  const [error, setError]       = useState<string | null>(null);
  const [open, setOpen]         = useState(true);
  const [view, setView]         = useState<ViewMode>("板塊");
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const toggle = (key: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      next.has(key) ? next.delete(key) : next.add(key);
      return next;
    });

  useEffect(() => {
    setLoading(true);
    fetch("/api/ma-ranking")
      .then((r) => r.json())
      .then((data: MASector[]) => { setSectors(data); setLastUpdated(new Date()); })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  // Flat list of all stocks
  const allStocks = useMemo(() =>
    sectors.flatMap((s) => s.stocks), [sectors]
  );

  const totalStocks = allStocks.length;

  // Group by tier
  const byTier = useMemo(() => {
    const map: Record<TierKey, MAStock[]> = {
      超大型: [], 巨型: [], 大型: [], 中大型: [], 中型: [], 小型: [],
    };
    for (const s of allStocks) {
      map[capTier(s.currentCap)].push(s);
    }
    for (const t of TIER_ORDER) {
      map[t].sort((a, b) => b.strength - a.strength);
    }
    return map;
  }, [allStocks]);

  const avgStr = (arr: MAStock[]) =>
    arr.length ? arr.reduce((s, x) => s + x.strength, 0) / arr.length : 0;

  return (
    <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800 flex-wrap gap-2">
        <button onClick={() => setOpen((v) => !v)}
          className="flex items-center gap-2 text-sm font-semibold text-white hover:text-gray-200">
          <span>{open ? "▾" : "▸"}</span>
          📊 市值多頭排列
          <span className="text-xs font-normal text-gray-600">10MA &gt; 30MA &gt; 60MA ｜ 股價 &gt; 60MA ｜ 7日小回檔</span>
          {!loading && (
            <span className="text-xs font-normal text-gray-500">{totalStocks} 支</span>
          )}
        </button>

        {open && (
          <div className="flex items-center gap-2">
            {/* View toggle */}
            <div className="flex rounded-lg border border-gray-700 overflow-hidden text-xs">
              {(["板塊", "規模"] as ViewMode[]).map((v) => (
                <button key={v} onClick={() => { setView(v); setExpanded(new Set()); }}
                  className={`px-3 py-1.5 transition-colors ${view === v ? "bg-blue-600 text-white" : "bg-gray-800 text-gray-400 hover:text-white"}`}>
                  依{v}
                </button>
              ))}
            </div>
            {lastUpdated && (
              <span className="text-xs text-gray-600">{lastUpdated.toLocaleTimeString("zh-TW")}</span>
            )}
          </div>
        )}
      </div>

      {open && (
        <div className="max-h-[600px] overflow-y-auto">
          {loading ? (
            <div className="flex items-center justify-center py-12 gap-3 text-gray-500 text-sm">
              <span className="w-4 h-4 border-2 border-gray-600 border-t-green-400 rounded-full animate-spin" />
              計算移動平均線…
            </div>
          ) : error ? (
            <div className="p-4 text-red-400 text-sm">{error}</div>
          ) : totalStocks === 0 ? (
            <p className="text-center py-10 text-gray-600 text-sm">目前無股票符合多頭排列條件</p>
          ) : view === "板塊" ? (
            /* ── 依板塊 ── */
            <div className="divide-y divide-gray-800/50">
              {sectors.map((sec) => {
                const c = SECTOR_COLORS[sec.sector] ?? { border: "#6b7280", text: "#9ca3af" };
                const isOpen = expanded.has(sec.sector);
                return (
                  <div key={sec.sector}>
                    <SectionHeader
                      label={sec.sector}
                      borderColor={c.border} textColor={c.text}
                      count={sec.stocks.length}
                      avgStrength={avgStr(sec.stocks)}
                      topTicker={sec.stocks[0]?.ticker ?? ""}
                      isOpen={isOpen} onClick={() => toggle(sec.sector)}
                    />
                    {isOpen && sec.stocks.map((stock, idx) => (
                      <StockRow key={stock.ticker} stock={stock} rank={idx + 1} showSector={false} />
                    ))}
                  </div>
                );
              })}
            </div>
          ) : (
            /* ── 依規模 ── */
            <div className="divide-y divide-gray-800/50">
              {TIER_ORDER.filter((t) => byTier[t].length > 0).map((tier) => {
                const stocks = byTier[tier];
                const isOpen = expanded.has(tier);
                const [minCap] = TIER_RANGES[tier];
                const rangeLabel = minCap === 0 ? "<$2B" :
                  minCap >= 1e12 ? `>$${(minCap/1e12).toFixed(0)}T` :
                  minCap >= 1e9  ? `>$${(minCap/1e9).toFixed(0)}B` : "";
                return (
                  <div key={tier}>
                    <SectionHeader
                      label={`${tier}　${rangeLabel}`}
                      borderColor={TIER_COLORS[tier].border}
                      textColor={TIER_COLORS[tier].text}
                      count={stocks.length}
                      avgStrength={avgStr(stocks)}
                      topTicker={stocks[0]?.ticker ?? ""}
                      isOpen={isOpen} onClick={() => toggle(tier)}
                    />
                    {isOpen && stocks.map((stock, idx) => (
                      <StockRow key={stock.ticker} stock={stock} rank={idx + 1} showSector />
                    ))}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
