"use client";

import { useEffect, useState } from "react";
import type { CombinedTier, CombinedStock, TierKey } from "@/types/combined";

// ── Styling ───────────────────────────────────────────────────────────────────

const TIER_COLORS: Record<TierKey, { bg: string; border: string; text: string }> = {
  超大型: { bg: "rgba(109,40,217,0.5)",  border: "#7c3aed", text: "#e9d5ff" },
  巨型:   { bg: "rgba(126,34,206,0.5)",  border: "#9333ea", text: "#e879f9" },
  大型:   { bg: "rgba(29,78,216,0.5)",   border: "#3b82f6", text: "#93c5fd" },
  中大型: { bg: "rgba(14,116,144,0.5)",  border: "#06b6d4", text: "#a5f3fc" },
  中型:   { bg: "rgba(21,128,61,0.5)",   border: "#22c55e", text: "#bbf7d0" },
  小型:   { bg: "rgba(75,85,99,0.5)",    border: "#6b7280", text: "#d1d5db" },
};

function formatCap(v: number): string {
  if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9)  return `$${(v / 1e9).toFixed(1)}B`;
  return `$${(v / 1e6).toFixed(0)}M`;
}

// ── Stock row ─────────────────────────────────────────────────────────────────

function StockRow({ s, rank }: { s: CombinedStock; rank: number }) {
  const tc = TIER_COLORS[s.tier];
  return (
    <div className="px-4 py-3 border-b border-gray-800/40 last:border-0 hover:bg-gray-800/20 transition-colors">
      {/* Row 1: rank · ticker · tier badge · name · cap · strength */}
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-gray-600 text-xs w-5 text-right shrink-0">{rank}</span>
        <span className="font-bold text-white">{s.ticker}</span>
        <span
          className="text-xs px-1.5 py-0.5 rounded-full border font-medium shrink-0"
          style={{ backgroundColor: tc.bg, borderColor: tc.border, color: tc.text }}
        >
          {s.tier}
        </span>
        <span className="text-gray-500 text-xs truncate max-w-[160px] hidden sm:block">
          {s.name.split(" ").slice(0, 4).join(" ")}
        </span>
        <span className="font-mono text-gray-300 text-xs ml-auto">{formatCap(s.currentCap)}</span>
      </div>

      {/* Row 2: MA 數值 + 超越資訊 */}
      <div className="flex items-center gap-4 mt-1 pl-7 flex-wrap text-xs font-mono">
        {/* MA */}
        <span className="text-green-400">3MA {formatCap(s.ma3)}</span>
        <span className="text-blue-400">10MA {formatCap(s.ma10)}</span>
        <span className="text-purple-400">30MA {formatCap(s.ma30)}</span>
        <span className="text-gray-500">60MA {formatCap(s.ma60)}</span>
        <span className="text-emerald-400 font-bold">↑{s.maStrength.toFixed(1)}%</span>

        {/* Crossover */}
        <span className="text-yellow-400 ml-2">⚔ ×{s.crossoverCount}</span>
        <span className="text-gray-500">{s.uniqueLosers} 支</span>
        <span className="text-gray-600">{s.lastCrossoverDate}</span>
      </div>

      {/* Row 3: dual progress bar */}
      <div className="flex gap-1 mt-2 pl-7 items-center">
        <span className="text-[10px] text-gray-600 w-14 shrink-0">多頭強度</span>
        <div className="flex-1 h-1 bg-gray-800 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full"
            style={{
              width: `${Math.min(100, (s.maStrength / 20) * 100)}%`,
              background: "linear-gradient(to right, #10b981, #34d399)",
            }}
          />
        </div>
        <span className="text-[10px] text-gray-600 w-12 shrink-0 text-right">超越次</span>
        <div className="flex-1 h-1 bg-gray-800 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full"
            style={{
              width: `${Math.min(100, (s.crossoverCount / 20) * 100)}%`,
              background: "linear-gradient(to right, #f59e0b, #fbbf24)",
            }}
          />
        </div>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function CombinedRanking() {
  const [tiers, setTiers]         = useState<CombinedTier[]>([]);
  const [loading, setLoading]     = useState(true);
  const [error, setError]         = useState<string | null>(null);
  const [open, setOpen]           = useState(true);
  const [days, setDays]           = useState<7 | 30>(30);
  const [expanded, setExpanded]   = useState<Set<TierKey>>(new Set());
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const toggle = (t: TierKey) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      next.has(t) ? next.delete(t) : next.add(t);
      return next;
    });

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/combined?days=${days}`)
      .then((r) => r.json())
      .then((data: CombinedTier[]) => { setTiers(data); setLastUpdated(new Date()); })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [days]);

  const total = tiers.reduce((s, t) => s + t.stocks.length, 0);

  return (
    <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800 flex-wrap gap-2">
        <button
          onClick={() => setOpen((v) => !v)}
          className="flex items-center gap-2 text-sm font-semibold text-white hover:text-gray-200"
        >
          <span>{open ? "▾" : "▸"}</span>
          🔥 超越 × 多頭排列
          <span className="text-xs font-normal text-gray-600">同時符合兩項條件</span>
          {!loading && (
            <span className="text-xs font-normal text-gray-500">
              {tiers.length} 個規模 · {total} 支
            </span>
          )}
        </button>

        {open && (
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-600">超越榜回看：</span>
            <div className="flex rounded-lg border border-gray-700 overflow-hidden text-xs">
              {([7, 30] as const).map((d) => (
                <button key={d} onClick={() => { setDays(d); setExpanded(new Set()); }}
                  className={`px-3 py-1.5 transition-colors ${days === d ? "bg-blue-600 text-white" : "bg-gray-800 text-gray-400 hover:text-white"}`}>
                  {d}天
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
        <div className="max-h-[560px] overflow-y-auto">
          {loading ? (
            <div className="flex items-center justify-center py-12 gap-3 text-gray-500 text-sm">
              <span className="w-4 h-4 border-2 border-gray-600 border-t-yellow-400 rounded-full animate-spin" />
              交叉比對中…
            </div>
          ) : error ? (
            <div className="p-4 text-red-400 text-sm">{error}</div>
          ) : total === 0 ? (
            <p className="text-center py-10 text-gray-600 text-sm">無股票同時符合兩項條件</p>
          ) : (
            <div className="divide-y divide-gray-800/50">
              {tiers.map((tierGroup) => {
                const tc = TIER_COLORS[tierGroup.tier];
                const isOpen = expanded.has(tierGroup.tier);
                const avgMA = tierGroup.stocks.reduce((s, x) => s + x.maStrength, 0) / tierGroup.stocks.length;
                const totalCross = tierGroup.stocks.reduce((s, x) => s + x.crossoverCount, 0);

                return (
                  <div key={tierGroup.tier}>
                    {/* Tier header */}
                    <button
                      onClick={() => toggle(tierGroup.tier)}
                      className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-gray-800/20 transition-colors border-l-2"
                      style={{ borderLeftColor: tc.border, color: tc.text }}
                    >
                      <span className="text-sm font-semibold flex-1 text-left">{tierGroup.tier}</span>
                      <div className="flex items-center gap-3 text-xs">
                        <span className="text-gray-400">{tierGroup.stocks.length} 支</span>
                        <span className="text-emerald-400">均多頭 +{avgMA.toFixed(1)}%</span>
                        <span className="text-yellow-400">⚔ 共{totalCross}次</span>
                        <span className="text-gray-400 hidden sm:inline">
                          領 {tierGroup.stocks[0]?.ticker}
                        </span>
                        <span className="text-gray-600">{isOpen ? "▲" : "▼"}</span>
                      </div>
                    </button>

                    {/* Stock list */}
                    {isOpen && tierGroup.stocks.map((s, idx) => (
                      <StockRow key={s.ticker} s={s} rank={idx + 1} />
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
