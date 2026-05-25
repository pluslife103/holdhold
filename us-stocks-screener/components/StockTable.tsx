"use client";

import { useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { StockQuote } from "@/app/api/stocks/route";

export type SortKey = keyof Pick<
  StockQuote,
  "marketCap" | "price" | "changePercent" | "volume"
>;
export type SortDir = "asc" | "desc";

interface Props {
  stocks: StockQuote[];
  sortKey: SortKey;
  sortDir: SortDir;
  onSort: (key: SortKey) => void;
}

const ROW_HEIGHT = 56;

function formatMarketCap(v: number | null): string {
  if (v === null) return "—";
  if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9) return `$${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(2)}M`;
  return `$${v.toFixed(0)}`;
}

function formatVolume(v: number | null): string {
  if (v === null) return "—";
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
  return `${v}`;
}

function marketCapCategory(cap: number | null): { label: string; tier: number } | null {
  if (cap === null) return null;
  if (cap >= 800e9) return { label: "超大型", tier: 1 };
  if (cap >= 200e9) return { label: "巨型",   tier: 2 };
  if (cap >= 70e9)  return { label: "大型",   tier: 3 };
  if (cap >= 10e9)  return { label: "中大型", tier: 4 };
  if (cap >= 2e9)   return { label: "中型",   tier: 5 };
  return               { label: "小型",   tier: 6 };
}

const CAP_COLORS: Record<number, string> = {
  1: "bg-violet-900/70 text-violet-200",
  2: "bg-purple-900/60 text-purple-300",
  3: "bg-blue-900/60   text-blue-300",
  4: "bg-cyan-900/60   text-cyan-300",
  5: "bg-green-900/60  text-green-300",
  6: "bg-gray-700      text-gray-400",
};

function SortIcon({ active, dir }: { active: boolean; dir: SortDir }) {
  if (!active) return <span className="ml-1 text-gray-600">↕</span>;
  return <span className="ml-1 text-blue-400">{dir === "asc" ? "↑" : "↓"}</span>;
}

const SORT_COLS: { key: SortKey; label: string }[] = [
  { key: "marketCap", label: "市值" },
  { key: "price", label: "股價" },
  { key: "changePercent", label: "漲跌幅" },
  { key: "volume", label: "成交量" },
];

// Tailwind grid template: rank | stock | sector | capBadge | marketCap | price | change | volume
const GRID = "grid-cols-[2rem_minmax(140px,1fr)_minmax(100px,160px)_100px_90px_80px_80px_80px]";

export default function StockTable({ stocks, sortKey, sortDir, onSort }: Props) {
  const parentRef = useRef<HTMLDivElement>(null);

  const virtualizer = useVirtualizer({
    count: stocks.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });

  if (stocks.length === 0) {
    return (
      <div className="text-center py-20 text-gray-500 text-lg">
        找不到符合條件的股票
      </div>
    );
  }

  return (
    <div className="rounded-xl border border-gray-700/50 overflow-hidden">
      {/* Fixed header */}
      <div className={`grid ${GRID} gap-2 px-3 py-2.5 bg-gray-800/80 border-b border-gray-700 text-xs text-gray-400 font-medium`}>
        <span>#</span>
        <span>股票</span>
        <span>行業</span>
        <span>類別</span>
        {SORT_COLS.map((col) => (
          <button
            key={col.key}
            onClick={() => onSort(col.key)}
            className="text-right cursor-pointer hover:text-white transition-colors select-none whitespace-nowrap"
          >
            {col.label}
            <SortIcon active={sortKey === col.key} dir={sortDir} />
          </button>
        ))}
      </div>

      {/* Virtual scroll body */}
      <div
        ref={parentRef}
        className="overflow-y-auto"
        style={{ height: "calc(100vh - 400px)", minHeight: "400px" }}
      >
        <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
          {virtualizer.getVirtualItems().map((vrow) => {
            const stock = stocks[vrow.index];
            const cat = marketCapCategory(stock.marketCap);
            const isUp = (stock.changePercent ?? 0) >= 0;
            const isNeutral = stock.changePercent === null;

            return (
              <div
                key={vrow.key}
                style={{
                  position: "absolute",
                  top: `${vrow.start}px`,
                  left: 0,
                  right: 0,
                  height: `${ROW_HEIGHT}px`,
                }}
                className={`grid ${GRID} gap-2 px-3 border-b border-gray-800/60 hover:bg-gray-800/40 transition-colors items-center`}
              >
                {/* Rank */}
                <span className="text-xs text-gray-600">{vrow.index + 1}</span>

                {/* Ticker + Name */}
                <div className="flex flex-col min-w-0">
                  <span className="font-bold text-white text-sm leading-tight">
                    {stock.ticker}
                  </span>
                  <span className="text-gray-500 text-xs truncate leading-tight mt-0.5">
                    {stock.name}
                  </span>
                </div>

                {/* Sector */}
                <span className="text-gray-400 text-xs truncate">
                  {stock.sector !== "Unknown" ? stock.sector : ""}
                </span>

                {/* Cap badge */}
                <div className="flex items-center">
                  {cat && (
                    <span className={`text-xs px-1.5 py-0.5 rounded-full font-medium whitespace-nowrap ${CAP_COLORS[cat.tier] ?? ""}`}>
                      {cat.label}
                    </span>
                  )}
                </div>

                {/* Market Cap */}
                <span className="text-right font-mono font-semibold text-white text-sm">
                  {formatMarketCap(stock.marketCap)}
                </span>

                {/* Price */}
                <span className="text-right font-mono text-gray-300 text-sm">
                  {stock.price !== null ? `$${stock.price.toFixed(2)}` : "—"}
                </span>

                {/* Change % */}
                <span className={`text-right font-mono font-medium text-sm ${
                  isNeutral ? "text-gray-500" : isUp ? "text-green-400" : "text-red-400"
                }`}>
                  {stock.changePercent !== null
                    ? `${isUp ? "+" : ""}${stock.changePercent.toFixed(2)}%`
                    : "—"}
                </span>

                {/* Volume */}
                <span className="text-right font-mono text-gray-400 text-xs">
                  {formatVolume(stock.volume)}
                </span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
