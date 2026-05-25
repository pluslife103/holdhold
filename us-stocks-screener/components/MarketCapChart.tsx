"use client";

import { useEffect, useState, useMemo } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine,
} from "recharts";

interface DataPoint {
  date: string;
  marketCap: number;
  price: number;
  changePct: number | null;
}

interface Series {
  ticker: string;
  name: string;
  data: DataPoint[];
}

interface Props {
  tickers: string[];
  mode: "absolute" | "normalized";
  range: 7 | 30 | 60;
}

const COLORS = [
  "#60a5fa", "#34d399", "#a78bfa", "#fb923c", "#f472b6",
  "#facc15", "#38bdf8", "#4ade80", "#f87171", "#e879f9",
];

function formatCap(v: number): string {
  if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9)  return `$${(v / 1e9).toFixed(1)}B`;
  return `$${(v / 1e6).toFixed(0)}M`;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CustomTooltip({ active, payload, label, mode }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-gray-900 border border-gray-700 rounded-xl p-3 shadow-xl text-sm min-w-[180px]">
      <p className="text-gray-400 mb-2 text-xs">{label}</p>
      {payload.map((entry: { color: string; name: string; value: number }) => (
        <div key={entry.name} className="flex items-center justify-between gap-4 mb-1">
          <div className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-full inline-block" style={{ backgroundColor: entry.color }} />
            <span className="font-bold text-white">{entry.name}</span>
          </div>
          <span style={{ color: entry.color }} className="font-mono">
            {mode === "normalized"
              ? `${entry.value >= 0 ? "+" : ""}${entry.value.toFixed(2)}%`
              : formatCap(entry.value)}
          </span>
        </div>
      ))}
    </div>
  );
}

export default function MarketCapChart({ tickers, mode, range }: Props) {
  const [series, setSeries] = useState<Series[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (tickers.length === 0) return;
    setLoading(true);
    setError(null);
    fetch(`/api/compare?tickers=${tickers.join(",")}`)
      .then((r) => r.json())
      .then((data) => {
        if (data.error) throw new Error(data.error);
        setSeries(data);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [tickers]);

  const chartData = useMemo(() => {
    if (series.length === 0) return [];

    // Build a map of all dates
    const allDates = new Set<string>();
    for (const s of series) {
      for (const d of s.data) allDates.add(d.date);
    }

    // Limit to range
    const sorted = Array.from(allDates).sort();
    const cutoff = sorted[Math.max(0, sorted.length - range)];
    const dates = sorted.filter((d) => d >= cutoff);

    // First values per ticker for normalization
    const firstCap: Record<string, number> = {};
    for (const s of series) {
      const first = s.data.find((d) => d.date >= cutoff);
      if (first) firstCap[s.ticker] = first.marketCap;
    }

    return dates.map((date) => {
      const row: Record<string, number | string> = { date };
      for (const s of series) {
        const point = s.data.find((d) => d.date === date);
        if (point) {
          if (mode === "normalized") {
            const base = firstCap[s.ticker];
            row[s.ticker] = base ? +((point.marketCap / base - 1) * 100).toFixed(3) : 0;
          } else {
            row[s.ticker] = point.marketCap;
          }
        }
      }
      return row;
    });
  }, [series, mode, range]);

  // Summary stats per ticker
  const stats = useMemo(() => {
    const sorted = chartData;
    if (sorted.length < 2) return [];
    const first = sorted[0];
    const last  = sorted[sorted.length - 1];
    return series.map((s) => {
      const start = first[s.ticker] as number | undefined;
      const end   = last[s.ticker]  as number | undefined;
      const raw = series.find((x) => x.ticker === s.ticker)?.data.at(-1);
      const pct = start != null && end != null && start !== 0
        ? mode === "normalized"
          ? (end - (first[s.ticker] as number))
          : (end - start) / start * 100
        : null;
      return { ticker: s.ticker, name: s.name, pct, lastCap: raw?.marketCap ?? null };
    });
  }, [chartData, series, mode]);

  if (loading) {
    return (
      <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 p-8 flex items-center justify-center">
        <div className="flex items-center gap-3 text-gray-400">
          <div className="w-5 h-5 border-2 border-gray-500 border-t-blue-400 rounded-full animate-spin" />
          載入歷史資料中…
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-900/20 border border-red-700/40 rounded-xl p-6 text-red-400 text-sm">
        載入失敗：{error}
      </div>
    );
  }

  if (chartData.length === 0) {
    return (
      <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 p-12 text-center text-gray-500">
        無歷史資料，請確認股票代號是否正確
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Stat cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        {stats.map((s, i) => {
          const isUp = (s.pct ?? 0) >= 0;
          return (
            <div
              key={s.ticker}
              className="bg-gray-800/50 rounded-xl p-3 border"
              style={{ borderColor: `${COLORS[i % COLORS.length]}40` }}
            >
              <div className="flex items-center gap-1.5 mb-1">
                <span className="w-2 h-2 rounded-full" style={{ backgroundColor: COLORS[i % COLORS.length] }} />
                <span className="font-bold text-white text-sm">{s.ticker}</span>
              </div>
              <p className="text-xs text-gray-500 truncate">{s.name.split(" ").slice(0, 3).join(" ")}</p>
              {s.lastCap != null && (
                <p className="text-sm font-mono text-gray-300 mt-1">{formatCap(s.lastCap)}</p>
              )}
              {s.pct != null && (
                <p className={`text-sm font-bold font-mono ${isUp ? "text-green-400" : "text-red-400"}`}>
                  {isUp ? "+" : ""}{s.pct.toFixed(2)}%
                </p>
              )}
            </div>
          );
        })}
      </div>

      {/* Chart */}
      <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 p-4">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold text-gray-300">
            {mode === "normalized" ? "市值成長率（%）" : "市值（絕對值）"}
            <span className="ml-2 text-gray-600 font-normal">最近 {range} 天</span>
          </h2>
          <span className="text-xs text-gray-600">{chartData.length} 個交易日</span>
        </div>

        <ResponsiveContainer width="100%" height={420}>
          <LineChart data={chartData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
            <XAxis
              dataKey="date"
              tick={{ fill: "#6b7280", fontSize: 11 }}
              tickLine={false}
              axisLine={{ stroke: "#374151" }}
              interval="preserveStartEnd"
              tickFormatter={(v: string) => v.slice(5)}
            />
            <YAxis
              tick={{ fill: "#6b7280", fontSize: 11 }}
              tickLine={false}
              axisLine={false}
              width={mode === "normalized" ? 55 : 75}
              tickFormatter={(v: number) =>
                mode === "normalized" ? `${v >= 0 ? "+" : ""}${v.toFixed(0)}%` : formatCap(v)
              }
            />
            <Tooltip content={<CustomTooltip mode={mode} />} />
            <Legend
              formatter={(value: string) => (
                <span style={{ color: "#9ca3af", fontSize: 12 }}>{value}</span>
              )}
            />
            {mode === "normalized" && (
              <ReferenceLine y={0} stroke="#374151" strokeDasharray="4 4" />
            )}
            {series.map((s, i) => (
              <Line
                key={s.ticker}
                type="monotone"
                dataKey={s.ticker}
                stroke={COLORS[i % COLORS.length]}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, strokeWidth: 0 }}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
