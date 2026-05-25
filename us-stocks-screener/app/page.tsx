"use client";

import { useEffect, useState, useMemo, useCallback, useRef } from "react";
import { StockQuote } from "@/app/api/stocks/route";
import StockTable, { SortKey, SortDir } from "@/components/StockTable";
import DateNav from "@/components/DateNav";
import CrossoverPanel from "@/components/CrossoverPanel";
import MARanking from "@/components/MARanking";
import CombinedRanking from "@/components/CombinedRanking";
import MAScreener from "@/components/MAScreener";
import MarketCapChart from "@/components/MarketCapChart";

const PRESETS = [
  { label: "科技巨頭", tickers: ["AAPL", "MSFT", "NVDA", "GOOGL", "META"] },
  { label: "AI 晶片",  tickers: ["NVDA", "AMD", "INTC", "AVGO", "QCOM"] },
  { label: "電動車",   tickers: ["TSLA", "RIVN", "F", "GM"] },
  { label: "金融巨頭", tickers: ["JPM", "BAC", "GS", "WFC", "MS"] },
];

const CHIP_COLORS = [
  { bg: "#1d3557", fg: "#93c5fd" }, { bg: "#1b4332", fg: "#6ee7b7" },
  { bg: "#3d1a78", fg: "#c4b5fd" }, { bg: "#7c2d12", fg: "#fed7aa" },
  { bg: "#064e3b", fg: "#a7f3d0" }, { bg: "#1e3a5f", fg: "#7dd3fc" },
  { bg: "#422006", fg: "#fde68a" }, { bg: "#312e81", fg: "#818cf8" },
  { bg: "#4a1942", fg: "#f0abfc" }, { bg: "#14532d", fg: "#bbf7d0" },
];

const CAP_RANGES = [
  { label: "全部市值",              min: 0,      max: Infinity },
  { label: "超大型 (>$800B)",       min: 800e9,  max: Infinity },
  { label: "巨型 ($200B–$800B)",    min: 200e9,  max: 800e9   },
  { label: "大型 ($70B–$200B)",     min: 70e9,   max: 200e9   },
  { label: "中大型 ($10B–$70B)",    min: 10e9,   max: 70e9    },
  { label: "中型 ($2B–$10B)",       min: 2e9,    max: 10e9    },
  { label: "小型 (<$2B)",           min: 0,      max: 2e9     },
] as const;

export default function Home() {
  const [stocks, setStocks] = useState<StockQuote[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  // Historical dates
  const [dates, setDates] = useState<string[]>([]);
  const [selectedDate, setSelectedDate] = useState<string | null>(null); // null = live
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState<string | null>(null);

  // Compare tool
  const [compareTickers, setCompareTickers] = useState<string[]>(["AAPL", "MSFT", "NVDA"]);
  const [compareOpen, setCompareOpen] = useState(false);
  const [compareInput, setCompareInput] = useState("");
  const [compareSuggestions, setCompareSuggestions] = useState<{ ticker: string; name: string; sector: string }[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [compareMode, setCompareMode] = useState<"normalized" | "absolute">("normalized");
  const [compareRange, setCompareRange] = useState<7 | 30 | 60>(30);
  const suggestionRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Filters
  const [search, setSearch] = useState("");
  const [sector, setSector] = useState("All");
  const [capRange, setCapRange] = useState(0);
  const [usOnly, setUsOnly] = useState(false);
  const [sortKey, setSortKey] = useState<SortKey>("marketCap");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  // Load available snapshot dates
  const loadDates = useCallback(async () => {
    try {
      const res = await fetch("/api/snapshots");
      if (res.ok) {
        const data = await res.json();
        setDates(data.dates ?? []);
      }
    } catch { /* ignore */ }
  }, []);

  // Fetch stocks — live or historical
  const fetchStocks = useCallback(async (date: string | null) => {
    setLoading(true);
    setError(null);
    try {
      const url = date ? `/api/snapshots/${date}` : "/api/stocks";
      const res = await fetch(url);
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error ?? `HTTP ${res.status}`);
      }
      const data: StockQuote[] = await res.json();
      setStocks(data);
      setLastUpdated(new Date());
    } catch (e) {
      setError(e instanceof Error ? e.message : "載入失敗");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadDates();
    fetchStocks(null);
  }, [fetchStocks, loadDates]);

  const handleSelectDate = useCallback((date: string | null) => {
    setSelectedDate(date);
    fetchStocks(date);
  }, [fetchStocks]);

  const handleSaveToday = useCallback(async () => {
    setSaving(true);
    setSaveMsg(null);
    try {
      const res = await fetch("/api/snapshots/save", { method: "POST", body: JSON.stringify({}) });
      const data = await res.json();
      if (data.skipped) {
        setSaveMsg("今日快照已存在");
      } else {
        setSaveMsg(`已儲存 ${data.saved?.toLocaleString()} 筆`);
        await loadDates();
      }
    } catch {
      setSaveMsg("儲存失敗");
    } finally {
      setSaving(false);
      setTimeout(() => setSaveMsg(null), 4000);
    }
  }, [loadDates]);

  const handleSort = useCallback((key: SortKey) => {
    setSortDir((prev) => (sortKey === key ? (prev === "desc" ? "asc" : "desc") : "desc"));
    setSortKey(key);
  }, [sortKey]);

  // Compare tool handlers
  const searchCompare = useCallback(async (q: string) => {
    if (!q.trim()) { setCompareSuggestions([]); return; }
    const res = await fetch(`/api/compare?search=${encodeURIComponent(q)}`);
    if (res.ok) setCompareSuggestions(await res.json());
  }, []);

  const handleCompareInput = (v: string) => {
    setCompareInput(v);
    setShowSuggestions(true);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => searchCompare(v), 200);
  };

  const addCompareTicker = (t: string) => {
    const u = t.trim().toUpperCase();
    if (u && !compareTickers.includes(u) && compareTickers.length < 10)
      setCompareTickers((prev) => [...prev, u]);
    setCompareInput(""); setCompareSuggestions([]); setShowSuggestions(false);
  };

  // Close suggestions on outside click
  useEffect(() => {
    const fn = (e: MouseEvent) => {
      if (suggestionRef.current && !suggestionRef.current.contains(e.target as Node))
        setShowSuggestions(false);
    };
    document.addEventListener("mousedown", fn);
    return () => document.removeEventListener("mousedown", fn);
  }, []);

  const sectors = useMemo(() => {
    const set = new Set<string>();
    for (const s of stocks) {
      if (s.sector && s.sector !== "Unknown") set.add(s.sector);
    }
    return ["All", ...Array.from(set).sort()];
  }, [stocks]);

  const filtered = useMemo(() => {
    const range = CAP_RANGES[capRange];
    const q = search.toLowerCase();
    return stocks
      .filter((s) => {
        if (usOnly && s.country !== "United States") return false;
        if (sector !== "All" && s.sector !== sector) return false;
        if (capRange > 0) {
          if (s.marketCap === null) return false;
          if (s.marketCap < range.min || s.marketCap > range.max) return false;
        }
        if (q && !s.ticker.toLowerCase().includes(q) && !s.name.toLowerCase().includes(q)) return false;
        return true;
      })
      .sort((a, b) => {
        const av = a[sortKey] ?? (sortDir === "desc" ? -Infinity : Infinity);
        const bv = b[sortKey] ?? (sortDir === "desc" ? -Infinity : Infinity);
        return sortDir === "desc"
          ? (bv as number) - (av as number)
          : (av as number) - (bv as number);
      });
  }, [stocks, sector, capRange, search, sortKey, sortDir, usOnly]);

  const stats = useMemo(() => {
    const withCap = filtered.filter((s) => s.marketCap !== null && s.marketCap > 0);
    const total = withCap.reduce((sum, s) => sum + (s.marketCap ?? 0), 0);
    const gainers = filtered.filter((s) => (s.changePercent ?? 0) > 0).length;
    const losers = filtered.filter((s) => (s.changePercent ?? 0) < 0).length;
    return { count: filtered.length, totalCap: total, gainers, losers };
  }, [filtered]);

  function formatLargeCap(v: number): string {
    if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
    if (v >= 1e9) return `$${(v / 1e9).toFixed(2)}B`;
    return `$${(v / 1e6).toFixed(2)}M`;
  }

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      {/* Header */}
      <header className="border-b border-gray-800 bg-gray-900/50 backdrop-blur sticky top-0 z-10">
        <div className="max-w-screen-2xl mx-auto px-4 py-3 flex items-center justify-between gap-4 flex-wrap">
          <div>
            <h1 className="text-lg font-bold text-white tracking-tight">
              🇺🇸 美股市值篩選器
            </h1>
            <p className="text-xs text-gray-500">
              {selectedDate
                ? `歷史資料：${selectedDate} · ${stocks.length.toLocaleString()} 支`
                : lastUpdated
                  ? `即時 · 更新於 ${lastUpdated.toLocaleTimeString("zh-TW")} · ${stocks.length.toLocaleString()} 支`
                  : "載入中..."}
              　·　NASDAQ
            </p>
          </div>
          <button
            onClick={() => fetchStocks(selectedDate)}
            disabled={loading}
            className="text-sm px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 disabled:opacity-50 disabled:cursor-not-allowed transition-colors font-medium"
          >
            {loading ? "載入中…" : "重新整理"}
          </button>
        </div>
      </header>

      <main className="max-w-screen-2xl mx-auto px-4 py-4 space-y-3">
        {/* Date navigation */}
        <DateNav
          dates={dates}
          selected={selectedDate}
          onSelect={handleSelectDate}
          saving={saving}
          onSaveToday={handleSaveToday}
        />

        {saveMsg && (
          <div className="bg-green-900/30 border border-green-700/50 rounded-xl px-4 py-2 text-green-400 text-sm">
            ✓ {saveMsg}
          </div>
        )}

        {/* Stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { label: "篩選股票數", value: stats.count.toLocaleString() },
            { label: "合計市值", value: stats.totalCap > 0 ? formatLargeCap(stats.totalCap) : "—" },
            { label: "上漲 / 下跌", value: `${stats.gainers} / ${stats.losers}` },
            { label: selectedDate ? "歷史日期" : "總上市股票", value: selectedDate ?? stocks.length.toLocaleString() },
          ].map((stat) => (
            <div key={stat.label} className="bg-gray-800/50 rounded-xl p-3 border border-gray-700/50">
              <p className="text-xs text-gray-500">{stat.label}</p>
              <p className="text-lg font-bold text-white mt-0.5">{stat.value}</p>
            </div>
          ))}
        </div>

        {/* Filters */}
        <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 p-4 space-y-3">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
            <div>
              <label className="block text-xs text-gray-500 mb-1">搜尋股票</label>
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="代號或公司名稱..."
                className="w-full bg-gray-800 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white placeholder-gray-600 focus:outline-none focus:border-blue-500 transition-colors"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">行業板塊</label>
              <select
                value={sector}
                onChange={(e) => setSector(e.target.value)}
                className="w-full bg-gray-800 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500 transition-colors cursor-pointer"
              >
                {sectors.map((s) => (
                  <option key={s} value={s}>{s === "All" ? "全部行業" : s}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">市值範圍</label>
              <select
                value={capRange}
                onChange={(e) => setCapRange(Number(e.target.value))}
                className="w-full bg-gray-800 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-blue-500 transition-colors cursor-pointer"
              >
                {CAP_RANGES.map((r, i) => (
                  <option key={r.label} value={i}>{r.label}</option>
                ))}
              </select>
            </div>
            <div className="flex items-end">
              <button
                onClick={() => setUsOnly((v) => !v)}
                className={`w-full py-2 px-3 rounded-lg text-sm font-medium border transition-colors ${
                  usOnly
                    ? "bg-blue-600 border-blue-500 text-white"
                    : "bg-gray-800 border-gray-600 text-gray-400 hover:text-white"
                }`}
              >
                {usOnly ? "🇺🇸 僅美國公司" : "🌍 包含 ADR / 外國"}
              </button>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            {CAP_RANGES.map((r, i) => (
              <button
                key={r.label}
                onClick={() => setCapRange(i)}
                className={`text-xs px-3 py-1 rounded-full font-medium transition-colors ${
                  capRange === i
                    ? "bg-blue-600 text-white"
                    : "bg-gray-800 text-gray-400 hover:bg-gray-700 hover:text-white"
                }`}
              >
                {r.label}
              </button>
            ))}
          </div>
        </div>

        {/* MA Screener */}
        <MAScreener />

        {/* MA ranking */}
        <MARanking />

        {/* Combined: crossover × MA bull */}
        <CombinedRanking />

        {/* Crossover leaderboard */}
        <CrossoverPanel />

        {/* Compare tool */}
        <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 overflow-hidden">
          <button
            onClick={() => setCompareOpen((v) => !v)}
            className="w-full flex items-center justify-between px-4 py-3 text-sm font-semibold text-white hover:bg-gray-800/30 transition-colors"
          >
            <span className="flex items-center gap-2">
              <span>{compareOpen ? "▾" : "▸"}</span>
              📈 市值成長比較
              <span className="text-xs font-normal text-gray-500">
                {compareTickers.length} 支
              </span>
            </span>
            <span className="text-xs text-gray-600 font-normal">點擊展開 / 收起</span>
          </button>

          {compareOpen && (
            <div className="px-4 pb-4 space-y-3 border-t border-gray-800">
              {/* Controls row */}
              <div className="flex flex-wrap gap-3 pt-3 items-end">
                {/* Ticker search */}
                <div className="flex-1 min-w-[180px]" ref={suggestionRef}>
                  <label className="block text-xs text-gray-500 mb-1">新增股票</label>
                  <div className="relative">
                    <input
                      type="text"
                      value={compareInput}
                      onChange={(e) => handleCompareInput(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter" && compareInput.trim()) addCompareTicker(compareInput);
                        if (e.key === "Escape") setShowSuggestions(false);
                      }}
                      onFocus={() => compareInput && setShowSuggestions(true)}
                      placeholder="輸入代號…"
                      className="w-full bg-gray-800 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white placeholder-gray-600 focus:outline-none focus:border-blue-500"
                    />
                    {showSuggestions && compareSuggestions.length > 0 && (
                      <div className="absolute top-full mt-1 left-0 right-0 bg-gray-800 border border-gray-600 rounded-lg shadow-xl z-20 overflow-hidden">
                        {compareSuggestions.filter((s) => !compareTickers.includes(s.ticker)).map((s) => (
                          <button
                            key={s.ticker}
                            onMouseDown={() => addCompareTicker(s.ticker)}
                            className="w-full text-left px-3 py-2 hover:bg-gray-700 flex items-center gap-3 text-sm"
                          >
                            <span className="font-bold text-white w-14 shrink-0">{s.ticker}</span>
                            <span className="text-gray-400 text-xs truncate flex-1">{s.name}</span>
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                </div>

                {/* Mode */}
                <div>
                  <label className="block text-xs text-gray-500 mb-1">模式</label>
                  <div className="flex rounded-lg border border-gray-600 overflow-hidden text-xs">
                    {(["normalized", "absolute"] as const).map((m) => (
                      <button key={m} onClick={() => setCompareMode(m)}
                        className={`px-3 py-2 transition-colors ${compareMode === m ? "bg-blue-600 text-white" : "bg-gray-800 text-gray-400 hover:text-white"}`}>
                        {m === "normalized" ? "% 成長" : "絕對值"}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Range */}
                <div>
                  <label className="block text-xs text-gray-500 mb-1">時間</label>
                  <div className="flex rounded-lg border border-gray-600 overflow-hidden text-xs">
                    {([7, 30, 60] as const).map((r) => (
                      <button key={r} onClick={() => setCompareRange(r)}
                        className={`px-3 py-2 transition-colors ${compareRange === r ? "bg-blue-600 text-white" : "bg-gray-800 text-gray-400 hover:text-white"}`}>
                        {r}天
                      </button>
                    ))}
                  </div>
                </div>
              </div>

              {/* Presets */}
              <div className="flex flex-wrap gap-1.5 items-center">
                <span className="text-xs text-gray-600">快速：</span>
                {PRESETS.map((p) => (
                  <button key={p.label} onClick={() => setCompareTickers(p.tickers)}
                    className="text-xs px-2.5 py-1 rounded-full bg-gray-800 text-gray-400 hover:bg-gray-700 hover:text-white border border-gray-700 transition-colors">
                    {p.label}
                  </button>
                ))}
              </div>

              {/* Chips */}
              <div className="flex flex-wrap gap-2">
                {compareTickers.map((t, i) => (
                  <div key={t} className="flex items-center gap-1.5 px-3 py-1 rounded-full text-sm font-medium"
                    style={{ backgroundColor: CHIP_COLORS[i % CHIP_COLORS.length].bg, color: CHIP_COLORS[i % CHIP_COLORS.length].fg }}>
                    <span>{t}</span>
                    <button onClick={() => setCompareTickers((prev) => prev.filter((x) => x !== t))}
                      className="opacity-60 hover:opacity-100">×</button>
                  </div>
                ))}
              </div>

              {/* Chart */}
              <MarketCapChart tickers={compareTickers} mode={compareMode} range={compareRange} />
            </div>
          )}
        </div>

        {error && (
          <div className="bg-red-900/30 border border-red-700/50 rounded-xl p-4 text-red-400 text-sm">
            載入失敗：{error}
          </div>
        )}

        {loading && stocks.length === 0 && (
          <div className="space-y-1.5">
            {Array.from({ length: 20 }).map((_, i) => (
              <div key={i} className="h-14 bg-gray-800/40 rounded-lg animate-pulse" />
            ))}
          </div>
        )}

        {(!loading || stocks.length > 0) && (
          <StockTable
            stocks={filtered}
            sortKey={sortKey}
            sortDir={sortDir}
            onSort={handleSort}
          />
        )}

        <p className="text-xs text-gray-700 text-center pb-4">
          本站資料僅供參考，不構成投資建議。即時資料延遲約 15 分鐘。
        </p>
      </main>
    </div>
  );
}
