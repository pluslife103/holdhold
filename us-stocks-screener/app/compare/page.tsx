"use client";

import { useState, useCallback, useRef, useEffect } from "react";
import Link from "next/link";
import MarketCapChart from "@/components/MarketCapChart";

interface SearchResult {
  ticker: string;
  name: string;
  sector: string;
}

const PRESETS = [
  { label: "科技巨頭", tickers: ["AAPL", "MSFT", "NVDA", "GOOGL", "META"] },
  { label: "AI 晶片", tickers: ["NVDA", "AMD", "INTC", "AVGO", "QCOM"] },
  { label: "電動車", tickers: ["TSLA", "RIVN", "F", "GM"] },
  { label: "金融巨頭", tickers: ["JPM", "BAC", "GS", "WFC", "MS"] },
];

export default function ComparePage() {
  const [tickers, setTickers] = useState<string[]>(["AAPL", "MSFT", "NVDA"]);
  const [input, setInput] = useState("");
  const [suggestions, setSuggestions] = useState<SearchResult[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [mode, setMode] = useState<"absolute" | "normalized">("normalized");
  const [range, setRange] = useState<7 | 30 | 60>(60);
  const searchRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Close dropdown on outside click
  useEffect(() => {
    function handle(e: MouseEvent) {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    }
    document.addEventListener("mousedown", handle);
    return () => document.removeEventListener("mousedown", handle);
  }, []);

  const searchTickers = useCallback(async (q: string) => {
    if (!q.trim()) { setSuggestions([]); return; }
    const res = await fetch(`/api/compare?search=${encodeURIComponent(q)}`);
    if (res.ok) {
      const data: SearchResult[] = await res.json();
      setSuggestions(data.filter((s) => !tickers.includes(s.ticker)));
    }
  }, [tickers]);

  const handleInput = (v: string) => {
    setInput(v);
    setShowSuggestions(true);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => searchTickers(v), 200);
  };

  const addTicker = (ticker: string) => {
    const t = ticker.trim().toUpperCase();
    if (t && !tickers.includes(t) && tickers.length < 10) {
      setTickers((prev) => [...prev, t]);
    }
    setInput("");
    setSuggestions([]);
    setShowSuggestions(false);
  };

  const removeTicker = (ticker: string) =>
    setTickers((prev) => prev.filter((t) => t !== ticker));

  const loadPreset = (preset: string[]) => setTickers(preset);

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      {/* Header */}
      <header className="border-b border-gray-800 bg-gray-900/50 backdrop-blur sticky top-0 z-10">
        <div className="max-w-screen-xl mx-auto px-4 py-3 flex items-center gap-4">
          <Link href="/" className="text-gray-400 hover:text-white transition-colors text-sm">
            ← 返回篩選器
          </Link>
          <div className="h-4 w-px bg-gray-700" />
          <h1 className="text-lg font-bold">📈 市值成長比較</h1>
        </div>
      </header>

      <main className="max-w-screen-xl mx-auto px-4 py-5 space-y-4">
        {/* Controls */}
        <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 p-4 space-y-4">
          <div className="flex flex-wrap gap-4 items-start">
            {/* Search box */}
            <div className="flex-1 min-w-[220px]" ref={searchRef}>
              <label className="block text-xs text-gray-500 mb-1.5">新增股票（最多 10 支）</label>
              <div className="relative">
                <input
                  type="text"
                  value={input}
                  onChange={(e) => handleInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && input.trim()) addTicker(input);
                    if (e.key === "Escape") setShowSuggestions(false);
                  }}
                  onFocus={() => input && setShowSuggestions(true)}
                  placeholder="輸入股票代號…"
                  className="w-full bg-gray-800 border border-gray-600 rounded-lg px-3 py-2 text-sm text-white placeholder-gray-600 focus:outline-none focus:border-blue-500"
                />
                {showSuggestions && suggestions.length > 0 && (
                  <div className="absolute top-full mt-1 left-0 right-0 bg-gray-800 border border-gray-600 rounded-lg shadow-xl z-20 overflow-hidden">
                    {suggestions.map((s) => (
                      <button
                        key={s.ticker}
                        onMouseDown={() => addTicker(s.ticker)}
                        className="w-full text-left px-3 py-2.5 hover:bg-gray-700 transition-colors flex items-center gap-3"
                      >
                        <span className="font-bold text-white text-sm w-16 shrink-0">{s.ticker}</span>
                        <span className="text-gray-400 text-xs truncate flex-1">{s.name}</span>
                        <span className="text-gray-600 text-xs shrink-0">{s.sector}</span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </div>

            {/* Mode toggle */}
            <div>
              <label className="block text-xs text-gray-500 mb-1.5">顯示方式</label>
              <div className="flex rounded-lg border border-gray-600 overflow-hidden text-sm">
                {(["normalized", "absolute"] as const).map((m) => (
                  <button
                    key={m}
                    onClick={() => setMode(m)}
                    className={`px-4 py-2 transition-colors ${
                      mode === m ? "bg-blue-600 text-white" : "bg-gray-800 text-gray-400 hover:text-white"
                    }`}
                  >
                    {m === "normalized" ? "% 成長" : "絕對市值"}
                  </button>
                ))}
              </div>
            </div>

            {/* Range */}
            <div>
              <label className="block text-xs text-gray-500 mb-1.5">時間範圍</label>
              <div className="flex rounded-lg border border-gray-600 overflow-hidden text-sm">
                {([7, 30, 60] as const).map((r) => (
                  <button
                    key={r}
                    onClick={() => setRange(r)}
                    className={`px-4 py-2 transition-colors ${
                      range === r ? "bg-blue-600 text-white" : "bg-gray-800 text-gray-400 hover:text-white"
                    }`}
                  >
                    {r}天
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* Presets */}
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs text-gray-600">快速選擇：</span>
            {PRESETS.map((p) => (
              <button
                key={p.label}
                onClick={() => loadPreset(p.tickers)}
                className="text-xs px-3 py-1.5 rounded-full bg-gray-800 text-gray-400 hover:bg-gray-700 hover:text-white transition-colors border border-gray-700"
              >
                {p.label}
              </button>
            ))}
          </div>

          {/* Ticker chips */}
          <div className="flex flex-wrap gap-2">
            {tickers.map((t, i) => (
              <div
                key={t}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm font-medium"
                style={{ backgroundColor: CHIP_BG[i % CHIP_BG.length], color: CHIP_FG[i % CHIP_FG.length] }}
              >
                <span>{t}</span>
                <button
                  onClick={() => removeTicker(t)}
                  className="opacity-60 hover:opacity-100 transition-opacity ml-0.5"
                >
                  ×
                </button>
              </div>
            ))}
            {tickers.length === 0 && (
              <span className="text-xs text-gray-600">請輸入股票代號或選擇預設組合</span>
            )}
          </div>
        </div>

        {/* Chart */}
        {tickers.length > 0 && (
          <MarketCapChart tickers={tickers} mode={mode} range={range} />
        )}
      </main>
    </div>
  );
}

const CHIP_BG = [
  "#1d3557", "#1b4332", "#3d1a78", "#7c2d12", "#064e3b",
  "#1e3a5f", "#422006", "#1a1a2e", "#312e81", "#4a1942",
];
const CHIP_FG = [
  "#93c5fd", "#6ee7b7", "#c4b5fd", "#fed7aa", "#a7f3d0",
  "#7dd3fc", "#fde68a", "#a5b4fc", "#818cf8", "#f0abfc",
];
