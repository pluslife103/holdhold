"use client";

import { useEffect, useState, useMemo } from "react";
import { CrossoverEvent } from "@/lib/db";

// ── Constants ─────────────────────────────────────────────────────────────────

const TIER_ORDER = ["超大型", "巨型", "大型"] as const;

const TIER_STYLE: Record<string, string> = {
  超大型: "bg-violet-900/60 text-violet-200 border-violet-700/40",
  巨型:   "bg-purple-900/60 text-purple-300 border-purple-700/40",
  大型:   "bg-blue-900/60   text-blue-300   border-blue-700/40",
};

type ViewMode = "日" | "週" | "月";

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatCap(v: number): string {
  if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9)  return `$${(v / 1e9).toFixed(0)}B`;
  return `$${(v / 1e6).toFixed(0)}M`;
}

function isoWeek(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00Z");
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const y = d.getUTCFullYear();
  const jan4 = new Date(Date.UTC(y, 0, 4));
  const w = Math.ceil(((d.getTime() - jan4.getTime()) / 86_400_000 + 1) / 7);
  return `${y}-W${String(w).padStart(2, "0")}`;
}

function weekLabel(wk: string): string {
  const [y, wn] = wk.split("-W").map(Number);
  const jan4 = new Date(Date.UTC(y, 0, 4));
  const mon = new Date(jan4);
  mon.setUTCDate(jan4.getUTCDate() - (jan4.getUTCDay() || 7) + 1 + (wn - 1) * 7);
  const fri = new Date(mon);
  fri.setUTCDate(mon.getUTCDate() + 4);
  const fmt = (d: Date) =>
    `${d.getUTCMonth() + 1}/${String(d.getUTCDate()).padStart(2, "0")}`;
  return `第${wn}週 (${fmt(mon)}–${fmt(fri)})`;
}

function monthKey(dateStr: string): string {
  return dateStr.slice(0, 7); // "YYYY-MM"
}

function monthLabel(mk: string): string {
  const [y, m] = mk.split("-");
  return `${y}年${parseInt(m)}月`;
}

// ── Aggregate stats ───────────────────────────────────────────────────────────

interface AggEntry {
  winner:      string;
  winnerName:  string;
  lastCap:     number;
  tier:        string;
  totalEvents: number;        // event count (can cross same stock on different days)
  uniqueLosers: string[];     // deduplicated list
  activeDays:  number;        // distinct dates
}

function aggregate(events: CrossoverEvent[]): AggEntry[] {
  const map = new Map<string, {
    winnerName: string; lastCap: number; tier: string;
    events: number; losers: Set<string>; days: Set<string>;
  }>();

  for (const e of events) {
    // 只統計大型以上；排除任何不在定義範圍的 tier
    if (!TIER_ORDER.includes(e.tier as typeof TIER_ORDER[number])) continue;
    if (!map.has(e.winner)) {
      map.set(e.winner, {
        winnerName: e.winnerName, lastCap: e.winnerCap, tier: e.tier,
        events: 0, losers: new Set(), days: new Set(),
      });
    }
    const entry = map.get(e.winner)!;
    entry.events++;
    entry.losers.add(e.loser);
    entry.days.add(e.date);
    if (e.winnerCap > entry.lastCap) entry.lastCap = e.winnerCap;
  }

  return [...map.entries()]
    .map(([winner, v]) => ({
      winner,
      winnerName:  v.winnerName,
      lastCap:     v.lastCap,
      tier:        v.tier,
      totalEvents: v.events,
      uniqueLosers: [...v.losers],
      activeDays:  v.days.size,
    }))
    .sort((a, b) => b.totalEvents - a.totalEvents);
}

// ── Daily grouped view helpers ────────────────────────────────────────────────

interface GroupedWinner {
  winner:     string; winnerName: string;
  winnerCap:  number; winnerPrev: number;
  losers: { ticker: string; name: string; cap: number }[];
}

function groupByWinner(events: CrossoverEvent[]): Record<string, GroupedWinner[]> {
  const result: Record<string, GroupedWinner[]> = { 超大型: [], 巨型: [], 大型: [] };
  const map = new Map<string, GroupedWinner>();

  for (const e of events) {
    if (!TIER_ORDER.includes(e.tier as typeof TIER_ORDER[number])) continue;
    const key = `${e.tier}|${e.winner}`;
    if (!map.has(key)) {
      const entry: GroupedWinner = {
        winner: e.winner, winnerName: e.winnerName,
        winnerCap: e.winnerCap, winnerPrev: e.winnerPrev, losers: [],
      };
      map.set(key, entry);
      result[e.tier]?.push(entry);
    }
    map.get(key)!.losers.push({ ticker: e.loser, name: e.loserName, cap: e.loserCap });
  }

  for (const t of TIER_ORDER) result[t].sort((a, b) => b.winnerCap - a.winnerCap);
  return result;
}

// ── Main component ────────────────────────────────────────────────────────────

export default function CrossoverPanel() {
  const [events, setEvents]   = useState<CrossoverEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [open, setOpen]       = useState(true);
  const [days, setDays]       = useState<30 | 60>(60);
  const [view, setView]       = useState<ViewMode>("日");
  const [selDate, setSelDate] = useState<string | null>(null);
  const [selWeek, setSelWeek] = useState<string | null>(null);
  const [selMonth, setSelMonth] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    fetch(`/api/crossovers?days=${days}`)
      .then((r) => r.json())
      .then((data: CrossoverEvent[]) => {
        setEvents(data);
        setSelDate(null); setSelWeek(null); setSelMonth(null);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [days]);

  // ── Derived period lists ─────────────────────────────────────────────────

  const dates = useMemo(() =>
    [...new Set(events.map((e) => e.date))].sort((a, b) => b.localeCompare(a)),
  [events]);

  const weeks = useMemo(() =>
    [...new Set(events.map((e) => isoWeek(e.date)))].sort((a, b) => b.localeCompare(a)),
  [events]);

  const months = useMemo(() =>
    [...new Set(events.map((e) => monthKey(e.date)))].sort((a, b) => b.localeCompare(a)),
  [events]);

  const curDate  = selDate  ?? dates[0]  ?? null;
  const curWeek  = selWeek  ?? weeks[0]  ?? null;
  const curMonth = selMonth ?? months[0] ?? null;

  const dateIdx  = curDate  ? dates.indexOf(curDate)   : 0;
  const weekIdx  = curWeek  ? weeks.indexOf(curWeek)   : 0;
  const monthIdx = curMonth ? months.indexOf(curMonth) : 0;

  // ── Filtered events for current period ──────────────────────────────────

  const dayEvents = useMemo(
    () => events.filter((e) => e.date === curDate),
    [events, curDate]
  );

  const weekEvents = useMemo(
    () => events.filter((e) => curWeek && isoWeek(e.date) === curWeek),
    [events, curWeek]
  );

  const monthEvents = useMemo(
    () => events.filter((e) => curMonth && monthKey(e.date) === curMonth),
    [events, curMonth]
  );

  // ── Grouped / aggregated ─────────────────────────────────────────────────

  const dailyGrouped = useMemo(() => groupByWinner(dayEvents),  [dayEvents]);
  const weekAgg      = useMemo(() => aggregate(weekEvents),    [weekEvents]);
  const monthAgg     = useMemo(() => aggregate(monthEvents),   [monthEvents]);

  const dailyTotal = TIER_ORDER.reduce((s, t) => s + (dailyGrouped[t]?.length ?? 0), 0);

  // ── Navigator component ──────────────────────────────────────────────────

  function Nav({
    items, current, idx, onPrev, onNext, onSelect, onReset, formatter,
  }: {
    items: string[]; current: string | null; idx: number;
    onPrev: () => void; onNext: () => void;
    onSelect: (v: string) => void; onReset: () => void;
    formatter: (v: string) => string;
  }) {
    return (
      <div className="px-4 py-2.5 border-b border-gray-800 flex items-center gap-2 flex-wrap bg-gray-900/30 text-xs">
        <button onClick={onPrev} disabled={idx >= items.length - 1}
          className="px-2.5 py-1 rounded-lg bg-gray-800 text-gray-400 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed transition-colors">
          ← 往前
        </button>
        <select value={current ?? ""} onChange={(e) => onSelect(e.target.value)}
          className="bg-gray-800 border border-gray-700 rounded-lg px-2 py-1 text-white focus:outline-none cursor-pointer">
          {items.map((d) => <option key={d} value={d}>{formatter(d)}</option>)}
        </select>
        <button onClick={onNext} disabled={idx <= 0}
          className="px-2.5 py-1 rounded-lg bg-gray-800 text-gray-400 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed transition-colors">
          往後 →
        </button>
        {(view === "日" ? selDate : view === "週" ? selWeek : selMonth) !== null && (
          <button onClick={onReset}
            className="px-2.5 py-1 rounded-lg bg-blue-700/40 text-blue-300 hover:bg-blue-700/60 transition-colors">
            最新
          </button>
        )}
        <span className="text-gray-600 ml-auto">共 {items.length} 個{view === "日" ? "交易日" : view === "週" ? "週" : "月"}</span>
      </div>
    );
  }

  // ── Aggregate leaderboard rows (grouped by tier) ────────────────────────

  function AggTable({ entries }: { entries: AggEntry[] }) {
    if (entries.length === 0)
      return <p className="text-center py-10 text-gray-600 text-sm">該期間無超越事件</p>;

    const byTier: Record<string, AggEntry[]> = { 超大型: [], 巨型: [], 大型: [] };
    for (const e of entries) byTier[e.tier]?.push(e);

    return (
      <div className="divide-y divide-gray-800/50">
        {TIER_ORDER.filter((t) => byTier[t].length > 0).map((tier) => (
          <div key={tier}>
            {/* Tier section header */}
            <div className="px-4 py-1.5 bg-gray-800/20 flex items-center gap-2 sticky top-0 backdrop-blur">
              <span className={`text-xs px-2 py-0.5 rounded-full border font-medium ${TIER_STYLE[tier]}`}>
                {tier}
              </span>
              <span className="text-xs text-gray-500">
                {byTier[tier].length} 支　共 {byTier[tier].reduce((s, e) => s + e.totalEvents, 0)} 件
              </span>
            </div>

            {/* Entries within tier */}
            {byTier[tier].map((e, idx) => (
              <div key={e.winner} className="px-4 py-2.5 hover:bg-gray-800/20 transition-colors">
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="text-gray-600 text-xs w-5 shrink-0 text-right">{idx + 1}</span>
                  <span className="text-green-400 font-bold text-sm">↑</span>
                  <span className="font-bold text-white text-sm">{e.winner}</span>
                  <span className="text-gray-500 text-xs truncate max-w-[160px] hidden sm:block">
                    {e.winnerName.split(" ").slice(0, 3).join(" ")}
                  </span>
                  <div className="ml-auto flex items-center gap-3 shrink-0">
                    <div className="text-right">
                      <span className="text-green-400 font-bold text-sm">×{e.totalEvents}</span>
                      <span className="text-gray-600 text-xs ml-1">超越</span>
                    </div>
                    <div className="text-right hidden sm:block">
                      <span className="text-gray-400 text-xs">{e.uniqueLosers.length} 支</span>
                      <span className="text-gray-600 text-xs ml-1">不重複</span>
                    </div>
                    <div className="text-right hidden md:block">
                      <span className="text-gray-400 text-xs">{e.activeDays} 天</span>
                      <span className="text-gray-600 text-xs ml-1">活躍</span>
                    </div>
                    <span className="font-mono text-gray-300 text-xs hidden md:block">{formatCap(e.lastCap)}</span>
                  </div>
                </div>
                <div className="flex flex-wrap gap-x-2 gap-y-0.5 mt-1 pl-8">
                  {e.uniqueLosers.slice(0, 20).map((loser) => (
                    <span key={loser} className="text-xs text-red-400/80">↓{loser}</span>
                  ))}
                  {e.uniqueLosers.length > 20 && (
                    <span className="text-xs text-gray-600">+{e.uniqueLosers.length - 20} 支</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        ))}
      </div>
    );
  }

  // ── Daily grouped rows ───────────────────────────────────────────────────

  function DailyBody() {
    if (dailyTotal === 0)
      return <p className="text-center py-10 text-gray-600 text-sm">該日期無超越事件</p>;

    return (
      <div className="divide-y divide-gray-800/50">
        {TIER_ORDER.filter((t) => dailyGrouped[t]?.length > 0).map((tier) => (
          <div key={tier}>
            <div className="px-4 py-1.5 bg-gray-800/20 flex items-center gap-2">
              <span className={`text-xs px-2 py-0.5 rounded-full border font-medium ${TIER_STYLE[tier]}`}>
                {tier}
              </span>
              <span className="text-xs text-gray-500">{dailyGrouped[tier].length} 支超越</span>
            </div>
            {dailyGrouped[tier].map((entry) => (
              <div key={entry.winner} className="px-4 py-2.5 hover:bg-gray-800/20 transition-colors">
                <div className="flex items-baseline gap-2 flex-wrap">
                  <span className="text-green-400 font-bold">↑</span>
                  <span className="font-bold text-white">{entry.winner}</span>
                  <span className="text-gray-400 text-xs truncate max-w-[200px]">
                    {entry.winnerName.split(" ").slice(0, 4).join(" ")}
                  </span>
                  <span className="font-mono text-green-400 text-sm ml-auto">{formatCap(entry.winnerCap)}</span>
                  {entry.winnerPrev > 0 && (
                    <span className="text-xs text-gray-600 font-mono">
                      ({((entry.winnerCap / entry.winnerPrev - 1) * 100).toFixed(1)}%)
                    </span>
                  )}
                  {entry.losers.length > 1 && (
                    <span className="text-xs bg-green-900/40 text-green-400 px-1.5 py-0.5 rounded-full">×{entry.losers.length}</span>
                  )}
                </div>
                <div className="flex flex-wrap gap-x-3 gap-y-0.5 mt-1 pl-4">
                  {entry.losers.map((l) => (
                    <span key={l.ticker} className="flex items-center gap-1 text-xs">
                      <span className="text-red-400">↓</span>
                      <span className="font-medium text-gray-300">{l.ticker}</span>
                      <span className="font-mono text-red-400/80">{formatCap(l.cap)}</span>
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        ))}
      </div>
    );
  }

  // ── Render ───────────────────────────────────────────────────────────────

  const bodyCount =
    view === "日" ? `${dailyTotal} 支超越 · ${dayEvents.length} 件` :
    view === "週" ? `${weekAgg.length} 支股票 · ${weekEvents.length} 件` :
                   `${monthAgg.length} 支股票 · ${monthEvents.length} 件`;

  return (
    <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800 flex-wrap gap-2">
        <button onClick={() => setOpen((v) => !v)}
          className="flex items-center gap-2 text-sm font-semibold text-white hover:text-gray-200">
          <span>{open ? "▾" : "▸"}</span>
          ⚔️ 市值超越榜
          <span className="text-xs font-normal text-gray-600">大型以上（$70B+）</span>
          {!loading && (
            <span className="text-xs font-normal text-gray-500">{bodyCount}</span>
          )}
        </button>

        {open && (
          <div className="flex items-center gap-2">
            {/* View tabs */}
            <div className="flex rounded-lg border border-gray-700 overflow-hidden text-xs">
              {(["日", "週", "月"] as ViewMode[]).map((v) => (
                <button key={v} onClick={() => setView(v)}
                  className={`px-3 py-1.5 transition-colors ${view === v ? "bg-blue-600 text-white" : "bg-gray-800 text-gray-400 hover:text-white"}`}>
                  {v}
                </button>
              ))}
            </div>
            {/* Days range */}
            <div className="flex rounded-lg border border-gray-700 overflow-hidden text-xs">
              {([30, 60] as const).map((d) => (
                <button key={d} onClick={() => setDays(d)}
                  className={`px-3 py-1.5 transition-colors ${days === d ? "bg-gray-700 text-white" : "bg-gray-800 text-gray-500 hover:text-white"}`}>
                  {d}天
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      {open && (
        <>
          {/* Navigator */}
          {view === "日" && (
            <Nav
              items={dates} current={curDate} idx={dateIdx}
              onPrev={() => setSelDate(dates[dateIdx + 1])}
              onNext={() => setSelDate(dates[dateIdx - 1] ?? null)}
              onSelect={setSelDate} onReset={() => setSelDate(null)}
              formatter={(d) => d}
            />
          )}
          {view === "週" && (
            <Nav
              items={weeks} current={curWeek} idx={weekIdx}
              onPrev={() => setSelWeek(weeks[weekIdx + 1])}
              onNext={() => setSelWeek(weeks[weekIdx - 1] ?? null)}
              onSelect={setSelWeek} onReset={() => setSelWeek(null)}
              formatter={weekLabel}
            />
          )}
          {view === "月" && (
            <Nav
              items={months} current={curMonth} idx={monthIdx}
              onPrev={() => setSelMonth(months[monthIdx + 1])}
              onNext={() => setSelMonth(months[monthIdx - 1] ?? null)}
              onSelect={setSelMonth} onReset={() => setSelMonth(null)}
              formatter={monthLabel}
            />
          )}

          {/* Body */}
          <div className="max-h-[520px] overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center py-10 text-gray-500 text-sm gap-2">
                <span className="w-4 h-4 border-2 border-gray-600 border-t-blue-400 rounded-full animate-spin" />
                計算中…
              </div>
            ) : view === "日" ? (
              <DailyBody />
            ) : view === "週" ? (
              <AggTable entries={weekAgg} />
            ) : (
              <AggTable entries={monthAgg} />
            )}
          </div>
        </>
      )}
    </div>
  );
}
