"use client";

interface Props {
  dates: string[];
  selected: string | null; // null = live
  onSelect: (date: string | null) => void;
  saving: boolean;
  onSaveToday: () => void;
}

export default function DateNav({ dates, selected, onSelect, saving, onSaveToday }: Props) {
  const idx = selected ? dates.indexOf(selected) : -1;
  const hasPrev = idx < dates.length - 1;
  const hasNext = idx > 0;

  return (
    <div className="bg-gray-900/50 rounded-xl border border-gray-700/50 p-3">
      <div className="flex items-center gap-3 flex-wrap">
        {/* Live button */}
        <button
          onClick={() => onSelect(null)}
          className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
            selected === null
              ? "bg-green-600 text-white"
              : "bg-gray-800 text-gray-400 hover:text-white hover:bg-gray-700"
          }`}
        >
          <span className={`w-1.5 h-1.5 rounded-full ${selected === null ? "bg-green-300 animate-pulse" : "bg-gray-600"}`} />
          即時
        </button>

        {/* Separator */}
        <div className="h-6 w-px bg-gray-700" />

        {/* Prev date */}
        <button
          onClick={() => hasPrev && onSelect(dates[idx + 1])}
          disabled={!hasPrev}
          className="px-2 py-1.5 rounded-lg text-sm bg-gray-800 text-gray-400 hover:text-white hover:bg-gray-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
        >
          ← 前一日
        </button>

        {/* Date selector */}
        <div className="flex items-center gap-2">
          <select
            value={selected ?? ""}
            onChange={(e) => onSelect(e.target.value || null)}
            className="bg-gray-800 border border-gray-600 rounded-lg px-3 py-1.5 text-sm text-white focus:outline-none focus:border-blue-500 cursor-pointer min-w-[140px]"
          >
            <option value="">— 即時資料 —</option>
            {dates.map((d) => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
        </div>

        {/* Next date */}
        <button
          onClick={() => hasNext && onSelect(dates[idx - 1])}
          disabled={!hasNext}
          className="px-2 py-1.5 rounded-lg text-sm bg-gray-800 text-gray-400 hover:text-white hover:bg-gray-700 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
        >
          後一日 →
        </button>

        {/* Separator */}
        <div className="h-6 w-px bg-gray-700" />

        {/* Save today */}
        <button
          onClick={onSaveToday}
          disabled={saving}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm bg-gray-800 text-gray-400 hover:text-white hover:bg-gray-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors border border-gray-700"
        >
          {saving ? (
            <>
              <span className="w-3 h-3 border-2 border-gray-400 border-t-transparent rounded-full animate-spin" />
              儲存中…
            </>
          ) : (
            <>💾 儲存今日快照</>
          )}
        </button>

        {/* Date count */}
        {dates.length > 0 && (
          <span className="text-xs text-gray-600 ml-auto">
            已儲存 {dates.length} 個日期
          </span>
        )}
      </div>

      {/* Date pills (last 7) */}
      {dates.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mt-2.5">
          {dates.slice(0, 14).map((d) => (
            <button
              key={d}
              onClick={() => onSelect(d)}
              className={`text-xs px-2.5 py-1 rounded-full transition-colors ${
                selected === d
                  ? "bg-blue-600 text-white"
                  : "bg-gray-800 text-gray-500 hover:bg-gray-700 hover:text-gray-300"
              }`}
            >
              {d}
            </button>
          ))}
          {dates.length > 14 && (
            <span className="text-xs text-gray-700 px-2 py-1">+{dates.length - 14} 更多</span>
          )}
        </div>
      )}
    </div>
  );
}
