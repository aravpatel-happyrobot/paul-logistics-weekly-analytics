import { Info } from "lucide-react";

type ColorKey = "indigo" | "emerald" | "amber" | "rose" | "cyan";

interface StatCardProps {
  label: string;
  value: number;
  total: number;
  percentage: number;
  color: ColorKey;
  tooltip?: string;
}

const colorClasses: Record<ColorKey, { bg: string; bar: string; text: string }> = {
  indigo: { bg: "bg-indigo-50", bar: "bg-indigo-500", text: "text-indigo-700" },
  emerald: { bg: "bg-emerald-50", bar: "bg-emerald-500", text: "text-emerald-700" },
  amber: { bg: "bg-amber-50", bar: "bg-amber-500", text: "text-amber-700" },
  rose: { bg: "bg-rose-50", bar: "bg-rose-500", text: "text-rose-700" },
  cyan: { bg: "bg-cyan-50", bar: "bg-cyan-500", text: "text-cyan-700" },
};

export function StatCard({ label, value, total, percentage, color, tooltip }: StatCardProps) {
  const colors = colorClasses[color];

  return (
    <div className={`${colors.bg} rounded-xl p-4 space-y-3`}>
      <div className="flex justify-between items-center">
        <span className="text-sm font-medium text-gray-700 flex items-center gap-1">
          {label}
          {tooltip && (
            <div className="group relative">
              <Info className="w-3.5 h-3.5 text-gray-400 cursor-help" />
              <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-200 w-56 z-50 shadow-lg whitespace-normal">
                {tooltip}
                <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-900"></div>
              </div>
            </div>
          )}
        </span>
        <span className={`text-lg font-bold ${colors.text}`}>
          {percentage.toFixed(1)}%
        </span>
      </div>
      <div className="h-2 bg-white/50 rounded-full overflow-hidden">
        <div
          className={`h-full ${colors.bar} rounded-full transition-all duration-500`}
          style={{ width: `${Math.min(percentage, 100)}%` }}
        />
      </div>
      <p className="text-xs text-gray-500">
        {value} of {total} calls
      </p>
    </div>
  );
}
