import { Info } from "lucide-react";

interface StatRowProps {
  label: string;
  value: number;
  total: number;
  percentage: number;
  tooltip?: string;
}

export function StatRow({ label, value, total, percentage, tooltip }: StatRowProps) {
  return (
    <div className="flex items-center justify-between py-3 border-b border-gray-100 last:border-0">
      <span className="text-sm text-gray-700 flex items-center gap-1">
        {label}
        {tooltip && (
          <div className="group relative">
            <Info className="w-3.5 h-3.5 text-gray-400 cursor-help" />
            <div className="absolute bottom-full left-0 mb-2 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-200 w-64 z-50 shadow-lg whitespace-normal">
              {tooltip}
              <div className="absolute top-full left-4 border-4 border-transparent border-t-gray-900"></div>
            </div>
          </div>
        )}
      </span>
      <div className="flex items-center gap-4">
        <span className="text-sm text-gray-500">
          {value} / {total}
        </span>
        <span className="text-sm font-semibold text-gray-900 w-16 text-right">
          {percentage.toFixed(1)}%
        </span>
      </div>
    </div>
  );
}
