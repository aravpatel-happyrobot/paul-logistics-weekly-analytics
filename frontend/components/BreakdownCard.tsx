import { DonutChart } from "@tremor/react";
import { formatNumber } from "@/lib/utils";

type ChartColor = "indigo" | "emerald" | "amber" | "rose" | "cyan" | "violet" | "slate";

interface ChartDataItem {
  name: string;
  value: number;
}

interface BreakdownCardProps {
  title: string;
  data: ChartDataItem[];
  color: ChartColor;
}

const colorClasses: Record<ChartColor, string> = {
  indigo: "bg-indigo-500",
  emerald: "bg-emerald-500",
  amber: "bg-amber-500",
  rose: "bg-rose-500",
  cyan: "bg-cyan-500",
  violet: "bg-violet-500",
  slate: "bg-slate-500",
};

const chartColors: Record<ChartColor, string[]> = {
  indigo: ["indigo", "blue", "violet", "purple", "slate"],
  emerald: ["emerald", "green", "teal", "cyan", "slate"],
  amber: ["amber", "yellow", "orange", "red", "slate"],
  rose: ["rose", "pink", "red", "orange", "slate"],
  cyan: ["cyan", "teal", "blue", "indigo", "slate"],
  violet: ["violet", "purple", "indigo", "blue", "slate"],
  slate: ["slate", "gray", "zinc", "neutral", "stone"],
};

export function BreakdownCard({ title, data, color }: BreakdownCardProps) {
  const total = data.reduce((sum, item) => sum + item.value, 0);

  return (
    <div className="bg-white rounded-2xl card-shadow-lg p-6">
      <h3 className="text-lg font-semibold text-gray-900 mb-4">{title}</h3>
      <div className="flex flex-col items-center gap-4">
        <DonutChart
          className="h-36 w-36"
          data={data}
          category="value"
          index="name"
          colors={chartColors[color]}
          showLabel={false}
          showAnimation={true}
        />
        <div className="w-full space-y-3">
          {data.slice(0, 5).map((item, idx) => {
            const percent = total > 0 ? ((item.value / total) * 100).toFixed(1) : "0";
            return (
              <div key={item.name} className="flex items-start gap-2 text-xs">
                <div className={`w-2.5 h-2.5 rounded-sm flex-shrink-0 mt-0.5 ${idx === 0 ? colorClasses[color] : "bg-gray-300"}`} />
                <div className="flex-1 min-w-0">
                  <div className="text-gray-700 leading-tight break-words">
                    {item.name}
                  </div>
                  <div className="flex items-center gap-2 mt-0.5 text-gray-500">
                    <span>{percent}%</span>
                    <span className="font-medium text-gray-900">{formatNumber(item.value)}</span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
