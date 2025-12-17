import { TrendingUp, TrendingDown, Info, type LucideIcon } from "lucide-react";

interface MetricCardProps {
  title: string;
  value: string;
  subtitle?: string;
  icon: LucideIcon;
  trend?: "up" | "down" | "neutral";
  trendValue?: string;
  gradient: string;
  tooltip?: string;
}

export function MetricCard({
  title,
  value,
  subtitle,
  icon: Icon,
  trend,
  trendValue,
  gradient,
  tooltip,
}: MetricCardProps) {
  return (
    <div className="relative bg-white rounded-2xl card-shadow-lg card-shadow-hover p-6">
      <div className="flex justify-between items-start">
        <div className="space-y-3">
          <div className="flex items-center gap-1">
            <p className="text-sm font-medium text-gray-500">{title}</p>
            {tooltip && (
              <div className="group relative">
                <Info className="w-3.5 h-3.5 text-gray-400 cursor-help" />
                <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-gray-900 text-white text-xs rounded-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-200 w-56 z-50 shadow-lg whitespace-normal">
                  {tooltip}
                  <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-900"></div>
                </div>
              </div>
            )}
          </div>
          <p className="text-3xl font-bold text-gray-900">{value}</p>
          {subtitle && <p className="text-sm text-gray-500">{subtitle}</p>}
          {trend && trendValue && (
            <div
              className={`inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium ${
                trend === "up"
                  ? "bg-green-100 text-green-700"
                  : trend === "down"
                  ? "bg-red-100 text-red-700"
                  : "bg-gray-100 text-gray-700"
              }`}
            >
              {trend === "up" ? (
                <TrendingUp className="w-3 h-3" />
              ) : trend === "down" ? (
                <TrendingDown className="w-3 h-3" />
              ) : null}
              {trendValue}
            </div>
          )}
        </div>
        <div className={`p-3 rounded-xl ${gradient} shadow-lg`}>
          <Icon className="w-6 h-6 text-white" />
        </div>
      </div>
      <div
        className={`absolute -right-8 -bottom-8 w-32 h-32 ${gradient} opacity-10 rounded-full blur-2xl`}
      />
    </div>
  );
}
