"use client";

import { useEffect, useState, useRef } from "react";
import { AreaChart, DonutChart, BarList } from "@tremor/react";
import {
  fetchLatestReport,
  fetchReports,
  fetchSchedulerStatus,
  type DailyReport,
  type ReportSummary,
  type SchedulerStatus,
} from "@/lib/api";
import { formatPercent, formatNumber, formatDate } from "@/lib/utils";
import { generatePDFFromElement } from "@/lib/pdf";

// Custom tooltip for donut charts
interface TooltipProps {
  payload: { name: string; value: number; color: string }[];
  active: boolean;
}

function ChartTooltip({ payload, active }: TooltipProps) {
  if (!active || !payload || payload.length === 0) return null;
  const item = payload[0];
  return (
    <div className="bg-white border border-gray-200 rounded-lg shadow-lg p-3 min-w-[180px] z-50">
      <p className="text-sm font-medium text-gray-900 mb-1">{item.name}</p>
      <p className="text-lg font-bold" style={{ color: item.color }}>
        {formatNumber(item.value)} calls
      </p>
    </div>
  );
}
import {
  Phone,
  Clock,
  CheckCircle2,
  Calendar,
  PhoneOff,
  Download,
  ArrowRight,
} from "lucide-react";

// Metric definitions for tooltips
const TOOLTIPS = {
  totalCalls: "Total number of calls received during this period",
  bookingRate: "Percentage of calls that were successfully transferred and resulted in a booking",
  nonConvertible: "Calls that couldn't result in a booking - carrier not qualified, declined load, rate issues, etc.",
  avgDuration: "Average length of each call in minutes",
};
import {
  MetricCard,
  DashboardSkeleton,
  ErrorState,
} from "@/components";

export default function Dashboard() {
  const [report, setReport] = useState<DailyReport | null>(null);
  const [recentReports, setRecentReports] = useState<ReportSummary[]>([]);
  const [schedulerStatus, setSchedulerStatus] = useState<SchedulerStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const reportRef = useRef<HTMLDivElement>(null);

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);

      const [latestReport, reportsData, scheduler] = await Promise.all([
        fetchLatestReport().catch(() => null),
        fetchReports(undefined, 7).catch(() => ({ reports: [] })),
        fetchSchedulerStatus().catch(() => null),
      ]);

      setReport(latestReport);
      setRecentReports(reportsData.reports || []);
      setSchedulerStatus(scheduler);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load data");
    } finally {
      setLoading(false);
    }
  };

  const formatNextRun = (isoString: string | null): string => {
    if (!isoString) return "Not scheduled";
    const date = new Date(isoString);
    return date.toLocaleString("en-US", {
      weekday: "short",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      timeZoneName: "short",
    });
  };

  useEffect(() => {
    loadData();
  }, []);

  const handleDownloadPDF = async () => {
    if (!reportRef.current || !report) return;

    try {
      setDownloading(true);
      await generatePDFFromElement(reportRef.current, {
        filename: `paul-logistics-report-${report.report_date}.pdf`,
        title: "Paul Logistics Daily Report",
        subtitle: `${formatDate(report.report_date)} • ${report.data.kpis.total_calls} total calls`,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to generate PDF");
    } finally {
      setDownloading(false);
    }
  };

  if (loading) {
    return <DashboardSkeleton />;
  }

  if (error && !report) {
    return (
      <ErrorState
        title="Error Loading Dashboard"
        message={error}
        onRetry={loadData}
      />
    );
  }

  if (!report) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="bg-white rounded-2xl card-shadow-lg p-12 text-center">
          <div className="w-20 h-20 gradient-primary rounded-2xl flex items-center justify-center mx-auto mb-6 shadow-lg shadow-indigo-500/30">
            <Calendar className="w-10 h-10 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-gray-900 mb-2">No Reports Yet</h2>
          <p className="text-gray-500 max-w-md mx-auto">
            Reports are generated automatically every day at 6:00 AM Pacific Time.
            Check back tomorrow to see yesterday&apos;s analytics.
          </p>
        </div>
      </div>
    );
  }

  const { kpis, breakdowns } = report.data;

  // Prepare chart data
  const classificationData = breakdowns.call_classification
    .map((item) => ({
      name: String(item.call_classification || "Unknown").replace(/_/g, " "),
      value: Number(item.count),
    }))
    .sort((a, b) => b.value - a.value);

  const carrierEndStateData = breakdowns.carrier_end_state
    .map((item) => ({
      name: String(item.carrier_end_state || "Unknown").replace(/_/g, " "),
      value: Number(item.count),
    }))
    .sort((a, b) => b.value - a.value);

  // Trend data for area chart
  const trendData = [...recentReports].reverse().map((r) => ({
    date: new Date(r.report_date).toLocaleDateString("en-US", {
      weekday: "short",
      month: "short",
      day: "numeric",
    }),
    "Total Calls": r.total_calls,
  }));

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-8">
      {/* Header */}
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
        <div>
          <h2 className="text-3xl font-bold text-gray-900">Daily Overview</h2>
          <p className="text-gray-500 mt-1 flex items-center gap-2">
            <Calendar className="w-4 h-4" />
            {formatDate(report.report_date)}
          </p>
          {schedulerStatus?.next_run && (
            <p className="text-sm text-gray-400 mt-1 flex items-center gap-2">
              <Clock className="w-3 h-3" />
              Next update: {formatNextRun(schedulerStatus.next_run)}
            </p>
          )}
        </div>
        <button
          onClick={handleDownloadPDF}
          disabled={downloading}
          className="inline-flex items-center gap-2 px-5 py-2.5 gradient-primary text-white font-medium rounded-xl hover:opacity-90 transition-all shadow-lg shadow-indigo-500/30 disabled:opacity-50"
        >
          <Download className={`w-4 h-4 ${downloading ? "animate-pulse" : ""}`} />
          {downloading ? "Generating..." : "Download Report"}
        </button>
      </div>

      {/* Report Content - wrapped for PDF capture */}
      <div ref={reportRef} className="space-y-6 bg-white p-6 rounded-2xl">
        {/* Primary KPI Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard
          title="Total Calls"
          value={formatNumber(kpis.total_calls)}
          subtitle={`${formatNumber(kpis.classified_calls)} classified`}
          icon={Phone}
          gradient="gradient-primary"
          tooltip={TOOLTIPS.totalCalls}
        />
        <MetricCard
          title="Booking Rate"
          value={formatPercent(kpis.successfully_transferred_for_booking?.successfully_transferred_for_booking_percentage)}
          subtitle={`${kpis.successfully_transferred_for_booking?.successfully_transferred_for_booking_count || 0} loads booked`}
          icon={CheckCircle2}
          gradient="gradient-success"
          tooltip={TOOLTIPS.bookingRate}
        />
        <MetricCard
          title="Non-Convertible"
          value={formatPercent(kpis.non_convertible_calls_with_carrier_not_qualified?.percentage)}
          subtitle={`${kpis.non_convertible_calls_with_carrier_not_qualified?.count || 0} calls`}
          icon={PhoneOff}
          gradient="gradient-warning"
          tooltip={TOOLTIPS.nonConvertible}
        />
        <MetricCard
          title="Avg Duration"
          value={`${kpis.avg_minutes_per_call?.toFixed(1) || 0} min`}
          subtitle={`${kpis.total_duration_hours?.toFixed(1) || 0} hrs total`}
          icon={Clock}
          gradient="gradient-info"
          tooltip={TOOLTIPS.avgDuration}
        />
      </div>

      {/* Charts Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-2xl card-shadow-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-6">Call Classification</h3>
          <div className="flex items-center gap-8">
            <DonutChart
              className="h-44 w-44"
              data={classificationData}
              category="value"
              index="name"
              colors={["indigo", "amber", "emerald", "rose", "cyan", "violet", "slate"]}
              showLabel={false}
              showAnimation={true}
              customTooltip={ChartTooltip}
            />
            <div className="flex-1 space-y-2">
              <BarList
                data={classificationData.slice(0, 5)}
                valueFormatter={(v: number) => formatNumber(v)}
                color="indigo"
              />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-2xl card-shadow-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-6">Carrier End State</h3>
          <div className="flex items-center gap-8">
            <DonutChart
              className="h-44 w-44"
              data={carrierEndStateData}
              category="value"
              index="name"
              colors={["emerald", "indigo", "amber", "rose", "cyan", "violet"]}
              showLabel={false}
              showAnimation={true}
              customTooltip={ChartTooltip}
            />
            <div className="flex-1 space-y-2">
              <BarList
                data={carrierEndStateData.slice(0, 5)}
                valueFormatter={(v: number) => formatNumber(v)}
                color="emerald"
              />
            </div>
          </div>
        </div>
      </div>

      {/* Trend Chart */}
      {trendData.length > 1 && (
        <div className="bg-white rounded-2xl card-shadow-lg p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-6">7-Day Call Volume</h3>
          <AreaChart
            className="h-72"
            data={trendData}
            index="date"
            categories={["Total Calls"]}
            colors={["indigo"]}
            valueFormatter={(v: number) => formatNumber(v)}
            showAnimation={true}
            showLegend={false}
            curveType="monotone"
          />
        </div>
      )}

      {/* Recent Reports Table */}
      <div className="bg-white rounded-2xl card-shadow-lg overflow-hidden">
        <div className="p-6 border-b border-gray-100">
          <div className="flex justify-between items-center">
            <h3 className="text-lg font-semibold text-gray-900">Recent Reports</h3>
            <a
              href="/reports"
              className="text-sm font-medium text-indigo-600 hover:text-indigo-700 flex items-center gap-1"
            >
              View All
              <ArrowRight className="w-4 h-4" />
            </a>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="bg-gray-50">
                <th className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Date
                </th>
                <th className="px-6 py-4 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Total Calls
                </th>
                <th className="px-6 py-4 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Success Rate
                </th>
                <th className="px-6 py-4 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Non-Convertible
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {recentReports.slice(0, 5).map((r, idx) => (
                <tr key={r.id} className="hover:bg-gray-50 transition-colors">
                  <td className="px-6 py-4">
                    <div className="flex items-center gap-3">
                      {idx === 0 && (
                        <span className="flex h-2 w-2">
                          <span className="animate-ping absolute inline-flex h-2 w-2 rounded-full bg-indigo-400 opacity-75" />
                          <span className="relative inline-flex rounded-full h-2 w-2 bg-indigo-500" />
                        </span>
                      )}
                      <a
                        href={`/reports/${r.report_date}`}
                        className="text-sm font-medium text-gray-900 hover:text-indigo-600"
                      >
                        {formatDate(r.report_date)}
                      </a>
                      {idx === 0 && (
                        <span className="px-2 py-0.5 text-xs font-medium bg-indigo-100 text-indigo-700 rounded-full">
                          Latest
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span className="text-sm font-medium text-gray-900">
                      {formatNumber(r.total_calls)}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span
                      className={`text-sm font-medium ${
                        (r.success_rate_percent || 0) >= 15 ? "text-emerald-600" : "text-amber-600"
                      }`}
                    >
                      {formatPercent(r.success_rate_percent)}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-right">
                    <span className="text-sm text-gray-600">
                      {formatPercent(r.non_convertible_percent)}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      </div> {/* End of reportRef container */}
    </div>
  );
}
