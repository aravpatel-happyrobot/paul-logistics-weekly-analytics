"use client";

import { useEffect, useState, useRef } from "react";
import { useParams } from "next/navigation";
import { fetchReport, type DailyReport } from "@/lib/api";
import { formatPercent, formatNumber, formatDate } from "@/lib/utils";
import { generatePDFFromElement } from "@/lib/pdf";
import { Phone, Clock, CheckCircle2, ArrowLeft, PhoneOff, Calendar, Download } from "lucide-react";
import {
  MetricCard,
  BreakdownCard,
  ReportDetailSkeleton,
  ErrorState,
} from "@/components";

// Metric definitions for tooltips
const TOOLTIPS = {
  totalCalls: "Total number of calls received during this period",
  bookingRate: "Percentage of calls that were successfully transferred and resulted in a booking",
  nonConvertible: "Calls that couldn't result in a booking - carrier not qualified, declined load, rate issues, etc.",
  avgDuration: "Average length of each call in minutes",
};

export default function ReportDetailPage() {
  const params = useParams();
  const date = params.date as string;

  const [report, setReport] = useState<DailyReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const reportRef = useRef<HTMLDivElement>(null);

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

  useEffect(() => {
    const loadReport = async () => {
      try {
        setLoading(true);
        setError(null);
        const data = await fetchReport(date);
        setReport(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load report");
      } finally {
        setLoading(false);
      }
    };

    if (date) {
      loadReport();
    }
  }, [date]);

  if (loading) {
    return <ReportDetailSkeleton />;
  }

  if (error) {
    return (
      <ErrorState
        title="Error Loading Report"
        message={error}
        backLink="/reports"
        backLabel="Back to Reports"
      />
    );
  }

  if (!report) {
    return (
      <ErrorState
        title="Report Not Found"
        message={`No report found for ${date}`}
        backLink="/reports"
        backLabel="Back to Reports"
      />
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

  const loadStatusData = breakdowns.load_status
    .map((item) => ({
      name: String(item.load_status || "Unknown").replace(/_/g, " "),
      value: Number(item.count),
    }))
    .sort((a, b) => b.value - a.value);

  const callStageData = breakdowns.call_stage
    .map((item) => ({
      name: String(item.call_stage || "Unknown").replace(/_/g, " "),
      value: Number(item.count),
    }))
    .sort((a, b) => b.value - a.value);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-8">
      {/* Header */}
      <div className="flex flex-col sm:flex-row justify-between items-start gap-4">
        <div className="flex items-center gap-4">
          <a
            href="/reports"
            className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <ArrowLeft className="w-5 h-5 text-gray-500" />
          </a>
          <div>
            <h2 className="text-3xl font-bold text-gray-900">Daily Report</h2>
            <p className="text-gray-500 mt-1 flex items-center gap-2">
              <Calendar className="w-4 h-4" />
              {formatDate(report.report_date)}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <div className="text-sm text-gray-500">
            Generated: {new Date(report.created_at).toLocaleString()}
          </div>
          <button
            onClick={handleDownloadPDF}
            disabled={downloading}
            className="inline-flex items-center gap-2 px-5 py-2.5 gradient-primary text-white font-medium rounded-xl hover:opacity-90 transition-all shadow-lg shadow-indigo-500/30 disabled:opacity-50"
          >
            <Download className={`w-4 h-4 ${downloading ? "animate-pulse" : ""}`} />
            {downloading ? "Generating..." : "Download PDF"}
          </button>
        </div>
      </div>

      {/* Report Content - wrapped for PDF capture */}
      <div ref={reportRef} className="space-y-6 bg-white p-6 rounded-2xl">
        {/* Primary KPIs */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
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

      {/* Breakdown Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <BreakdownCard title="Call Classification" data={classificationData} color="indigo" />
        <BreakdownCard title="Carrier End State" data={carrierEndStateData} color="emerald" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <BreakdownCard title="Load Status" data={loadStatusData} color="amber" />
        <BreakdownCard title="Call Stage" data={callStageData} color="cyan" />
      </div>

      {/* Date Range Info */}
      <div className="bg-white rounded-2xl card-shadow-lg p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Date Range</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
          <div className="bg-gray-50 rounded-xl p-4">
            <p className="text-gray-500 mb-1">Timezone</p>
            <p className="font-medium text-gray-900">{report.data.date_range.tz}</p>
          </div>
          <div className="bg-gray-50 rounded-xl p-4">
            <p className="text-gray-500 mb-1">Start</p>
            <p className="font-medium text-gray-900">
              {new Date(report.data.date_range.start_date).toLocaleString()}
            </p>
          </div>
          <div className="bg-gray-50 rounded-xl p-4">
            <p className="text-gray-500 mb-1">End</p>
            <p className="font-medium text-gray-900">
              {new Date(report.data.date_range.end_date).toLocaleString()}
            </p>
          </div>
        </div>
      </div>
      </div> {/* End of reportRef container */}
    </div>
  );
}
