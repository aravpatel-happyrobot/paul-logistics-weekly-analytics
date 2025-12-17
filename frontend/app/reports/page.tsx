"use client";

import { useEffect, useState } from "react";
import { fetchReports, generateReport, type ReportSummary } from "@/lib/api";
import { formatPercent, formatNumber, formatDate } from "@/lib/utils";
import { RefreshCw, FileText } from "lucide-react";
import { TableSkeleton, ErrorState } from "@/components";

export default function ReportsPage() {
  const [reports, setReports] = useState<ReportSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadReports = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await fetchReports(undefined, 30);
      setReports(data.reports || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load reports");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadReports();
  }, []);

  const handleGenerateReport = async () => {
    try {
      setGenerating(true);
      await generateReport();
      await loadReports();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to generate report");
    } finally {
      setGenerating(false);
    }
  };

  if (loading) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <TableSkeleton rows={5} />
      </div>
    );
  }

  if (error && reports.length === 0) {
    return <ErrorState title="Error Loading Reports" message={error} onRetry={loadReports} />;
  }

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
        <div>
          <h2 className="text-3xl font-bold text-gray-900">Historical Reports</h2>
          <p className="text-gray-500 mt-1">View and manage daily analytics reports</p>
        </div>
        <button
          onClick={handleGenerateReport}
          disabled={generating}
          className="inline-flex items-center gap-2 px-5 py-2.5 gradient-primary text-white font-medium rounded-xl hover:opacity-90 transition-opacity shadow-lg shadow-indigo-500/30 disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 ${generating ? "animate-spin" : ""}`} />
          {generating ? "Generating..." : "Generate New Report"}
        </button>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4">
          <p className="text-red-600 text-sm">{error}</p>
        </div>
      )}

      {reports.length === 0 ? (
        <div className="bg-white rounded-2xl card-shadow-lg p-12 text-center">
          <div className="w-16 h-16 bg-gray-100 rounded-2xl flex items-center justify-center mx-auto mb-4">
            <FileText className="w-8 h-8 text-gray-400" />
          </div>
          <h3 className="text-xl font-bold text-gray-900 mb-2">No Reports Available</h3>
          <p className="text-gray-500 max-w-md mx-auto">
            No daily reports have been generated yet. Click the button above to generate
            yesterday&apos;s report.
          </p>
        </div>
      ) : (
        <div className="bg-white rounded-2xl card-shadow-lg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-100">
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
                  <th className="px-6 py-4 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Generated
                  </th>
                  <th className="px-6 py-4 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {reports.map((report, idx) => (
                  <tr
                    key={report.id}
                    className={`hover:bg-gray-50 transition-colors ${idx === 0 ? "bg-indigo-50/50" : ""}`}
                  >
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-medium text-gray-900">
                          {formatDate(report.report_date)}
                        </span>
                        {idx === 0 && (
                          <span className="px-2 py-0.5 text-xs font-medium bg-indigo-100 text-indigo-700 rounded-full">
                            Latest
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-6 py-4 text-right">
                      <span className="text-sm font-medium text-gray-900">
                        {formatNumber(report.total_calls)}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-right">
                      <span
                        className={`text-sm font-medium ${
                          (report.success_rate_percent || 0) >= 15
                            ? "text-emerald-600"
                            : "text-amber-600"
                        }`}
                      >
                        {formatPercent(report.success_rate_percent)}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-right">
                      <span className="text-sm text-gray-600">
                        {formatPercent(report.non_convertible_percent)}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-right">
                      <span className="text-sm text-gray-500">
                        {new Date(report.created_at).toLocaleString()}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-right">
                      <a
                        href={`/reports/${report.report_date}`}
                        className="text-sm font-medium text-indigo-600 hover:text-indigo-700"
                      >
                        View Details
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
