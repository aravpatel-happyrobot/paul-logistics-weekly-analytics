// API client for FastAPI backend

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export interface Organization {
  id: number;
  org_id: string;
  name: string;
  node_persistent_id: string;
  timezone: string;
  is_active: boolean;
  created_at: string;
}

export interface ReportSummary {
  id: number;
  report_date: string;
  created_at: string;
  total_calls: number;
  success_rate_percent: number | null;
  non_convertible_percent: number | null;
}

export interface KPIs {
  total_calls: number;
  classified_calls: number;
  total_duration_hours: number;
  avg_minutes_per_call: number;
  success_rate_percent: number | null;
  non_convertible_calls_with_carrier_not_qualified: {
    count: number;
    total_calls: number;
    percentage: number;
  } | null;
  non_convertible_calls_without_carrier_not_qualified: {
    count: number;
    total_calls: number;
    percentage: number;
  } | null;
  carrier_not_qualified: {
    count: number;
    total_calls: number;
    percentage: number;
  } | null;
  carrier_transfer_over_total_transfer_attempts: {
    carrier_asked_count: number;
    total_transfer_attempts: number;
    carrier_asked_percentage: number;
  } | null;
  carrier_transfer_over_total_call_attempts: {
    carrier_asked_count: number;
    total_call_attempts: number;
    carrier_asked_percentage: number;
  } | null;
  successfully_transferred_for_booking: {
    successfully_transferred_for_booking_count: number;
    total_calls: number;
    successfully_transferred_for_booking_percentage: number;
  } | null;
}

export interface BreakdownItem {
  [key: string]: string | number;
}

export interface DailyReport {
  id: number;
  org_id: string;
  report_date: string;
  created_at: string;
  data: {
    date_range: {
      tz: string;
      start_date: string;
      end_date: string;
    };
    kpis: KPIs;
    breakdowns: {
      call_stage: BreakdownItem[];
      call_classification: BreakdownItem[];
      load_status: BreakdownItem[];
      pricing_notes: BreakdownItem[];
      carrier_end_state: BreakdownItem[];
    };
    metadata?: {
      org_id: string;
      org_name: string;
      generated_at: string;
    };
  };
}

export interface SchedulerStatus {
  enabled: boolean;
  running: boolean;
  scheduled_time: string;
  timezone: string;
  next_run: string | null;
  jobs: {
    id: string;
    name: string;
    next_run_time: string | null;
  }[];
}

// API Functions

export async function fetchOrganizations(): Promise<Organization[]> {
  const res = await fetch(`${API_BASE}/api/orgs`);
  if (!res.ok) throw new Error("Failed to fetch organizations");
  const data = await res.json();
  return data.organizations;
}

export async function fetchReports(
  orgId?: string,
  limit?: number
): Promise<{ org_id: string; org_name: string; reports: ReportSummary[] }> {
  const params = new URLSearchParams();
  if (orgId) params.set("org_id", orgId);
  if (limit) params.set("limit", limit.toString());

  const res = await fetch(`${API_BASE}/api/reports?${params}`);
  if (!res.ok) throw new Error("Failed to fetch reports");
  return res.json();
}

export async function fetchReport(
  reportDate: string,
  orgId?: string
): Promise<DailyReport> {
  const params = new URLSearchParams();
  if (orgId) params.set("org_id", orgId);

  const res = await fetch(`${API_BASE}/api/reports/${reportDate}?${params}`);
  if (!res.ok) throw new Error("Failed to fetch report");
  return res.json();
}

export async function fetchLatestReport(orgId?: string): Promise<DailyReport> {
  const params = new URLSearchParams();
  if (orgId) params.set("org_id", orgId);

  const res = await fetch(`${API_BASE}/api/reports/latest?${params}`);
  if (!res.ok) throw new Error("Failed to fetch latest report");
  return res.json();
}

export async function fetchSchedulerStatus(): Promise<SchedulerStatus> {
  const res = await fetch(`${API_BASE}/api/scheduler/status`);
  if (!res.ok) throw new Error("Failed to fetch scheduler status");
  return res.json();
}

interface GenerateReportResult {
  org_id: string;
  org_name: string;
  success: boolean;
}

export async function generateReport(
  orgId?: string,
  date?: string
): Promise<{ success: boolean; results?: GenerateReportResult[] }> {
  const res = await fetch(`${API_BASE}/api/reports/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId, date }),
  });
  if (!res.ok) throw new Error("Failed to generate report");
  return res.json();
}

export async function fetchLiveReport(
  date?: string,
  tz?: string
): Promise<DailyReport["data"]> {
  const params = new URLSearchParams();
  if (date) params.set("date", date);
  if (tz) params.set("tz", tz);

  const res = await fetch(`${API_BASE}/daily-report?${params}`);
  if (!res.ok) throw new Error("Failed to fetch live report");
  return res.json();
}
