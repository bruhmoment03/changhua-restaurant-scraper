"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { Badge } from "@/components/Badge";
import { Card } from "@/components/Card";
import {
  ApproveDiscoveryCandidatesResponse,
  DataHealthSummary,
  DbStats,
  DiscoveryCandidate,
  Job,
  LogTailEntry,
  PlaceValidationResult,
  ProgressReport,
  approveDiscoveryCandidates,
  getDataHealthSummary,
  getDbStats,
  getDiscoveryCandidates,
  getJobs,
  getLogTail,
  getProgress,
  rebuildPlaceTotals,
  rejectDiscoveryCandidates,
  scrapeAllWithSettings,
  scrapeTargets,
  searchDiscoveryCandidates,
  validatePlaces,
} from "@/lib/api";

const CONFIG_PATH = "batch/config.top50.yaml";
const DEFAULT_MIN_REVIEWS = 100;
const THRESHOLD_OPTIONS = [100, 150, 200, 250, 300];
const MAX_REVIEW_OPTIONS = [100, 150, 200, 250, 300, 400, 500];
const DISCOVERY_LIMIT_OPTIONS = [50, 100, 150, 200];
const DISCOVERY_MIN_RATING_OPTIONS = [0, 3.5, 4.0, 4.2, 4.5];
const DISCOVERY_MIN_TOTAL_OPTIONS = [0, 50, 100, 200, 500];
const DISCOVERY_RADIUS_OPTIONS = [0, 1000, 3000, 5000, 10000, 20000];

function fmtTs(value?: string | null): string {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

function StatCard({ label, value, sub, tone }: { label: string; value: string | number; sub?: string; tone?: "good" | "warn" | "bad" }) {
  const borderColor = tone === "good" ? "border-green-500/40" : tone === "warn" ? "border-amber-500/40" : tone === "bad" ? "border-red-500/40" : "border-border/60";
  return (
    <div className={`rounded-2xl border ${borderColor} bg-panel/80 p-5 shadow-card backdrop-blur-sm`}>
      <div className="text-3xl font-bold tracking-tight text-text">{value}</div>
      <div className="mt-1 text-sm font-medium text-muted">{label}</div>
      {sub ? <div className="mt-2 text-xs text-muted">{sub}</div> : null}
    </div>
  );
}

function statusTone(status: string): "default" | "good" | "warn" | "bad" {
  if (status === "valid" || status === "approved" || status === "with_reviews") return "good";
  if (
    status === "invalid_not_found" ||
    status === "invalid_closed" ||
    status === "invalid_mismatch" ||
    status === "failed"
  ) {
    return "bad";
  }
  if (
    status === "staged" ||
    status === "duplicate_db" ||
    status === "present_zero_reviews" ||
    status === "exhausted_under_threshold"
  ) {
    return "warn";
  }
  return "default";
}

function jobSummary(jobs: Job[]) {
  const by = { pending: 0, running: 0, completed: 0, failed: 0, cancelled: 0 };
  for (const job of jobs) {
    if (job.status in by) {
      by[job.status as keyof typeof by] += 1;
    }
  }
  return by;
}

export default function HomePage() {
  const [stats, setStats] = useState<DbStats | null>(null);
  const [progress, setProgress] = useState<ProgressReport | null>(null);
  const [health, setHealth] = useState<DataHealthSummary | null>(null);
  const [errors, setErrors] = useState<LogTailEntry[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [candidates, setCandidates] = useState<DiscoveryCandidate[]>([]);
  const [validationResults, setValidationResults] = useState<PlaceValidationResult[]>([]);

  const [loading, setLoading] = useState(true);
  const [actionBusy, setActionBusy] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [pageError, setPageError] = useState("");

  const [minReviews, setMinReviews] = useState(DEFAULT_MIN_REVIEWS);
  const [defaultMaxReviews, setDefaultMaxReviews] = useState(300);
  const [onlyBelowThreshold, setOnlyBelowThreshold] = useState(true);

  const [searchQuery, setSearchQuery] = useState("restaurants in Changhua City");
  const [searchLimit, setSearchLimit] = useState(50);
  const [minRating, setMinRating] = useState(0);
  const [minRatingsTotal, setMinRatingsTotal] = useState(100);
  const [location, setLocation] = useState("");
  const [radiusM, setRadiusM] = useState(0);

  const [selectedCandidateIds, setSelectedCandidateIds] = useState<number[]>([]);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem("reviews_ops_settings");
      if (!raw) return;
      const parsed = JSON.parse(raw) as {
        min_reviews?: number;
        default_max_reviews?: number;
        only_below_threshold?: boolean;
      };
      if (typeof parsed.min_reviews === "number" && THRESHOLD_OPTIONS.includes(parsed.min_reviews)) {
        setMinReviews(parsed.min_reviews);
      }
      if (
        typeof parsed.default_max_reviews === "number" &&
        MAX_REVIEW_OPTIONS.includes(parsed.default_max_reviews)
      ) {
        setDefaultMaxReviews(parsed.default_max_reviews);
      }
      if (typeof parsed.only_below_threshold === "boolean") {
        setOnlyBelowThreshold(parsed.only_below_threshold);
      }
    } catch {
      // Ignore invalid local storage payloads.
    }
  }, []);

  useEffect(() => {
    window.localStorage.setItem(
      "reviews_ops_settings",
      JSON.stringify({
        min_reviews: minReviews,
        default_max_reviews: defaultMaxReviews,
        only_below_threshold: onlyBelowThreshold,
      })
    );
  }, [defaultMaxReviews, minReviews, onlyBelowThreshold]);

  const loadDashboard = useCallback(async () => {
    setLoading(true);
    setPageError("");
    try {
      const [statsRes, progressRes, healthRes, errorsRes, jobsRes, candidatesRes] = await Promise.all([
        getDbStats(),
        getProgress(CONFIG_PATH, minReviews),
        getDataHealthSummary(CONFIG_PATH, minReviews),
        getLogTail("ERROR", 20),
        getJobs(100),
        getDiscoveryCandidates({ configPath: CONFIG_PATH, limit: 40 }),
      ]);
      setStats(statsRes);
      setProgress(progressRes);
      setHealth(healthRes);
      setErrors(errorsRes);
      setJobs(jobsRes);
      setCandidates(candidatesRes);
    } catch (err) {
      setPageError(err instanceof Error ? err.message : "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  }, [minReviews]);

  useEffect(() => {
    void loadDashboard();
  }, [loadDashboard]);

  useEffect(() => {
    let pollInFlight = false;
    const timer = setInterval(() => {
      if (document.visibilityState !== "visible" || pollInFlight) return;
      pollInFlight = true;
      void Promise.all([getJobs(100), getProgress(CONFIG_PATH, minReviews), getDataHealthSummary(CONFIG_PATH, minReviews)])
        .then(([jobsRes, progressRes, healthRes]) => {
          setJobs(jobsRes);
          setProgress(progressRes);
          setHealth(healthRes);
        })
        .catch(() => {
          // Keep current state on poll failure.
        })
        .finally(() => {
          pollInFlight = false;
        });
    }, 5000);
    return () => clearInterval(timer);
  }, [minReviews]);

  const selectedCandidates = useMemo(
    () => candidates.filter((candidate) => selectedCandidateIds.includes(candidate.candidate_id)),
    [candidates, selectedCandidateIds]
  );
  const allCandidateIds = useMemo(() => candidates.map((candidate) => candidate.candidate_id), [candidates]);
  const allCandidatesSelected = useMemo(
    () => candidates.length > 0 && selectedCandidateIds.length === candidates.length,
    [candidates.length, selectedCandidateIds.length]
  );
  const summary = useMemo(() => jobSummary(jobs), [jobs]);
  const incompleteTargets = useMemo(
    () => (progress?.targets || []).filter((target) => !target.meets_min_reviews && target.status !== "exhausted_under_threshold"),
    [progress]
  );
  const exhaustedTargets = useMemo(
    () => (progress?.targets || []).filter((target) => target.status === "exhausted_under_threshold"),
    [progress]
  );
  const queuedCandidateGooglePlaceIds = useMemo(
    () =>
      selectedCandidates
        .filter((candidate) => candidate.status === "approved" || candidate.status === "duplicate_config")
        .map((candidate) => candidate.google_place_id),
    [selectedCandidates]
  );

  useEffect(() => {
    setSelectedCandidateIds((prev) => {
      if (candidates.length === 0) return [];
      const candidateIds = candidates.map((candidate) => candidate.candidate_id);
      const prevSet = new Set(prev);
      const stillSelected = candidateIds.filter((candidateId) => prevSet.has(candidateId));
      if (stillSelected.length > 0) {
        return stillSelected;
      }
      return candidateIds;
    });
  }, [candidates]);

  const toggleCandidate = useCallback((candidateId: number) => {
    setSelectedCandidateIds((prev) =>
      prev.includes(candidateId) ? prev.filter((value) => value !== candidateId) : [...prev, candidateId]
    );
  }, []);

  const selectAllCandidates = useCallback(() => {
    setSelectedCandidateIds(allCandidateIds);
  }, [allCandidateIds]);

  const clearCandidateSelection = useCallback(() => {
    setSelectedCandidateIds([]);
  }, []);

  const refreshAfterMutation = useCallback(async () => {
    await loadDashboard();
  }, [loadDashboard]);

  const onQueueBelowThreshold = useCallback(async () => {
    setActionBusy("queue-below-threshold");
    setActionMessage("");
    try {
      const res = await scrapeAllWithSettings({
        configPath: CONFIG_PATH,
        minReviews,
        defaultMaxReviews,
        onlyBelowThreshold,
      });
      setActionMessage(`Queued ${res.created_count} RPA job(s) for active targets.`);
      await refreshAfterMutation();
    } catch (err) {
      setActionMessage(err instanceof Error ? err.message : "Failed to queue RPA jobs");
    } finally {
      setActionBusy("");
    }
  }, [defaultMaxReviews, minReviews, onlyBelowThreshold, refreshAfterMutation]);

  const onValidateActivePlaces = useCallback(async () => {
    setActionBusy("validate-places");
    setActionMessage("");
    try {
      const res = await validatePlaces({ configPath: CONFIG_PATH });
      setValidationResults(res.results);
      setActionMessage(
        `Validated ${res.validated_count} target(s): ${res.valid_count} valid, ${res.invalid_count} invalid, ${res.error_count} errors.`
      );
      await refreshAfterMutation();
    } catch (err) {
      setActionMessage(err instanceof Error ? err.message : "Failed to validate active places");
    } finally {
      setActionBusy("");
    }
  }, [refreshAfterMutation]);

  const onSearchDiscovery = useCallback(async () => {
    setActionBusy("search-discovery");
    setActionMessage("");
    try {
      const res = await searchDiscoveryCandidates({
        configPath: CONFIG_PATH,
        query: searchQuery,
        limit: searchLimit,
        minRating,
        minRatingsTotal,
        location: location.trim() || null,
        radiusM: radiusM > 0 ? radiusM : null,
      });
      setCandidates(res.candidates);
      setSelectedCandidateIds([]);
      setActionMessage(`Stored ${res.candidate_count} candidate(s); ${res.staged_count} are ready for approval.`);
      await refreshAfterMutation();
    } catch (err) {
      setActionMessage(err instanceof Error ? err.message : "Failed to search discovery candidates");
    } finally {
      setActionBusy("");
    }
  }, [location, minRating, minRatingsTotal, radiusM, refreshAfterMutation, searchLimit, searchQuery]);

  const onApproveSelected = useCallback(async () => {
    if (selectedCandidateIds.length === 0) return;
    setActionBusy("approve-candidates");
    setActionMessage("");
    try {
      const res: ApproveDiscoveryCandidatesResponse = await approveDiscoveryCandidates({
        configPath: CONFIG_PATH,
        candidateIds: selectedCandidateIds,
      });
      setSelectedCandidateIds(
        res.candidates
          .filter((candidate) => candidate.status === "approved" || candidate.status === "duplicate_config")
          .map((candidate) => candidate.candidate_id)
      );
      setActionMessage(
        `Approved ${res.approved_count} candidate(s); ${res.skipped_count} were already present in config.`
      );
      await refreshAfterMutation();
    } catch (err) {
      setActionMessage(err instanceof Error ? err.message : "Failed to approve candidates");
    } finally {
      setActionBusy("");
    }
  }, [refreshAfterMutation, selectedCandidateIds]);

  const onRejectSelected = useCallback(async () => {
    if (selectedCandidateIds.length === 0) return;
    setActionBusy("reject-candidates");
    setActionMessage("");
    try {
      const res = await rejectDiscoveryCandidates({
        configPath: CONFIG_PATH,
        candidateIds: selectedCandidateIds,
      });
      setSelectedCandidateIds([]);
      setActionMessage(`Rejected ${res.updated_count} candidate(s).`);
      await refreshAfterMutation();
    } catch (err) {
      setActionMessage(err instanceof Error ? err.message : "Failed to reject candidates");
    } finally {
      setActionBusy("");
    }
  }, [refreshAfterMutation, selectedCandidateIds]);

  const onQueueSelectedCandidates = useCallback(async () => {
    if (queuedCandidateGooglePlaceIds.length === 0) {
      setActionMessage("Select approved or duplicate-config candidates before queueing RPA jobs.");
      return;
    }
    setActionBusy("queue-selected-candidates");
    setActionMessage("");
    try {
      const res = await scrapeTargets({
        configPath: CONFIG_PATH,
        googlePlaceIds: queuedCandidateGooglePlaceIds,
        maxReviews: defaultMaxReviews,
      });
      setActionMessage(`Queued ${res.created_count} RPA job(s) from selected discovery candidates.`);
      await refreshAfterMutation();
    } catch (err) {
      setActionMessage(err instanceof Error ? err.message : "Failed to queue selected candidates");
    } finally {
      setActionBusy("");
    }
  }, [defaultMaxReviews, queuedCandidateGooglePlaceIds, refreshAfterMutation]);

  const onRebuildTotals = useCallback(async () => {
    setActionBusy("rebuild-totals");
    setActionMessage("");
    try {
      const res = await rebuildPlaceTotals();
      setActionMessage(`Rebuilt cached totals for ${res.updated_count} place(s).`);
      await refreshAfterMutation();
    } catch (err) {
      setActionMessage(err instanceof Error ? err.message : "Failed to rebuild cached totals");
    } finally {
      setActionBusy("");
    }
  }, [refreshAfterMutation]);

  return (
    <div className="min-w-0 flex flex-col gap-8">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-text">Review Scraper Dashboard</h1>
          <p className="mt-1.5 text-sm text-muted">
            Discover restaurants, scrape reviews, and monitor progress.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            onClick={() => void loadDashboard()}
            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text"
          >
            Refresh
          </button>
          <button
            onClick={() => void onValidateActivePlaces()}
            disabled={actionBusy.length > 0 || !health?.google_places_api_configured}
            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
          >
            {actionBusy === "validate-places" ? "Validating..." : "Validate Active Places"}
          </button>
          <button
            onClick={() => void onQueueBelowThreshold()}
            disabled={actionBusy.length > 0}
            className="rounded-lg border border-accent/50 bg-accent/10 px-3 py-1.5 text-xs font-semibold text-accent disabled:opacity-50"
          >
            {actionBusy === "queue-below-threshold" ? "Queueing..." : "Queue RPA Scrape"}
          </button>
        </div>
      </div>

      {pageError ? <div className="rounded-lg border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-300 break-words">{pageError}</div> : null}
      {actionMessage ? <div className="rounded-lg border border-border/60 bg-bg/40 p-3 text-sm text-text break-words">{actionMessage}</div> : null}
      {!health?.google_places_api_configured ? (
        <div className="rounded-lg border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-200 break-words">
          Google Places API credentials are not configured. Discovery search and validation are disabled until
          `GOOGLE_PLACES_API_KEY` or `GOOGLE_MAPS_API_KEY` is available.
        </div>
      ) : null}

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-6">
        <StatCard label="In Config" value={health?.active_config_targets ?? (loading ? "..." : "-")} sub="Restaurants tracked for scraping" />
        <StatCard label="In Database" value={health?.db_places_count ?? (loading ? "..." : "-")} sub="Have at least some reviews scraped" />
        <StatCard label={`Goal Met (${minReviews}+)`} value={progress?.meeting_min_reviews ?? (loading ? "..." : "-")} sub="Ready for export & analysis" tone="good" />
        <StatCard label="Still Scraping" value={progress?.under_min_reviews ?? (loading ? "..." : "-")} sub={`Have < ${minReviews} reviews so far`} tone="warn" />
        <StatCard
          label="Exhausted"
          value={health?.exhausted_under_threshold_count ?? (loading ? "..." : "-")}
          sub="Google has no more reviews"
          tone="bad"
        />
        <StatCard label="Awaiting Approval" value={health?.staged_candidate_count ?? (loading ? "..." : "-")} sub="New discoveries to review" />
      </div>

      <Card title="Scrape Settings">
        <div className="mb-3 text-sm text-muted">
          Each restaurant needs at least <strong className="text-text/80">{minReviews}</strong> reviews to be considered complete.
          The scraper collects up to <strong className="text-text/80">{defaultMaxReviews}</strong> reviews per run — if a restaurant has more,
          it may take multiple runs. Discovery filters restaurants with fewer than {minReviews} total Google ratings automatically.
        </div>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
          <label className="flex flex-col gap-1">
            <span className="text-xs uppercase tracking-wide text-muted">Min Reviews Goal</span>
            <select
              value={minReviews}
              onChange={(event) => setMinReviews(Number(event.target.value))}
              className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
            >
              {THRESHOLD_OPTIONS.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-1">
            <span className="text-xs uppercase tracking-wide text-muted">Reviews Per Run</span>
            <select
              value={defaultMaxReviews}
              onChange={(event) => setDefaultMaxReviews(Number(event.target.value))}
              className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
            >
              {MAX_REVIEW_OPTIONS.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-2 rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text">
            <input
              type="checkbox"
              checked={onlyBelowThreshold}
              onChange={(event) => setOnlyBelowThreshold(event.target.checked)}
            />
            Only queue restaurants below goal
          </label>
        </div>
      </Card>

      <div className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1.35fr)_minmax(0,1fr)]">
        <Card
          title="Discovery Staging"
          right={<Badge tone={health?.google_places_api_configured ? "good" : "warn"}>{candidates.length} rows</Badge>}
          className="min-w-0"
        >
          <div className="mb-3 space-y-1 text-sm text-muted">
            <p>
              Search Google Places API to discover new restaurants. Duplicates are automatically
              filtered — restaurants already in your config or database won&apos;t be added twice.
            </p>
            <p>
              <strong className="text-text/80">Workflow:</strong> Search → Review candidates → Approve → Queue RPA to scrape reviews.
            </p>
            <p>
              <strong className="text-text/80">Tip:</strong> To find more restaurants, try different queries
              (e.g. &quot;cafes in Changhua&quot;, &quot;food in Changhua District&quot;) or adjust the search center/radius.
              Each search can return up to 200 results, and duplicates are auto-skipped.
            </p>
          </div>
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <label className="flex flex-col gap-1">
              <span className="text-xs uppercase tracking-wide text-muted">Search Query</span>
              <input
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
                placeholder="Example: restaurants in Changhua City"
                className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs uppercase tracking-wide text-muted">Max Results from API</span>
              <select
                value={searchLimit}
                onChange={(event) => setSearchLimit(Number(event.target.value))}
                className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
              >
                {DISCOVERY_LIMIT_OPTIONS.map((value) => (
                  <option key={value} value={value}>
                    Up to {value} places
                  </option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs uppercase tracking-wide text-muted">Minimum Rating</span>
              <select
                value={minRating}
                onChange={(event) => setMinRating(Number(event.target.value))}
                className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
              >
                {DISCOVERY_MIN_RATING_OPTIONS.map((value) => (
                  <option key={value} value={value}>
                    {value === 0 ? "Any rating" : `${value}+ stars`}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs uppercase tracking-wide text-muted">Minimum Ratings Total</span>
              <select
                value={minRatingsTotal}
                onChange={(event) => setMinRatingsTotal(Number(event.target.value))}
                className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
              >
                {DISCOVERY_MIN_TOTAL_OPTIONS.map((value) => (
                  <option key={value} value={value}>
                    {value === 0 ? "Any volume" : `${value}+ ratings`}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs uppercase tracking-wide text-muted">Search Center</span>
              <input
                value={location}
                onChange={(event) => setLocation(event.target.value)}
                placeholder="Optional lat,lng"
                className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
              />
            </label>
            <label className="flex flex-col gap-1">
              <span className="text-xs uppercase tracking-wide text-muted">Radius</span>
              <select
                value={radiusM}
                onChange={(event) => setRadiusM(Number(event.target.value))}
                className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
              >
                {DISCOVERY_RADIUS_OPTIONS.map((value) => (
                  <option key={value} value={value}>
                    {value === 0 ? "Auto radius" : `${value.toLocaleString()} meters`}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <div className="mt-3 flex flex-wrap items-center gap-2">
            <button
              onClick={() => void onSearchDiscovery()}
              disabled={actionBusy.length > 0 || !health?.google_places_api_configured}
              className="rounded-lg border border-accent/50 bg-accent/10 px-3 py-1.5 text-xs font-semibold text-accent disabled:opacity-50"
            >
              {actionBusy === "search-discovery" ? "Searching..." : "Find Restaurants via Google API"}
            </button>
            <button
              onClick={() => void onApproveSelected()}
              disabled={actionBusy.length > 0 || selectedCandidateIds.length === 0}
              className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
            >
              {actionBusy === "approve-candidates" ? "Approving..." : "Approve Selected"}
            </button>
            <button
              onClick={() => void onRejectSelected()}
              disabled={actionBusy.length > 0 || selectedCandidateIds.length === 0}
              className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
            >
              {actionBusy === "reject-candidates" ? "Rejecting..." : "Reject Selected"}
            </button>
            <button
              onClick={() => void onQueueSelectedCandidates()}
              disabled={actionBusy.length > 0 || queuedCandidateGooglePlaceIds.length === 0}
              className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
            >
              {actionBusy === "queue-selected-candidates" ? "Queueing..." : "Queue Selected For RPA"}
            </button>
            <button
              onClick={selectAllCandidates}
              disabled={candidates.length === 0}
              className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
            >
              {allCandidatesSelected ? `All Selected (${candidates.length})` : `Select All (${candidates.length})`}
            </button>
            <button
              onClick={clearCandidateSelection}
              disabled={selectedCandidateIds.length === 0}
              className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
            >
              Clear Selection
            </button>
          </div>
          <div className="mt-4 max-h-[min(48dvh,420px)] space-y-2 overflow-y-auto pr-1">
            {candidates.length === 0 ? (
              <div className="text-sm text-muted">No staged discovery candidates yet.</div>
            ) : null}
            {candidates.map((candidate) => {
              const selected = selectedCandidateIds.includes(candidate.candidate_id);
              return (
                <label
                  key={candidate.candidate_id}
                  className={[
                    "block rounded-xl border p-3",
                    selected ? "border-accent/60 bg-accent/10" : "border-border/50 bg-bg/40",
                  ].join(" ")}
                >
                  <div className="flex items-start gap-3">
                    <input
                      type="checkbox"
                      checked={selected}
                      onChange={() => toggleCandidate(candidate.candidate_id)}
                      className="mt-1"
                    />
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="text-sm font-semibold text-text break-words">
                          {candidate.name || "(unnamed candidate)"}
                        </div>
                        <div className="flex flex-wrap items-center gap-1.5">
                          <Badge tone={statusTone(candidate.status)}>{candidate.status}</Badge>
                          {candidate.duplicate_source ? <Badge tone="default">{candidate.duplicate_source}</Badge> : null}
                        </div>
                      </div>
                      <div className="mt-1 text-xs text-muted break-all">{candidate.google_place_id}</div>
                      <div className="mt-1 text-xs text-muted break-words">{candidate.formatted_address || "-"}</div>
                      <div className="mt-1 text-xs text-muted">
                        rating: {candidate.rating ?? "-"} · ratings total:{" "}
                        <span className={candidate.user_ratings_total != null && candidate.user_ratings_total < minReviews ? "text-red-300 font-semibold" : ""}>
                          {candidate.user_ratings_total ?? "-"}
                          {candidate.user_ratings_total != null && candidate.user_ratings_total < minReviews ? ` (below ${minReviews} goal)` : ""}
                        </span>
                        {" "}· updated: {fmtTs(candidate.updated_at)}
                      </div>
                    </div>
                  </div>
                </label>
              );
            })}
          </div>
        </Card>

        <Card title="Validation Results" right={<Badge tone="default">{validationResults.length}</Badge>}>
          {validationResults.length === 0 ? (
            <div className="text-sm text-muted">
              Run “Validate Active Places” to classify targets as valid, invalid, or error.
            </div>
          ) : null}
          <div className="max-h-[min(48dvh,420px)] space-y-2 overflow-y-auto pr-1">
            {validationResults.map((row) => (
              <div key={`${row.google_place_id}-${row.checked_at}`} className="rounded-xl border border-border/50 bg-bg/40 p-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="text-sm font-semibold text-text break-words">{row.company || row.google_place_id}</div>
                  <Badge tone={statusTone(row.status)}>{row.status}</Badge>
                </div>
                <div className="mt-1 text-xs text-muted break-all">{row.google_place_id || row.place_id || "-"}</div>
                <div className="mt-1 text-xs text-muted break-words">{row.reason || "-"}</div>
                <div className="mt-1 text-xs text-muted">
                  API: {row.api_name || "-"} · business_status: {row.business_status || "-"} · checked:{" "}
                  {fmtTs(row.checked_at)}
                </div>
              </div>
            ))}
          </div>
        </Card>
      </div>

      <div className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
        <Card title="Data Health" right={<Badge tone={health?.stale_total_count ? "warn" : "good"}>{health?.stale_total_count ?? 0}</Badge>}>
          <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">stale totals: {health?.stale_total_count ?? "-"}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">conflict groups: {health?.conflict_group_count ?? "-"}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">invalid archive: {health?.invalid_archive_count ?? "-"}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">jobs: {jobs.length}</div>
          </div>
          <div className="mt-3 flex flex-wrap items-center gap-2">
            <button
              onClick={() => void onRebuildTotals()}
              disabled={actionBusy.length > 0 || !health || health.stale_total_count === 0}
              className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
            >
              {actionBusy === "rebuild-totals" ? "Rebuilding..." : "Rebuild Cached Totals"}
            </button>
          </div>
          <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
            <div>
              <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">Stale Totals</div>
              <div className="space-y-2">
                {health?.stale_total_examples.length ? null : <div className="text-sm text-muted">No stale cached totals.</div>}
                {health?.stale_total_examples.map((row) => (
                  <div key={row.place_id} className="rounded-xl border border-border/50 bg-bg/40 p-3">
                    <div className="text-sm font-semibold text-text break-words">{row.place_name || row.place_id}</div>
                    <div className="mt-1 text-xs text-muted break-all">{row.place_id}</div>
                    <div className="mt-1 text-xs text-muted">
                      live: {row.total_reviews} · cached: {row.cached_total_reviews}
                    </div>
                  </div>
                ))}
              </div>
            </div>
            <div>
              <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">Recently Archived Invalid Places</div>
              <div className="space-y-2">
                {health?.recent_invalid_places.length ? null : <div className="text-sm text-muted">No archived invalid places yet.</div>}
                {health?.recent_invalid_places.map((row) => (
                  <div key={row.archive_id} className="rounded-xl border border-border/50 bg-bg/40 p-3">
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-sm font-semibold text-text break-words">{row.place_name || row.google_place_id || "-"}</div>
                      <Badge tone={statusTone(row.validation_status || "")}>{row.validation_status || "archived"}</Badge>
                    </div>
                    <div className="mt-1 text-xs text-muted break-all">{row.google_place_id || row.place_id || "-"}</div>
                    <div className="mt-1 text-xs text-muted break-words">{row.validation_reason || "-"}</div>
                    <div className="mt-1 text-xs text-muted">archived: {fmtTs(row.archived_at)}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </Card>

        <Card title="Job Monitor" right={<Badge tone={summary.failed > 0 ? "bad" : summary.running > 0 ? "warn" : "good"}>{loading ? "Loading" : `${summary.running} running`}</Badge>}>
          <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-5">
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">pending: {summary.pending}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">running: {summary.running}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">completed: {summary.completed}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">failed: {summary.failed}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">cancelled: {summary.cancelled}</div>
          </div>
          <div className="mt-3 grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">below {minReviews}: {progress?.under_min_reviews ?? "-"}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">exhausted: {progress?.exhausted_under_threshold_count ?? "-"}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">staged candidates: {health?.staged_candidate_count ?? "-"}</div>
            <div className="rounded-lg border border-border/50 bg-bg/40 p-2">db reviews: {stats?.reviews_count ?? "-"}</div>
          </div>
          <div className="mt-4 max-h-[min(44dvh,340px)] space-y-2 overflow-y-auto pr-1">
            {jobs.length === 0 ? <div className="text-sm text-muted">No scrape jobs have been created yet.</div> : null}
            {jobs.map((job) => (
              <div key={job.job_id} className="rounded-xl border border-border/50 bg-bg/40 p-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="text-sm font-semibold text-text break-all">{job.url}</div>
                  <Badge tone={statusTone(job.status)}>{job.status}</Badge>
                </div>
                <div className="mt-1 text-xs text-muted">
                  started: {fmtTs(job.started_at)} · completed: {fmtTs(job.completed_at)}
                </div>
                {job.error_message ? <div className="mt-1 text-xs text-red-300 break-words">{job.error_message}</div> : null}
              </div>
            ))}
          </div>
        </Card>
      </div>

      <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
        <Card title={`Queue-Eligible Targets (< ${minReviews})`} right={<Badge tone="warn">{incompleteTargets.length}</Badge>}>
          <div className="max-h-[min(48dvh,380px)] space-y-2 overflow-y-auto pr-1">
            {incompleteTargets.length === 0 ? <div className="text-sm text-muted">No queue-eligible targets below threshold.</div> : null}
            {incompleteTargets.map((target) => {
              const cannotReachGoal = target.cached_total_reviews > 0 && target.cached_total_reviews < minReviews;
              return (
                <div key={`${target.google_place_id}-${target.url}`} className={`rounded-xl border p-3 ${cannotReachGoal ? "border-red-500/40 bg-red-500/5" : "border-border/50 bg-bg/40"}`}>
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="text-sm font-semibold text-text break-words">{target.company || target.place_name || target.google_place_id}</div>
                    <div className="flex flex-wrap items-center gap-1.5">
                      {cannotReachGoal ? (
                        <Badge tone="bad">only {target.cached_total_reviews} on Google</Badge>
                      ) : (
                        <Badge tone="warn">
                          scraped {target.review_count} / {target.cached_total_reviews} total
                        </Badge>
                      )}
                    </div>
                  </div>
                  <div className="mt-1 text-xs text-muted break-all">{target.google_place_id || target.place_id || "-"}</div>
                  <div className="mt-1 text-xs text-muted">
                    last scraped: {fmtTs(target.last_scraped)} · validation: {target.validation_status || "unknown"}
                  </div>
                  {cannotReachGoal ? (
                    <div className="mt-1 text-xs text-red-300">
                      This restaurant only has {target.cached_total_reviews} reviews on Google — cannot reach the {minReviews} goal.
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </Card>

        <Card title={`Exhausted Under Threshold`} right={<Badge tone="default">{exhaustedTargets.length}</Badge>}>
          <div className="max-h-[min(48dvh,380px)] space-y-2 overflow-y-auto pr-1">
            {exhaustedTargets.length === 0 ? <div className="text-sm text-muted">No exhausted under-threshold targets.</div> : null}
            {exhaustedTargets.map((target) => (
              <div key={`${target.google_place_id}-${target.url}`} className="rounded-xl border border-border/50 bg-bg/40 p-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="text-sm font-semibold text-text break-words">{target.company || target.place_name || target.google_place_id}</div>
                  <Badge tone="default">{target.status}</Badge>
                </div>
                <div className="mt-1 text-xs text-muted break-all">{target.google_place_id || target.place_id || "-"}</div>
                <div className="mt-1 text-xs text-muted">
                  reviews: {target.review_count} · validation: {target.validation_status || "unknown"}
                </div>
              </div>
            ))}
          </div>
        </Card>
      </div>

      <Card title="Recent Errors" right={<Badge tone="bad">{errors.length}</Badge>}>
        <div className="max-h-[min(40dvh,320px)] space-y-2 overflow-y-auto pr-1">
          {errors.length === 0 ? <div className="text-sm text-muted">No recent ERROR logs.</div> : null}
          {errors.map((entry, idx) => (
            <div key={`${entry.ts}-${idx}`} className="rounded-xl border border-border/50 bg-bg/40 p-3">
              <div className="text-xs text-muted break-words">
                {fmtTs(entry.ts)} · {entry.logger || "-"}
              </div>
              <div className="u-wrap-anywhere mt-1 whitespace-pre-wrap text-sm text-text">
                {entry.msg || entry.raw || "(empty log)"}
              </div>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}
