export type Place = {
  place_id: string;
  place_name: string | null;
  original_url: string;
  resolved_url: string | null;
  latitude: number | null;
  longitude: number | null;
  first_seen: string;
  last_scraped: string | null;
  total_reviews: number;
  cached_total_reviews: number;
  reviews_exhausted: boolean;
  exhausted_at: string | null;
  validation_status: string;
  validation_checked_at: string | null;
  validation_reason: string | null;
};

export type Review = {
  review_id: string;
  place_id: string;
  author: string | null;
  rating: number | null;
  review_text: Record<string, string> | null;
  review_date: string | null;
  raw_date: string | null;
  likes: number;
  user_images: string[] | null;
  s3_images: string[] | null;
  profile_url: string | null;
  profile_picture: string | null;
  s3_profile_picture: string | null;
  owner_responses: Record<string, unknown> | null;
  created_date: string;
  last_modified: string;
  last_seen_session: number | null;
  last_changed_session: number | null;
  is_deleted: number;
  content_hash: string | null;
  engagement_hash: string | null;
  row_version: number;
};

export type PaginatedReviews = {
  place_id: string;
  total: number;
  limit: number;
  offset: number;
  reviews: Review[];
};

export type DbStats = {
  places_count: number;
  reviews_count: number;
  scrape_sessions_count: number;
  review_history_count: number;
  sync_checkpoints_count: number;
  place_aliases_count: number;
  db_size_bytes: number;
  places: Array<{
    place_id: string;
    place_name: string | null;
    total_reviews: number;
    cached_total_reviews: number;
    last_scraped: string | null;
  }>;
};

export type ProgressTarget = {
  company: string;
  url: string;
  google_place_id: string;
  status: "with_reviews" | "present_zero_reviews" | "missing_from_db" | string;
  review_count: number;
  cached_total_reviews: number;
  place_id: string | null;
  place_name: string | null;
  last_scraped: string | null;
  reviews_exhausted: boolean;
  validation_status: string;
  validation_checked_at: string | null;
  validation_reason: string | null;
  meets_min_reviews: boolean;
  reviews_needed: number;
};

export type ProgressReport = {
  targets_total: number;
  with_reviews: number;
  present_zero_reviews: number;
  missing_from_db: number;
  incomplete_total: number;
  completed_percent: number;
  min_reviews: number;
  meeting_min_reviews: number;
  under_min_reviews: number;
  exhausted_under_threshold_count: number;
  targets: ProgressTarget[];
};

export type LogTailEntry = {
  ts: string | null;
  level: string | null;
  logger: string | null;
  msg: string | null;
  raw: string | null;
};

export type Job = {
  job_id: string;
  status: "pending" | "running" | "completed" | "failed" | "cancelled" | string;
  url: string;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
  reviews_count: number | null;
  images_count: number | null;
  progress: Record<string, unknown> | null;
};

export type ScrapeAllResponse = {
  config_path: string;
  min_reviews: number;
  selected_targets: number;
  created_count: number;
  queued_count: number;
  skipped_count: number;
  error_count: number;
  created_jobs: Array<Record<string, unknown>>;
  queued_jobs: Array<Record<string, unknown>>;
  skipped_targets: Array<Record<string, unknown>>;
  errors: Array<Record<string, unknown>>;
};

export type ScrapeSettings = {
  max_concurrent_jobs: number;
};

export type ResetExhaustedResponse = {
  config_path: string;
  min_reviews: number;
  reset_count: number;
  reset_targets: Array<Record<string, unknown>>;
};

export type ExportFormat = "json" | "csv" | "xlsx";

export type DataQualityConflict = {
  review_id: string;
  place_ids: string[];
  place_names: string[];
  place_count: number;
  last_seen_sessions: Record<string, number | null>;
  has_hash_placeholder: boolean;
  has_multiple_real_places: boolean;
};

export type DataQualityConflictReport = {
  total_conflicts: number;
  conflicts: DataQualityConflict[];
};

export type PlaceValidationResult = {
  place_id: string | null;
  company: string;
  google_place_id: string;
  status: string;
  reason: string;
  api_name: string | null;
  api_address: string | null;
  business_status: string | null;
  checked_at: string;
};

export type ValidatePlacesResponse = {
  config_path: string;
  validated_count: number;
  valid_count: number;
  invalid_count: number;
  error_count: number;
  results: PlaceValidationResult[];
};

export type InvalidPlaceArchive = {
  archive_id: number;
  archived_at: string;
  config_path: string | null;
  place_id: string | null;
  google_place_id: string | null;
  place_name: string | null;
  original_url: string | null;
  resolved_url: string | null;
  live_total_reviews: number;
  cached_total_reviews: number;
  validation_status: string | null;
  validation_checked_at: string | null;
  validation_reason: string | null;
};

export type ArchiveInvalidPlaceResponse = {
  archived: InvalidPlaceArchive;
  deleted_counts: Record<string, unknown>;
};

export type DiscoveryCandidate = {
  candidate_id: number;
  config_path: string;
  query: string;
  google_place_id: string;
  name: string | null;
  formatted_address: string | null;
  rating: number | null;
  user_ratings_total: number | null;
  maps_url: string;
  status: string;
  duplicate_source: string | null;
  discovered_at: string;
  updated_at: string;
};

export type DiscoverySearchResponse = {
  config_path: string;
  query: string;
  candidate_count: number;
  staged_count: number;
  candidates: DiscoveryCandidate[];
};

export type CandidateMutationResponse = {
  config_path: string;
  updated_count: number;
  candidates: DiscoveryCandidate[];
};

export type ApproveDiscoveryCandidatesResponse = {
  config_path: string;
  approved_count: number;
  skipped_count: number;
  approved_google_place_ids: string[];
  candidates: DiscoveryCandidate[];
};

export type ScrapeTargetsResponse = {
  config_path: string;
  requested_count: number;
  created_count: number;
  queued_count: number;
  skipped_count: number;
  created_jobs: Array<Record<string, unknown>>;
  skipped_targets: Array<Record<string, unknown>>;
};

export type RebuildPlaceTotalsResponse = {
  checked_count: number;
  updated_count: number;
  updated_places: Array<Record<string, unknown>>;
};

export type DataHealthSummary = {
  config_path: string;
  min_reviews: number;
  google_places_api_configured: boolean;
  active_config_targets: number;
  db_places_count: number;
  stale_total_count: number;
  conflict_group_count: number;
  exhausted_under_threshold_count: number;
  staged_candidate_count: number;
  invalid_archive_count: number;
  stale_total_examples: DbStats["places"];
  recent_invalid_places: InvalidPlaceArchive[];
};

export type DatasetBundleManifestArtifact = {
  filename: string;
  format: string;
  row_count: number | null;
  sha256: string | null;
  columns: string[];
};

export type DatasetBundleSampleArtifact = {
  filename: string;
  row_cap: number;
  selection_rule: string;
};

export type DatasetBundleManifest = {
  generated_at: string | null;
  bundle_version: string;
  preprocessing_version: string;
  config_path: string;
  config_snapshot_sha256: string | null;
  db_path_basename: string;
  db_schema_version: number;
  scope: string;
  min_reviews: number;
  include_deleted: boolean;
  raw_sqlite_authoritative: boolean;
  derived_artifacts_only: boolean;
  artifact_count: number;
  artifacts: DatasetBundleManifestArtifact[];
  summary: Record<string, unknown>;
  lineage_completeness: Record<string, Record<string, number>>;
  provenance_caveats: string[];
  qa_sample_pack: {
    selection_version: string;
    artifacts: DatasetBundleSampleArtifact[];
  };
};

export type DatasetBundleFollowupTarget = {
  config_order: number | null;
  company: string;
  config_source: string;
  google_place_id: string;
  place_id: string | null;
  target_status: string;
  db_review_count: number;
  reviews_needed: number;
  validation_status: string;
  has_validation_lineage: boolean;
  has_discovery_lineage: boolean;
  missing_lineage_flag_count: number;
  lineage_flags: string;
  followup_priority_rank: number;
  followup_reasons: string;
};

export type DatasetBundleQaReportExcerpt = {
  generated_at: string | null;
  summary: Record<string, unknown>;
  review_flag_summary: Record<string, number>;
  lineage_completeness: Record<string, Record<string, number>>;
  followup_targets_summary: {
    total: number;
    counts_by_reason: Record<string, number>;
  };
  followup_targets: DatasetBundleFollowupTarget[];
};

export type DatasetBundleArtifact = DatasetBundleManifestArtifact & {
  exists: boolean;
  size_bytes: number | null;
  download_path: string;
  previewable: boolean;
  preview_path: string | null;
};

export type DatasetBundleArtifactPreview = {
  kind: string;
  columns: string[];
  rows: Array<Record<string, string>>;
  sample_row_count: number;
  total_row_count: number;
  truncated: boolean;
};

export type DatasetBundleArtifactPreviewResponse = {
  artifact: DatasetBundleArtifact;
  preview: DatasetBundleArtifactPreview;
};

export type DatasetBundleSummary = {
  output_dir: string;
  manifest: DatasetBundleManifest;
  qa_report_excerpt: DatasetBundleQaReportExcerpt;
  artifacts: DatasetBundleArtifact[];
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:8000";

async function apiFetch<T>(path: string): Promise<T> {
  const headers: Record<string, string> = {
    Accept: "application/json",
  };

  const res = await fetch(`${API_BASE}${path}`, {
    headers,
    cache: "no-store",
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }

  return (await res.json()) as T;
}

async function apiPost<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    cache: "no-store",
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }

  return (await res.json()) as T;
}

function _defaultExportFilename(scope: "all" | "place", format: ExportFormat, placeId?: string): string {
  if (scope === "all") return `reviews_all.${format}`;
  const safe = (placeId || "place").replace(/[^A-Za-z0-9_.-]+/g, "_");
  return `reviews_${safe}.${format}`;
}

function _parseFilenameFromContentDisposition(value: string | null): string | null {
  if (!value) return null;
  const utf8Match = value.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1]);
    } catch {
      return utf8Match[1];
    }
  }

  const match = value.match(/filename="?([^\";]+)"?/i);
  return match?.[1] || null;
}

async function apiDownload(path: string, fallbackFilename: string): Promise<void> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "GET",
    cache: "no-store",
    headers: {
      Accept: "*/*",
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }

  const blob = await res.blob();
  const filename =
    _parseFilenameFromContentDisposition(res.headers.get("Content-Disposition")) || fallbackFilename;
  const blobUrl = window.URL.createObjectURL(blob);
  try {
    const anchor = document.createElement("a");
    anchor.href = blobUrl;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
  } finally {
    window.URL.revokeObjectURL(blobUrl);
  }
}

export async function getDbStats(): Promise<DbStats> {
  return apiFetch<DbStats>("/db-stats");
}

export async function getProgress(configPath = "batch/config.top50.yaml", minReviews = 1): Promise<ProgressReport> {
  return apiFetch<ProgressReport>(
    `/progress?config_path=${encodeURIComponent(configPath)}&min_reviews=${minReviews}`
  );
}

export async function getPlaces(): Promise<Place[]> {
  return apiFetch<Place[]>("/places");
}

export async function getPlace(placeId: string): Promise<Place> {
  return apiFetch<Place>(`/places/${encodeURIComponent(placeId)}`);
}

export async function getReviews(placeId: string, limit = 50, offset = 0, includeDeleted = false): Promise<PaginatedReviews> {
  return apiFetch<PaginatedReviews>(
    `/reviews/${encodeURIComponent(placeId)}?limit=${limit}&offset=${offset}&include_deleted=${includeDeleted}`
  );
}

export async function getLogTail(level = "ERROR", limit = 100): Promise<LogTailEntry[]> {
  return apiFetch<LogTailEntry[]>(`/system/log-tail?level=${encodeURIComponent(level)}&limit=${limit}`);
}

export async function getDataQualityConflicts(includeHashOnly = false): Promise<DataQualityConflictReport> {
  return apiFetch<DataQualityConflictReport>(
    `/system/data-quality/conflicts?include_hash_only=${includeHashOnly}`
  );
}

export async function getDataHealthSummary(
  configPath = "batch/config.top50.yaml",
  minReviews = 100
): Promise<DataHealthSummary> {
  return apiFetch<DataHealthSummary>(
    `/system/data-health/summary?config_path=${encodeURIComponent(configPath)}&min_reviews=${minReviews}`
  );
}

export async function getJobs(limit = 100): Promise<Job[]> {
  return apiFetch<Job[]>(`/jobs?limit=${limit}`);
}

export async function scrapeAllUnderThreshold(configPath = "batch/config.top50.yaml", minReviews = 50): Promise<ScrapeAllResponse> {
  return apiPost<ScrapeAllResponse>("/ops/scrape-all", {
    config_path: configPath,
    min_reviews: minReviews,
    only_below_threshold: true,
  });
}

export async function scrapeAllWithSettings(payload: {
  configPath?: string;
  minReviews?: number;
  defaultMaxReviews?: number | null;
  onlyBelowThreshold?: boolean;
  excludeKnownBelowGoal?: boolean;
}): Promise<ScrapeAllResponse> {
  return apiPost<ScrapeAllResponse>("/ops/scrape-all", {
    config_path: payload.configPath || "batch/config.top50.yaml",
    min_reviews: payload.minReviews ?? 100,
    default_max_reviews:
      typeof payload.defaultMaxReviews === "number" ? payload.defaultMaxReviews : null,
    only_below_threshold: payload.onlyBelowThreshold ?? true,
    exclude_known_below_goal: payload.excludeKnownBelowGoal ?? false,
  });
}

export async function getScrapeSettings(): Promise<ScrapeSettings> {
  return apiFetch<ScrapeSettings>("/ops/scrape/settings");
}

export async function updateScrapeSettings(maxConcurrentJobs: number): Promise<ScrapeSettings> {
  return apiPost<ScrapeSettings>("/ops/scrape/settings", {
    max_concurrent_jobs: maxConcurrentJobs,
  });
}

export async function scrapeTarget(payload: {
  configPath?: string;
  googlePlaceId?: string;
  placeId?: string;
  url?: string;
  maxReviews?: number;
}): Promise<Record<string, unknown>> {
  return apiPost<Record<string, unknown>>("/ops/scrape-target", {
    config_path: payload.configPath || "batch/config.top50.yaml",
    google_place_id: payload.googlePlaceId || null,
    place_id: payload.placeId || null,
    url: payload.url || null,
    max_reviews: payload.maxReviews ?? null,
  });
}

export async function scrapeTargets(payload: {
  configPath?: string;
  googlePlaceIds: string[];
  scrapeMode?: string | null;
  maxReviews?: number | null;
}): Promise<ScrapeTargetsResponse> {
  return apiPost<ScrapeTargetsResponse>("/ops/scrape-targets", {
    config_path: payload.configPath || "batch/config.top50.yaml",
    google_place_ids: payload.googlePlaceIds,
    scrape_mode: payload.scrapeMode ?? null,
    max_reviews: typeof payload.maxReviews === "number" ? payload.maxReviews : null,
  });
}

export async function updateTargetMaxReviews(
  googlePlaceId: string,
  maxReviews: number,
  configPath = "batch/config.top50.yaml"
): Promise<Record<string, unknown>> {
  return apiPost<Record<string, unknown>>("/ops/targets/max-reviews", {
    config_path: configPath,
    google_place_id: googlePlaceId,
    max_reviews: maxReviews,
  });
}

export async function resetExhaustedTargets(payload: {
  configPath?: string;
  minReviews?: number;
  googlePlaceId?: string;
  placeId?: string;
}): Promise<ResetExhaustedResponse> {
  return apiPost<ResetExhaustedResponse>("/ops/targets/reset-exhausted", {
    config_path: payload.configPath || "batch/config.top50.yaml",
    min_reviews: payload.minReviews ?? 100,
    google_place_id: payload.googlePlaceId || null,
    place_id: payload.placeId || null,
  });
}

export async function validatePlaces(payload: {
  configPath?: string;
  googlePlaceIds?: string[];
  placeIds?: string[];
  language?: string | null;
  timeoutS?: number;
}): Promise<ValidatePlacesResponse> {
  return apiPost<ValidatePlacesResponse>("/ops/places/validate", {
    config_path: payload.configPath || "batch/config.top50.yaml",
    google_place_ids: payload.googlePlaceIds || [],
    place_ids: payload.placeIds || [],
    language: payload.language ?? null,
    timeout_s: payload.timeoutS ?? 30,
  });
}

export async function archiveInvalidPlace(payload: {
  configPath?: string;
  googlePlaceId?: string | null;
  placeId?: string | null;
}): Promise<ArchiveInvalidPlaceResponse> {
  return apiPost<ArchiveInvalidPlaceResponse>("/ops/places/archive-invalid", {
    config_path: payload.configPath || "batch/config.top50.yaml",
    google_place_id: payload.googlePlaceId || null,
    place_id: payload.placeId || null,
  });
}

export async function getInvalidPlaceArchive(limit = 20): Promise<InvalidPlaceArchive[]> {
  return apiFetch<InvalidPlaceArchive[]>(`/ops/places/invalid-archive?limit=${limit}`);
}

export async function searchDiscoveryCandidates(payload: {
  configPath?: string;
  query: string;
  limit?: number;
  rankBy?: "relevance" | "composite";
  dedupeMode?: "place_id" | "name_highest_ratings_total";
  minRating?: number;
  minRatingsTotal?: number;
  location?: string | null;
  radiusM?: number | null;
  region?: string | null;
  language?: string | null;
  timeoutS?: number;
}): Promise<DiscoverySearchResponse> {
  return apiPost<DiscoverySearchResponse>("/ops/discovery/search", {
    config_path: payload.configPath || "batch/config.top50.yaml",
    query: payload.query,
    limit: payload.limit ?? 20,
    rank_by: payload.rankBy ?? "relevance",
    dedupe_mode: payload.dedupeMode ?? "place_id",
    min_rating: payload.minRating ?? 0,
    min_ratings_total: payload.minRatingsTotal ?? 0,
    location: payload.location ?? null,
    radius_m: typeof payload.radiusM === "number" ? payload.radiusM : null,
    region: payload.region ?? null,
    language: payload.language ?? null,
    timeout_s: payload.timeoutS ?? 30,
  });
}

export async function getDiscoveryCandidates(payload?: {
  configPath?: string;
  status?: string | null;
  limit?: number;
}): Promise<DiscoveryCandidate[]> {
  const params = new URLSearchParams();
  params.set("config_path", payload?.configPath || "batch/config.top50.yaml");
  if (payload?.status) params.set("status", payload.status);
  if (typeof payload?.limit === "number") params.set("limit", String(payload.limit));
  return apiFetch<DiscoveryCandidate[]>(`/ops/discovery/candidates?${params.toString()}`);
}

export async function approveDiscoveryCandidates(payload: {
  configPath?: string;
  candidateIds: number[];
}): Promise<ApproveDiscoveryCandidatesResponse> {
  return apiPost<ApproveDiscoveryCandidatesResponse>("/ops/discovery/approve", {
    config_path: payload.configPath || "batch/config.top50.yaml",
    candidate_ids: payload.candidateIds,
  });
}

export async function rejectDiscoveryCandidates(payload: {
  configPath?: string;
  candidateIds: number[];
}): Promise<CandidateMutationResponse> {
  return apiPost<CandidateMutationResponse>("/ops/discovery/reject", {
    config_path: payload.configPath || "batch/config.top50.yaml",
    candidate_ids: payload.candidateIds,
  });
}

export async function rebuildPlaceTotals(placeIds: string[] = []): Promise<RebuildPlaceTotalsResponse> {
  return apiPost<RebuildPlaceTotalsResponse>("/ops/maintenance/rebuild-place-totals", {
    place_ids: placeIds,
  });
}

export async function downloadPlaceExport(
  placeId: string,
  format: ExportFormat = "xlsx",
  includeDeleted = false,
  excludeEmptyText = false,
  sheetName?: string,
  columns?: string[],
): Promise<void> {
  const params = new URLSearchParams();
  params.set("format", format);
  params.set("include_deleted", String(includeDeleted));
  params.set("exclude_empty_text", String(excludeEmptyText));
  if (sheetName) params.set("sheet_name", sheetName);
  if (columns && columns.length > 0) params.set("columns", columns.join(","));
  await apiDownload(
    `/exports/places/${encodeURIComponent(placeId)}?${params.toString()}`,
    _defaultExportFilename("place", format, placeId)
  );
}

export async function downloadAllExport(
  format: ExportFormat = "xlsx",
  includeDeleted = false,
  excludeEmptyText = false,
  minReviewCount?: number | null,
  sheetName?: string,
  columns?: string[],
): Promise<void> {
  const params = new URLSearchParams();
  params.set("format", format);
  params.set("include_deleted", String(includeDeleted));
  params.set("exclude_empty_text", String(excludeEmptyText));
  if (typeof minReviewCount === "number" && Number.isFinite(minReviewCount) && minReviewCount > 0) {
    params.set("min_review_count", String(Math.floor(minReviewCount)));
  }
  if (sheetName) params.set("sheet_name", sheetName);
  if (columns && columns.length > 0) params.set("columns", columns.join(","));
  await apiDownload(`/exports/all?${params.toString()}`, _defaultExportFilename("all", format));
}

export async function getLatestDatasetBundle(): Promise<DatasetBundleSummary> {
  return apiFetch<DatasetBundleSummary>("/exports/dataset-bundle/latest");
}

export async function generateDatasetBundle(payload?: {
  configPath?: string;
  minReviews?: number;
  includeDeleted?: boolean;
}): Promise<DatasetBundleSummary> {
  return apiPost<DatasetBundleSummary>("/exports/dataset-bundle/generate", {
    config_path: payload?.configPath || "batch/config.top50.yaml",
    min_reviews: payload?.minReviews ?? 100,
    include_deleted: payload?.includeDeleted ?? false,
  });
}

export async function downloadDatasetBundleArtifact(
  downloadPath: string,
  fallbackFilename: string,
): Promise<void> {
  await apiDownload(downloadPath, fallbackFilename);
}

export async function getDatasetBundleArtifactPreview(
  previewPath: string,
): Promise<DatasetBundleArtifactPreviewResponse> {
  return apiFetch<DatasetBundleArtifactPreviewResponse>(previewPath);
}
