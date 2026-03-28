"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { Badge } from "@/components/Badge";
import { Card } from "@/components/Card";
import {
  DatasetBundleArtifact,
  DatasetBundleArtifactPreviewResponse,
  DatasetBundleSummary,
  downloadDatasetBundleArtifact,
  generateDatasetBundle,
  getDatasetBundleArtifactPreview,
  getLatestDatasetBundle,
} from "@/lib/api";

const CONFIG_PATH = "batch/config.top50.yaml";
const MIN_REVIEWS = 100;

function fmtTs(value?: string | null): string {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

function fmtCount(value: unknown): string {
  if (typeof value === "number") return value.toLocaleString();
  if (typeof value === "string" && value.trim()) return value;
  return "-";
}

function fmtBytes(value?: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) return "-";
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function StatCard(props: { label: string; value: string; sub?: string }) {
  return (
    <div className="rounded-2xl border border-border/60 bg-panel/80 p-5 shadow-card backdrop-blur-sm">
      <div className="text-3xl font-bold tracking-tight text-text">{props.value}</div>
      <div className="mt-1 text-sm font-medium text-muted">{props.label}</div>
      {props.sub ? <div className="mt-2 text-xs text-muted">{props.sub}</div> : null}
    </div>
  );
}

export default function DatasetExportPage() {
  const [bundle, setBundle] = useState<DatasetBundleSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [emptyState, setEmptyState] = useState(false);
  const [error, setError] = useState("");
  const [actionBusy, setActionBusy] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [previewArtifactName, setPreviewArtifactName] = useState("");
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState("");
  const [previewPayload, setPreviewPayload] = useState<DatasetBundleArtifactPreviewResponse | null>(null);

  const loadLatest = useCallback(async (mode: "initial" | "refresh" = "initial") => {
    if (mode === "initial") {
      setLoading(true);
    } else {
      setActionBusy("refresh");
    }
    setError("");

    try {
      const payload = await getLatestDatasetBundle();
      setBundle(payload);
      setEmptyState(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to load latest dataset bundle";
      if (message.startsWith("404 ")) {
        setBundle(null);
        setEmptyState(true);
        setError("");
      } else {
        setError(message);
      }
    } finally {
      if (mode === "initial") {
        setLoading(false);
      } else {
        setActionBusy("");
      }
    }
  }, []);

  useEffect(() => {
    void loadLatest("initial");
  }, [loadLatest]);

  const manifest = bundle?.manifest || null;
  const qa = bundle?.qa_report_excerpt || null;
  const summary = (qa?.summary || {}) as Record<string, unknown>;
  const reviewFlags = (qa?.review_flag_summary || {}) as Record<string, number>;
  const followupTargets = qa?.followup_targets || [];
  const followupSummary = qa?.followup_targets_summary || { total: 0, counts_by_reason: {} };
  const artifacts = bundle?.artifacts || [];
  const preview = previewPayload?.preview || null;
  const previewArtifact = previewPayload?.artifact || null;

  const reasonEntries = useMemo(
    () => Object.entries(followupSummary.counts_by_reason || {}),
    [followupSummary.counts_by_reason],
  );

  async function onGenerate() {
    setActionBusy("generate");
    setActionMessage("");
    setError("");
    try {
      const payload = await generateDatasetBundle({
        configPath: CONFIG_PATH,
        minReviews: MIN_REVIEWS,
        includeDeleted: false,
      });
      setBundle(payload);
      setEmptyState(false);
      setActionMessage(`Generated canonical latest dataset bundle at ${payload.output_dir}.`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to generate dataset bundle");
    } finally {
      setActionBusy("");
      setLoading(false);
    }
  }

  async function onDownloadArtifact(artifact: DatasetBundleArtifact) {
    setActionBusy(`download:${artifact.filename}`);
    setActionMessage("");
    setError("");
    try {
      await downloadDatasetBundleArtifact(artifact.download_path, artifact.filename);
      setActionMessage(`Downloaded ${artifact.filename}.`);
    } catch (err) {
      setError(err instanceof Error ? err.message : `Failed to download ${artifact.filename}`);
    } finally {
      setActionBusy("");
    }
  }

  const closePreview = useCallback(() => {
    setPreviewArtifactName("");
    setPreviewLoading(false);
    setPreviewError("");
    setPreviewPayload(null);
  }, []);

  async function onInspectArtifact(artifact: DatasetBundleArtifact) {
    if (!artifact.previewable || !artifact.preview_path) return;
    setPreviewArtifactName(artifact.filename);
    setPreviewLoading(true);
    setPreviewError("");
    setPreviewPayload(null);
    try {
      const payload = await getDatasetBundleArtifactPreview(artifact.preview_path);
      setPreviewPayload(payload);
    } catch (err) {
      setPreviewError(err instanceof Error ? err.message : `Failed to inspect ${artifact.filename}`);
    } finally {
      setPreviewLoading(false);
    }
  }

  useEffect(() => {
    if (!previewArtifactName) return;
    const onEsc = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !previewLoading) {
        closePreview();
      }
    };
    window.addEventListener("keydown", onEsc);
    return () => window.removeEventListener("keydown", onEsc);
  }, [closePreview, previewArtifactName, previewLoading]);

  return (
    <div className="min-w-0 flex flex-col gap-6">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-text">Dataset / QA Export</h1>
          <p className="mt-1 text-sm text-muted">
            Generate and inspect the canonical latest derived dataset bundle for data quality and provenance work.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge tone="default">{manifest ? `${artifacts.length} artifacts` : "No bundle yet"}</Badge>
          <button
            onClick={() => void loadLatest("refresh")}
            disabled={actionBusy.length > 0}
            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
          >
            {actionBusy === "refresh" ? "Refreshing..." : "Refresh Latest Export Summary"}
          </button>
          <button
            onClick={() => void onGenerate()}
            disabled={actionBusy.length > 0}
            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
          >
            {actionBusy === "generate" ? "Generating..." : "Generate Dataset Bundle"}
          </button>
        </div>
      </div>

      <Card>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
          <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
            <div className="text-xs text-muted">Config Path</div>
            <div className="u-wrap-anywhere mt-1 text-sm text-text">{manifest?.config_path || CONFIG_PATH}</div>
          </div>
          <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
            <div className="text-xs text-muted">Minimum Text Reviews Threshold</div>
            <div className="mt-1 text-sm text-text">{manifest?.min_reviews ?? MIN_REVIEWS}</div>
          </div>
          <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
            <div className="text-xs text-muted">Canonical Latest Output Dir</div>
            <div className="u-wrap-anywhere mt-1 text-sm text-text">{bundle?.output_dir || "-"}</div>
          </div>
        </div>
      </Card>

      {actionMessage ? <div className="u-wrap-anywhere rounded-lg border border-border/60 bg-bg/40 p-3 text-sm text-text">{actionMessage}</div> : null}
      {error ? <div className="u-wrap-anywhere rounded-lg border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-300">{error}</div> : null}

      {loading ? (
        <Card>
          <div className="text-sm text-muted">Loading latest dataset bundle...</div>
        </Card>
      ) : null}

      {!loading && emptyState ? (
        <Card>
          <div className="space-y-2">
            <div className="text-lg font-semibold text-text">No canonical latest dataset bundle found.</div>
            <div className="text-sm text-muted">
              Generate the bundle from this page to create the canonical latest export used by the dashboard.
            </div>
          </div>
        </Card>
      ) : null}

      {!loading && manifest ? (
        <>
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-6">
            <StatCard label="Generated At" value={fmtTs(manifest.generated_at)} />
            <StatCard label="Bundle Version" value={manifest.bundle_version || "-"} />
            <StatCard label="Targets Total" value={fmtCount(summary.targets_total)} />
            <StatCard label="QA-Flagged Reviews" value={fmtCount(summary.review_with_any_qa_flag_count)} />
            <StatCard label="Targets With Lineage Gaps" value={fmtCount(summary.lineage_gap_target_count)} />
            <StatCard label="Follow-up Targets" value={fmtCount(summary.followup_target_count)} />
          </div>

          <Card
            title="Artifact Downloads"
            right={<Badge tone="default">{artifacts.length} files</Badge>}
            className="min-w-0"
          >
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="text-xs uppercase tracking-wide text-muted">
                  <tr>
                    <th className="pb-3 pr-4">Filename</th>
                    <th className="pb-3 pr-4">Format</th>
                    <th className="pb-3 pr-4">Rows</th>
                    <th className="pb-3 pr-4">Size</th>
                    <th className="pb-3 pr-4">Status</th>
                    <th className="pb-3 text-right">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {artifacts.map((artifact) => (
                    <tr key={artifact.filename} className="border-t border-border/40">
                      <td className="u-wrap-anywhere py-3 pr-4 text-text">{artifact.filename}</td>
                      <td className="py-3 pr-4 text-muted">{artifact.format}</td>
                      <td className="py-3 pr-4 text-muted">{artifact.row_count ?? "-"}</td>
                      <td className="py-3 pr-4 text-muted">{fmtBytes(artifact.size_bytes)}</td>
                      <td className="py-3 pr-4">
                        <Badge tone={artifact.exists ? "good" : "warn"}>{artifact.exists ? "ready" : "missing"}</Badge>
                      </td>
                      <td className="py-3 text-right">
                        <div className="flex justify-end gap-2">
                          <button
                            disabled={!artifact.previewable || actionBusy.length > 0 || previewLoading}
                            onClick={() => void onInspectArtifact(artifact)}
                            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
                          >
                            {previewLoading && previewArtifactName === artifact.filename ? "Inspecting..." : "Inspect"}
                          </button>
                          <button
                            disabled={!artifact.exists || actionBusy.length > 0 || previewLoading}
                            onClick={() => void onDownloadArtifact(artifact)}
                            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
                          >
                            {actionBusy === `download:${artifact.filename}` ? "Downloading..." : "Download"}
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>

          <div className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)]">
            <Card
              title="QA Summary"
              right={<Badge tone="default">{fmtCount(reviewFlags.reviews_total)} reviews</Badge>}
              className="min-w-0"
            >
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                  <div className="text-xs text-muted">Review Flag Summary</div>
                  <div className="mt-2 space-y-1 text-sm text-text">
                    <div>Empty text: {fmtCount(reviewFlags.empty_text_count)}</div>
                    <div>Low information: {fmtCount(reviewFlags.low_information_text_count)}</div>
                    <div>Duplicate within place: {fmtCount(reviewFlags.duplicate_text_within_place_count)}</div>
                    <div>Format anomaly: {fmtCount(reviewFlags.format_anomaly_count)}</div>
                    <div>Any QA flag: {fmtCount(reviewFlags.with_any_qa_flag_count)}</div>
                  </div>
                </div>
                <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                  <div className="text-xs text-muted">Follow-up Summary</div>
                  <div className="mt-2 space-y-1 text-sm text-text">
                    <div>Total follow-up targets: {fmtCount(followupSummary.total)}</div>
                    {reasonEntries.length === 0 ? <div className="text-muted">No follow-up reasons.</div> : null}
                    {reasonEntries.map(([reason, count]) => (
                      <div key={reason} className="u-wrap-anywhere">
                        {reason}: {fmtCount(count)}
                      </div>
                    ))}
                  </div>
                </div>
                <div className="rounded-xl border border-border/50 bg-bg/40 p-3 md:col-span-2">
                  <div className="text-xs text-muted">Lineage Completeness</div>
                  <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
                    <div className="rounded-lg border border-border/40 bg-panel/40 p-3">
                      <div className="text-xs uppercase tracking-wide text-muted">Targets</div>
                      <div className="mt-1 space-y-1 text-sm text-text">
                        <div>Total: {fmtCount(qa?.lineage_completeness?.targets?.total)}</div>
                        <div>With validation lineage: {fmtCount(qa?.lineage_completeness?.targets?.with_validation_lineage)}</div>
                        <div>With discovery lineage: {fmtCount(qa?.lineage_completeness?.targets?.with_discovery_lineage)}</div>
                        <div>With any lineage gap: {fmtCount(qa?.lineage_completeness?.targets?.with_any_lineage_gap)}</div>
                      </div>
                    </div>
                    <div className="rounded-lg border border-border/40 bg-panel/40 p-3">
                      <div className="text-xs uppercase tracking-wide text-muted">Reviews</div>
                      <div className="mt-1 space-y-1 text-sm text-text">
                        <div>Total: {fmtCount(qa?.lineage_completeness?.reviews?.total)}</div>
                        <div>Missing google_maps_auth_mode: {fmtCount(qa?.lineage_completeness?.reviews?.missing_google_maps_auth_mode)}</div>
                        <div>Missing sort_order_confirmed: {fmtCount(qa?.lineage_completeness?.reviews?.missing_sort_order_confirmed)}</div>
                        <div>With any provenance gap: {fmtCount(qa?.lineage_completeness?.reviews?.with_any_provenance_gap)}</div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </Card>

            <Card
              title="Provenance Caveats"
              right={<Badge tone="warn">{manifest.provenance_caveats.length} caveats</Badge>}
              className="min-w-0"
            >
              <div className="space-y-2">
                {manifest.provenance_caveats.map((caveat) => (
                  <div key={caveat} className="u-wrap-anywhere rounded-xl border border-border/50 bg-bg/40 p-3 text-sm text-text">
                    {caveat}
                  </div>
                ))}
              </div>
            </Card>
          </div>

          <Card
            title="Follow-up Targets"
            right={<Badge tone={followupTargets.length > 0 ? "warn" : "good"}>{followupTargets.length} rows</Badge>}
            className="min-w-0"
          >
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="text-xs uppercase tracking-wide text-muted">
                  <tr>
                    <th className="pb-3 pr-4">Company</th>
                    <th className="pb-3 pr-4">Google Place ID</th>
                    <th className="pb-3 pr-4">Status</th>
                    <th className="pb-3 pr-4">Text Reviews Needed</th>
                    <th className="pb-3 pr-4">Validation</th>
                    <th className="pb-3 pr-4">Lineage Gaps</th>
                    <th className="pb-3">Reasons</th>
                  </tr>
                </thead>
                <tbody>
                  {followupTargets.map((target) => (
                    <tr key={`${target.google_place_id}-${target.place_id || ""}`} className="border-t border-border/40">
                      <td className="u-wrap-anywhere py-3 pr-4 text-text">{target.company || "-"}</td>
                      <td className="u-wrap-anywhere py-3 pr-4 text-muted">{target.google_place_id || "-"}</td>
                      <td className="py-3 pr-4 text-muted">{target.target_status || "-"}</td>
                      <td className="py-3 pr-4 text-muted">{target.reviews_needed ?? "-"}</td>
                      <td className="py-3 pr-4 text-muted">{target.validation_status || "-"}</td>
                      <td className="py-3 pr-4 text-muted">{target.missing_lineage_flag_count ?? "-"}</td>
                      <td className="u-wrap-anywhere py-3 text-text">{target.followup_reasons || "-"}</td>
                    </tr>
                  ))}
                  {followupTargets.length === 0 ? (
                    <tr>
                      <td colSpan={7} className="py-4 text-sm text-muted">
                        No follow-up targets in the latest bundle.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </Card>
        </>
      ) : null}

      {previewArtifactName ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 px-4 py-6"
          onClick={() => {
            if (!previewLoading) closePreview();
          }}
        >
          <div
            className="flex max-h-[90vh] w-full max-w-7xl flex-col overflow-hidden rounded-2xl border border-border/70 bg-bg shadow-2xl"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-4 border-b border-border/40 px-5 py-4">
              <div>
                <h2 className="u-wrap-anywhere text-lg font-semibold text-text">{previewArtifactName}</h2>
                <p className="mt-1 text-sm text-muted">
                  Inspect the first rows of the canonical latest CSV artifact before downloading.
                </p>
              </div>
              <button
                onClick={() => closePreview()}
                disabled={previewLoading}
                className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
              >
                Close
              </button>
            </div>

            <div className="flex-1 overflow-y-auto p-5">
              {previewLoading ? <div className="text-sm text-muted">Loading CSV preview...</div> : null}

              {!previewLoading && previewError ? (
                <div className="u-wrap-anywhere rounded-lg border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-300">
                  {previewError}
                </div>
              ) : null}

              {!previewLoading && !previewError && preview && previewArtifact ? (
                <div className="space-y-5">
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-4">
                    <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                      <div className="text-xs text-muted">Format</div>
                      <div className="mt-1 text-sm text-text">{previewArtifact.format}</div>
                    </div>
                    <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                      <div className="text-xs text-muted">Rows</div>
                      <div className="mt-1 text-sm text-text">{fmtCount(preview.total_row_count)}</div>
                    </div>
                    <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                      <div className="text-xs text-muted">Size</div>
                      <div className="mt-1 text-sm text-text">{fmtBytes(previewArtifact.size_bytes)}</div>
                    </div>
                    <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                      <div className="text-xs text-muted">Columns</div>
                      <div className="mt-1 text-sm text-text">{fmtCount(preview.columns.length)}</div>
                    </div>
                  </div>

                  <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                    <div className="text-xs text-muted">Preview Window</div>
                    <div className="mt-1 text-sm text-text">
                      Showing first {fmtCount(preview.sample_row_count)} of {fmtCount(preview.total_row_count)} rows
                      {preview.truncated ? " (truncated)." : "."}
                    </div>
                  </div>

                  <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                    <div className="text-xs text-muted">Columns</div>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {preview.columns.map((column) => (
                        <Badge key={column} tone="default">
                          {column}
                        </Badge>
                      ))}
                      {preview.columns.length === 0 ? <div className="text-sm text-muted">No columns found.</div> : null}
                    </div>
                  </div>

                  <div className="overflow-x-auto rounded-xl border border-border/50 bg-bg/40">
                    <table className="min-w-max text-left text-sm">
                      <thead className="bg-panel/50 text-xs uppercase tracking-wide text-muted">
                        <tr>
                          {preview.columns.map((column) => (
                            <th key={column} className="whitespace-nowrap border-b border-border/40 px-3 py-2 align-top">
                              {column}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {preview.rows.map((row, index) => (
                          <tr key={`${previewArtifact.filename}-row-${index}`} className="border-t border-border/30 align-top">
                            {preview.columns.map((column) => (
                              <td key={`${previewArtifact.filename}-${index}-${column}`} className="whitespace-nowrap px-3 py-2 text-text">
                                {row[column] || ""}
                              </td>
                            ))}
                          </tr>
                        ))}
                        {preview.rows.length === 0 ? (
                          <tr>
                            <td colSpan={Math.max(preview.columns.length, 1)} className="px-3 py-4 text-sm text-muted">
                              No sample rows available in this artifact.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
