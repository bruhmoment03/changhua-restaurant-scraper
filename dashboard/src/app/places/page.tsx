"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Badge } from "@/components/Badge";
import { Card } from "@/components/Card";
import { ExportDialog } from "@/components/ExportDialog";
import {
  Place,
  ProgressReport,
  archiveInvalidPlace,
  downloadAllExport,
  getPlaces,
  getProgress,
  resetExhaustedTargets,
  scrapeTarget,
  validatePlaces,
} from "@/lib/api";

const CONFIG_PATH = "batch/config.top50.yaml";
const MIN_REVIEWS = 100;

type StatusFilter =
  | "all"
  | "active_config"
  | "out_of_scope"
  | "under_threshold"
  | "exhausted"
  | "invalid";

function fmtTs(value?: string | null): string {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

function toneForStatus(status: string): "default" | "good" | "warn" | "bad" {
  if (status === "valid" || status === "with_reviews") return "good";
  if (status.startsWith("invalid_")) return "bad";
  if (status === "exhausted_under_threshold" || status === "present_zero_reviews") return "warn";
  return "default";
}

export default function PlacesPage() {
  const [places, setPlaces] = useState<Place[]>([]);
  const [progress, setProgress] = useState<ProgressReport | null>(null);
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [actionBusy, setActionBusy] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [exportOpen, setExportOpen] = useState(false);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const [placeRows, progressReport] = await Promise.all([
        getPlaces(),
        getProgress(CONFIG_PATH, MIN_REVIEWS),
      ]);
      setPlaces(placeRows);
      setProgress(progressReport);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load places");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  const targetByPlaceId = useMemo(() => {
    const mapping = new Map<string, ProgressReport["targets"][number]>();
    for (const target of progress?.targets || []) {
      if (target.place_id) mapping.set(target.place_id, target);
    }
    return mapping;
  }, [progress]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return places
      .map((place) => {
        const target = targetByPlaceId.get(place.place_id) || null;
        const inActiveConfig = !!target;
        const rowStatus = target?.status || "out_of_scope";
        const isInvalid = place.validation_status.startsWith("invalid_");
        return {
          place,
          target,
          inActiveConfig,
          rowStatus,
          isInvalid,
        };
      })
      .filter((row) => {
        if (statusFilter === "active_config" && !row.inActiveConfig) return false;
        if (statusFilter === "out_of_scope" && row.inActiveConfig) return false;
        if (statusFilter === "under_threshold" && (row.rowStatus === "exhausted_under_threshold" || row.target?.meets_min_reviews || !row.inActiveConfig)) {
          return false;
        }
        if (statusFilter === "exhausted" && row.rowStatus !== "exhausted_under_threshold") return false;
        if (statusFilter === "invalid" && !row.isInvalid) return false;

        if (!q) return true;
        const haystack = [
          row.place.place_id,
          row.place.place_name || "",
          row.place.original_url || "",
          row.place.resolved_url || "",
          row.target?.company || "",
          row.target?.google_place_id || "",
          row.place.validation_status || "",
          row.place.validation_reason || "",
        ]
          .join(" ")
          .toLowerCase();
        return haystack.includes(q);
      })
      .sort((a, b) => {
        const da = a.place.last_scraped ? new Date(a.place.last_scraped).getTime() : 0;
        const db = b.place.last_scraped ? new Date(b.place.last_scraped).getTime() : 0;
        return db - da;
      });
  }, [places, query, statusFilter, targetByPlaceId]);

  const onScrapeSpecific = useCallback(
    async (placeId: string) => {
      setActionBusy(`scrape:${placeId}`);
      setActionMessage("");
      try {
        const result = await scrapeTarget({ placeId, configPath: CONFIG_PATH });
        setActionMessage(`Queued scrape job ${String(result.job_id || "(unknown)")} for ${placeId}.`);
      } catch (err) {
        setActionMessage(err instanceof Error ? err.message : "Failed to queue scrape job");
      } finally {
        setActionBusy("");
      }
    },
    []
  );

  const onValidatePlace = useCallback(
    async (placeId: string) => {
      setActionBusy(`validate:${placeId}`);
      setActionMessage("");
      try {
        const result = await validatePlaces({ configPath: CONFIG_PATH, placeIds: [placeId] });
        const row = result.results[0];
        setActionMessage(
          row
            ? `Validation result for ${row.company || placeId}: ${row.status} (${row.reason || "no reason"})`
            : `No validation result returned for ${placeId}.`
        );
        await loadData();
      } catch (err) {
        setActionMessage(err instanceof Error ? err.message : "Failed to validate place");
      } finally {
        setActionBusy("");
      }
    },
    [loadData]
  );

  const onArchiveInvalid = useCallback(
    async (placeId: string, label: string) => {
      if (!window.confirm(`Archive ${label || placeId} and remove it from the active workflow?`)) return;
      setActionBusy(`archive:${placeId}`);
      setActionMessage("");
      try {
        const result = await archiveInvalidPlace({ configPath: CONFIG_PATH, placeId });
        setActionMessage(
          `Archived ${result.archived.place_name || placeId}. Deleted ${String(result.deleted_counts.reviews || 0)} review rows from the active DB.`
        );
        await loadData();
      } catch (err) {
        setActionMessage(err instanceof Error ? err.message : "Failed to archive invalid place");
      } finally {
        setActionBusy("");
      }
    },
    [loadData]
  );

  const onRestoreExhausted = useCallback(
    async (placeId?: string) => {
      const busyKey = placeId ? `restore:${placeId}` : "restore:all";
      setActionBusy(busyKey);
      setActionMessage("");
      try {
        const result = await resetExhaustedTargets({
          configPath: CONFIG_PATH,
          minReviews: MIN_REVIEWS,
          placeId,
        });
        if (result.reset_count > 0) {
          setActionMessage(`Restored ${result.reset_count} exhausted target(s) to the queue.`);
        } else {
          setActionMessage("No exhausted under-threshold targets needed restoring.");
        }
        await loadData();
      } catch (err) {
        setActionMessage(err instanceof Error ? err.message : "Failed to restore exhausted targets");
      } finally {
        setActionBusy("");
      }
    },
    [loadData]
  );

  const exhaustedUnderThreshold = useMemo(
    () => (progress?.targets || []).filter((target) => target.status === "exhausted_under_threshold"),
    [progress]
  );

  return (
    <div className="min-w-0 flex flex-col gap-6">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-text">Places</h1>
          <p className="mt-1 text-sm text-muted">Validate, archive, inspect, and queue place-level work.</p>
        </div>
        <div className="flex items-center gap-2">
          <Badge tone="default">{filtered.length} shown</Badge>
          <button
            disabled={actionBusy.length > 0}
            onClick={() => setExportOpen(true)}
            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
          >
            Export Data
          </button>
          <button
            disabled={exhaustedUnderThreshold.length === 0 || actionBusy.length > 0}
            onClick={() => void onRestoreExhausted()}
            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
          >
            {actionBusy === "restore:all" ? "Restoring..." : `Restore Exhausted (${exhaustedUnderThreshold.length})`}
          </button>
          <button
            onClick={() => void loadData()}
            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text"
          >
            Refresh
          </button>
        </div>
      </div>

      {actionMessage ? <div className="rounded-lg border border-border/60 bg-bg/40 p-3 text-sm text-text break-words">{actionMessage}</div> : null}

      <Card>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-[1fr_220px]">
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search by place name, place_id, google_place_id, validation, or URL"
            className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
          />
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value as StatusFilter)}
            className="rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
          >
            <option value="all">All</option>
            <option value="active_config">active_config</option>
            <option value="out_of_scope">out_of_scope</option>
            <option value="under_threshold">{`under_${MIN_REVIEWS}`}</option>
            <option value="exhausted">exhausted_under_threshold</option>
            <option value="invalid">invalid</option>
          </select>
        </div>
      </Card>

      {error ? <div className="rounded-lg border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-300 break-words">{error}</div> : null}

      <Card title="Place Table" right={loading ? <span className="text-xs text-muted">Loading...</span> : null} className="min-w-0">
        <p className="mb-3 text-xs text-muted md:hidden">Swipe horizontally to view all columns.</p>
        <div className="min-w-0 overflow-x-auto overscroll-x-contain">
          <table className="min-w-[1320px] table-fixed text-left text-sm">
            <thead className="text-xs uppercase tracking-wide text-muted">
              <tr>
                <th className="w-[28%] px-3 py-2">Place</th>
                <th className="w-[10%] px-3 py-2">Scope</th>
                <th className="w-[14%] px-3 py-2">Queue Status</th>
                <th className="w-[14%] px-3 py-2">Validation</th>
                <th className="w-[12%] px-3 py-2">Text Reviews</th>
                <th className="w-[12%] px-3 py-2">Last Scraped</th>
                <th className="w-[10%] px-3 py-2">Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(({ place, target, inActiveConfig, rowStatus }) => {
                const targetId = target?.google_place_id;
                const invalid = place.validation_status.startsWith("invalid_");
                return (
                  <tr key={place.place_id} className="border-t border-border/40 align-top">
                    <td className="px-3 py-3">
                      <div className="font-medium text-text break-words">{place.place_name || "(unnamed place)"}</div>
                      <div className="mt-1 text-xs text-muted break-all">{place.place_id}</div>
                      {targetId ? <div className="mt-1 text-xs text-muted break-all">{targetId}</div> : null}
                    </td>
                    <td className="px-3 py-3">
                      <Badge tone={inActiveConfig ? "good" : "default"}>{inActiveConfig ? "active_config" : "out_of_scope"}</Badge>
                    </td>
                    <td className="px-3 py-3">
                      <div className="flex flex-col gap-1">
                        <Badge tone={toneForStatus(rowStatus)}>{rowStatus}</Badge>
                        {target ? (
                          <div className="text-xs text-muted">
                            {target.meets_min_reviews ? `>=${MIN_REVIEWS} text` : target.reviews_exhausted ? "exhausted" : `<${MIN_REVIEWS} text`}
                          </div>
                        ) : null}
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <div className="flex flex-col gap-1">
                        <Badge tone={toneForStatus(place.validation_status)}>{place.validation_status || "unknown"}</Badge>
                        <div className="text-xs text-muted break-words">{place.validation_reason || "-"}</div>
                        <div className="text-xs text-muted">{fmtTs(place.validation_checked_at)}</div>
                      </div>
                    </td>
                    <td className="px-3 py-3 text-text">
                      <div>text: {place.total_reviews}</div>
                      <div className="text-xs text-muted">cached: {place.cached_total_reviews}</div>
                    </td>
                    <td className="px-3 py-3 text-muted">{fmtTs(target?.last_scraped || place.last_scraped)}</td>
                    <td className="px-3 py-3">
                      <div className="flex flex-col gap-1">
                        <Link href={`/places/${encodeURIComponent(place.place_id)}`} className="text-xs font-semibold text-accent hover:underline">
                          Open
                        </Link>
                        <button
                          disabled={!inActiveConfig || actionBusy.length > 0}
                          onClick={() => void onValidatePlace(place.place_id)}
                          className="text-left text-xs text-muted hover:text-text disabled:opacity-50"
                        >
                          {actionBusy === `validate:${place.place_id}` ? "Validating..." : "Validate"}
                        </button>
                        <button
                          disabled={!invalid || actionBusy.length > 0}
                          onClick={() => void onArchiveInvalid(place.place_id, place.place_name || place.place_id)}
                          className="text-left text-xs text-muted hover:text-text disabled:opacity-50"
                        >
                          {actionBusy === `archive:${place.place_id}` ? "Archiving..." : "Archive Invalid"}
                        </button>
                        <button
                          disabled={!inActiveConfig || actionBusy.length > 0}
                          onClick={() => void onScrapeSpecific(place.place_id)}
                          className="text-left text-xs text-muted hover:text-text disabled:opacity-50"
                        >
                          {actionBusy === `scrape:${place.place_id}` ? "Queueing..." : "Queue Scrape"}
                        </button>
                        <button
                          disabled={rowStatus !== "exhausted_under_threshold" || actionBusy.length > 0}
                          onClick={() => void onRestoreExhausted(place.place_id)}
                          className="text-left text-xs text-muted hover:text-text disabled:opacity-50"
                        >
                          {actionBusy === `restore:${place.place_id}` ? "Restoring..." : "Restore Exhausted"}
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>

      <ExportDialog
        open={exportOpen}
        onClose={() => setExportOpen(false)}
        scope="all"
        onSubmit={async ({ format, includeDeleted, excludeEmptyText, minReviewCount, sheetName, columns }) => {
          setActionBusy("export:all");
          setActionMessage("");
          try {
            await downloadAllExport(
              format,
              includeDeleted,
              excludeEmptyText,
              minReviewCount,
              sheetName || undefined,
              columns
            );
            setActionMessage(`Downloaded all places as ${String(format).toUpperCase()}.`);
          } finally {
            setActionBusy("");
          }
        }}
      />
    </div>
  );
}
