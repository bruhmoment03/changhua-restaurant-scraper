"use client";

import { useEffect, useState } from "react";
import { ExportFormat } from "@/lib/api";

/** All available export columns — order matches the backend CSV_COLUMNS. */
export const ALL_EXPORT_COLUMNS = [
  "place_id",
  "place_name",
  "review_id",
  "author",
  "rating",
  "review_text_primary",
  "review_text_all_json",
  "review_date",
  "raw_date",
  "likes",
  "profile_url",
  "is_deleted",
  "created_date",
  "last_modified",
  "last_seen_session",
  "last_changed_session",
  "owner_responses_json",
  "user_images_json",
  "s3_images_json",
  "source_url",
  "resolved_place_url",
  "scrape_session_id",
  "scrape_started_at",
  "scrape_completed_at",
  "scrape_mode",
  "google_maps_auth_mode",
  "sort_order_requested",
  "sort_order_confirmed",
  "extraction_confidence",
  "source_locale",
] as const;

/** Columns selected by default — the most commonly useful ones. */
const DEFAULT_COLUMNS = new Set([
  "place_name",
  "author",
  "rating",
  "review_text_primary",
  "review_date",
  "likes",
  "owner_responses_json",
]);

export type ExportPayload = {
  format: ExportFormat;
  includeDeleted: boolean;
  excludeEmptyText: boolean;
  minReviewCount: number | null;
  sheetName: string;
  columns: string[];
};

type ExportDialogProps = {
  open: boolean;
  onClose: () => void;
  scope: "all" | "place";
  placeName?: string;
  placeId?: string;
  onSubmit: (payload: ExportPayload) => Promise<void>;
};

export function ExportDialog(props: ExportDialogProps) {
  const { open, onClose, scope, placeName, placeId, onSubmit } = props;
  const [format, setFormat] = useState<ExportFormat>("xlsx");
  const [includeDeleted, setIncludeDeleted] = useState(false);
  const [excludeEmptyText, setExcludeEmptyText] = useState(false);
  const [minReviewCountInput, setMinReviewCountInput] = useState("");
  const [sheetName, setSheetName] = useState("");
  const [selectedCols, setSelectedCols] = useState<Set<string>>(new Set(DEFAULT_COLUMNS));
  const [colsExpanded, setColsExpanded] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!open) return;
    setFormat("xlsx");
    setIncludeDeleted(false);
    setExcludeEmptyText(false);
    setMinReviewCountInput("");
    setSheetName("");
    setSelectedCols(new Set(DEFAULT_COLUMNS));
    setColsExpanded(false);
    setBusy(false);
    setError("");
  }, [open, scope, placeId]);

  useEffect(() => {
    if (!open) return;
    const onEsc = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !busy) onClose();
    };
    window.addEventListener("keydown", onEsc);
    return () => window.removeEventListener("keydown", onEsc);
  }, [open, busy, onClose]);

  if (!open) return null;

  const scopeLabel =
    scope === "all"
      ? "All Places"
      : `${placeName || "This Place"}${placeId ? ` (${placeId})` : ""}`;

  function toggleCol(col: string) {
    setSelectedCols((prev) => {
      const next = new Set(prev);
      if (next.has(col)) next.delete(col);
      else next.add(col);
      return next;
    });
  }

  function selectAllCols() {
    setSelectedCols(new Set(ALL_EXPORT_COLUMNS));
  }

  function selectNoneCols() {
    setSelectedCols(new Set());
  }

  async function handleDownload() {
    if (selectedCols.size === 0) {
      setError("Please select at least one column to export.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const columns = ALL_EXPORT_COLUMNS.filter((c) => selectedCols.has(c));
      const parsedMinReviewCount = minReviewCountInput.trim();
      const minReviewCount =
        scope === "all" && parsedMinReviewCount
          ? Math.max(1, Number.parseInt(parsedMinReviewCount, 10) || 0)
          : null;
      await onSubmit({
        format,
        includeDeleted,
        excludeEmptyText,
        minReviewCount,
        sheetName: sheetName.trim(),
        columns,
      });
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Export failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4"
      onClick={() => {
        if (!busy) onClose();
      }}
    >
      <div
        className="w-full max-w-lg rounded-2xl border border-border/70 bg-bg p-6 shadow-2xl"
        onClick={(event) => event.stopPropagation()}
      >
        <h2 className="text-lg font-semibold text-text">Export Reviews</h2>
        <p className="mt-1 text-sm text-muted">Scope: {scopeLabel}</p>

        <div className="mt-5 space-y-4">
          <label className="block">
            <span className="mb-1 block text-xs uppercase tracking-wide text-muted">Format</span>
            <select
              value={format}
              onChange={(event) => setFormat(event.target.value as ExportFormat)}
              disabled={busy}
              className="w-full rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
            >
              <option value="xlsx">Excel (.xlsx)</option>
              <option value="json">JSON</option>
              <option value="csv">CSV</option>
            </select>
          </label>

          {format === "xlsx" ? (
            <label className="block">
              <span className="mb-1 block text-xs uppercase tracking-wide text-muted">
                Sheet Name
                <span className="ml-1 normal-case tracking-normal text-muted/60">(optional)</span>
              </span>
              <input
                type="text"
                value={sheetName}
                onChange={(event) => setSheetName(event.target.value)}
                disabled={busy}
                placeholder={scope === "all" ? "index" : "reviews"}
                maxLength={31}
                className="w-full rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
              />
            </label>
          ) : null}

          {/* Column Selection */}
          <div>
            <button
              type="button"
              onClick={() => setColsExpanded((v) => !v)}
              className="flex w-full items-center justify-between text-left"
            >
              <span className="text-xs uppercase tracking-wide text-muted">
                Columns
                <span className="ml-2 normal-case tracking-normal text-muted/60">
                  ({selectedCols.size} / {ALL_EXPORT_COLUMNS.length} selected)
                </span>
              </span>
              <span className={`text-xs text-muted transition-transform ${colsExpanded ? "rotate-90" : ""}`}>&#9654;</span>
            </button>

            {colsExpanded && (
              <div className="mt-2 rounded-xl border border-border/50 bg-bg/30 p-3">
                <div className="mb-2 flex gap-2">
                  <button
                    type="button"
                    onClick={selectAllCols}
                    disabled={busy}
                    className="rounded border border-border/50 px-2 py-0.5 text-[10px] font-semibold text-muted hover:text-text disabled:opacity-50"
                  >
                    Select All
                  </button>
                  <button
                    type="button"
                    onClick={selectNoneCols}
                    disabled={busy}
                    className="rounded border border-border/50 px-2 py-0.5 text-[10px] font-semibold text-muted hover:text-text disabled:opacity-50"
                  >
                    Deselect All
                  </button>
                </div>
                <div className="grid max-h-48 grid-cols-2 gap-x-3 gap-y-1 overflow-y-auto text-xs">
                  {ALL_EXPORT_COLUMNS.map((col) => (
                    <label key={col} className="flex items-center gap-1.5 text-text">
                      <input
                        type="checkbox"
                        checked={selectedCols.has(col)}
                        disabled={busy}
                        onChange={() => toggleCol(col)}
                      />
                      <span className="truncate" title={col}>{col}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className="space-y-2">
            <span className="block text-xs uppercase tracking-wide text-muted">Filters</span>

            {scope === "all" ? (
              <label className="block">
                <span className="mb-1 block text-xs uppercase tracking-wide text-muted">
                  Minimum Reviews Count
                  <span className="ml-1 normal-case tracking-normal text-muted/60">(per place)</span>
                </span>
                <input
                  type="number"
                  inputMode="numeric"
                  min={1}
                  step={1}
                  value={minReviewCountInput}
                  disabled={busy}
                  onChange={(event) => setMinReviewCountInput(event.target.value)}
                  placeholder="Leave blank to export all places"
                  className="w-full rounded-xl border border-border/60 bg-bg/40 px-3 py-2 text-sm text-text outline-none focus:border-accent/60"
                />
                <span className="mt-1 block text-xs text-muted/70">
                  Keeps only places with at least this many exported reviews after the current filters.
                </span>
              </label>
            ) : null}

            <label className="flex items-center gap-2 text-sm text-text">
              <input
                type="checkbox"
                checked={excludeEmptyText}
                disabled={busy}
                onChange={(event) => setExcludeEmptyText(event.target.checked)}
              />
              Exclude star-only reviews (no text content)
            </label>

            <label className="flex items-center gap-2 text-sm text-text">
              <input
                type="checkbox"
                checked={includeDeleted}
                disabled={busy}
                onChange={(event) => setIncludeDeleted(event.target.checked)}
              />
              Include deleted reviews
            </label>
          </div>
        </div>

        {error ? (
          <div className="mt-4 rounded-lg border border-red-500/40 bg-red-500/10 p-2 text-xs text-red-300">
            {error}
          </div>
        ) : null}

        <div className="mt-6 flex items-center justify-end gap-2">
          <button
            type="button"
            disabled={busy}
            onClick={onClose}
            className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() => void handleDownload()}
            className="rounded-lg border border-accent/60 bg-accent/15 px-3 py-1.5 text-xs font-semibold text-text hover:bg-accent/25 disabled:opacity-50"
          >
            {busy ? "Preparing..." : "Download"}
          </button>
        </div>
      </div>
    </div>
  );
}
