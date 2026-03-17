"use client";

import { useEffect, useState } from "react";
import { ExportFormat } from "@/lib/api";

export type ExportPayload = {
  format: ExportFormat;
  includeDeleted: boolean;
  excludeEmptyText: boolean;
  sheetName: string;
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
  const [sheetName, setSheetName] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!open) return;
    setFormat("xlsx");
    setIncludeDeleted(false);
    setExcludeEmptyText(false);
    setSheetName("");
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

  async function handleDownload() {
    setBusy(true);
    setError("");
    try {
      await onSubmit({ format, includeDeleted, excludeEmptyText, sheetName: sheetName.trim() });
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

          <div className="space-y-2">
            <span className="block text-xs uppercase tracking-wide text-muted">Filters</span>

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
