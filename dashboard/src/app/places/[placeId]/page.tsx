"use client";

import { useParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { Badge } from "@/components/Badge";
import { Card } from "@/components/Card";
import { ExportDialog } from "@/components/ExportDialog";
import { Place, Review, downloadPlaceExport, getPlace, getReviews } from "@/lib/api";

function fmtTs(value?: string | null): string {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

const PAGE_SIZE = 20;

export default function PlaceDetailPage() {
  const params = useParams<{ placeId: string }>();
  const placeId = decodeURIComponent(params.placeId || "");

  const [place, setPlace] = useState<Place | null>(null);
  const [reviews, setReviews] = useState<Review[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [includeDeleted, setIncludeDeleted] = useState(false);
  const [selectedReview, setSelectedReview] = useState<Review | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [exportOpen, setExportOpen] = useState(false);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");

    Promise.all([getPlace(placeId), getReviews(placeId, PAGE_SIZE, offset, includeDeleted)])
      .then(([placeRow, page]) => {
        if (!active) return;
        setPlace(placeRow);
        setReviews(page.reviews);
        setTotal(page.total);
        setSelectedReview((prev) => {
          if (!prev) return page.reviews[0] || null;
          return page.reviews.find((r) => r.review_id === prev.review_id) || page.reviews[0] || null;
        });
      })
      .catch((err) => {
        if (!active) return;
        setError(err instanceof Error ? err.message : "Failed to load place details");
      })
      .finally(() => {
        if (active) setLoading(false);
      });

    return () => {
      active = false;
    };
  }, [placeId, offset, includeDeleted]);

  const pageIndex = Math.floor(offset / PAGE_SIZE);
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  const reviewText = useMemo(() => {
    if (!selectedReview?.review_text) return "";
    return Object.values(selectedReview.review_text).join("\n\n");
  }, [selectedReview]);

  return (
    <div className="min-w-0 flex flex-col gap-6">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-text">Place Details</h1>
          <p className="u-wrap-anywhere mt-1 text-sm text-muted">{place?.place_name || "Loading..."}</p>
          <p className="u-wrap-anywhere text-xs text-muted">{placeId}</p>
        </div>
        <button
          onClick={() => setExportOpen(true)}
          className="rounded-lg border border-border/60 px-3 py-1.5 text-xs font-semibold text-muted hover:bg-accent/10 hover:text-text"
        >
          Export
        </button>
      </div>

      {error ? <div className="u-wrap-anywhere rounded-lg border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-300">{error}</div> : null}
      {actionMessage ? <div className="u-wrap-anywhere rounded-lg border border-border/60 bg-bg/40 p-3 text-sm text-text">{actionMessage}</div> : null}

      <Card title="Summary" right={<Badge tone="default">{total} reviews</Badge>} className="min-w-0">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-5">
          <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
            <div className="text-xs text-muted">Last scraped</div>
            <div className="mt-1 text-sm text-text">{fmtTs(place?.last_scraped)}</div>
          </div>
          <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
            <div className="text-xs text-muted">Live text reviews</div>
            <div className="mt-1 text-sm text-text">{place?.total_reviews ?? "-"}</div>
          </div>
          <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
            <div className="text-xs text-muted">Cached text reviews</div>
            <div className="mt-1 text-sm text-text">{place?.cached_total_reviews ?? "-"}</div>
          </div>
          <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
            <div className="text-xs text-muted">Validation</div>
            <div className="mt-1 text-sm text-text">{place?.validation_status || "unknown"}</div>
            <div className="mt-1 text-xs text-muted">{fmtTs(place?.validation_checked_at)}</div>
          </div>
          <label className="rounded-xl border border-border/50 bg-bg/40 p-3 text-sm text-text">
            <span className="text-xs text-muted">Include deleted</span>
            <div className="mt-1">
              <input
                type="checkbox"
                checked={includeDeleted}
                onChange={(e) => {
                  setOffset(0);
                  setIncludeDeleted(e.target.checked);
                }}
              />
            </div>
          </label>
        </div>
      </Card>

      <div className="grid min-w-0 grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)]">
        <Card
          title="Reviews"
          className="min-w-0"
          right={
            <div className="flex items-center gap-2 text-xs">
              <button
                className="rounded border border-border/60 px-2 py-1 text-muted disabled:opacity-40"
                disabled={pageIndex <= 0 || loading}
                onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
              >
                Prev
              </button>
              <span className="text-muted">{pageIndex + 1}/{totalPages}</span>
              <button
                className="rounded border border-border/60 px-2 py-1 text-muted disabled:opacity-40"
                disabled={pageIndex >= totalPages - 1 || loading}
                onClick={() => setOffset(offset + PAGE_SIZE)}
              >
                Next
              </button>
            </div>
          }
        >
          <div className="max-h-[min(60dvh,600px)] space-y-2 overflow-y-auto pr-1">
            {reviews.map((review) => {
              const hasText = Object.values(review.review_text || {}).some((t) => typeof t === "string" && t.trim().length > 0);
              return (
                <button
                  key={review.review_id}
                  onClick={() => setSelectedReview(review)}
                  className={[
                    "w-full rounded-xl border p-3 text-left",
                    selectedReview?.review_id === review.review_id ? "border-accent/60 bg-accent/10" : "border-border/50 bg-bg/40",
                  ].join(" ")}
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-sm font-semibold text-text">{review.author || "Anonymous"}</div>
                    <div className="flex items-center gap-1.5">
                      {!hasText ? <Badge tone="warn">star-only</Badge> : null}
                      <span className="text-xs text-muted">{review.rating ?? "-"}★</span>
                    </div>
                  </div>
                  <div className="mt-1 line-clamp-2 text-xs text-muted">
                    {hasText ? Object.values(review.review_text!).join(" ") : "(no text — rating only)"}
                  </div>
                  <div className="mt-1 text-xs text-muted">{fmtTs(review.review_date || review.raw_date)}</div>
                </button>
              );
            })}
            {!loading && reviews.length === 0 ? <div className="text-sm text-muted">No reviews on this page.</div> : null}
          </div>
        </Card>

        <Card title="Review Inspector" right={selectedReview ? <Badge tone={selectedReview.is_deleted ? "warn" : "good"}>{selectedReview.is_deleted ? "deleted" : "active"}</Badge> : null} className="min-w-0">
          {!selectedReview ? <div className="text-sm text-muted">Select a review to inspect.</div> : null}
          {selectedReview ? (
            <div className="max-h-[min(60dvh,600px)] space-y-3 overflow-y-auto pr-1">
              <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                <div className="text-xs text-muted">Author</div>
                <div className="text-sm text-text">{selectedReview.author || "Anonymous"}</div>
                <div className="mt-2 text-xs text-muted">Date</div>
                <div className="text-sm text-text">{fmtTs(selectedReview.review_date || selectedReview.raw_date)}</div>
              </div>
              <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                <div className="text-xs text-muted">Text</div>
                {reviewText ? (
                  <div className="u-wrap-anywhere mt-1 whitespace-pre-wrap text-sm text-text">{reviewText}</div>
                ) : (
                  <div className="mt-1 flex items-center gap-2">
                    <Badge tone="warn">star-only</Badge>
                    <span className="text-sm text-muted">This review has a rating but no text content.</span>
                  </div>
                )}
              </div>
              <div className="rounded-xl border border-border/50 bg-bg/40 p-3">
                <div className="mb-1 text-xs text-muted">Raw JSON</div>
                <div className="max-w-full overflow-x-auto">
                  <pre className="max-h-[360px] min-w-max overflow-y-auto whitespace-pre text-xs text-text">{JSON.stringify(selectedReview, null, 2)}</pre>
                </div>
              </div>
            </div>
          ) : null}
        </Card>
      </div>

      <ExportDialog
        open={exportOpen}
        onClose={() => setExportOpen(false)}
        scope="place"
        placeName={place?.place_name || ""}
        placeId={placeId}
        onSubmit={async ({ format, includeDeleted, excludeEmptyText, sheetName, columns }) => {
          await downloadPlaceExport(placeId, format, includeDeleted, excludeEmptyText, sheetName || undefined, columns);
          setActionMessage(
            `Downloaded ${place?.place_name || placeId} as ${String(format).toUpperCase()}.`
          );
        }}
      />
    </div>
  );
}
