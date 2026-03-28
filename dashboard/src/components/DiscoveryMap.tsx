"use client";

import { useEffect, useRef } from "react";
import type { Place } from "@/lib/api";

// Changhua City default center
const DEFAULT_CENTER: [number, number] = [24.0809, 120.5382];

type DiscoveryMapProps = {
  places: Place[];
  center: string; // "lat,lng" or empty
  radiusM: number; // 0 = no circle
};

export function DiscoveryMap({ places, center, radiusM }: DiscoveryMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<L.Map | null>(null);
  const markersRef = useRef<L.LayerGroup | null>(null);
  const circleRef = useRef<L.Circle | null>(null);
  const centerMarkerRef = useRef<L.Marker | null>(null);

  // Parse center string to coordinates
  const parsedCenter = (() => {
    if (!center.trim()) return null;
    const parts = center.split(",").map((s) => parseFloat(s.trim()));
    if (parts.length === 2 && !isNaN(parts[0]) && !isNaN(parts[1])) {
      return [parts[0], parts[1]] as [number, number];
    }
    return null;
  })();

  // Determine map center: use search center, or fit to places, or default
  const mapCenter = parsedCenter || DEFAULT_CENTER;

  // Init map
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    let cancelled = false;

    (async () => {
      const L = (await import("leaflet")).default;

      // Inject Leaflet CSS if not already present
      if (!document.getElementById("leaflet-css")) {
        const link = document.createElement("link");
        link.id = "leaflet-css";
        link.rel = "stylesheet";
        link.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
        document.head.appendChild(link);
      }

      if (cancelled || !containerRef.current) return;

      // Fix default marker icons for webpack/next.js
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      delete (L.Icon.Default.prototype as any)._getIconUrl;
      L.Icon.Default.mergeOptions({
        iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
        iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
        shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
      });

      // Restrict map to roughly Changhua County area so users can't scroll too far
      const maxBounds = L.latLngBounds(
        [23.85, 120.25], // southwest
        [24.35, 120.85], // northeast
      );

      const map = L.map(containerRef.current, {
        maxBounds,
        maxBoundsViscosity: 0.8,
        minZoom: 11,
        maxZoom: 18,
      }).setView(mapCenter, 13);

      L.tileLayer("https://mt{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}&hl=zh-TW", {
        subdomains: ["0", "1", "2", "3"],
        attribution: "&copy; Google Maps",
        maxZoom: 20,
      }).addTo(map);

      markersRef.current = L.layerGroup().addTo(map);
      mapRef.current = map;

      // Trigger update
      setTimeout(() => map.invalidateSize(), 100);
    })();

    return () => {
      cancelled = true;
      if (mapRef.current) {
        mapRef.current.remove();
        mapRef.current = null;
        markersRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Invalidate map size when container becomes visible (e.g. after expand)
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const observer = new ResizeObserver(() => {
      if (el.clientHeight > 0 && mapRef.current) {
        mapRef.current.invalidateSize();
      }
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  // Update markers, circle, and center marker
  useEffect(() => {
    const map = mapRef.current;
    const markerLayer = markersRef.current;
    if (!map || !markerLayer) return;

    let active = true;

    (async () => {
      const L = (await import("leaflet")).default;
      if (!active) return;

      // Clear previous
      markerLayer.clearLayers();
      if (circleRef.current) {
        circleRef.current.remove();
        circleRef.current = null;
      }
      if (centerMarkerRef.current) {
        centerMarkerRef.current.remove();
        centerMarkerRef.current = null;
      }

      // Draw radius circle if center + radius are set
      if (parsedCenter && radiusM > 0) {
        circleRef.current = L.circle(parsedCenter, {
          radius: radiusM,
          color: "#6366f1",
          fillColor: "#6366f1",
          fillOpacity: 0.08,
          weight: 2,
          dashArray: "6 4",
        }).addTo(map);

        circleRef.current.bindTooltip(
          `Search radius: ${radiusM.toLocaleString()}m`,
          { permanent: false, direction: "top" }
        );
      }

      // Center marker
      if (parsedCenter) {
        const centerIcon = L.divIcon({
          html: `<div style="width:14px;height:14px;background:#6366f1;border:3px solid #fff;border-radius:50%;box-shadow:0 1px 4px rgba(0,0,0,0.4);"></div>`,
          iconSize: [14, 14],
          iconAnchor: [7, 7],
          className: "",
        });

        centerMarkerRef.current = L.marker(parsedCenter, { icon: centerIcon })
          .addTo(map)
          .bindTooltip("Search Center", { permanent: false, direction: "top" });
      }

      // Place markers
      const bounds: [number, number][] = [];

      for (const place of places) {
        if (place.latitude == null || place.longitude == null) continue;
        const pos: [number, number] = [place.latitude, place.longitude];
        bounds.push(pos);

        const toneColor =
          place.validation_status === "valid"
            ? "#10b981"
            : place.validation_status?.startsWith("invalid")
              ? "#ef4444"
              : "#f59e0b";

        const icon = L.divIcon({
          html: `<div style="width:10px;height:10px;background:${toneColor};border:2px solid #fff;border-radius:50%;box-shadow:0 1px 3px rgba(0,0,0,0.3);"></div>`,
          iconSize: [10, 10],
          iconAnchor: [5, 5],
          className: "",
        });

        L.marker(pos, { icon })
          .addTo(markerLayer)
          .bindPopup(
            `<div style="font-size:13px;max-width:220px;">
              <strong>${place.place_name || place.place_id}</strong><br/>
              <span style="color:#888;">Text reviews: ${place.total_reviews} / ${place.cached_total_reviews}</span><br/>
              <span style="color:#888;">Status: ${place.validation_status || "unknown"}</span>
            </div>`
          );
      }

      // Fit bounds
      if (parsedCenter && radiusM > 0 && circleRef.current) {
        map.fitBounds(circleRef.current.getBounds(), { padding: [30, 30] });
      } else if (bounds.length > 0) {
        if (parsedCenter) bounds.push(parsedCenter);
        map.fitBounds(bounds, { padding: [30, 30], maxZoom: 15 });
      } else if (parsedCenter) {
        map.setView(parsedCenter, 14);
      } else {
        map.setView(DEFAULT_CENTER, 13);
      }
    })();

    return () => {
      active = false;
    };
  }, [places, parsedCenter, radiusM]);

  return (
    <div className="relative">
      <div ref={containerRef} className="h-[360px] w-full rounded-xl border border-border/50 overflow-hidden" />
      <div className="mt-2 flex flex-wrap items-center gap-4 text-xs text-muted">
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-2.5 w-2.5 rounded-full bg-emerald-500 border border-white/60" /> Valid
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-2.5 w-2.5 rounded-full bg-amber-500 border border-white/60" /> Pending
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-2.5 w-2.5 rounded-full bg-red-500 border border-white/60" /> Invalid
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-2.5 w-2.5 rounded-full bg-indigo-500 border-2 border-white/60" /> Search Center
        </span>
        {radiusM > 0 && (
          <span className="text-indigo-400">
            Radius: {radiusM.toLocaleString()}m
          </span>
        )}
        <span className="ml-auto">{places.filter((p) => p.latitude != null).length} places on map</span>
      </div>
    </div>
  );
}
