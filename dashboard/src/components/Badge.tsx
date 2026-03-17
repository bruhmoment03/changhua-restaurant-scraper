import type { ReactNode } from "react";

export function Badge(props: { children: ReactNode; tone?: "default" | "good" | "warn" | "bad" }) {
  const tone = props.tone || "default";
  const cls =
    tone === "good"
      ? "bg-emerald-500/10 border-emerald-500/20 text-emerald-600 dark:text-emerald-400"
      : tone === "warn"
        ? "bg-amber-500/10 border-amber-500/20 text-amber-600 dark:text-amber-400"
        : tone === "bad"
          ? "bg-red-500/10 border-red-500/20 text-red-600 dark:text-red-400"
          : "bg-accent/5 border-border/60 text-muted";
  return <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium ${cls}`}>{props.children}</span>;
}
