import type { ReactNode } from "react";

export function Card(props: { title?: ReactNode; children: ReactNode; right?: ReactNode; className?: string }) {
  return (
    <div className={`rounded-2xl border border-border/60 bg-panel/80 shadow-card backdrop-blur-sm ${props.className || ""}`}>
      {(props.title || props.right) && (
        <div className="flex items-center justify-between gap-3 border-b border-border/40 px-5 py-3.5">
          <div className="text-sm font-semibold tracking-wide text-text">{props.title}</div>
          <div>{props.right}</div>
        </div>
      )}
      <div className="p-5">{props.children}</div>
    </div>
  );
}
