"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";

const sections = [
  {
    label: "Overview",
    items: [{ href: "/", label: "Dashboard Home" }],
  },
  {
    label: "Derived Data",
    items: [{ href: "/dataset-export", label: "Dataset / QA Export" }],
  },
  {
    label: "Inspection",
    items: [{ href: "/places", label: "Places" }, { href: "/logs", label: "Logs" }],
  },
];

function IconMenu() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="3" y1="6" x2="21" y2="6" />
      <line x1="3" y1="12" x2="21" y2="12" />
      <line x1="3" y1="18" x2="21" y2="18" />
    </svg>
  );
}

function IconX() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="18" y1="6" x2="6" y2="18" />
      <line x1="6" y1="6" x2="18" y2="18" />
    </svg>
  );
}

export function Sidebar() {
  const pathname = usePathname();
  const [mobileOpen, setMobileOpen] = useState(false);

  function isActive(href: string) {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  }

  const navContent = (
    <>
      <div className="border-b border-border/50 px-5 py-5">
        <Link href="/" className="text-lg font-semibold text-text" onClick={() => setMobileOpen(false)}>
          Reviews Ops
        </Link>
        <p className="mt-1 text-xs text-muted">Google Reviews DB Inspector</p>
      </div>

      <nav className="flex min-h-0 flex-1 flex-col gap-0.5 overflow-y-auto px-3 py-4">
        {sections.map((s) => (
          <div key={s.label} className="mb-3">
            <div className="mb-1.5 px-3 text-[10px] font-semibold uppercase tracking-[0.1em] text-muted/70">{s.label}</div>
            {s.items.map((it) => (
              <Link
                key={it.href}
                href={it.href}
                onClick={() => setMobileOpen(false)}
                className={[
                  "flex items-center gap-3 rounded-xl px-3 py-2 text-[13px] font-medium transition-all",
                  isActive(it.href) ? "bg-accent/12 text-accent shadow-sm" : "text-muted hover:bg-accent/5 hover:text-text",
                ].join(" ")}
              >
                {it.label}
              </Link>
            ))}
          </div>
        ))}
      </nav>
    </>
  );

  return (
    <>
      <button onClick={() => setMobileOpen(!mobileOpen)} className="fixed left-4 top-4 z-50 flex h-10 w-10 items-center justify-center rounded-xl border bg-panel text-text shadow-card lg:hidden">
        {mobileOpen ? <IconX /> : <IconMenu />}
      </button>

      {mobileOpen && <div className="fixed inset-0 z-30 bg-black/40 backdrop-blur-sm lg:hidden" onClick={() => setMobileOpen(false)} />}

      <aside className="sticky top-0 hidden h-dvh flex-col border-r border-border/50 bg-panel/70 backdrop-blur lg:flex">{navContent}</aside>

      <aside
        className={[
          "fixed inset-y-0 left-0 z-40 flex w-[280px] flex-col bg-panel/95 backdrop-blur lg:hidden",
          "transform transition-transform duration-300 ease-out",
          mobileOpen ? "translate-x-0" : "-translate-x-full",
        ].join(" ")}
      >
        {navContent}
      </aside>
    </>
  );
}
