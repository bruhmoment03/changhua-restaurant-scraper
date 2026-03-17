import type { Metadata } from "next";
import type { ReactNode } from "react";
import "./globals.css";
import { Sidebar } from "@/components/Sidebar";

export const metadata: Metadata = {
  title: "Google Reviews Scraper - DB Inspector",
  description: "Read-only dashboard for progress, places, reviews, and logs",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="min-h-screen min-h-dvh">
          <div className="mx-auto grid min-h-screen min-h-dvh max-w-[1600px] grid-cols-1 lg:grid-cols-[260px_minmax(0,1fr)]">
            <Sidebar />
            <main className="min-w-0 p-6 pt-16 lg:p-8 lg:pt-8">{children}</main>
          </div>
        </div>
      </body>
    </html>
  );
}
