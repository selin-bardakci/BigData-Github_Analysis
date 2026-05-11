import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  TrendingUp,
  LineChart,
  GitBranch,
  Users,
  Network,
  Github,
} from "lucide-react";
import { cn } from "@/lib/utils";

const NAV = [
  { to: "/",   label: "Overview",     icon: LayoutDashboard, sub: "Project summary" },
  { to: "/d1", label: "D1 · Trends",  icon: TrendingUp,      sub: "Language growth" },
  { to: "/d2", label: "D2 · Forecast", icon: LineChart,      sub: "Adoption forecasts" },
  { to: "/d3", label: "D3 · Migration", icon: GitBranch,     sub: "Tech transitions" },
  { to: "/d4", label: "D4 · PageRank",  icon: Users,         sub: "Influential devs" },
  { to: "/d5", label: "D5 · Community", icon: Network,       sub: "Developer clusters" },
];

export default function Sidebar() {
  return (
    <aside className="hidden lg:flex flex-col w-72 shrink-0 border-r border-border bg-surface/40 backdrop-blur-md">
      <div className="px-6 pt-7 pb-6 flex items-center gap-3">
        <div className="size-9 rounded-xl bg-gradient-to-br from-accent to-accent2 grid place-items-center shadow-glow">
          <svg viewBox="0 0 32 32" className="size-5">
            <path d="M6 22 L11 14 L16 19 L21 9 L26 22"
                  fill="none" stroke="#0B0E14" strokeWidth="2.6"
                  strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </div>
        <div>
          <div className="font-semibold leading-tight">Tech Trends</div>
          <div className="text-xs text-sub">GitHub · Big Data</div>
        </div>
      </div>

      <nav className="px-3 flex flex-col gap-1">
        {NAV.map(({ to, label, icon: Icon, sub }) => (
          <NavLink
            key={to}
            to={to}
            end={to === "/"}
            className={({ isActive }) =>
              cn(
                "group flex items-start gap-3 px-3 py-2.5 rounded-xl border border-transparent transition-all",
                isActive
                  ? "bg-accent/10 border-accent/30 text-ink shadow-glow"
                  : "text-sub hover:text-ink hover:bg-surface",
              )
            }
          >
            {({ isActive }) => (
              <>
                <Icon
                  className={cn(
                    "size-5 mt-0.5 shrink-0",
                    isActive ? "text-accent" : "text-sub group-hover:text-ink",
                  )}
                />
                <div className="min-w-0">
                  <div className="text-sm font-medium">{label}</div>
                  <div className="text-[11px] text-sub">{sub}</div>
                </div>
              </>
            )}
          </NavLink>
        ))}
      </nav>

      <div className="mt-auto px-5 py-5 text-xs text-sub border-t border-border">
        <div className="flex items-center gap-2 mb-2">
          <Github className="size-3.5" />
          <span>bigquery-public-data.github_repos</span>
        </div>
        <div>Time window: 2015-01 → 2025-06</div>
        <div className="mt-2 text-[11px] opacity-70">
          Gebze Technical University · CSE
        </div>
      </div>
    </aside>
  );
}
