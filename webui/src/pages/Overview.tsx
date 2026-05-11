import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  Languages,
  GitCommit,
  Users2,
  Network,
  ArrowRight,
  TrendingUp,
  GitBranch,
  LineChart as LineIcon,
} from "lucide-react";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from "recharts";

import Topbar from "@/components/Topbar";
import StatCard from "@/components/StatCard";
import ChartCard from "@/components/ChartCard";
import ChartTooltip from "@/components/Tooltip";
import Loading from "@/components/Loading";
import { loadD1, loadD5, type D1Row } from "@/lib/data";
import { colorFor, fmtNum } from "@/lib/utils";

const HIGHLIGHT = ["python", "typescript", "rust", "go"];

export default function Overview() {
  const [d1, setD1] = useState<{ top_langs: string[]; rows: D1Row[] } | null>(null);
  const [d5Stats, setD5Stats] = useState<{
    total_developers: number;
    total_communities: number;
    largest_community: number;
  } | null>(null);

  useEffect(() => {
    loadD1().then(setD1);
    loadD5().then((d) =>
      setD5Stats({
        total_developers: d.total_developers,
        total_communities: d.total_communities,
        largest_community: d.largest_community,
      }),
    );
  }, []);

  if (!d1 || !d5Stats) return <Loading />;

  // Build pivot-by-month for highlighted langs
  const months = Array.from(new Set(d1.rows.map((r) => r.year_month))).sort();
  const data = months.map((ym) => {
    const row: Record<string, number | string> = { ym };
    for (const lang of HIGHLIGHT) {
      const r = d1.rows.find((x) => x.language === lang && x.year_month === ym);
      row[lang] = r ? r.repo_count : 0;
    }
    return row;
  });

  // Year of last row
  const lastYM = months[months.length - 1];

  return (
    <>
      <Topbar
        title="GitHub Tech Trends Dashboard"
        subtitle="A scalable big-data pipeline over the public GitHub dataset — language growth, forecasts, migration graphs, developer influence and community structure."
      />

      <div className="px-6 md:px-10 pb-12 space-y-6">
        {/* hero stats */}
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <StatCard
            label="Languages tracked"
            value="542"
            delta="Top-20 visualised across deliverables"
            icon={Languages}
            accent="purple"
          />
          <StatCard
            label="Monthly data points"
            value="53.6K"
            delta={`2015-01 → ${lastYM}`}
            icon={GitCommit}
            accent="mint"
          />
          <StatCard
            label="Developers (hashed)"
            value={fmtNum(d5Stats.total_developers)}
            delta={`${fmtNum(d5Stats.total_communities)} communities detected`}
            icon={Users2}
            accent="pink"
          />
          <StatCard
            label="Largest community"
            value={fmtNum(d5Stats.largest_community)}
            delta="Members in single cluster"
            icon={Network}
            accent="amber"
          />
        </div>

        {/* hero chart */}
        <ChartCard
          title="Top movers — monthly active repositories"
          subtitle="Repo count per month for Python, TypeScript, Rust and Go (the fastest-growing languages in the dataset)."
        >
          <div className="h-[340px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={data} margin={{ top: 10, right: 18, bottom: 0, left: 0 }}>
                <defs>
                  {HIGHLIGHT.map((lang) => {
                    const c = colorFor(lang, d1.top_langs);
                    return (
                      <linearGradient
                        key={lang}
                        id={`g-${lang}`}
                        x1="0" y1="0" x2="0" y2="1"
                      >
                        <stop offset="0%" stopColor={c} stopOpacity={0.45} />
                        <stop offset="95%" stopColor={c} stopOpacity={0.02} />
                      </linearGradient>
                    );
                  })}
                </defs>
                <CartesianGrid stroke="#1F2733" vertical={false} />
                <XAxis
                  dataKey="ym"
                  tick={{ fill: "#8B95A8", fontSize: 11 }}
                  tickLine={false}
                  axisLine={false}
                  minTickGap={50}
                />
                <YAxis
                  tick={{ fill: "#8B95A8", fontSize: 11 }}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(v) => fmtNum(Number(v))}
                />
                <Tooltip content={<ChartTooltip />} />
                {HIGHLIGHT.map((lang) => {
                  const c = colorFor(lang, d1.top_langs);
                  return (
                    <Area
                      key={lang}
                      type="monotone"
                      dataKey={lang}
                      name={lang}
                      stroke={c}
                      strokeWidth={2}
                      fill={`url(#g-${lang})`}
                      isAnimationActive
                    />
                  );
                })}
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>

        {/* deliverable cards */}
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <DeliverableCard
            to="/d1"
            n="D1"
            title="Language Growth Trends"
            icon={TrendingUp}
            desc="Monthly repo / commit / byte volume per language, normalised growth index, and K-Means clustering by trajectory shape."
            accent="#A78BFA"
          />
          <DeliverableCard
            to="/d2"
            n="D2"
            title="Adoption Forecasting"
            icon={LineIcon}
            desc="Per-language Holt-Winters and Prophet forecasts with 24-month horizon and confidence bands."
            accent="#5EEAD4"
          />
          <DeliverableCard
            to="/d3"
            n="D3"
            title="Migration Graph"
            icon={GitBranch}
            desc="Directed graph of tech transitions inferred from commit messages, Sankey flows and PageRank win/loss table."
            accent="#F472B6"
          />
          <DeliverableCard
            to="/d4"
            n="D4"
            title="Developer PageRank"
            icon={Users2}
            desc="Collaboration graph PageRank for the top 25K developers — hub and authority scores, degree, commit counts."
            accent="#FBBF24"
          />
          <DeliverableCard
            to="/d5"
            n="D5"
            title="Community Map"
            icon={Network}
            desc="Louvain communities with size vs influence scatter, top organisations per cluster, bridge developer ranking."
            accent="#60A5FA"
          />
          <PipelineCard />
        </div>
      </div>
    </>
  );
}

function DeliverableCard({
  to, n, title, desc, icon: Icon, accent,
}: {
  to: string;
  n: string;
  title: string;
  desc: string;
  icon: typeof TrendingUp;
  accent: string;
}) {
  return (
    <Link
      to={to}
      className="card card-pad group hover:border-mute transition-all relative overflow-hidden animate-fadeUp"
    >
      <div
        className="absolute -top-12 -right-12 size-40 rounded-full blur-3xl opacity-30 group-hover:opacity-50 transition-opacity"
        style={{ background: accent }}
      />
      <div className="relative">
        <div className="flex items-center justify-between mb-3">
          <span
            className="text-xs font-mono px-2 py-0.5 rounded-md border"
            style={{ color: accent, borderColor: accent + "55", background: accent + "11" }}
          >
            {n}
          </span>
          <Icon className="size-5" style={{ color: accent }} />
        </div>
        <h3 className="text-base font-semibold mb-1.5">{title}</h3>
        <p className="text-xs text-sub leading-relaxed">{desc}</p>
        <div className="flex items-center gap-1.5 text-xs font-medium mt-4 text-ink/80 group-hover:text-ink">
          Open
          <ArrowRight className="size-3.5 transition-transform group-hover:translate-x-0.5" />
        </div>
      </div>
    </Link>
  );
}

function PipelineCard() {
  return (
    <div className="card card-pad animate-fadeUp">
      <div className="label mb-3">Pipeline</div>
      <ol className="space-y-2.5 text-xs text-sub">
        <Step c="#A78BFA" t="BigQuery — public GitHub dataset queries (Q1–Q4)" />
        <Step c="#5EEAD4" t="GCS — Parquet snapshots, time window 2015–2025" />
        <Step c="#F472B6" t="Spark on Dataproc — 5 deliverables" />
        <Step c="#FBBF24" t="Parquet → JSON export script" />
        <Step c="#60A5FA" t="React + Recharts dashboard (this page)" />
      </ol>
    </div>
  );
}

function Step({ c, t }: { c: string; t: string }) {
  return (
    <li className="flex items-center gap-2.5">
      <span className="size-1.5 rounded-full" style={{ background: c }} />
      <span>{t}</span>
    </li>
  );
}
