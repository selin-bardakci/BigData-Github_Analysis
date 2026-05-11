import { useEffect, useMemo, useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ScatterChart,
  Scatter,
  ZAxis,
  Cell,
} from "recharts";
import { Search, Hash, Users2 } from "lucide-react";

import Topbar from "@/components/Topbar";
import ChartCard from "@/components/ChartCard";
import StatCard from "@/components/StatCard";
import ChartTooltip from "@/components/Tooltip";
import Loading from "@/components/Loading";
import { loadD4, type D4Dev } from "@/lib/data";
import { fmtNum } from "@/lib/utils";

export default function D4PageRank() {
  const [devs, setDevs] = useState<D4Dev[] | null>(null);
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<"pagerank" | "hub_score" | "degree" | "total_commits">(
    "pagerank",
  );

  useEffect(() => { loadD4().then(setDevs); }, []);

  const filtered = useMemo(() => {
    if (!devs) return [];
    const q = query.toLowerCase();
    const f = q ? devs.filter((d) => d.dev_short.toLowerCase().includes(q)) : devs;
    return [...f].sort((a, b) => (b[sort] as number) - (a[sort] as number));
  }, [devs, query, sort]);

  if (!devs) return <Loading />;

  const top20 = devs.slice().sort((a, b) => b.pagerank - a.pagerank).slice(0, 20);
  const meanPR = devs.reduce((s, d) => s + d.pagerank, 0) / devs.length;
  const maxPR = Math.max(...devs.map((d) => d.pagerank));
  const totalCommits = devs.reduce((s, d) => s + d.total_commits, 0);

  return (
    <>
      <Topbar
        title="D4 · Developer Influence (PageRank)"
        subtitle="Spark GraphFrames PageRank over the developer-developer collaboration graph. Hashed IDs preserve developer privacy."
      />

      <div className="px-6 md:px-10 pb-12 space-y-6">
        <div className="grid md:grid-cols-2 xl:grid-cols-4 gap-4">
          <StatCard
            label="Developers"
            value={fmtNum(25_000)}
            delta="Top-25K by total commits"
            icon={Users2}
            accent="purple"
          />
          <StatCard
            label="Top PageRank"
            value={maxPR.toExponential(2)}
            delta={`dev ${top20[0]?.dev_short}…`}
            icon={Hash}
            accent="mint"
          />
          <StatCard
            label="Mean PageRank"
            value={meanPR.toExponential(2)}
            delta="Long-tail distribution"
            accent="pink"
          />
          <StatCard
            label="Total commits"
            value={fmtNum(totalCommits)}
            delta="Across sampled cohort"
            accent="amber"
          />
        </div>

        <div className="grid lg:grid-cols-5 gap-6">
          <ChartCard
            className="lg:col-span-3"
            title="Top 20 developers by PageRank"
            subtitle="Higher PageRank = more central in the collaboration graph (worked with many other influential developers)."
          >
            <div className="h-[440px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={top20.map((d) => ({ name: d.dev_short, pagerank: d.pagerank }))}
                  layout="vertical"
                  margin={{ top: 8, right: 18, bottom: 0, left: 0 }}
                >
                  <CartesianGrid stroke="#1F2733" horizontal={false} />
                  <XAxis
                    type="number"
                    tick={{ fill: "#8B95A8", fontSize: 11 }}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(v) => Number(v).toExponential(0)}
                  />
                  <YAxis
                    type="category"
                    dataKey="name"
                    tick={{ fill: "#E4E8F1", fontSize: 10, fontFamily: "JetBrains Mono" }}
                    tickLine={false}
                    axisLine={false}
                    width={110}
                  />
                  <Tooltip
                    content={(p) => (
                      <ChartTooltip
                        {...p}
                        valueFormatter={(v) => Number(v).toExponential(2)}
                      />
                    )}
                  />
                  <Bar dataKey="pagerank" radius={[0, 6, 6, 0]}>
                    {top20.map((_, i) => (
                      <Cell key={i} fill={`hsl(${260 - i * 6}, 65%, ${70 - i * 1.2}%)`} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>

          <ChartCard
            className="lg:col-span-2"
            title="Hub vs Authority"
            subtitle="HITS algorithm scores. Hubs link to many authoritative developers; authorities are linked to by many hubs. Bubble size = degree."
          >
            <div className="h-[440px]">
              <ResponsiveContainer width="100%" height="100%">
                <ScatterChart margin={{ top: 10, right: 14, bottom: 8, left: 0 }}>
                  <CartesianGrid stroke="#1F2733" />
                  <XAxis
                    type="number"
                    dataKey="hub_score"
                    name="hub"
                    tick={{ fill: "#8B95A8", fontSize: 11 }}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(v) => Number(v).toExponential(0)}
                  />
                  <YAxis
                    type="number"
                    dataKey="auth_score"
                    name="auth"
                    tick={{ fill: "#8B95A8", fontSize: 11 }}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(v) => Number(v).toExponential(0)}
                  />
                  <ZAxis type="number" dataKey="degree" range={[40, 320]} />
                  <Tooltip
                    cursor={{ stroke: "#2A3344" }}
                    content={({ active, payload }) => {
                      if (!active || !payload?.[0]) return null;
                      const p = payload[0].payload as D4Dev;
                      return (
                        <div className="rounded-xl border border-border bg-surface/95 px-3 py-2 shadow-glow text-xs space-y-1">
                          <div className="font-mono text-ink">{p.dev_short}…</div>
                          <Row k="PageRank" v={p.pagerank.toExponential(2)} />
                          <Row k="Hub" v={p.hub_score.toExponential(2)} />
                          <Row k="Auth" v={p.auth_score.toExponential(2)} />
                          <Row k="Degree" v={String(p.degree)} />
                          <Row k="Commits" v={fmtNum(p.total_commits)} />
                        </div>
                      );
                    }}
                  />
                  <Scatter name="developers" data={devs} fill="#A78BFA" fillOpacity={0.55} />
                </ScatterChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>
        </div>

        <ChartCard
          title="Developer ranking"
          subtitle="Sortable / searchable ranking. Dev IDs are SHA-256 hashes of commit emails (privacy-preserving)."
          right={
            <div className="flex items-center gap-2">
              <div className="relative">
                <Search className="size-3.5 text-sub absolute left-2.5 top-1/2 -translate-y-1/2" />
                <input
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Search hash…"
                  className="pl-8 pr-3 py-1.5 rounded-lg text-xs bg-surface border border-border focus:outline-none focus:border-accent/60 placeholder:text-sub w-44 font-mono"
                />
              </div>
            </div>
          }
        >
          <div className="overflow-auto max-h-[480px] -mx-2">
            <table className="w-full text-xs">
              <thead className="text-sub sticky top-0 bg-card">
                <tr className="border-b border-border">
                  <Th>#</Th>
                  <Th>Dev ID</Th>
                  <Th sortable active={sort === "pagerank"} onClick={() => setSort("pagerank")}>
                    PageRank
                  </Th>
                  <Th sortable active={sort === "hub_score"} onClick={() => setSort("hub_score")}>
                    Hub
                  </Th>
                  <Th>Auth</Th>
                  <Th sortable active={sort === "degree"} onClick={() => setSort("degree")}>
                    Degree
                  </Th>
                  <Th sortable active={sort === "total_commits"} onClick={() => setSort("total_commits")}>
                    Commits
                  </Th>
                </tr>
              </thead>
              <tbody>
                {filtered.slice(0, 100).map((d, i) => (
                  <tr key={i} className="border-b border-border/50 hover:bg-mute/20">
                    <td className="py-2 px-3 text-sub font-mono">{i + 1}</td>
                    <td className="py-2 px-3 font-mono text-ink">{d.dev_short}…</td>
                    <td className="py-2 px-3 font-mono text-accent">{d.pagerank.toExponential(2)}</td>
                    <td className="py-2 px-3 font-mono">{d.hub_score.toExponential(2)}</td>
                    <td className="py-2 px-3 font-mono">{d.auth_score.toExponential(2)}</td>
                    <td className="py-2 px-3 font-mono">{d.degree}</td>
                    <td className="py-2 px-3 font-mono">{fmtNum(d.total_commits)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </ChartCard>
      </div>
    </>
  );
}

function Row({ k, v }: { k: string; v: string }) {
  return (
    <div className="flex justify-between gap-6">
      <span className="text-sub">{k}</span>
      <span className="font-mono text-ink">{v}</span>
    </div>
  );
}

function Th({
  children, sortable, active, onClick,
}: {
  children: React.ReactNode;
  sortable?: boolean;
  active?: boolean;
  onClick?: () => void;
}) {
  return (
    <th
      onClick={onClick}
      className={[
        "py-2 px-3 text-left text-[11px] font-medium uppercase tracking-wider",
        sortable && "cursor-pointer hover:text-ink",
        active && "text-accent",
      ].filter(Boolean).join(" ")}
    >
      {children}
    </th>
  );
}
