import { useEffect, useState } from "react";
import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ZAxis,
  BarChart,
  Bar,
  Cell,
} from "recharts";
import { Users, GitFork, Network, Layers } from "lucide-react";

import Topbar from "@/components/Topbar";
import ChartCard from "@/components/ChartCard";
import StatCard from "@/components/StatCard";
import ChartTooltip from "@/components/Tooltip";
import Loading from "@/components/Loading";
import { loadD5, type D5Community, type D5BridgeDev } from "@/lib/data";
import { ACCENTS, fmtNum } from "@/lib/utils";

export default function D5Communities() {
  const [data, setData] = useState<{
    communities: D5Community[];
    bridge_devs: D5BridgeDev[];
    total_developers: number;
    total_communities: number;
    largest_community: number;
    singleton_communities: number;
  } | null>(null);

  useEffect(() => { loadD5().then(setData); }, []);
  if (!data) return <Loading />;

  const colorOf = (cid: number) => ACCENTS[cid % ACCENTS.length];
  const top10ByInfluence = [...data.communities]
    .sort((a, b) => b.max_pagerank - a.max_pagerank).slice(0, 10);

  return (
    <>
      <Topbar
        title="D5 · Developer Community Map"
        subtitle="Louvain communities detected on the developer collaboration graph, plus the bridge developers connecting them."
      />

      <div className="px-6 md:px-10 pb-12 space-y-6">
        <div className="grid md:grid-cols-2 xl:grid-cols-4 gap-4">
          <StatCard
            label="Communities"
            value={fmtNum(data.total_communities)}
            delta="Louvain modularity"
            icon={Layers}
            accent="purple"
          />
          <StatCard
            label="Developers clustered"
            value={fmtNum(data.total_developers)}
            delta="Hashed identifiers"
            icon={Users}
            accent="mint"
          />
          <StatCard
            label="Largest community"
            value={fmtNum(data.largest_community)}
            delta="Members in a single cluster"
            icon={Network}
            accent="pink"
          />
          <StatCard
            label="Singleton communities"
            value={fmtNum(data.singleton_communities)}
            delta="Lone developers, no in-cohort co-authoring"
            icon={GitFork}
            accent="amber"
          />
        </div>

        <ChartCard
          title="Community size vs influence"
          subtitle="X = members, Y = max PageRank in community. Bubble size = average degree. Identifies large, influential clusters vs small but powerful ones."
        >
          <div className="h-[420px]">
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart margin={{ top: 10, right: 14, bottom: 8, left: 0 }}>
                <CartesianGrid stroke="#1F2733" />
                <XAxis
                  type="number"
                  dataKey="size"
                  name="size"
                  tick={{ fill: "#8B95A8", fontSize: 11 }}
                  tickLine={false}
                  axisLine={false}
                />
                <YAxis
                  type="number"
                  dataKey="max_pagerank"
                  name="max PR"
                  tick={{ fill: "#8B95A8", fontSize: 11 }}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(v) => Number(v).toExponential(0)}
                />
                <ZAxis type="number" dataKey="avg_degree" range={[60, 320]} />
                <Tooltip
                  cursor={{ stroke: "#2A3344" }}
                  content={({ active, payload }) => {
                    if (!active || !payload?.[0]) return null;
                    const p = payload[0].payload as D5Community;
                    return (
                      <div className="rounded-xl border border-border bg-surface/95 px-3 py-2 shadow-glow text-xs space-y-1 max-w-[260px]">
                        <div className="text-ink font-medium">
                          Community #{p.community_id}
                        </div>
                        <div className="flex justify-between gap-4">
                          <span className="text-sub">Size</span>
                          <span className="font-mono">{fmtNum(p.size)}</span>
                        </div>
                        <div className="flex justify-between gap-4">
                          <span className="text-sub">Max PR</span>
                          <span className="font-mono">{p.max_pagerank.toExponential(2)}</span>
                        </div>
                        <div className="flex justify-between gap-4">
                          <span className="text-sub">Avg degree</span>
                          <span className="font-mono">{p.avg_degree.toFixed(1)}</span>
                        </div>
                        <div className="pt-1 text-sub border-t border-border">
                          {p.top_orgs}
                        </div>
                      </div>
                    );
                  }}
                />
                <Scatter
                  name="communities"
                  data={data.communities}
                  shape={(props: unknown) => {
                    const p = props as {
                      cx?: number;
                      cy?: number;
                      node?: { z?: number };
                      payload?: D5Community;
                    };
                    const cx = p.cx ?? 0;
                    const cy = p.cy ?? 0;
                    const r = Math.max(5, Math.sqrt((p.node?.z ?? 80)) * 0.7);
                    const c = colorOf(p.payload?.community_id ?? 0);
                    return (
                      <circle
                        cx={cx}
                        cy={cy}
                        r={r}
                        fill={c}
                        fillOpacity={0.65}
                        stroke={c}
                      />
                    );
                  }}
                />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>

        <div className="grid xl:grid-cols-5 gap-6">
          <ChartCard
            className="xl:col-span-3"
            title="Top communities by member count"
            subtitle="Each tile shows the dominant GitHub organisations in the cluster — extracted from `repo_name` prefixes."
          >
            <div className="grid sm:grid-cols-2 gap-3">
              {data.communities
                .slice()
                .sort((a, b) => b.size - a.size)
                .slice(0, 12)
                .map((c) => (
                  <div
                    key={c.community_id}
                    className="rounded-xl bg-surface/60 border border-border p-3.5 hover:border-mute transition-colors"
                  >
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <span
                          className="size-2.5 rounded-full"
                          style={{ background: colorOf(c.community_id) }}
                        />
                        <span className="text-sm font-medium text-ink">
                          Community #{c.community_id}
                        </span>
                      </div>
                      <span className="font-mono text-xs text-sub">
                        {fmtNum(c.size)} devs
                      </span>
                    </div>
                    <div className="text-xs text-sub leading-relaxed line-clamp-2">
                      {c.top_orgs}
                    </div>
                    <div className="flex gap-3 mt-2.5 text-[11px] text-sub">
                      <span>
                        max PR{" "}
                        <span className="font-mono text-ink">
                          {c.max_pagerank.toExponential(1)}
                        </span>
                      </span>
                      <span>
                        avg deg{" "}
                        <span className="font-mono text-ink">
                          {c.avg_degree.toFixed(1)}
                        </span>
                      </span>
                    </div>
                  </div>
                ))}
            </div>
          </ChartCard>

          <ChartCard
            className="xl:col-span-2"
            title="Top 10 — max PageRank"
            subtitle="Communities ranked by their single most influential member."
          >
            <div className="h-[420px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={top10ByInfluence.map((c) => ({
                    name: `#${c.community_id}`,
                    pr: c.max_pagerank,
                  }))}
                  layout="vertical"
                  margin={{ top: 5, right: 14, bottom: 0, left: 0 }}
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
                    tick={{ fill: "#E4E8F1", fontSize: 11 }}
                    tickLine={false}
                    axisLine={false}
                    width={56}
                  />
                  <Tooltip
                    content={(p) => (
                      <ChartTooltip
                        {...p}
                        valueFormatter={(v) => Number(v).toExponential(2)}
                      />
                    )}
                  />
                  <Bar dataKey="pr" radius={[0, 6, 6, 0]}>
                    {top10ByInfluence.map((c, i) => (
                      <Cell key={i} fill={colorOf(c.community_id)} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>
        </div>

        <ChartCard
          title="Bridge developers"
          subtitle="Highest-degree developers — they collaborate across many communities and act as connectors."
        >
          <div className="grid md:grid-cols-2 xl:grid-cols-4 gap-3">
            {data.bridge_devs.slice(0, 20).map((d, i) => (
              <div
                key={i}
                className="rounded-xl bg-surface/50 border border-border p-3 flex items-center gap-3"
              >
                <div
                  className="size-9 rounded-lg grid place-items-center font-mono text-[10px] shrink-0"
                  style={{
                    background: colorOf(d.community_id) + "22",
                    color: colorOf(d.community_id),
                  }}
                >
                  #{d.community_id}
                </div>
                <div className="min-w-0">
                  <div className="text-xs font-mono text-ink truncate">{d.dev_short}…</div>
                  <div className="text-[11px] text-sub flex gap-2 mt-0.5">
                    <span>deg {d.degree}</span>
                    <span>pr {d.pagerank.toExponential(1)}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </ChartCard>
      </div>
    </>
  );
}
