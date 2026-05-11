import { useEffect, useMemo, useState } from "react";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ScatterChart,
  Scatter,
  ZAxis,
  BarChart,
  Bar,
  Cell,
} from "recharts";
import { ArrowDownUp, BarChart3, LayoutGrid } from "lucide-react";

import Topbar from "@/components/Topbar";
import ChartCard from "@/components/ChartCard";
import LangChips from "@/components/LangChips";
import RangeSlider from "@/components/RangeSlider";
import ChartTooltip from "@/components/Tooltip";
import Loading from "@/components/Loading";
import { loadD1, loadD1Clusters, type D1Cluster, type D1Row } from "@/lib/data";
import { cn, colorFor, fmtNum } from "@/lib/utils";

const CLUSTER_NAMES = [
  "Explosive growth",
  "Strong growth",
  "Stable / moderate",
  "Declining",
];
const CLUSTER_COLORS = ["#A78BFA", "#5EEAD4", "#FBBF24", "#F87171"];

export default function D1Trends() {
  const [d1, setD1] = useState<{ top_langs: string[]; rows: D1Row[] } | null>(null);
  const [clusters, setClusters] = useState<D1Cluster[] | null>(null);
  const [selected, setSelected] = useState<string[]>([
    "python", "typescript", "rust", "go", "javascript", "java",
  ]);
  const [yearRange, setYearRange] = useState<[number, number]>([2015, 2025]);
  const [yScale, setYScale] = useState<"linear" | "log">("linear");
  const [normalize, setNormalize] = useState(false);
  const [metric, setMetric] = useState<"repo_count" | "commit_count" | "total_bytes">(
    "repo_count",
  );

  useEffect(() => {
    loadD1().then(setD1);
    loadD1Clusters().then(setClusters);
  }, []);

  const series = useMemo(() => {
    if (!d1) return [];
    const months = Array.from(new Set(d1.rows.map((r) => r.year_month))).sort();
    const filtered = months.filter((ym) => {
      const y = +ym.slice(0, 4);
      return y >= yearRange[0] && y <= yearRange[1];
    });

    // baseline (for normalization) = first month within filter for each lang
    const baseline: Record<string, number> = {};
    if (normalize) {
      for (const lang of selected) {
        const first = filtered
          .map((ym) => d1.rows.find((x) => x.language === lang && x.year_month === ym))
          .find((r) => r);
        baseline[lang] = first?.[metric] ?? 1;
      }
    }

    return filtered.map((ym) => {
      const row: Record<string, number | string> = { ym };
      for (const lang of selected) {
        const r = d1.rows.find((x) => x.language === lang && x.year_month === ym);
        const v = r ? (r[metric] as number) : 0;
        row[lang] = normalize && baseline[lang] ? (v / baseline[lang]) * 100 : v;
      }
      return row;
    });
  }, [d1, selected, yearRange, normalize, metric]);

  if (!d1 || !clusters) return <Loading />;

  // Cluster scatter: simple growth × volatility view
  const scatterByCluster = Array.from({ length: 4 }).map((_, c) =>
    clusters
      .filter((x) => x.cluster === c)
      .map((x) => ({
        x: Math.log10(Math.max(1, x.avg_repo_count)),
        y: x.avg_repo_growth,
        z: x.commit_volatility * 100,
        language: x.language,
      })),
  );

  // Latest month — top languages by total_bytes
  const months = Array.from(new Set(d1.rows.map((r) => r.year_month))).sort();
  const lastYM = months[months.length - 1];
  const byVolume = d1.rows
    .filter((r) => r.year_month === lastYM)
    .sort((a, b) => b.total_bytes - a.total_bytes)
    .slice(0, 10);

  return (
    <>
      <Topbar
        title="D1 · Language Growth Trends"
        subtitle="Monthly active repositories, commit volume and code byte volume per language, plus a K-Means clustering by trajectory shape."
      />

      <div className="px-6 md:px-10 pb-12 space-y-6">
        {/* controls */}
        <div className="card card-pad space-y-5">
          <div className="flex flex-wrap items-center gap-4">
            <div className="flex-1 min-w-[260px]">
              <div className="label mb-2.5">Languages ({selected.length}/12)</div>
              <LangChips
                options={d1.top_langs}
                selected={selected}
                onToggle={(l) =>
                  setSelected((s) =>
                    s.includes(l) ? s.filter((x) => x !== l) : [...s, l].slice(0, 12),
                  )
                }
              />
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => setSelected(["python", "typescript", "rust", "go"])}
                className="btn"
              >
                Top movers
              </button>
              <button
                onClick={() => setSelected(d1.top_langs.slice(0, 10))}
                className="btn"
              >
                Top 10
              </button>
              <button onClick={() => setSelected([])} className="btn">
                Clear
              </button>
            </div>
          </div>

          <div className="grid md:grid-cols-3 gap-5">
            <div>
              <RangeSlider
                min={2015}
                max={2025}
                value={yearRange}
                onChange={setYearRange}
                label="Year range"
              />
            </div>
            <div>
              <div className="label mb-2.5">Metric</div>
              <div className="flex gap-1.5">
                <SegBtn active={metric === "repo_count"} onClick={() => setMetric("repo_count")}>
                  Repos
                </SegBtn>
                <SegBtn active={metric === "commit_count"} onClick={() => setMetric("commit_count")}>
                  Commits
                </SegBtn>
                <SegBtn active={metric === "total_bytes"} onClick={() => setMetric("total_bytes")}>
                  Bytes
                </SegBtn>
              </div>
            </div>
            <div>
              <div className="label mb-2.5">View options</div>
              <div className="flex gap-1.5">
                <SegBtn active={yScale === "linear"} onClick={() => setYScale("linear")}>
                  Linear
                </SegBtn>
                <SegBtn active={yScale === "log"} onClick={() => setYScale("log")}>
                  Log
                </SegBtn>
                <SegBtn
                  active={normalize}
                  onClick={() => setNormalize((n) => !n)}
                  icon={<ArrowDownUp className="size-3.5" />}
                >
                  Index = 100
                </SegBtn>
              </div>
            </div>
          </div>
        </div>

        {/* time series */}
        <ChartCard
          title={normalize ? "Normalised growth index (base = 100)" : "Monthly time-series"}
          subtitle={
            normalize
              ? "Each language indexed to 100 at the start of the selected range — pure shape comparison."
              : "Raw values per month — toggle log scale for orders-of-magnitude comparison."
          }
        >
          <div className="h-[420px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={series} margin={{ top: 8, right: 14, bottom: 0, left: 0 }}>
                <CartesianGrid stroke="#1F2733" vertical={false} />
                <XAxis
                  dataKey="ym"
                  tick={{ fill: "#8B95A8", fontSize: 11 }}
                  tickLine={false}
                  axisLine={false}
                  minTickGap={48}
                />
                <YAxis
                  tick={{ fill: "#8B95A8", fontSize: 11 }}
                  tickLine={false}
                  axisLine={false}
                  scale={yScale}
                  domain={yScale === "log" ? [1, "auto"] : ["auto", "auto"]}
                  tickFormatter={(v) => fmtNum(Number(v))}
                />
                <Tooltip content={<ChartTooltip />} />
                <Legend
                  wrapperStyle={{ fontSize: 12, paddingTop: 12 }}
                  iconType="circle"
                  formatter={(value) => <span className="text-sub">{value}</span>}
                />
                {selected.map((lang) => (
                  <Line
                    key={lang}
                    type="monotone"
                    dataKey={lang}
                    stroke={colorFor(lang, d1.top_langs)}
                    strokeWidth={2}
                    dot={false}
                    activeDot={{ r: 4 }}
                    isAnimationActive={false}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </ChartCard>

        {/* cluster + bar grid */}
        <div className="grid xl:grid-cols-5 gap-6">
          <ChartCard
            className="xl:col-span-3"
            title="K-Means clusters — log(repo count) × avg growth"
            subtitle="4 clusters identified by Spark's MLlib K-Means on growth, volatility and volume features. Bubble size = commit volatility."
            right={<LayoutGrid className="size-4 text-sub" />}
          >
            <div className="h-[360px]">
              <ResponsiveContainer width="100%" height="100%">
                <ScatterChart margin={{ top: 10, right: 14, bottom: 8, left: 0 }}>
                  <CartesianGrid stroke="#1F2733" />
                  <XAxis
                    type="number"
                    dataKey="x"
                    name="log10(avg repos)"
                    tick={{ fill: "#8B95A8", fontSize: 11 }}
                    tickLine={false}
                    axisLine={false}
                  />
                  <YAxis
                    type="number"
                    dataKey="y"
                    name="growth"
                    tick={{ fill: "#8B95A8", fontSize: 11 }}
                    tickLine={false}
                    axisLine={false}
                  />
                  <ZAxis type="number" dataKey="z" range={[60, 320]} />
                  <Tooltip
                    cursor={{ stroke: "#2A3344" }}
                    content={({ active, payload }) => {
                      if (!active || !payload?.[0]) return null;
                      const p = payload[0].payload as {
                        language: string; x: number; y: number; z: number;
                      };
                      return (
                        <div className="rounded-xl border border-border bg-surface/95 backdrop-blur px-3 py-2 shadow-glow text-xs">
                          <div className="font-medium text-ink">{p.language}</div>
                          <div className="text-sub mt-1">
                            log10 repos: <span className="font-mono text-ink">{p.x.toFixed(2)}</span>
                          </div>
                          <div className="text-sub">
                            growth: <span className="font-mono text-ink">{(p.y * 100).toFixed(1)}%</span>
                          </div>
                          <div className="text-sub">
                            volatility: <span className="font-mono text-ink">{p.z.toFixed(1)}</span>
                          </div>
                        </div>
                      );
                    }}
                  />
                  {scatterByCluster.map((data, c) => (
                    <Scatter
                      key={c}
                      name={CLUSTER_NAMES[c]}
                      data={data}
                      fill={CLUSTER_COLORS[c]}
                      fillOpacity={0.75}
                    />
                  ))}
                  <Legend
                    wrapperStyle={{ fontSize: 11, paddingTop: 10 }}
                    iconType="circle"
                    formatter={(v) => <span className="text-sub">{v}</span>}
                  />
                </ScatterChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>

          <ChartCard
            className="xl:col-span-2"
            title="Top 10 by code volume"
            subtitle={`Total bytes per language in ${lastYM}.`}
            right={<BarChart3 className="size-4 text-sub" />}
          >
            <div className="h-[360px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={byVolume.map((r) => ({
                    name: r.language,
                    gb: r.total_bytes / 1e9,
                  }))}
                  layout="vertical"
                  margin={{ top: 5, right: 14, bottom: 0, left: 8 }}
                >
                  <CartesianGrid stroke="#1F2733" horizontal={false} />
                  <XAxis
                    type="number"
                    tick={{ fill: "#8B95A8", fontSize: 11 }}
                    tickLine={false}
                    axisLine={false}
                    tickFormatter={(v) => `${fmtNum(Number(v))}GB`}
                  />
                  <YAxis
                    type="category"
                    dataKey="name"
                    tick={{ fill: "#E4E8F1", fontSize: 11 }}
                    tickLine={false}
                    axisLine={false}
                    width={92}
                  />
                  <Tooltip
                    content={(p) => (
                      <ChartTooltip {...p} valueFormatter={(v) => `${v.toFixed(1)} GB`} />
                    )}
                  />
                  <Bar dataKey="gb" radius={[0, 6, 6, 0]}>
                    {byVolume.map((r, i) => (
                      <Cell key={i} fill={colorFor(r.language, d1.top_langs)} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>
        </div>

        {/* cluster profile table */}
        <ChartCard
          title="Cluster membership"
          subtitle="Languages grouped by their growth trajectory and volatility characteristics."
        >
          <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4">
            {[0, 1, 2, 3].map((c) => {
              const langs = clusters.filter((x) => x.cluster === c);
              return (
                <div key={c} className="rounded-xl bg-surface/60 border border-border p-4">
                  <div className="flex items-center gap-2 mb-2.5">
                    <span
                      className="size-2.5 rounded-full"
                      style={{ background: CLUSTER_COLORS[c] }}
                    />
                    <span className="text-sm font-semibold">{CLUSTER_NAMES[c]}</span>
                    <span className="ml-auto text-xs text-sub font-mono">
                      {langs.length}
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-1.5">
                    {langs.map((l) => (
                      <span
                        key={l.language}
                        className="text-xs px-2 py-0.5 rounded-md bg-mute/40 text-ink"
                      >
                        {l.language}
                      </span>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </ChartCard>
      </div>
    </>
  );
}

function SegBtn({
  active, onClick, children, icon,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
  icon?: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "btn text-xs",
        active ? "btn-primary" : "",
      )}
    >
      {icon}
      {children}
    </button>
  );
}
