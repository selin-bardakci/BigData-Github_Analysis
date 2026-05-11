import { useEffect, useMemo, useState } from "react";
import { sankey, sankeyLinkHorizontal, sankeyJustify } from "d3-sankey";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from "recharts";
import { ArrowRight, GitBranch } from "lucide-react";

import Topbar from "@/components/Topbar";
import ChartCard from "@/components/ChartCard";
import ChartTooltip from "@/components/Tooltip";
import Loading from "@/components/Loading";
import { loadD3, type D3Edge, type D3Node } from "@/lib/data";
import { cn, colorFor, fmtNum } from "@/lib/utils";

const WINDOWS = [
  { id: "full", label: "Full (2015 → 2024)", filter: () => true },
  { id: "early", label: "2015 – 2018",
    filter: (e: D3Edge) => e.first_year <= 2018 },
  { id: "mid", label: "2018 – 2021",
    filter: (e: D3Edge) => e.last_year >= 2018 && e.first_year <= 2021 },
  { id: "late", label: "2021 – 2024",
    filter: (e: D3Edge) => e.last_year >= 2021 },
];

export default function D3Migration() {
  const [data, setData] = useState<{ edges: D3Edge[]; nodes: D3Node[] } | null>(null);
  const [windowId, setWindowId] = useState("full");

  useEffect(() => { loadD3().then(setData); }, []);

  const filtered = useMemo(() => {
    if (!data) return null;
    const w = WINDOWS.find((x) => x.id === windowId)!;
    return data.edges.filter(w.filter);
  }, [data, windowId]);

  if (!data || !filtered) return <Loading />;

  return (
    <>
      <Topbar
        title="D3 · Technology Migration Graph"
        subtitle="Directed transitions between technologies inferred from commit messages (e.g. 'migrate from mocha to jest'). Edge weight = migration count."
      />

      <div className="px-6 md:px-10 pb-12 space-y-6">
        {/* window selector */}
        <div className="card card-pad flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="label mb-2">Time window</div>
            <div className="flex flex-wrap gap-1.5">
              {WINDOWS.map((w) => (
                <button
                  key={w.id}
                  onClick={() => setWindowId(w.id)}
                  className={cn("btn text-xs", windowId === w.id && "btn-primary")}
                >
                  {w.label}
                </button>
              ))}
            </div>
          </div>
          <div className="text-xs text-sub max-w-md">
            <p>
              Pre-computed windows — each was aggregated once by Spark, so switching
              filters is instant. No re-computation needed.
            </p>
          </div>
        </div>

        {/* sankey */}
        <ChartCard
          title="Migration Sankey"
          subtitle="Source (left) → target (right). Hover edges to inspect counts and time-spans."
          right={<GitBranch className="size-4 text-sub" />}
        >
          <SankeyChart edges={filtered} />
        </ChartCard>

        {/* PageRank-style win/loss */}
        <div className="grid lg:grid-cols-5 gap-6">
          <ChartCard
            className="lg:col-span-3"
            title="Net flow — winners vs losers"
            subtitle="Net flow = incoming − outgoing migrations. Positive bars are technologies people moved toward."
          >
            <NetFlowChart nodes={data.nodes} />
          </ChartCard>

          <ChartCard
            className="lg:col-span-2"
            title="Top migrations"
            subtitle={`${filtered.length} edges in selected window.`}
          >
            <div className="space-y-1.5">
              {filtered
                .slice()
                .sort((a, b) => b.migration_count - a.migration_count)
                .slice(0, 10)
                .map((e, i) => (
                  <div
                    key={i}
                    className="flex items-center gap-2 px-3 py-2 rounded-lg bg-surface/60 border border-border hover:border-mute transition-colors"
                  >
                    <span className="text-xs text-sub w-4">{i + 1}.</span>
                    <span
                      className="text-xs px-2 py-0.5 rounded font-mono"
                      style={{
                        background: colorFor(e.from_tech) + "22",
                        color: colorFor(e.from_tech),
                      }}
                    >
                      {e.from_tech}
                    </span>
                    <ArrowRight className="size-3 text-sub shrink-0" />
                    <span
                      className="text-xs px-2 py-0.5 rounded font-mono"
                      style={{
                        background: colorFor(e.to_tech) + "22",
                        color: colorFor(e.to_tech),
                      }}
                    >
                      {e.to_tech}
                    </span>
                    <span className="ml-auto text-xs font-mono text-ink">
                      {fmtNum(e.migration_count)}
                    </span>
                  </div>
                ))}
            </div>
          </ChartCard>
        </div>
      </div>
    </>
  );
}

/**
 * d3-sankey requires a DAG. Real migration data has cycles
 * (e.g. yarn → npm AND npm → yarn). Greedy break: add edges in
 * descending weight order, skip any that would create a cycle.
 */
function breakCycles(edges: D3Edge[]): D3Edge[] {
  const sorted = [...edges].sort((a, b) => b.migration_count - a.migration_count);
  const accepted: D3Edge[] = [];

  function hasCycle(eds: D3Edge[]): boolean {
    const adj = new Map<string, string[]>();
    const nodes = new Set<string>();
    for (const e of eds) {
      nodes.add(e.from_tech);
      nodes.add(e.to_tech);
      if (!adj.has(e.from_tech)) adj.set(e.from_tech, []);
      adj.get(e.from_tech)!.push(e.to_tech);
    }
    const WHITE = 0, GRAY = 1, BLACK = 2;
    const state = new Map<string, number>();
    for (const n of nodes) state.set(n, WHITE);

    function dfs(n: string): boolean {
      state.set(n, GRAY);
      for (const nx of adj.get(n) ?? []) {
        if (state.get(nx) === GRAY) return true;
        if (state.get(nx) === WHITE && dfs(nx)) return true;
      }
      state.set(n, BLACK);
      return false;
    }
    for (const n of nodes) {
      if (state.get(n) === WHITE && dfs(n)) return true;
    }
    return false;
  }

  for (const e of sorted) {
    accepted.push(e);
    if (hasCycle(accepted)) accepted.pop();
  }
  return accepted;
}

function SankeyChart({ edges }: { edges: D3Edge[] }) {
  const { width, ref } = useResizeWidth();
  const height = 460;

  const layout = useMemo(() => {
    if (!width) return null;
    const cleaned = breakCycles(edges);
    if (cleaned.length === 0) return null;

    // Build nodes from edges
    const nameSet = new Set<string>();
    cleaned.forEach((e) => { nameSet.add(e.from_tech); nameSet.add(e.to_tech); });
    const names = [...nameSet];

    const nodes = names.map((n) => ({ name: n }));
    const links = cleaned.map((e) => ({
      source: names.indexOf(e.from_tech),
      target: names.indexOf(e.to_tech),
      value: e.migration_count,
      years_active: e.years_active,
    }));

    const generator = sankey<{ name: string }, { years_active: number }>()
      .nodeWidth(14)
      .nodePadding(14)
      .nodeAlign(sankeyJustify)
      .extent([[10, 10], [width - 10, height - 10]]);

    return generator({
      nodes: nodes.map((d) => ({ ...d })),
      links: links.map((d) => ({ ...d })),
    });
  }, [edges, width]);

  return (
    <div ref={ref} className="w-full" style={{ height }}>
      {!layout && (
        <div className="h-full grid place-items-center text-sub text-sm">
          No migrations in selected window.
        </div>
      )}
      {layout && (
        <svg width={width} height={height}>
          <defs>
            {layout.links.map((l, i) => {
              const src = (l.source as { name: string }).name;
              const tgt = (l.target as { name: string }).name;
              return (
                <linearGradient
                  key={i}
                  id={`sankey-${i}`}
                  gradientUnits="userSpaceOnUse"
                  x1={(l.source as { x1: number }).x1}
                  x2={(l.target as { x0: number }).x0}
                >
                  <stop offset="0%" stopColor={colorFor(src)} stopOpacity={0.55} />
                  <stop offset="100%" stopColor={colorFor(tgt)} stopOpacity={0.55} />
                </linearGradient>
              );
            })}
          </defs>
          <g>
            {layout.links.map((l, i) => (
              <path
                key={i}
                d={sankeyLinkHorizontal()(l) ?? undefined}
                fill="none"
                stroke={`url(#sankey-${i})`}
                strokeWidth={Math.max(1.5, l.width ?? 0)}
                className="hover:opacity-100 opacity-80 transition-opacity"
              >
                <title>
                  {(l.source as { name: string }).name} → {(l.target as { name: string }).name}: {l.value}
                </title>
              </path>
            ))}
          </g>
          <g>
            {layout.nodes.map((n, i) => {
              const c = colorFor(n.name);
              return (
                <g key={i}>
                  <rect
                    x={n.x0}
                    y={n.y0}
                    width={(n.x1 ?? 0) - (n.x0 ?? 0)}
                    height={(n.y1 ?? 0) - (n.y0 ?? 0)}
                    fill={c}
                    rx={2}
                  />
                  <text
                    x={(n.x0 ?? 0) < (width ?? 0) / 2 ? (n.x1 ?? 0) + 6 : (n.x0 ?? 0) - 6}
                    y={((n.y0 ?? 0) + (n.y1 ?? 0)) / 2}
                    textAnchor={(n.x0 ?? 0) < (width ?? 0) / 2 ? "start" : "end"}
                    alignmentBaseline="middle"
                    fontSize={11}
                    fill="#E4E8F1"
                    className="font-mono"
                  >
                    {n.name}
                  </text>
                </g>
              );
            })}
          </g>
        </svg>
      )}
    </div>
  );
}

function NetFlowChart({ nodes }: { nodes: D3Node[] }) {
  const data = nodes
    .slice()
    .sort((a, b) => b.net_flow - a.net_flow)
    .map((n) => ({ tech: n.tech, net: n.net_flow }));
  return (
    <div className="h-[360px]">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} layout="vertical" margin={{ top: 8, right: 14, bottom: 0, left: 0 }}>
          <CartesianGrid stroke="#1F2733" horizontal={false} />
          <XAxis
            type="number"
            tick={{ fill: "#8B95A8", fontSize: 11 }}
            tickLine={false}
            axisLine={false}
          />
          <YAxis
            type="category"
            dataKey="tech"
            tick={{ fill: "#E4E8F1", fontSize: 11 }}
            tickLine={false}
            axisLine={false}
            width={92}
          />
          <Tooltip content={<ChartTooltip />} />
          <Bar dataKey="net" radius={[0, 6, 6, 0]}>
            {data.map((d, i) => (
              <Cell key={i} fill={d.net >= 0 ? "#34D399" : "#F87171"} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// hook: track container width for SVG sizing
function useResizeWidth() {
  const [width, setWidth] = useState(0);
  const [el, setEl] = useState<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      for (const ent of entries) setWidth(ent.contentRect.width);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [el]);

  return { width, ref: setEl };
}
