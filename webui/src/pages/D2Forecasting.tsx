import { useEffect, useMemo, useState } from "react";
import {
  ResponsiveContainer,
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
} from "recharts";
import { ChevronDown, TrendingUp } from "lucide-react";

import Topbar from "@/components/Topbar";
import ChartCard from "@/components/ChartCard";
import ChartTooltip from "@/components/Tooltip";
import Loading from "@/components/Loading";
import { loadD2, type D2Row } from "@/lib/data";
import { colorFor, fmtNum } from "@/lib/utils";

const TOP_FORECAST = ["typescript", "python", "go", "c#", "rust", "java"];

export default function D2Forecasting() {
  const [rows, setRows] = useState<D2Row[] | null>(null);
  const [lang, setLang] = useState<string>("typescript");
  const [open, setOpen] = useState(false);

  useEffect(() => {
    loadD2().then(setRows);
  }, []);

  const langs = useMemo(() => {
    if (!rows) return [];
    return Array.from(new Set(rows.map((r) => r.language))).sort();
  }, [rows]);

  const series = useMemo(() => {
    if (!rows) return [];
    return rows
      .filter((r) => r.language === lang)
      .sort((a, b) => a.year_month.localeCompare(b.year_month))
      .map((r) => ({
        ym: r.year_month,
        actual: r.actual ?? null,
        yhat: r.arima_yhat ?? r.prophet_yhat ?? null,
        lower: r.prophet_lower ?? null,
        upper: r.prophet_upper ?? null,
        is_forecast: r.is_forecast,
      }));
  }, [rows, lang]);

  // Boundary between history and forecast (first is_forecast ym)
  const forecastStart = series.find((s) => s.is_forecast)?.ym;

  // Summary stats for the selected language
  const summary = useMemo(() => {
    const last = [...series].reverse().find((s) => s.actual != null);
    const first = series.find((s) => s.actual != null);
    const finalFc = [...series].reverse().find((s) => s.yhat != null && s.is_forecast);
    if (!last || !first || !finalFc) return null;
    return {
      latest: last.actual!,
      growth: (last.actual! - first.actual!) / Math.max(1, first.actual!),
      proj: finalFc.yhat!,
      projChange: (finalFc.yhat! - last.actual!) / Math.max(1, last.actual!),
    };
  }, [series]);

  if (!rows) return <Loading />;

  return (
    <>
      <Topbar
        title="D2 · Technology Adoption Forecasting"
        subtitle="24-month Holt-Winters forecasts (damped trend + additive seasonality) trained on monthly repo counts per language. Shaded band shows the prediction interval."
      />

      <div className="px-6 md:px-10 pb-12 space-y-6">
        {/* lang switch + summary */}
        <div className="grid lg:grid-cols-[260px_1fr] gap-4">
          <div className="card card-pad">
            <div className="label mb-2.5">Language</div>
            <div className="relative">
              <button
                onClick={() => setOpen((o) => !o)}
                className="w-full flex items-center justify-between px-3 py-2.5 rounded-lg bg-surface border border-border hover:border-mute"
              >
                <span className="text-ink font-medium">{lang}</span>
                <ChevronDown className="size-4 text-sub" />
              </button>
              {open && (
                <div className="absolute z-20 mt-1 w-full max-h-72 overflow-auto rounded-lg bg-surface border border-border shadow-glow">
                  {langs.map((l) => (
                    <button
                      key={l}
                      onClick={() => { setLang(l); setOpen(false); }}
                      className="w-full text-left px-3 py-2 hover:bg-mute/30 flex items-center gap-2 text-sm"
                    >
                      <span
                        className="size-2 rounded-full"
                        style={{ background: colorFor(l) }}
                      />
                      {l}
                    </button>
                  ))}
                </div>
              )}
            </div>

            <div className="mt-5 space-y-3 text-xs">
              <SummaryRow label="Latest actual" value={summary ? fmtNum(summary.latest) : "—"} />
              <SummaryRow
                label="Lifetime growth"
                value={summary ? `${(summary.growth * 100).toFixed(0)}%` : "—"}
                positive={summary && summary.growth >= 0}
              />
              <SummaryRow label="24-mo projection" value={summary ? fmtNum(summary.proj) : "—"} />
              <SummaryRow
                label="Change vs today"
                value={summary ? `${(summary.projChange * 100).toFixed(1)}%` : "—"}
                positive={summary && summary.projChange >= 0}
              />
            </div>
          </div>

          <ChartCard
            title={`${lang} — repo count history + 24-month forecast`}
            subtitle="Solid line: historical actuals. Dashed: forecast. Band: prediction interval."
            right={
              <span className="chip">
                <TrendingUp className="size-3 text-accent" /> Holt-Winters · damped
              </span>
            }
          >
            <div className="h-[420px]">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={series} margin={{ top: 10, right: 18, bottom: 0, left: 0 }}>
                  <defs>
                    <linearGradient id="band" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={colorFor(lang)} stopOpacity={0.30} />
                      <stop offset="100%" stopColor={colorFor(lang)} stopOpacity={0.04} />
                    </linearGradient>
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
                  <Legend
                    wrapperStyle={{ fontSize: 12, paddingTop: 12 }}
                    iconType="line"
                    formatter={(v) => <span className="text-sub">{v}</span>}
                  />
                  {forecastStart && (
                    <ReferenceLine
                      x={forecastStart}
                      stroke="#8B95A8"
                      strokeDasharray="4 4"
                      label={{
                        value: "forecast →",
                        fill: "#8B95A8",
                        fontSize: 10,
                        position: "insideTopRight",
                      }}
                    />
                  )}
                  <Area
                    type="monotone"
                    dataKey="upper"
                    stroke="none"
                    fill="url(#band)"
                    name="interval high"
                    legendType="none"
                  />
                  <Area
                    type="monotone"
                    dataKey="lower"
                    stroke="none"
                    fill="#0B0E14"
                    name="interval low"
                    legendType="none"
                  />
                  <Line
                    type="monotone"
                    dataKey="actual"
                    name="actual"
                    stroke={colorFor(lang)}
                    strokeWidth={2.4}
                    dot={false}
                    isAnimationActive={false}
                  />
                  <Line
                    type="monotone"
                    dataKey="yhat"
                    name="forecast"
                    stroke={colorFor(lang)}
                    strokeWidth={2}
                    strokeDasharray="5 4"
                    dot={false}
                    isAnimationActive={false}
                  />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>
        </div>

        {/* mini multi-grid */}
        <ChartCard
          title="Top 6 highest-growth languages"
          subtitle="Side-by-side mini forecasts for the languages identified by the pipeline as the steepest movers."
        >
          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {TOP_FORECAST.map((l) => (
              <MiniForecast key={l} lang={l} rows={rows} onSelect={() => setLang(l)} />
            ))}
          </div>
        </ChartCard>
      </div>
    </>
  );
}

function SummaryRow({
  label, value, positive,
}: {
  label: string;
  value: string;
  positive?: boolean | null;
}) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-sub">{label}</span>
      <span
        className="font-mono text-ink"
        style={{ color: positive === true ? "#34D399" : positive === false ? "#F87171" : undefined }}
      >
        {value}
      </span>
    </div>
  );
}

function MiniForecast({
  lang, rows, onSelect,
}: {
  lang: string;
  rows: D2Row[];
  onSelect: () => void;
}) {
  const data = rows
    .filter((r) => r.language === lang)
    .sort((a, b) => a.year_month.localeCompare(b.year_month))
    .map((r) => ({
      ym: r.year_month,
      actual: r.actual ?? null,
      yhat: r.arima_yhat ?? null,
    }));

  const last = [...data].reverse().find((d) => d.actual != null)?.actual ?? 0;
  const finalFc = [...data].reverse().find((d) => d.yhat != null)?.yhat ?? 0;
  const change = (finalFc - last) / Math.max(1, last);

  return (
    <button
      onClick={onSelect}
      className="text-left rounded-xl border border-border bg-surface/40 p-3 hover:border-mute transition-colors group"
    >
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-sm font-medium">{lang}</span>
        <span
          className="text-xs font-mono"
          style={{ color: change >= 0 ? "#34D399" : "#F87171" }}
        >
          {change >= 0 ? "▲" : "▼"} {(change * 100).toFixed(0)}%
        </span>
      </div>
      <div className="h-[80px]">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
            <Line
              type="monotone"
              dataKey="actual"
              stroke={colorFor(lang)}
              strokeWidth={1.6}
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="yhat"
              stroke={colorFor(lang)}
              strokeWidth={1.6}
              strokeDasharray="3 3"
              dot={false}
              isAnimationActive={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </button>
  );
}
