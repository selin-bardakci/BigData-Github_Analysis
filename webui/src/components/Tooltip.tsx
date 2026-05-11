// Recharts custom tooltip styled to match dark theme.
import { fmtNum } from "@/lib/utils";

type Item = {
  name?: string | number;
  value?: number | string | (string | number)[];
  color?: string;
  dataKey?: string | number;
};

type Props = {
  active?: boolean;
  payload?: readonly Item[];
  label?: string | number;
  valueFormatter?: (v: number) => string;
};

export default function ChartTooltip({
  active,
  payload,
  label,
  valueFormatter = (v) => fmtNum(Number(v)),
}: Props) {
  if (!active || !payload || payload.length === 0) return null;

  return (
    <div className="rounded-xl border border-border bg-surface/95 backdrop-blur px-3 py-2 shadow-glow text-xs">
      {label !== undefined && (
        <div className="text-sub mb-1.5 font-mono">{String(label)}</div>
      )}
      <div className="flex flex-col gap-1">
        {payload.map((p, i) => (
          <div key={i} className="flex items-center gap-2">
            <span
              className="size-2 rounded-full"
              style={{ backgroundColor: p.color }}
            />
            <span className="text-ink">{String(p.name ?? p.dataKey ?? "")}</span>
            <span className="ml-auto font-mono text-ink">
              {p.value != null
                ? valueFormatter(Number(Array.isArray(p.value) ? p.value[0] : p.value))
                : "—"}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
