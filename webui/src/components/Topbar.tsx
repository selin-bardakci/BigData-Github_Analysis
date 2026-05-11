import { Sparkles, Database } from "lucide-react";

export default function Topbar({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <header className="flex items-center justify-between px-6 md:px-10 pt-7 pb-4">
      <div>
        <div className="text-xs label mb-1">Deliverable</div>
        <h1 className="text-2xl md:text-3xl font-semibold tracking-tight">{title}</h1>
        {subtitle && <p className="text-sub text-sm mt-1.5 max-w-2xl">{subtitle}</p>}
      </div>
      <div className="hidden md:flex items-center gap-2">
        <span className="chip">
          <Database className="size-3" /> BigQuery → Spark → JSON
        </span>
        <span className="chip">
          <Sparkles className="size-3 text-accent" /> 542 langs · 25K devs
        </span>
      </div>
    </header>
  );
}
