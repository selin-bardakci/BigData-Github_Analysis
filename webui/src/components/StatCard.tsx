import { type LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

type Props = {
  label: string;
  value: string;
  delta?: string;
  icon?: LucideIcon;
  accent?: "purple" | "mint" | "pink" | "amber";
  className?: string;
};

const accentMap = {
  purple: "from-accent/30 to-transparent text-accent",
  mint:   "from-accent2/30 to-transparent text-accent2",
  pink:   "from-accent3/30 to-transparent text-accent3",
  amber:  "from-accent4/30 to-transparent text-accent4",
};

export default function StatCard({
  label, value, delta, icon: Icon, accent = "purple", className,
}: Props) {
  return (
    <div className={cn("card card-pad relative overflow-hidden animate-fadeUp", className)}>
      <div className={cn(
        "absolute inset-0 bg-gradient-to-br opacity-50 pointer-events-none",
        accentMap[accent].split(" ").slice(0, 2).join(" "),
      )} />
      <div className="relative">
        <div className="flex items-center justify-between mb-3">
          <span className="label">{label}</span>
          {Icon && <Icon className={cn("size-5", accentMap[accent].split(" ").pop())} />}
        </div>
        <div className="stat-num">{value}</div>
        {delta && (
          <div className="text-xs text-sub mt-1.5">{delta}</div>
        )}
      </div>
    </div>
  );
}
