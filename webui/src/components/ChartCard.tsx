import { type ReactNode } from "react";
import { cn } from "@/lib/utils";

type Props = {
  title: string;
  subtitle?: string;
  right?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
};

export default function ChartCard({
  title,
  subtitle,
  right,
  children,
  className,
  bodyClassName,
}: Props) {
  return (
    <section className={cn("card card-pad animate-fadeUp", className)}>
      <div className="flex items-start justify-between gap-4 mb-4">
        <div>
          <h3 className="text-base font-semibold tracking-tight">{title}</h3>
          {subtitle && (
            <p className="text-xs text-sub mt-1 leading-relaxed">{subtitle}</p>
          )}
        </div>
        {right && <div className="shrink-0">{right}</div>}
      </div>
      <div className={cn("min-w-0", bodyClassName)}>{children}</div>
    </section>
  );
}
