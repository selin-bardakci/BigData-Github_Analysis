export default function Loading({ label = "Loading" }: { label?: string }) {
  return (
    <div className="flex items-center justify-center py-24 text-sub">
      <div className="flex items-center gap-3">
        <div className="size-2.5 rounded-full bg-accent animate-pulse" />
        <div className="size-2.5 rounded-full bg-accent2 animate-pulse [animation-delay:120ms]" />
        <div className="size-2.5 rounded-full bg-accent3 animate-pulse [animation-delay:240ms]" />
        <span className="ml-2 text-sm">{label}…</span>
      </div>
    </div>
  );
}
