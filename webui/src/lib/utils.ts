import clsx, { type ClassValue } from "clsx";

export function cn(...inputs: ClassValue[]) {
  return clsx(inputs);
}

export const ACCENTS = [
  "#A78BFA", // purple
  "#5EEAD4", // mint
  "#F472B6", // pink
  "#FBBF24", // amber
  "#60A5FA", // blue
  "#34D399", // green
  "#FB7185", // rose
  "#C084FC", // lighter purple
  "#FACC15", // yellow
  "#22D3EE", // cyan
  "#F97316", // orange
  "#A3E635", // lime
  "#E879F9", // fuchsia
  "#94A3B8", // slate
  "#F0ABFC", // light pink
  "#FCA5A5", // light red
  "#86EFAC", // light green
  "#7DD3FC", // light blue
  "#FDE68A", // light yellow
  "#FCD34D", // gold
];

export function colorFor(label: string, langs?: string[]): string {
  if (langs) {
    const i = langs.indexOf(label);
    if (i !== -1) return ACCENTS[i % ACCENTS.length];
  }
  let h = 0;
  for (let i = 0; i < label.length; i++) h = (h * 31 + label.charCodeAt(i)) >>> 0;
  return ACCENTS[h % ACCENTS.length];
}

export function fmtNum(n: number, digits = 0): string {
  if (n == null || isNaN(n)) return "—";
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return n.toFixed(digits);
}

export function fmtPct(n: number, digits = 1): string {
  if (n == null || isNaN(n)) return "—";
  return (n * 100).toFixed(digits) + "%";
}
