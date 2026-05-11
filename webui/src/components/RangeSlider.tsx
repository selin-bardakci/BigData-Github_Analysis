type Props = {
  min: number;
  max: number;
  value: [number, number];
  onChange: (v: [number, number]) => void;
  label?: string;
  formatLabel?: (n: number) => string;
};

export default function RangeSlider({
  min, max, value, onChange, label, formatLabel = (n) => `${n}`,
}: Props) {
  const [lo, hi] = value;

  return (
    <div className="w-full">
      {label && (
        <div className="flex justify-between mb-2 text-xs text-sub">
          <span>{label}</span>
          <span className="font-mono text-ink">
            {formatLabel(lo)} – {formatLabel(hi)}
          </span>
        </div>
      )}
      <div className="relative h-8 select-none">
        <div className="absolute left-0 right-0 top-1/2 -translate-y-1/2 h-1.5 rounded-full bg-mute" />
        <div
          className="absolute top-1/2 -translate-y-1/2 h-1.5 rounded-full bg-gradient-to-r from-accent to-accent2"
          style={{
            left: `${((lo - min) / (max - min)) * 100}%`,
            right: `${100 - ((hi - min) / (max - min)) * 100}%`,
          }}
        />
        <input
          type="range"
          min={min}
          max={max}
          value={lo}
          onChange={(e) => {
            const v = Math.min(Number(e.target.value), hi);
            onChange([v, hi]);
          }}
          className="range-thumb"
        />
        <input
          type="range"
          min={min}
          max={max}
          value={hi}
          onChange={(e) => {
            const v = Math.max(Number(e.target.value), lo);
            onChange([lo, v]);
          }}
          className="range-thumb"
        />
      </div>
      <style>{`
        .range-thumb {
          position: absolute; inset: 0; width: 100%; height: 100%;
          background: transparent; -webkit-appearance: none; appearance: none;
          pointer-events: none;
        }
        .range-thumb::-webkit-slider-thumb {
          -webkit-appearance: none; appearance: none;
          width: 18px; height: 18px; border-radius: 9999px;
          background: #E4E8F1; border: 2px solid #A78BFA;
          box-shadow: 0 0 0 4px rgba(167,139,250,0.20);
          pointer-events: auto; cursor: pointer;
        }
        .range-thumb::-moz-range-thumb {
          width: 18px; height: 18px; border-radius: 9999px;
          background: #E4E8F1; border: 2px solid #A78BFA;
          pointer-events: auto; cursor: pointer;
        }
      `}</style>
    </div>
  );
}
