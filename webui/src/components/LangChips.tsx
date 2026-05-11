import { cn, colorFor } from "@/lib/utils";

type Props = {
  options: string[];
  selected: string[];
  onToggle: (lang: string) => void;
  max?: number;
};

export default function LangChips({ options, selected, onToggle, max = 12 }: Props) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {options.map((lang) => {
        const active = selected.includes(lang);
        const disabled = !active && selected.length >= max;
        const color = colorFor(lang, options);
        return (
          <button
            key={lang}
            disabled={disabled}
            onClick={() => onToggle(lang)}
            className={cn(
              "chip",
              active && "chip-active",
              disabled && "opacity-40 cursor-not-allowed",
            )}
            style={
              active
                ? { backgroundColor: color + "22", color, borderColor: color + "55" }
                : undefined
            }
          >
            <span
              className="size-2 rounded-full"
              style={{ backgroundColor: color }}
            />
            {lang}
          </button>
        );
      })}
    </div>
  );
}
