/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        bg:       "#0B0E14",
        surface:  "#11151F",
        card:     "#151B27",
        border:   "#1F2733",
        mute:     "#2A3344",
        ink:      "#E4E8F1",
        sub:      "#8B95A8",
        accent:   "#A78BFA", // electric purple
        accent2:  "#5EEAD4", // mint
        accent3:  "#F472B6", // pink
        accent4:  "#FBBF24", // amber
        good:     "#34D399",
        bad:      "#F87171",
      },
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "SFMono-Regular", "monospace"],
      },
      boxShadow: {
        glow: "0 0 0 1px rgba(167,139,250,0.25), 0 8px 30px rgba(167,139,250,0.12)",
      },
      keyframes: {
        fadeUp: {
          "0%": { opacity: "0", transform: "translateY(8px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
      },
      animation: {
        fadeUp: "fadeUp 0.35s ease-out both",
      },
    },
  },
  plugins: [],
};
