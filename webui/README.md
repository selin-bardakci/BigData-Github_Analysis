# Tech Trends Dashboard

Modern dark-mode React UI for the **Detecting and Forecasting Emerging
Programming Technologies on GitHub** project (D1–D5).

The UI is **fully static** — Spark runs once (offline, hours), its parquet
output is converted to slim JSON, and the React app reads those JSON files
directly. Date filters, language toggles, sort, search are all client-side —
no Spark round-trip.

```
Spark / Dataproc (one-time, hours)
        ▼
Parquet on GCS  →  scripts/export_to_json.py  →  webui/public/data/*.json
                                                          ▼
                                          Vite + React + Recharts (this app)
```

---

## Stack

- **Vite 5** + **React 18** + **TypeScript**
- **Tailwind CSS** (dark theme tokens)
- **Recharts** for time-series, scatter, bar
- **d3-sankey** for the migration Sankey
- **react-router-dom** for the 6 pages

---

## Run

From `webui/`:

```bash
npm install
npm run dev      # http://localhost:5173
```

Production build:

```bash
npm run build
npm run preview
```

---

## Replace seed data with your real Spark outputs

The repo ships with realistic seed JSON generated from the notebooks'
descriptive outputs (real Sankey edges, real top-language list, etc.). To
swap in your live Spark output, run from the **project root**:

```bash
# Read from GCS (default — uses your gcloud auth)
python scripts/export_to_json.py --bucket github-tech-trends-data

# OR: read from a local mirror of the processed/ folder
python scripts/export_to_json.py --local-base ./processed
```

It writes 6 JSON files into `webui/public/data/`. The dev server picks them
up on next refresh.

---

## Pages

| Route | Page | Highlights |
|---|---|---|
| `/`   | Overview      | Stat cards, top-movers area chart, deliverable grid |
| `/d1` | Trends        | Multi-lang chips, year slider, log/linear/index toggle, K-Means scatter, volume bar |
| `/d2` | Forecasting   | Per-language forecast with prediction band, top-6 mini grid, summary stats |
| `/d3` | Migration     | Sankey diagram, time-window selector, net-flow chart, top transitions |
| `/d4` | PageRank      | Top-20 bar chart, hub/authority scatter, sortable searchable table |
| `/d5` | Communities   | Size × influence scatter, community grid with top orgs, bridge developers |

---

## Design tokens

| Token  | Hex       | Use |
|---|---|---|
| bg     | `#0B0E14` | Page background |
| card   | `#151B27` | Card surface |
| accent | `#A78BFA` | Electric purple — primary |
| accent2| `#5EEAD4` | Mint — secondary |
| accent3| `#F472B6` | Pink — D3/D5 |
| accent4| `#FBBF24` | Amber — D4 |

Fonts: **Inter** (UI), **JetBrains Mono** (numbers, hashes).
