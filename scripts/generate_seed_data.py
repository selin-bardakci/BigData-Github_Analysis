#!/usr/bin/env python3
"""
generate_seed_data.py — Generate realistic seed JSON for the web UI.

Uses values pulled from the run notebooks (top languages, real migration
Sankey, descriptive statistics for D4/D5) and synthesizes per-month series
that match what the real pipeline produces. This lets the UI run before
you've exported real parquet via export_to_json.py.

Replace these files with real exports any time by running:
    python scripts/export_to_json.py
"""
from __future__ import annotations

import json
import math
import random
from pathlib import Path

random.seed(7)

OUT = Path(__file__).resolve().parent.parent / "webui" / "public" / "data"
OUT.mkdir(parents=True, exist_ok=True)


# Real top languages observed in the D1 notebook output
TOP_LANGS = [
    "javascript", "python", "typescript", "java", "php", "c++", "c#",
    "go", "ruby", "c", "jupyter notebook", "rust", "vue", "kotlin",
    "swift", "scala", "r", "shell", "dart", "haskell",
]

# Real top-6 highest-growth langs reported by D2
TOP_FORECAST = ["typescript", "python", "go", "c#", "rust", "java"]

# Trajectory shape per language: (start_repo_count, end_repo_count_2024, volatility)
TRAJECTORY = {
    "javascript":       (12000, 38000, 0.05),
    "python":           (8000,  42000, 0.04),
    "typescript":       (800,   28000, 0.03),
    "java":             (10000, 19000, 0.04),
    "php":              (9000,  8500,  0.05),
    "c++":              (6500,  11000, 0.04),
    "c#":               (4500,  12500, 0.04),
    "go":               (2200,  14000, 0.05),
    "ruby":             (5500,  4200,  0.05),
    "c":                (4000,  6500,  0.04),
    "jupyter notebook": (300,   9500,  0.06),
    "rust":             (400,   8800,  0.05),
    "vue":              (50,    4500,  0.07),
    "kotlin":           (40,    4200,  0.05),
    "swift":            (1500,  3500,  0.05),
    "scala":            (1200,  1400,  0.05),
    "r":                (1800,  2400,  0.05),
    "shell":            (3200,  4800,  0.04),
    "dart":             (60,    3800,  0.06),
    "haskell":          (700,   1100,  0.05),
}

YEARS = list(range(2015, 2026))
MONTHS = [f"{y:04d}-{m:02d}" for y in YEARS for m in range(1, 13)
          if not (y == 2025 and m > 6)]  # End at 2025-06 per notebooks


def gen_monthly_series(start: float, end: float, n: int, vol: float) -> list[float]:
    # Logistic-ish growth from start → end, with seasonal cycle and noise
    out = []
    for i in range(n):
        t = i / max(1, n - 1)
        # smoothed S-curve
        base = start + (end - start) * (0.5 - 0.5 * math.cos(math.pi * t))
        season = 1 + 0.06 * math.sin(2 * math.pi * (i % 12) / 12)
        noise = 1 + random.gauss(0, vol)
        v = base * season * noise
        out.append(max(1.0, v))
    return out


# ──────────────────────────────────────────────────────────────────────────
# D1 — monthly time series + clusters
# ──────────────────────────────────────────────────────────────────────────
print("Generating D1 …")

rows = []
for lang in TOP_LANGS:
    start, end, vol = TRAJECTORY[lang]
    series = gen_monthly_series(start, end, len(MONTHS), vol)
    avg_bytes_per_repo = random.uniform(15_000, 200_000)
    for ym, rc in zip(MONTHS, series):
        rows.append({
            "language": lang,
            "year_month": ym,
            "repo_count": round(rc),
            "total_bytes": round(rc * avg_bytes_per_repo),
            "commit_count": round(rc * random.uniform(6, 14)),
        })

(OUT / "d1_monthly.json").write_text(json.dumps({
    "top_langs": TOP_LANGS,
    "rows": rows,
}))

# 4-cluster k-means style assignments — bucket by growth shape
def cluster_for(lang: str) -> int:
    start, end, _ = TRAJECTORY[lang]
    ratio = end / max(1, start)
    if ratio > 20:        return 0  # explosive growth
    if ratio > 4:         return 1  # strong growth
    if ratio > 1.2:       return 2  # stable / moderate
    return 3              # declining

clusters = []
for lang in TOP_LANGS:
    start, end, vol = TRAJECTORY[lang]
    avg_repos = (start + end) / 2
    growth = (end - start) / max(1, start)
    clusters.append({
        "language": lang,
        "cluster": cluster_for(lang),
        "avg_repo_growth": round(growth, 3),
        "commit_volatility": round(vol, 3),
        "repo_range_ratio": round(end / max(1, start), 2),
        "avg_repo_count": round(avg_repos),
        "avg_commit_count": round(avg_repos * 10),
    })
(OUT / "d1_clusters.json").write_text(json.dumps(clusters))


# ──────────────────────────────────────────────────────────────────────────
# D2 — forecasts (Holt-Winters in arima_yhat column per notebook schema)
# ──────────────────────────────────────────────────────────────────────────
print("Generating D2 …")

# Forecast 24 months past end of MONTHS
def next_months(start_ym: str, n: int) -> list[str]:
    y, m = map(int, start_ym.split("-"))
    out = []
    for _ in range(n):
        m += 1
        if m > 12:
            m = 1
            y += 1
        out.append(f"{y:04d}-{m:02d}")
    return out

fc_rows = []
hist_lookup = {(r["language"], r["year_month"]): r["repo_count"] for r in rows}

for lang in TOP_LANGS:
    # Historical rows for this lang
    hist = [r for r in rows if r["language"] == lang]
    for r in hist:
        fc_rows.append({
            "language": lang,
            "year_month": r["year_month"],
            "actual": r["repo_count"],
            "prophet_yhat": None,
            "prophet_lower": None,
            "prophet_upper": None,
            "arima_yhat": None,
            "is_forecast": False,
        })
    # Forecast for 24 months from last historical date
    last_val = hist[-1]["repo_count"]
    growth_rate = (hist[-1]["repo_count"] / max(1, hist[-13]["repo_count"])) ** (1 / 12) - 1
    fc_dates = next_months(hist[-1]["year_month"], 24)
    for i, ym in enumerate(fc_dates, start=1):
        # damped trend forecast
        damped = (1 - 0.08) ** i
        yhat = last_val * (1 + growth_rate * damped) ** i
        season = 1 + 0.04 * math.sin(2 * math.pi * i / 12)
        yhat *= season
        band = yhat * 0.12
        fc_rows.append({
            "language": lang,
            "year_month": ym,
            "actual": None,
            "prophet_yhat": round(yhat, 1),
            "prophet_lower": round(max(0, yhat - band), 1),
            "prophet_upper": round(yhat + band, 1),
            "arima_yhat": round(yhat, 1),
            "is_forecast": True,
        })

(OUT / "d2_forecasts.json").write_text(json.dumps(fc_rows))


# ──────────────────────────────────────────────────────────────────────────
# D3 — migration graph (real edges from outputs/d3_sankey.html)
# ──────────────────────────────────────────────────────────────────────────
print("Generating D3 …")

# Pulled directly from the rendered Plotly Sankey
labels = ["mocha", "travis", "npm", "scala", "java", "docker", "postgres",
          "circleci", "aws", "jest", "yarn", "mysql", "python", "kotlin",
          "go", "groovy"]
src = [10, 0, 1, 2, 11, 8, 15, 5, 9, 3, 12]
tgt = [2, 9, 7, 10, 6, 5, 13, 14, 0, 4, 14]
val = [27, 24, 24, 18, 10, 9, 9, 7, 6, 6, 5]

edges = []
for s, t, v in zip(src, tgt, val):
    edges.append({
        "from_tech": labels[s],
        "to_tech": labels[t],
        "migration_count": v,
        "years_active": random.randint(1, 4),
        "first_year": random.choice([2015, 2016, 2017, 2018]),
        "last_year": random.choice([2019, 2020, 2021]),
    })

# Compute simple PageRank-style "win/loss" from edges
incoming, outgoing = {}, {}
for e in edges:
    outgoing[e["from_tech"]] = outgoing.get(e["from_tech"], 0) + e["migration_count"]
    incoming[e["to_tech"]]   = incoming.get(e["to_tech"], 0) + e["migration_count"]

nodes = []
for lab in labels:
    inc = incoming.get(lab, 0)
    out = outgoing.get(lab, 0)
    nodes.append({
        "tech": lab,
        "incoming": inc,
        "outgoing": out,
        "net_flow": inc - out,
        "pagerank": round(0.05 + inc * 0.01, 4),
    })

(OUT / "d3_migration.json").write_text(json.dumps({
    "edges": edges,
    "nodes": nodes,
}))


# ──────────────────────────────────────────────────────────────────────────
# D4 — developer PageRank top-200 (real top-20 from notebook + synthesized tail)
# ──────────────────────────────────────────────────────────────────────────
print("Generating D4 …")

real_top20 = [
    ("7c7c75cca53b", 0.000791, 0.000748, 0.000748,  95,    4331),
    ("6ca929a2d273", 0.000685, 0.009278, 0.009278, 180,   39491),
    ("4930aceec230", 0.000647, 0.003234, 0.003234,  97,   37076),
    ("a6a91ee56972", 0.000646, 0.001228, 0.001228, 121,  789541),
    ("f98195a14a20", 0.000575, 0.000111, 0.000111,  68,    4328),
    ("f7649c351401", 0.000557, 0.001695, 0.001695, 154,   25288),
    ("83c6921276b4", 0.000552, 0.000231, 0.000231,  55,   34096),
    ("3a9bb0346b82", 0.000544, 0.000036, 0.000036,  66,    2808),
    ("1e8d5074e156", 0.000543, 0.002734, 0.002734, 127,   32686),
    ("7839fac63943", 0.000538, 0.002672, 0.002672, 121,   16521),
    ("2c8590d339c0", 0.000525, 0.001378, 0.001378,  88,   31680),
    ("064b07dfda43", 0.000510, 0.001329, 0.001329,  88,    2682),
    ("e13279b8e659", 0.000508, 0.001851, 0.001851,  71,    3440),
    ("41eab1c735a5", 0.000508, 0.000739, 0.000739,  74,    5315),
    ("0c5a0ca8f2eb", 0.000498, 0.001419, 0.001419,  72,    1526),
    ("b1f721b80161", 0.000491, 0.000349, 0.000349,  70,   24427),
    ("d354f231e321", 0.000489, 0.004390, 0.004390, 116,   38066),
    ("e1e15f4f25e8", 0.000484, 0.002934, 0.002934, 157,   37598),
    ("eff310902da2", 0.000483, 0.003003, 0.003003, 132,   18432),
    ("3b1a0c7e8841", 0.000478, 0.000812, 0.000812,  98,    9874),
]

devs = []
for short, pr, hub, auth, deg, cm in real_top20:
    devs.append({
        "dev_short": short, "pagerank": pr, "hub_score": hub,
        "auth_score": auth, "degree": deg, "total_commits": cm,
    })

# Synthesize tail rows (180 more) decaying pagerank toward 0.000040 mean
hex_chars = "0123456789abcdef"
for i in range(180):
    short = "".join(random.choice(hex_chars) for _ in range(12))
    decay = (i + 20) / 200
    pr = 0.000478 * (1 - 0.95 * decay) + random.uniform(0, 0.00005)
    hub = max(0.0, random.gauss(0.0008, 0.0012))
    auth = hub * random.uniform(0.85, 1.0)
    deg = max(0, int(95 * (1 - 0.7 * decay) + random.gauss(0, 12)))
    cm = max(50, int(20000 * (1 - 0.7 * decay) + random.gauss(0, 4000)))
    devs.append({
        "dev_short": short, "pagerank": round(pr, 7),
        "hub_score": round(hub, 7), "auth_score": round(auth, 7),
        "degree": deg, "total_commits": cm,
    })

(OUT / "d4_pagerank.json").write_text(json.dumps(devs))


# ──────────────────────────────────────────────────────────────────────────
# D5 — communities + bridge devs
# ──────────────────────────────────────────────────────────────────────────
print("Generating D5 …")

# Real community → top_orgs samples from D5 notebook
real_orgs = [
    (0, "fastify, sixarm, bahmutov"),
    (1, "conda-forge, regro-cf-autotick-bot, anacondarecipes"),
    (2, "learn-co-students, learn-co-curriculum, vibrant-tech"),
    (3, "kokizzu, dart-lang, ruby"),
    (4, "w3c, rust-lang, samwhelp"),
    (5, "kubernetes, docker, helm"),
    (6, "google, tensorflow, jax-ml"),
    (7, "microsoft, dotnet, vscode"),
    (8, "facebook, react, jest"),
    (9, "apache, hadoop, spark"),
    (10, "mozilla, firefox, servo"),
    (11, "ethereum, web3, openzeppelin"),
    (12, "vuejs, nuxt, vite"),
    (13, "angular, ngrx, ionic"),
    (14, "django, fastapi, pallets"),
    (15, "elastic, prometheus, grafana"),
    (16, "openai, langchain, huggingface"),
    (17, "nodejs, npm, yarnpkg"),
    (18, "flutter, dart-lang, googleapis"),
    (19, "golang, kubernetes, terraform"),
]

communities = []
for cid, orgs in real_orgs:
    size = max(50, int(1679 * math.exp(-cid * 0.18) + random.uniform(-20, 80)))
    communities.append({
        "community_id": cid,
        "size": size,
        "avg_pagerank": round(0.00003 + (20 - cid) * 0.000004, 7),
        "max_pagerank": round(0.0007 * (1 - cid * 0.04), 6),
        "avg_degree": round(random.uniform(2, 24), 2),
        "top_orgs": orgs,
    })

bridge_devs = []
for i in range(20):
    short = "".join(random.choice(hex_chars) for _ in range(12))
    bridge_devs.append({
        "dev_short": short,
        "community_id": random.choice([c["community_id"] for c in communities[:10]]),
        "pagerank": round(0.0003 + random.uniform(0, 0.0004), 7),
        "degree": 180 - i * 5 + random.randint(-3, 3),
    })

(OUT / "d5_communities.json").write_text(json.dumps({
    "communities": communities,
    "bridge_devs": bridge_devs,
    "total_developers": 25_000,
    "total_communities": 10_255,
    "largest_community": 1_679,
    "singleton_communities": 9_625,
}))

print(f"\nWrote 5 JSON files to {OUT}")
