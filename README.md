# Detecting and Forecasting Emerging Programming Technologies on GitHub
**Selin Bardakcı, Bengisu Duru Göksu, Zeynep Yiğit**  
Department of Computer Engineering, Gebze Technical University

A scalable big data analytics pipeline that processes the public GitHub dataset
on Google BigQuery and Apache Spark to produce five deliverables covering
trend analysis, forecasting, graph-based analyses, and community detection.

---

## Deliverables

| # | Deliverable | Key Output |
|---|---|---|
| D1 | Language Growth Trend Visualizations | Growth charts, market share, K-Means clusters, emergence score ranking |
| D2 | Technology Adoption Forecasting | Per-language ARIMA/Prophet models with MAE/RMSE, 2025 forecasts |
| D3 | Technology Migration Graph | Directed migration graph, PageRank win/loss table, Sankey flows |
| D4 | Developer Influence PageRank | Developer collaboration graph, PageRank ranking per tech community |
| D5 | Developer Community Map | Louvain communities, alluvial evolution chart, bridge developer analysis |

---

## Architecture

```
BigQuery public dataset
(bigquery-public-data.github_repos)
        │
        │  Q1–Q4  SQL queries
        ▼
  Staging tables in BigQuery
        │
        │  bq extract  →  Parquet + Snappy
        ▼
  Google Cloud Storage
  gs://github-tech-trends-data/raw/
        │
        │  PySpark on Cloud Dataproc
        ▼
  GCS processed/ + models/
        │
        │  Jupyter notebooks (local)
        ▼
  outputs/figures/  +  outputs/results/
```

---

## Prerequisites

- Google Cloud SDK (`gcloud`, `bq`, `gsutil`) installed and authenticated
- Python 3.10+
- A GCP project with billing enabled

---

## Setup

### 1. Install Python dependencies (local)
```bash
pip install -r requirements.txt
```

### 2. Configure GCP (edit `scripts/setup_gcp.sh` first)
```bash
# Edit PROJECT_ID and BUCKET in scripts/setup_gcp.sh, then:
bash scripts/setup_gcp.sh
```
This enables APIs, creates the GCS bucket, BigQuery dataset, Dataproc cluster,
and service account.

### 3. Run BigQuery extractions
Open each file in `bigquery/` in the BigQuery console and run it.
Save each result as a table in your `staging` dataset.
See [bigquery/README.md](bigquery/README.md) for per-query cost estimates.

### 4. Export to GCS
```bash
bash scripts/export_bigquery_to_gcs.sh
```

### 5. Run Spark jobs
```bash
bash scripts/run_all_spark_jobs.sh
```

### 6. Open notebooks
```bash
jupyter notebook notebooks/
```
Run notebooks in order: D1 → D2 → D3 → D4 → D5 → final_report.

---

## Project Structure

```
github-tech-trends/
├── bigquery/              SQL extraction queries (Q1–Q4)
├── spark/                 PySpark processing jobs
├── notebooks/             Deliverable notebooks + final report
├── scripts/               GCP setup, export, and job submission scripts
├── outputs/
│   ├── figures/           Saved plots (PNG/SVG)
│   └── results/           Saved result tables (CSV/Parquet)
├── docs/                  Architecture diagrams
├── requirements.txt
└── .gitignore
```

---

## Data Sources

All data comes from `bigquery-public-data.github_repos` (Google BigQuery public dataset).

| Table | Columns used | Purpose |
|---|---|---|
| `languages` | `repo_name`, `language[].name`, `language[].bytes` | D1, D2 |
| `commits` | `repo_name[]`, `committer.date.seconds`, `commit_count` | D1, D2 |
| `commits` | `message` | D3 |
| `commits` | `author.email`, `author.time_sec`, `repo_name[]` | D4, D5 |

Time window: **January 2015 – December 2024**

---

## Security Notes

- Developer emails (from `commits.author.email`) are **SHA-256 hashed** in Spark
  before any graph construction. Raw emails are never written to disk or displayed.
- `service-account-key.json` is in `.gitignore` — never commit it.
- BigQuery queries use column-projection only (no `SELECT *`) to minimise scan cost.
