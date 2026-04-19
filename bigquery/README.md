## BigQuery SQL Queries

All four queries extract data from `bigquery-public-data.github_repos` and are
exported to Google Cloud Storage as **Parquet + Snappy** before any Spark processing.

| File | Destination Table | GCS Path | Deliverables |
|---|---|---|---|
| `Q1_languages_flat.sql` | `staging.languages_flat` | `raw/languages/` | D1, D2 |
| `Q2_commits_monthly.sql` | `staging.commits_monthly` | `raw/commits/` | D1, D2 |
| `Q3_migration_signals.sql` | `staging.migration_signals` | `raw/migrations/` | D3 |
| `Q4_developer_repos.sql` | `staging.developer_repos` | `raw/developers/` | D4, D5 |

### Running the queries

**Option A — BigQuery console (recommended for first run)**
1. Open [console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery)
2. Paste the contents of each `.sql` file
3. Click **Run** — BigQuery will show estimated bytes scanned before executing
4. Save the result as a table in your project's `staging` dataset
5. Then run `../scripts/export_bigquery_to_gcs.sh` to export to GCS

**Option B — bq CLI**
```bash
bq query \
  --use_legacy_sql=false \
  --destination_table="YOUR_PROJECT:staging.languages_flat" \
  --replace \
  "$(cat Q1_languages_flat.sql)"
```

### Cost estimates (approximate)

| Query | Estimated scan | Free tier impact |
|---|---|---|
| Q1 (languages) | ~5–15 GB | ~1–1.5% of monthly free TB |
| Q2 (commits monthly) | ~30–50 GB | ~3–5% |
| Q3 (migration, message col) | ~80–120 GB | ~8–12% |
| Q4 (developer repos) | ~30–50 GB | ~3–5% |
| **Total** | **~145–235 GB** | **~15–24% of free tier** |

> Run each query **once** then export. Never re-run the full scans — work
> from the GCS Parquet files in all subsequent steps.

### Development workflow (cost control)
Each query has a commented-out `LIMIT` clause at the bottom.
Uncomment it when testing logic changes to avoid full scans.
