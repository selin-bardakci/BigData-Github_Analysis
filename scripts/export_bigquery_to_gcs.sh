#!/usr/bin/env bash
# =============================================================================
# export_bigquery_to_gcs.sh
# Exports all four staging BigQuery tables to GCS as Parquet + Snappy.
#
# Run AFTER executing the four BigQuery queries and saving their results
# as tables in the staging dataset (see bigquery/README.md).
#
# Parquet + Snappy is chosen because:
#   - Columnar format: Spark reads only the columns it needs
#   - Snappy compression: ~3–5× smaller than CSV, fast decompression
#   - Splittable: each Spark executor reads a separate Parquet partition
#
# Usage:
#   bash scripts/export_bigquery_to_gcs.sh
#
# Edit the CONFIG block to match your project.
# =============================================================================

set -euo pipefail

# =============================================================================
# CONFIG — must match setup_gcp.sh
# =============================================================================
PROJECT_ID="github-tech-trends"
BQ_DATASET="staging"
BUCKET="github-tech-trends-data"
# =============================================================================

export_table () {
    local TABLE="$1"
    local GCS_PATH="$2"
    local DESC="$3"

    echo "==> Exporting ${TABLE} → gs://${BUCKET}/${GCS_PATH}"
    echo "    ${DESC}"

    bq extract \
        --destination_format PARQUET \
        --compression SNAPPY \
        "${PROJECT_ID}:${BQ_DATASET}.${TABLE}" \
        "gs://${BUCKET}/${GCS_PATH}/*.parquet"

    echo "    Done."
    echo ""
}

# Verify bq CLI is available
if ! command -v bq &>/dev/null; then
    echo "ERROR: bq CLI not found. Install the Google Cloud SDK first."
    exit 1
fi

echo "=== BigQuery → GCS Export ==="
echo "Project : ${PROJECT_ID}"
echo "Dataset : ${BQ_DATASET}"
echo "Bucket  : gs://${BUCKET}"
echo ""

# ---------------------------------------------------------------------------
# D1 + D2 sources
# ---------------------------------------------------------------------------
export_table \
    "languages_flat" \
    "raw/languages" \
    "Flat (repo_name, language, bytes) — ~5–15 GB raw"

export_table \
    "commits_monthly" \
    "raw/commits" \
    "Monthly commit counts per repo — ~30–50 GB raw"

# ---------------------------------------------------------------------------
# D3 source
# ---------------------------------------------------------------------------
export_table \
    "migration_signals" \
    "raw/migrations" \
    "Parsed (from_tech, to_tech, year, count) migration pairs — small"

# ---------------------------------------------------------------------------
# D4 + D5 source
# ---------------------------------------------------------------------------
export_table \
    "developer_repos" \
    "raw/developers" \
    "Developer-repo edges (hashed in Spark) — ~30–50 GB raw"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "=== Export complete ==="
echo ""
echo "Verify files in GCS:"
echo "  gsutil ls gs://${BUCKET}/raw/languages/"
echo "  gsutil ls gs://${BUCKET}/raw/commits/"
echo "  gsutil ls gs://${BUCKET}/raw/migrations/"
echo "  gsutil ls gs://${BUCKET}/raw/developers/"
echo ""
echo "Next step:"
echo "  bash scripts/run_all_spark_jobs.sh"
