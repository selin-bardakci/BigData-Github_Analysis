#!/usr/bin/env bash
# =============================================================================
# run_all_spark_jobs.sh
# Submits all Spark jobs to the Dataproc cluster in the correct order.
#
# Execution order:
#   D1/D2 pipeline:  d1_preprocess → d1_aggregate → d1_features_cluster → d2_forecasting
#   D3 pipeline:     d3_migration_graph   (independent of D1/D2 except for reading raw data)
#   D4/D5 pipeline:  d4d5_developer_graph (independent of D1/D2/D3)
#
# Usage:
#   bash scripts/run_all_spark_jobs.sh
#
# Edit CONFIG to match your project.
# To run a single job, call submit_job directly, e.g.:
#   submit_job "spark/d1_preprocess.py" "D1 Preprocessing"
# =============================================================================

set -euo pipefail

# =============================================================================
# CONFIG
# =============================================================================
PROJECT_ID="github-tech-trends"
REGION="us-central1"
CLUSTER="spark-cluster"
BUCKET="github-tech-trends-data"
# =============================================================================

submit_job () {
    local SCRIPT="$1"
    local DESC="$2"

    echo ""
    echo "==> Submitting: ${DESC}"
    echo "    Script : ${SCRIPT}"

    gcloud dataproc jobs submit pyspark \
        "gs://${BUCKET}/spark_scripts/$(basename "${SCRIPT}")" \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --cluster="${CLUSTER}" \
        --properties="spark.executor.memory=4g,spark.driver.memory=4g" \
        -- \
        --bucket="${BUCKET}" \
        --project="${PROJECT_ID}"

    echo "    Completed: ${DESC}"
}

# Upload all Spark scripts to GCS so Dataproc can access them
echo "==> Uploading Spark scripts to GCS..."
gsutil -m cp spark/*.py "gs://${BUCKET}/spark_scripts/"
echo "    Upload done."

# ---------------------------------------------------------------------------
# D1 + D2 pipeline (sequential: each step depends on the previous)
# ---------------------------------------------------------------------------
submit_job "spark/d1_preprocess.py"          "D1 — Preprocessing (clean + join)"
submit_job "spark/d1_aggregate.py"           "D1 — Aggregation (monthly metrics per language)"
submit_job "spark/d1_features_cluster.py"    "D1 — Feature engineering + K-Means clustering"
submit_job "spark/d2_forecasting.py"         "D2 — Forecasting models (ARIMA / Prophet)"

# ---------------------------------------------------------------------------
# D3 pipeline (can run in parallel with D4/D5, but kept sequential for simplicity)
# ---------------------------------------------------------------------------
submit_job "spark/d3_migration_graph.py"     "D3 — Migration graph + PageRank"

# ---------------------------------------------------------------------------
# D4 + D5 pipeline
# ---------------------------------------------------------------------------
submit_job "spark/d4d5_developer_graph.py"   "D4+D5 — Developer graph + PageRank + Louvain"

echo ""
echo "=== All Spark jobs complete ==="
echo ""
echo "Next step: open notebooks/ and run each deliverable notebook."
