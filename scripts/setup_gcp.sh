#!/usr/bin/env bash
# =============================================================================
# setup_gcp.sh
# One-time GCP project bootstrap for the github-tech-trends project.
#
# What this script does:
#   1. Sets the active GCP project
#   2. Enables all required APIs
#   3. Creates the GCS bucket with the required folder structure
#   4. Creates the BigQuery staging dataset
#   5. Creates a Dataproc cluster (preemptible workers to cut cost)
#   6. Creates a service account with the minimum required permissions
#
# Prerequisites:
#   - gcloud CLI installed and authenticated  (gcloud auth login)
#   - Billing enabled on the target project
#
# Usage:
#   bash scripts/setup_gcp.sh
#
# Edit the variables in the CONFIG block below before running.
# =============================================================================

set -euo pipefail

# =============================================================================
# CONFIG — edit these
# =============================================================================
PROJECT_ID="github-tech-trends"          # your GCP project ID
REGION="us-central1"
ZONE="us-central1-a"
BUCKET="github-tech-trends-data"         # globally unique GCS bucket name
BQ_DATASET="staging"
DATAPROC_CLUSTER="spark-cluster"
SERVICE_ACCOUNT_NAME="github-trends-sa"
# =============================================================================

echo "==> Setting active project to: ${PROJECT_ID}"
gcloud config set project "${PROJECT_ID}"

# ---------------------------------------------------------------------------
# 1. Enable required APIs
# ---------------------------------------------------------------------------
echo "==> Enabling APIs..."
gcloud services enable \
    bigquery.googleapis.com \
    storage.googleapis.com \
    dataproc.googleapis.com \
    compute.googleapis.com \
    iam.googleapis.com \
    cloudresourcemanager.googleapis.com

echo "    APIs enabled."

# ---------------------------------------------------------------------------
# 2. Create GCS bucket (multi-region US to match BigQuery public data location)
# ---------------------------------------------------------------------------
echo "==> Creating GCS bucket: gs://${BUCKET}"
if gsutil ls -b "gs://${BUCKET}" &>/dev/null; then
    echo "    Bucket already exists — skipping."
else
    gsutil mb -l US "gs://${BUCKET}"
    echo "    Bucket created."
fi

# Create logical folder structure by writing empty placeholder objects
echo "==> Creating bucket folder structure..."
for folder in \
    raw/languages/ \
    raw/commits/ \
    raw/migrations/ \
    raw/developers/ \
    processed/cleaned/ \
    processed/aggregated/ \
    processed/features/ \
    processed/migration_graph/ \
    processed/developer_graph/ \
    models/kmeans/ \
    models/forecasting/
do
    echo "" | gsutil cp - "gs://${BUCKET}/${folder}.keep" 2>/dev/null || true
done
echo "    Folder placeholders created."

# ---------------------------------------------------------------------------
# 3. Create BigQuery staging dataset
# ---------------------------------------------------------------------------
echo "==> Creating BigQuery dataset: ${BQ_DATASET}"
if bq ls --dataset "${PROJECT_ID}:${BQ_DATASET}" &>/dev/null; then
    echo "    Dataset already exists — skipping."
else
    bq mk --dataset \
        --location=US \
        --description="Staging tables from BigQuery public github_repos dataset" \
        "${PROJECT_ID}:${BQ_DATASET}"
    echo "    Dataset created."
fi

# ---------------------------------------------------------------------------
# 4. Create service account with least-privilege permissions
# ---------------------------------------------------------------------------
SA_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "==> Creating service account: ${SA_EMAIL}"
if gcloud iam service-accounts describe "${SA_EMAIL}" &>/dev/null; then
    echo "    Service account already exists — skipping creation."
else
    gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
        --display-name="GitHub Trends Pipeline SA"
    echo "    Service account created."
fi

echo "==> Granting IAM roles to service account..."
for ROLE in \
    roles/bigquery.dataViewer \
    roles/bigquery.jobUser \
    roles/storage.admin \
    roles/dataproc.worker
do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="${ROLE}" \
        --quiet
done
echo "    IAM roles granted."

# Create and download a key for local use (store securely, never commit)
KEY_FILE="service-account-key.json"
if [ ! -f "${KEY_FILE}" ]; then
    echo "==> Downloading service account key to ${KEY_FILE}"
    gcloud iam service-accounts keys create "${KEY_FILE}" \
        --iam-account="${SA_EMAIL}"
    echo "    Key saved.  Add ${KEY_FILE} to .gitignore immediately."
    echo "    export GOOGLE_APPLICATION_CREDENTIALS=\$(pwd)/${KEY_FILE}"
else
    echo "    Key file already exists — skipping."
fi

# ---------------------------------------------------------------------------
# 5. Create Dataproc cluster
# ---------------------------------------------------------------------------
echo "==> Creating Dataproc cluster: ${DATAPROC_CLUSTER}"
if gcloud dataproc clusters describe "${DATAPROC_CLUSTER}" \
       --region="${REGION}" &>/dev/null; then
    echo "    Cluster already exists — skipping."
else
    gcloud dataproc clusters create "${DATAPROC_CLUSTER}" \
        --region="${REGION}" \
        --zone="${ZONE}" \
        --master-machine-type=n1-standard-4 \
        --master-boot-disk-size=100GB \
        --num-workers=2 \
        --worker-machine-type=n1-standard-4 \
        --worker-boot-disk-size=100GB \
        --num-secondary-workers=2 \
        --secondary-worker-type=preemptible \
        --image-version=2.1-debian11 \
        --optional-components=JUPYTER \
        --enable-component-gateway \
        --service-account="${SA_EMAIL}" \
        --scopes=cloud-platform \
        --no-address \
        --properties="spark:spark.executor.memory=4g,spark:spark.driver.memory=4g"
    echo "    Cluster created."
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "=== GCP setup complete ==="
echo ""
echo "Next step:"
echo "  1. Run all four BigQuery queries (see bigquery/README.md)"
echo "  2. Then run: bash scripts/export_bigquery_to_gcs.sh"
