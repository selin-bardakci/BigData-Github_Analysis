#!/usr/bin/env python3
"""
upload_hf_q1_to_gcs.py — Replace Q1 (language snapshot) with HuggingFace dataset

WHY: bigquery-public-data.github_repos.languages is a ~2017 static snapshot.
     Joining it with GH Archive commits caps all language analysis at 2017,
     causing every language to show artificial decline in D1/D2.

FIX: ibragim-bad/github-repos-metadata-40M covers repos created through July 2025,
     has Linguist-based language labels, and includes created_at timestamps so we
     can prevent pre-creation backfill in d1_preprocess.py.

What this script does:
  1. Streams the full HF dataset (40 M rows, 6 Parquet shards) in batches
  2. Keeps only rows where language IS NOT NULL (drops ~26 %)
  3. Normalises: lowercase language, strip whitespace
  4. Excludes markup/config languages (same list as d1_preprocess._EXCLUDE)
  5. Writes filtered rows as Parquet shards to GCS raw/languages/
     — overwrites the old github_repos snapshot

Output schema (same as old Q1 so Spark jobs need no changes):
  repo_name   STRING   e.g. "owner/repo"
  language    STRING   e.g. "python"
  bytes       LONG     set to 0 — HF dataset has repo.size (KB), not per-language bytes.
                       d1_aggregate.py uses repo_count and commit_count; bytes is unused
                       by any downstream Spark feature so 0 is safe.
  created_at  STRING   ISO-8601 or NULL — used by d1_preprocess to skip pre-birth months

Usage:
  python scripts/upload_hf_q1_to_gcs.py --bucket <your-gcs-bucket>

Cost: free (HF dataset is public; GCS write cost is negligible for ~2 GB)
"""

import argparse
import io
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_dataset
from google.cloud import storage

# ── Config ─────────────────────────────────────────────────────────────────────
KEY_PATH = os.path.join(os.path.dirname(__file__), "..", "service-account-key.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(KEY_PATH)

GCS_PREFIX = "raw/languages"
BATCH_SIZE  = 50_000      # rows buffered before flushing one Parquet shard to GCS
LOG_EVERY   = 500_000     # print progress every N rows processed

# Languages that are markup, config, or too generic — same list as d1_preprocess
_EXCLUDE = {
    "other", "markdown", "text", "unknown", "data", "json", "yaml",
    "xml", "html", "css", "makefile", "dockerfile", "cmake", "shell",
    "batchfile", "ignore list", "rich text format", "ant build system",
}

# Output Parquet schema — matches what d1_preprocess.py expects from raw/languages/
OUTPUT_SCHEMA = pa.schema([
    pa.field("repo_name",   pa.string()),
    pa.field("language",    pa.string()),
    pa.field("bytes",       pa.int64()),
    pa.field("created_at",  pa.string()),   # nullable ISO-8601 timestamp
])


def _should_keep(lang: str | None) -> bool:
    if not lang:
        return False
    return lang.lower().strip() not in _EXCLUDE


def upload_buffer(client: storage.Client, bucket_name: str, blob_name: str,
                  table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_file(buf, content_type="application/octet-stream")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload HF Q1 dataset to GCS")
    parser.add_argument("--bucket", required=True, help="GCS bucket name (no gs://)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Process but do not upload — prints stats only")
    args = parser.parse_args()

    gcs_client = storage.Client() if not args.dry_run else None

    print(f"Streaming ibragim-bad/github-repos-metadata-40M (full split) …")
    print(f"Target: gs://{args.bucket}/{GCS_PREFIX}/")
    print(f"Dry run: {args.dry_run}\n")

    ds = load_dataset(
        "ibragim-bad/github-repos-metadata-40M",
        split="full",
        streaming=True,
    )

    shard_idx      = 0
    total_seen     = 0
    total_kept     = 0
    total_skipped  = 0
    batch_rows: list[dict] = []
    t0 = time.time()

    def flush_batch() -> None:
        nonlocal shard_idx, total_kept
        if not batch_rows:
            return

        table = pa.table(
            {
                "repo_name":  [r["repo_name"]  for r in batch_rows],
                "language":   [r["language"]   for r in batch_rows],
                "bytes":      [0] * len(batch_rows),      # not available per-language
                "created_at": [r.get("created_at") for r in batch_rows],
            },
            schema=OUTPUT_SCHEMA,
        )

        if not args.dry_run:
            blob_name = f"{GCS_PREFIX}/hf_part_{shard_idx:05d}.parquet"
            upload_buffer(gcs_client, args.bucket, blob_name, table)

        total_kept += len(batch_rows)
        shard_idx  += 1
        batch_rows.clear()

    for row in ds:
        total_seen += 1
        lang = row.get("language")

        if not _should_keep(lang):
            total_skipped += 1
            if total_seen % LOG_EVERY == 0:
                elapsed = time.time() - t0
                rate    = total_seen / elapsed
                print(f"  [{total_seen:>12,}] kept={total_kept:,} "
                      f"skipped={total_skipped:,}  "
                      f"{rate:,.0f} rows/s")
            continue

        batch_rows.append({
            "repo_name":  row["repo_name"],
            "language":   lang.lower().strip(),
            "created_at": row.get("created_at"),
        })

        if len(batch_rows) >= BATCH_SIZE:
            flush_batch()

        if total_seen % LOG_EVERY == 0:
            elapsed = time.time() - t0
            rate    = total_seen / elapsed
            print(f"  [{total_seen:>12,}] kept={total_kept:,} "
                  f"skipped={total_skipped:,}  shards={shard_idx}  "
                  f"{rate:,.0f} rows/s")

    flush_batch()   # final partial batch

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"Done in {elapsed/60:.1f} min")
    print(f"  Total rows seen : {total_seen:,}")
    print(f"  Rows kept       : {total_kept:,}  ({100*total_kept/total_seen:.1f}%)")
    print(f"  Rows skipped    : {total_skipped:,}  ({100*total_skipped/total_seen:.1f}%)")
    print(f"  Parquet shards  : {shard_idx}")
    if not args.dry_run:
        print(f"  GCS path        : gs://{args.bucket}/{GCS_PREFIX}/")
    print(f"{'='*60}")
    print("\nNext step: re-run the Spark pipeline (d1_preprocess → d1_aggregate → "
          "d1_features_cluster → d2_forecasting)")


if __name__ == "__main__":
    main()
