#!/usr/bin/env python3
"""
d1_preprocess.py — D1/D2 Preprocessing

Reads raw Q1 (HuggingFace language snapshot) and Q2 (GH Archive commits_monthly)
Parquet exports from GCS, joins them on repo_name, applies basic cleaning, and
writes a single enriched Parquet dataset partitioned by language.

Q1 source change:
  Was: bigquery-public-data.github_repos.languages (~2017 static snapshot)
  Now: ibragim-bad/github-repos-metadata-40M via upload_hf_q1_to_gcs.py
       — repos created through July 2025, same Linguist-based language labels,
       adds created_at column used below to prevent pre-birth month backfill.

Q2 source change:
  Was: bigquery-public-data.github_repos.commits (frozen)
  Now: githubarchive.month.* PushEvent counts — true time series 2015→today.

GCS input:
  gs://<bucket>/raw/languages/     (Q1: repo_name, language, bytes, created_at)
  gs://<bucket>/raw/commits/       (Q2: repo_name, year_month, commit_count)

GCS output:
  gs://<bucket>/processed/d1_preprocessed/
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

# Languages that are markup, config, or too generic to be informative
_EXCLUDE = {
    "other", "markdown", "text", "unknown", "data", "json", "yaml",
    "xml", "html", "css", "makefile", "dockerfile", "cmake", "shell",
    "batchfile", "ignore list", "rich text format", "ant build system",
}

# bytes column is set to 0 by the HF uploader (not available per-language).
# We keep the filter at 0 so no rows are dropped on bytes — downstream
# d1_aggregate uses repo_count and commit_count, not bytes.
MIN_BYTES = 0


def main() -> None:
    parser = argparse.ArgumentParser(description="D1 preprocessing")
    parser.add_argument("--bucket",  required=True, help="GCS bucket name")
    parser.add_argument("--project", required=False, help="GCP project (unused)")
    args = parser.parse_args()
    bucket = args.bucket

    spark = (
        SparkSession.builder
        .appName("D1-Preprocess")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Q1: language flat table (HuggingFace 40M dataset) ─────────────────────
    languages = (
        spark.read.parquet(f"gs://{bucket}/raw/languages/")
        .withColumn("language", F.lower(F.trim(F.col("language"))))
        .filter(F.col("language").isNotNull())
        .filter(F.col("language") != "")
        .filter(~F.col("language").isin(list(_EXCLUDE)))
        # created_at: parse ISO-8601 string to date; NULL rows are kept (no filter)
        .withColumn(
            "created_month",
            F.date_format(
                F.to_timestamp(F.col("created_at")),
                "yyyy-MM",
            ),
        )
    )

    # ── Q2: monthly push counts (GH Archive) ──────────────────────────────────
    # Cap at 2025-12: GH Archive is live so the export contains partial 2026
    # months. Q1 (HF dataset) only covers repos created through July 2025, so
    # 2026 rows are both incomplete and outside the analysis window.
    commits = (
        spark.read.parquet(f"gs://{bucket}/raw/commits/")
        .filter(F.col("year_month").isNotNull())
        .filter(F.col("commit_count").cast(LongType()) > 0)
        .filter(F.col("year_month") <= "2025-12")
    )

    # ── Join diagnostics ──────────────────────────────────────────────────────
    langs_repos   = languages.select("repo_name").distinct()
    commits_repos = commits.select("repo_name").distinct()
    only_in_commits = commits_repos.subtract(langs_repos).count()
    only_in_langs   = langs_repos.subtract(commits_repos).count()
    print(
        f"d1_preprocess: join diagnostics — "
        f"{only_in_commits:,} repos in commits only (dropped), "
        f"{only_in_langs:,} repos in languages only (dropped)"
    )

    # ── Join: one row per (repo, language, year_month) ────────────────────────
    joined = (
        languages.alias("l")
        .join(commits.alias("c"), on="repo_name", how="inner")
        .select(
            F.col("repo_name"),
            F.col("language"),
            F.col("bytes").cast(LongType()).alias("bytes"),
            F.col("year_month"),
            F.col("commit_count").cast(LongType()).alias("commit_count"),
            F.col("created_month"),
        )
        # Pre-birth filter: drop activity rows that precede the repo's creation.
        # This prevents the static language label being backfilled onto months
        # before the repo existed — a key cause of the artificial early spike.
        # Rows where created_month IS NULL are kept (conservative: no filter).
        .filter(
            F.col("created_month").isNull()
            | (F.col("year_month") >= F.col("created_month"))
        )
        .drop("created_month")
    )

    (
        joined
        .repartition(200, "language")
        .write
        .mode("overwrite")
        .partitionBy("language")
        .parquet(f"gs://{bucket}/processed/d1_preprocessed/")
    )

    print(f"d1_preprocess: wrote to gs://{bucket}/processed/d1_preprocessed/")
    spark.stop()


if __name__ == "__main__":
    main()
