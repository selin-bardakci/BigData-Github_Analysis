#!/usr/bin/env python3
"""
d1_preprocess.py — D1/D2 Preprocessing

Reads raw Q1 (languages_flat) and Q2 (commits_monthly) Parquet exports from
GCS, joins them on repo_name, applies basic cleaning, and writes a single
enriched Parquet dataset partitioned by language.

GCS input:
  gs://<bucket>/raw/languages/     (Q1: repo_name, language, bytes)
  gs://<bucket>/raw/commits/       (Q2: repo_name, year_month, commit_count)

GCS output:
  gs://<bucket>/processed/d1_preprocessed/

Spark invocation (via run_all_spark_jobs.sh):
  gcloud dataproc jobs submit pyspark ... -- --bucket=<bucket>
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

# Minimum byte threshold: filters out trivially small language presence
MIN_BYTES = 1_000


def main() -> None:
    parser = argparse.ArgumentParser(description="D1 preprocessing")
    parser.add_argument("--bucket",  required=True, help="GCS bucket name")
    parser.add_argument("--project", required=False, help="GCP project (unused, kept for interface parity)")
    args = parser.parse_args()
    bucket = args.bucket

    spark = (
        SparkSession.builder
        .appName("D1-Preprocess")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Q1: language flat table ────────────────────────────────────────────────
    languages = (
        spark.read.parquet(f"gs://{bucket}/raw/languages/")
        .withColumn("language", F.lower(F.trim(F.col("language"))))
        .filter(F.col("language").isNotNull())
        .filter(F.col("language") != "")
        .filter(F.col("bytes").cast(LongType()) >= MIN_BYTES)
        .filter(~F.col("language").isin(list(_EXCLUDE)))
    )

    # ── Q2: monthly commit counts ──────────────────────────────────────────────
    commits = (
        spark.read.parquet(f"gs://{bucket}/raw/commits/")
        .filter(F.col("year_month").isNotNull())
        .filter(F.col("commit_count").cast(LongType()) > 0)
    )

    # ── Join: one row per (repo, language, year_month) ─────────────────────────
    # languages: one row per (repo, language)
    # commits:   one row per (repo, year_month)
    # Inner join produces the cross-product on shared repos
    joined = (
        languages.alias("l")
        .join(commits.alias("c"), on="repo_name", how="inner")
        .select(
            F.col("repo_name"),
            F.col("language"),
            F.col("bytes").cast(LongType()).alias("bytes"),
            F.col("year_month"),
            F.col("commit_count").cast(LongType()).alias("commit_count"),
        )
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
