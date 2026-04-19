#!/usr/bin/env python3
"""
d1_aggregate.py — D1 Monthly Language Metrics

Aggregates the preprocessed data to one row per (language, year_month) with:
  - total_bytes  : sum of bytes across all repos using that language
  - repo_count   : number of distinct repos using that language that month
  - commit_count : sum of commits from repos using that language that month

Only languages with data in at least 12 distinct months (≥1 full year) are kept,
ensuring that forecasting models in d2 have enough history.

GCS input:  gs://<bucket>/processed/d1_preprocessed/
GCS output: gs://<bucket>/processed/d1_monthly/
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser(description="D1 monthly aggregation")
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--project", required=False)
    args = parser.parse_args()
    bucket = args.bucket

    spark = (
        SparkSession.builder
        .appName("D1-Aggregate")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(f"gs://{bucket}/processed/d1_preprocessed/")

    # ── Monthly aggregation ───────────────────────────────────────────────────
    monthly = (
        df.groupBy("language", "year_month")
        .agg(
            F.sum("bytes").alias("total_bytes"),
            F.countDistinct("repo_name").alias("repo_count"),
            F.sum("commit_count").alias("commit_count"),
        )
    )

    # ── Minimum coverage filter ───────────────────────────────────────────────
    # Keep only languages with at least 12 months of data across the dataset
    lang_coverage = (
        monthly
        .groupBy("language")
        .agg(F.count("year_month").alias("month_count"))
        .filter(F.col("month_count") >= 12)
        .select("language")
    )

    result = (
        monthly
        .join(lang_coverage, on="language", how="inner")
        .orderBy("language", "year_month")
    )

    (
        result
        .repartition(50)
        .write
        .mode("overwrite")
        .parquet(f"gs://{bucket}/processed/d1_monthly/")
    )

    row_count = result.count()
    print(f"d1_aggregate: wrote {row_count:,} rows to gs://{bucket}/processed/d1_monthly/")
    spark.stop()


if __name__ == "__main__":
    main()
