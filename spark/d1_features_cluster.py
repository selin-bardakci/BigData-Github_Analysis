#!/usr/bin/env python3
"""
d1_features_cluster.py — D1 Feature Engineering & KMeans Clustering

Builds per-language feature vectors from the monthly metrics, scales them,
and runs KMeans (via Spark MLlib) to cluster languages by growth trajectory.

Feature vector per language (5 dimensions):
  avg_bytes_growth   — mean month-over-month % change in total_bytes
  commit_volatility  — std(commit_count) / mean(commit_count)  (coefficient of variation)
  bytes_range_ratio  — max(total_bytes) / min(total_bytes)     (overall growth magnitude)
  avg_repo_count     — mean monthly repo_count                 (adoption breadth)
  avg_commit_count   — mean monthly commit_count               (activity level)

Outputs:
  d1_clusters/        — (language, cluster, feature cols)
  d1_cluster_centers/ — (cluster_id, center_<feature>…)

GCS input:  gs://<bucket>/processed/d1_monthly/
GCS output: gs://<bucket>/processed/d1_clusters/
            gs://<bucket>/processed/d1_cluster_centers/
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler

NUM_CLUSTERS = 6  # emerging / fast-growing / stable / declining / niche / legacy

FEATURE_COLS = [
    "avg_bytes_growth",
    "commit_volatility",
    "bytes_range_ratio",
    "avg_repo_count",
    "avg_commit_count",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="D1 feature engineering & KMeans clustering")
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--project", required=False)
    parser.add_argument("--k",       type=int, default=NUM_CLUSTERS, help="Number of KMeans clusters")
    args = parser.parse_args()
    bucket = args.bucket
    k      = args.k

    spark = (
        SparkSession.builder
        .appName("D1-Features-Cluster")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(f"gs://{bucket}/processed/d1_monthly/")

    # ── Month-over-month byte growth ──────────────────────────────────────────
    w = Window.partitionBy("language").orderBy("year_month")
    df = (
        df
        .withColumn("bytes_prev", F.lag("total_bytes", 1).over(w))
        .withColumn(
            "bytes_growth",
            F.when(
                F.col("bytes_prev") > 0,
                (F.col("total_bytes") - F.col("bytes_prev")) / F.col("bytes_prev"),
            ).otherwise(None),
        )
    )

    # ── Per-language feature aggregation ──────────────────────────────────────
    features = (
        df.groupBy("language")
        .agg(
            F.avg("bytes_growth").alias("avg_bytes_growth"),
            F.stddev("commit_count").alias("_commit_std"),
            F.avg("commit_count").alias("avg_commit_count"),
            F.max("total_bytes").alias("_max_bytes"),
            F.min("total_bytes").alias("_min_bytes"),
            F.avg("repo_count").alias("avg_repo_count"),
        )
        .withColumn(
            "commit_volatility",
            F.when(
                F.col("avg_commit_count") > 0,
                F.col("_commit_std") / F.col("avg_commit_count"),
            ).otherwise(0.0),
        )
        .withColumn(
            "bytes_range_ratio",
            F.when(
                F.col("_min_bytes") > 0,
                F.col("_max_bytes") / F.col("_min_bytes"),
            ).otherwise(1.0),
        )
        .drop("_commit_std", "_max_bytes", "_min_bytes")
        .fillna(0.0, subset=["avg_bytes_growth", "commit_volatility"])
    )

    # ── Assemble & scale ──────────────────────────────────────────────────────
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features", handleInvalid="skip")
    scaler    = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)

    assembled    = assembler.transform(features)
    scaler_model = scaler.fit(assembled)
    scaled       = scaler_model.transform(assembled)

    # ── KMeans ────────────────────────────────────────────────────────────────
    kmeans    = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=42, maxIter=50)
    km_model  = kmeans.fit(scaled)
    clustered = km_model.transform(scaled)

    silhouette = ClusteringEvaluator(featuresCol="features", predictionCol="cluster").evaluate(clustered)
    print(f"d1_features_cluster: KMeans silhouette ({k} clusters) = {silhouette:.4f}")

    # ── Write per-language clusters ───────────────────────────────────────────
    (
        clustered
        .select("language", "cluster", *FEATURE_COLS)
        .repartition(10)
        .write
        .mode("overwrite")
        .parquet(f"gs://{bucket}/processed/d1_clusters/")
    )

    # ── Write cluster centroids (used in notebook for labelling) ──────────────
    centers_rows = [
        (int(i), *[float(v) for v in c])
        for i, c in enumerate(km_model.clusterCenters())
    ]
    centers_df = spark.createDataFrame(
        centers_rows,
        schema=["cluster_id"] + [f"center_{c}" for c in FEATURE_COLS],
    )
    (
        centers_df
        .write
        .mode("overwrite")
        .parquet(f"gs://{bucket}/processed/d1_cluster_centers/")
    )

    print(f"d1_features_cluster: wrote clusters to gs://{bucket}/processed/d1_clusters/")
    spark.stop()


if __name__ == "__main__":
    main()
