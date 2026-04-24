#!/usr/bin/env python3
"""
d3_migration_graph.py — D3 Technology Migration Graph

Reads Q3 migration-signal Parquet data, applies a technology allowlist to
remove noise, builds a directed weighted graph with NetworkX, and computes
node-level metrics (PageRank, betweenness centrality, in/out flow).

Pipeline:
  1. Read raw migration signals (from_tech, to_tech, year, migration_count)
  2. Filter both endpoints against the technology allowlist
  3. Aggregate across years: total migration_count, years active, first/last year
  4. Build a NetworkX DiGraph on the driver (graph is small after filtering)
  5. Compute PageRank and betweenness centrality
  6. Write edge list and node-metrics table as Parquet

GCS input:  gs://<bucket>/raw/migrations/
GCS output: gs://<bucket>/processed/d3_edges/
            gs://<bucket>/processed/d3_nodes/
"""

import argparse

import networkx as nx
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

# ── Technology allowlist ──────────────────────────────────────────────────────
# Only migrations between these known technologies are kept; everything else is
# regex noise from natural-language commit messages.
TECH_ALLOWLIST: set[str] = {
    # Languages
    "python", "javascript", "typescript", "java", "go", "rust", "kotlin",
    "swift", "scala", "ruby", "php", "c", "cpp", "r", "dart",
    "elixir", "clojure", "haskell", "lua", "perl", "groovy", "coffeescript",
    # Frameworks & libraries
    "flask", "fastapi", "django", "rails", "spring", "express", "react",
    "angular", "vue", "svelte", "nextjs", "nuxt", "laravel", "symfony",
    # Testing
    "junit", "pytest", "mocha", "jest", "rspec",
    # Build & CI
    "webpack", "vite", "rollup", "parcel", "gradle", "maven", "bazel",
    "travis", "jenkins", "github-actions", "circleci", "gitlab-ci",
    # Databases
    "mysql", "postgres", "postgresql", "mongodb", "redis", "elasticsearch",
    "sqlite", "cassandra", "dynamodb", "firebase",
    # Infrastructure & Cloud
    "docker", "kubernetes", "terraform", "ansible", "helm",
    "aws", "gcp", "azure", "heroku", "netlify", "vercel",
    # Package managers
    "npm", "yarn", "pnpm", "pip", "poetry", "cargo",
}

# Edges with fewer total migration signals than this are dropped.
# Lowered from 10 → 3: the Q3 data from GH Archive is sparser than the old
# frozen snapshot because commit messages are extracted per-push rather than
# per-commit, so real signals survive at lower counts.
MIN_EDGE_WEIGHT = 3

EDGE_SCHEMA = StructType([
    StructField("from_tech",       StringType(),  True),
    StructField("to_tech",         StringType(),  True),
    StructField("migration_count", IntegerType(), True),
    StructField("years_active",    IntegerType(), True),
    StructField("first_year",      StringType(),  True),
    StructField("last_year",       StringType(),  True),
])

NODE_SCHEMA = StructType([
    StructField("tech",        StringType(),  True),
    StructField("out_weight",  IntegerType(), True),  # sum of outgoing migration_count
    StructField("in_weight",   IntegerType(), True),  # sum of incoming migration_count
    StructField("net_flow",    IntegerType(), True),  # in_weight - out_weight
    StructField("pagerank",    DoubleType(),  True),
    StructField("betweenness", DoubleType(),  True),
])


def main() -> None:
    parser = argparse.ArgumentParser(description="D3 migration graph")
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--project", required=False)
    args   = parser.parse_args()
    bucket = args.bucket

    spark = (
        SparkSession.builder
        .appName("D3-MigrationGraph")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.parquet(f"gs://{bucket}/raw/migrations/")

    # ── Apply allowlist ───────────────────────────────────────────────────────
    filtered = (
        raw
        .filter(F.col("from_tech").isin(list(TECH_ALLOWLIST)))
        .filter(F.col("to_tech").isin(list(TECH_ALLOWLIST)))
        .filter(F.col("from_tech") != F.col("to_tech"))
    )

    # ── Aggregate across years ────────────────────────────────────────────────
    edges_sdf = (
        filtered
        .groupBy("from_tech", "to_tech")
        .agg(
            F.sum("migration_count").cast(IntegerType()).alias("migration_count"),
            F.countDistinct("year").cast(IntegerType()).alias("years_active"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        .filter(F.col("migration_count") >= MIN_EDGE_WEIGHT)
        .orderBy(F.col("migration_count").desc())
    )

    edges_pd: pd.DataFrame = edges_sdf.toPandas()
    print(f"d3_migration_graph: {len(edges_pd)} edges after allowlist + weight filter")

    # ── Build NetworkX DiGraph on driver ──────────────────────────────────────
    G: nx.DiGraph = nx.DiGraph()
    for _, row in edges_pd.iterrows():
        G.add_edge(
            row["from_tech"],
            row["to_tech"],
            weight=int(row["migration_count"]),
        )

    print(
        f"d3_migration_graph: graph — {G.number_of_nodes()} nodes, "
        f"{G.number_of_edges()} edges"
    )

    # ── Node metrics ──────────────────────────────────────────────────────────
    pagerank    = nx.pagerank(G, weight="weight", alpha=0.85)
    betweenness = nx.betweenness_centrality(G, weight="weight", normalized=True)

    node_rows = []
    for node in G.nodes():
        out_w = sum(d["weight"] for _, _, d in G.out_edges(node, data=True))
        in_w  = sum(d["weight"] for _, _, d in G.in_edges(node, data=True))
        node_rows.append((
            str(node),
            int(out_w),
            int(in_w),
            int(in_w - out_w),
            float(pagerank.get(node, 0.0)),
            float(betweenness.get(node, 0.0)),
        ))

    # ── Write ─────────────────────────────────────────────────────────────────
    edges_out = spark.createDataFrame(
        edges_pd.astype({
            "migration_count": int, "years_active": int,
            "first_year": str,      "last_year":    str,
        }),
        schema=EDGE_SCHEMA,
    )
    nodes_out = spark.createDataFrame(node_rows, schema=NODE_SCHEMA)

    edges_out.write.mode("overwrite").parquet(f"gs://{bucket}/processed/d3_edges/")
    nodes_out.write.mode("overwrite").parquet(f"gs://{bucket}/processed/d3_nodes/")

    print(f"d3_migration_graph: wrote edges and nodes to gs://{bucket}/processed/d3_*/")
    spark.stop()


if __name__ == "__main__":
    main()
