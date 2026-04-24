#!/usr/bin/env python3
"""
d4d5_developer_graph.py — D4 Developer Influence PageRank & D5 Community Map

Builds a developer co-contribution graph from Q4 (developer-repo) data,
computes PageRank + HITS scores (D4), and runs Leiden community detection (D5).

Pipeline:
  1. Read raw developer-repo Parquet from GCS
  2. Hash developer emails with SHA-256 immediately (raw emails NEVER written)
  3. Filter to top-N developers by total commits
  4. Build developer–developer co-contributor edges via a Spark self-join:
       two devs share an edge if they committed to the same repo in the same year
  5. Collect edge list to driver; build an igraph undirected weighted graph
  6. D4: PageRank + HITS (hub/authority) via igraph
  7. D5: Leiden community detection via leidenalg
  8. Write PageRank table and community table as Parquet

GCS input:  gs://<bucket>/raw/developers/
GCS output: gs://<bucket>/processed/d4_pagerank/
            gs://<bucket>/processed/d5_communities/

Privacy:
  Developer emails are irreversibly hashed on the Spark cluster using SHA-256
  before any graph, output file, or log entry is created.  The raw email column
  is dropped from the DataFrame immediately after hashing.
"""

import argparse
import hashlib
import importlib
import subprocess
import sys

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_COMMITS_PER_EDGE  = 1    # minimum commits for a dev→repo edge to be kept
MIN_SHARED_REPOS      = 1    # minimum shared repos for a dev–dev edge
TOP_N_DEVS            = 25_000  # select by distinct repos, not total commits
MAX_REPOS_PER_DEV     = 2_000  # devs with >2k repos are almost certainly bots
MAX_DEVS_PER_REPO     = 200   # exclude megarepos (linux, tensorflow) from join to prevent explosion

PAGERANK_SCHEMA = StructType([
    StructField("dev_id",       StringType(),  True),  # SHA-256 of email
    StructField("pagerank",     DoubleType(),  True),
    StructField("hub_score",    DoubleType(),  True),
    StructField("auth_score",   DoubleType(),  True),
    StructField("degree",       IntegerType(), True),
    StructField("total_commits",IntegerType(), True),
])

COMMUNITY_SCHEMA = StructType([
    StructField("dev_id",       StringType(),  True),
    StructField("community_id", IntegerType(), True),
    StructField("pagerank",     DoubleType(),  True),
    StructField("degree",       IntegerType(), True),
])


def _ensure_module(module_name: str, pip_spec: str):
    """Install module on the Dataproc driver if missing, then import it."""
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        print(f"Installing missing dependency: {pip_spec}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pip_spec, "--quiet"])
        return importlib.import_module(module_name)


def _sha256(email: str) -> str:
    """One-way hash used to anonymise developer identities."""
    return hashlib.sha256(email.strip().lower().encode()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="D4/D5 developer graph")
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--project", required=False)
    parser.add_argument("--top-n",   type=int, default=TOP_N_DEVS)
    args   = parser.parse_args()
    bucket = args.bucket
    top_n  = args.top_n

    # Install igraph/leidenalg on the driver BEFORE creating SparkSession.
    # igraph requires C++ compilation (~10 min); if done after SparkSession
    # creation, Dataproc's dynamic-allocation drops idle executors before the
    # first Spark task fires, causing "no resources accepted" stall.
    ig = _ensure_module("igraph", "python-igraph==0.11.6")
    leidenalg = _ensure_module("leidenalg", "leidenalg==0.10.2")

    spark = (
        SparkSession.builder
        .appName("D4D5-DeveloperGraph")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Read + hash emails immediately ────────────────────────────────────────
    sha256_udf = F.udf(_sha256, StringType())

    raw = (
        spark.read.parquet(f"gs://{bucket}/raw/developers/")
        .filter(F.col("developer_email").isNotNull())
        .filter(F.col("developer_email") != "")
        .filter(F.col("commit_count") >= MIN_COMMITS_PER_EDGE)
        # Cap at 2025: GH Archive export includes partial 2026 months
        .filter(F.col("year") <= "2025")
        .withColumn("dev_id", sha256_udf(F.col("developer_email")))
        .drop("developer_email")   # raw email permanently removed
    )

    # ── Select top-N developers by distinct repos, not total commits ──────────
    # Selecting by total commits biases toward CI bots that push thousands of
    # commits to a single repo. Selecting by distinct repo count gives real
    # contributors who collaborate across multiple projects.
    dev_stats = (
        raw
        .groupBy("dev_id")
        .agg(
            F.countDistinct("repo_name").cast(IntegerType()).alias("distinct_repos"),
            F.sum("commit_count").cast(IntegerType()).alias("total_commits"),
        )
        # Hard bot ceiling: >5k distinct repos is automation (package mirrors, forks)
        .filter(F.col("distinct_repos") <= MAX_REPOS_PER_DEV)
        # Require at least 2 repos to filter one-shot drive-by commits
        .filter(F.col("distinct_repos") >= 2)
    )

    top_devs = (
        dev_stats
        .orderBy(F.col("distinct_repos").desc())
        .limit(top_n)
    )

    hashed = raw.join(top_devs.select("dev_id", "total_commits"), on="dev_id", how="inner")

    # ── Build dev–dev co-contributor edges via self-join ──────────────────────
    # Two developers are connected if they both committed to the same repo
    # in the same year.  We use (dev_a < dev_b) to deduplicate undirected pairs.
    # Exclude megarepos (>MAX_DEVS_PER_REPO contributors) — joining on linux or
    # tensorflow would explode: N*(N-1)/2 pairs for N=10k is 50M rows.
    dev_repo_raw = hashed.select("dev_id", "repo_name", "year").distinct()

    repo_dev_counts = (
        dev_repo_raw.groupBy("repo_name")
        .agg(F.countDistinct("dev_id").alias("repo_dev_count"))
        .filter(F.col("repo_dev_count") <= MAX_DEVS_PER_REPO)
    )

    dev_repo = dev_repo_raw.join(
        repo_dev_counts.select("repo_name"), on="repo_name", how="inner"
    )

    dev_dev = (
        dev_repo.alias("a")
        .join(dev_repo.alias("b"), on=["repo_name", "year"], how="inner")
        .filter(F.col("a.dev_id") < F.col("b.dev_id"))
        .groupBy(
            F.col("a.dev_id").alias("dev_a"),
            F.col("b.dev_id").alias("dev_b"),
        )
        .agg(F.countDistinct("repo_name").cast(IntegerType()).alias("shared_repos"))
        .filter(F.col("shared_repos") >= MIN_SHARED_REPOS)
    )

    dev_dev_pd:    pd.DataFrame = dev_dev.toPandas()
    top_devs_pd:   pd.DataFrame = top_devs.select("dev_id", "total_commits").toPandas()

    print(
        f"d4d5_developer_graph: {len(top_devs_pd):,} developers, "
        f"{len(dev_dev_pd):,} co-contributor edges"
    )

    # ── Build igraph undirected weighted graph ────────────────────────────────
    all_devs  = sorted(top_devs_pd["dev_id"].tolist())
    dev_index = {dev: i for i, dev in enumerate(all_devs)}
    n         = len(all_devs)

    edge_list = []
    weights   = []
    for _, row in dev_dev_pd.iterrows():
        a, b = row["dev_a"], row["dev_b"]
        if a in dev_index and b in dev_index:
            edge_list.append((dev_index[a], dev_index[b]))
            weights.append(int(row["shared_repos"]))

    G = ig.Graph(n=n, edges=edge_list, directed=False)
    G.vs["dev_id"] = all_devs
    G.es["weight"] = weights

    print(f"d4d5_developer_graph: igraph — {G.vcount():,} vertices, {G.ecount():,} edges")

    # ── D4: PageRank + betweenness centrality ────────────────────────────────
    # HITS hub/authority scores are mathematically identical on undirected graphs
    # (hub_score == authority_score always). Replaced with betweenness centrality
    # which meaningfully measures bridge nodes in an undirected co-contributor graph.
    pagerank_scores     = G.pagerank(weights="weight", damping=0.85)
    raw_betweenness     = G.betweenness(weights="weight", directed=False)
    n                   = G.vcount()
    denom               = (n - 1) * (n - 2) / 2 if n > 2 else 1.0
    betweenness_scores  = [b / denom for b in raw_betweenness]
    degrees             = G.degree()

    dev_to_commits = top_devs_pd.set_index("dev_id")["total_commits"].to_dict()

    pagerank_rows = [
        (
            dev,
            float(pagerank_scores[i]),
            float(betweenness_scores[i]),   # hub_score slot → betweenness
            float(betweenness_scores[i]),   # auth_score slot → betweenness (schema compat)
            int(degrees[i]),
            int(dev_to_commits.get(dev, 0)),
        )
        for i, dev in enumerate(all_devs)
    ]

    # ── D5: Leiden community detection ────────────────────────────────────────
    partition      = leidenalg.find_partition(
        G,
        leidenalg.ModularityVertexPartition,
        weights="weight",
        seed=42,
    )
    community_ids  = partition.membership

    community_rows = [
        (
            dev,
            int(community_ids[i]),
            float(pagerank_scores[i]),
            int(degrees[i]),
        )
        for i, dev in enumerate(all_devs)
    ]

    n_communities = len(set(community_ids))
    print(f"d4d5_developer_graph: {n_communities} Leiden communities detected")

    # ── Write outputs ─────────────────────────────────────────────────────────
    pagerank_sdf  = spark.createDataFrame(pagerank_rows,  schema=PAGERANK_SCHEMA)
    community_sdf = spark.createDataFrame(community_rows, schema=COMMUNITY_SCHEMA)

    pagerank_sdf.write.mode("overwrite").parquet(f"gs://{bucket}/processed/d4_pagerank/")
    community_sdf.write.mode("overwrite").parquet(f"gs://{bucket}/processed/d5_communities/")

    print("d4d5_developer_graph: wrote d4_pagerank/ and d5_communities/")
    spark.stop()


if __name__ == "__main__":
    main()
