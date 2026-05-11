#!/usr/bin/env python3
"""
export_to_json.py — Convert Spark output parquet to slim JSON for the web UI.

Reads from either:
  - GCS:   gs://<bucket>/processed/...  (default)
  - Local: a path you point to with --local-base

Writes JSON files into webui/public/data/.

Usage:
    python scripts/export_to_json.py                            # read from GCS
    python scripts/export_to_json.py --local-base ./processed   # read from local parquet
    python scripts/export_to_json.py --bucket my-bucket --top-langs 20

Only top-N languages (by avg repo_count) are written to keep JSON small.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pandas as pd


def base_url(bucket: str | None, local_base: str | None, subpath: str) -> str:
    if local_base:
        return os.path.join(local_base, subpath)
    return f"gs://{bucket}/processed/{subpath}"


def export_d1(out_dir: Path, bucket: str | None, local_base: str | None, top_n: int) -> list[str]:
    monthly = pd.read_parquet(base_url(bucket, local_base, "d1_monthly/"))
    clusters = pd.read_parquet(base_url(bucket, local_base, "d1_clusters/"))

    monthly = monthly[monthly["year_month"] <= "2025-06"].copy()

    top_langs = (
        monthly.groupby("language")["repo_count"].mean()
        .sort_values(ascending=False).head(top_n).index.tolist()
    )
    sub = monthly[monthly["language"].isin(top_langs)].copy()
    sub = sub.sort_values(["language", "year_month"])

    # Slim payload
    records = sub[["language", "year_month", "repo_count",
                   "total_bytes", "commit_count"]].to_dict(orient="records")
    (out_dir / "d1_monthly.json").write_text(json.dumps({
        "top_langs": top_langs,
        "rows": records,
    }))

    cluster_rows = clusters[clusters["language"].isin(top_langs)].to_dict(orient="records")
    (out_dir / "d1_clusters.json").write_text(json.dumps(cluster_rows))
    return top_langs


def export_d2(out_dir: Path, bucket: str | None, local_base: str | None) -> None:
    df = pd.read_parquet(base_url(bucket, local_base, "d2_forecasts/"))
    # Drop rows with all-NaN forecast values to slim payload
    df = df.copy()
    keep = ["language", "year_month", "actual",
            "prophet_yhat", "prophet_lower", "prophet_upper",
            "arima_yhat", "is_forecast"]
    df = df[keep]
    (out_dir / "d2_forecasts.json").write_text(
        df.to_json(orient="records")
    )


def export_d3(out_dir: Path, bucket: str | None, local_base: str | None) -> None:
    edges = pd.read_parquet(base_url(bucket, local_base, "d3_edges/"))
    try:
        nodes = pd.read_parquet(base_url(bucket, local_base, "d3_nodes/"))
        node_rows = nodes.to_dict(orient="records")
    except Exception:
        node_rows = []
    (out_dir / "d3_migration.json").write_text(json.dumps({
        "edges": edges.to_dict(orient="records"),
        "nodes": node_rows,
    }))


def export_d4(out_dir: Path, bucket: str | None, local_base: str | None) -> None:
    df = pd.read_parquet(base_url(bucket, local_base, "d4_pagerank/"))
    df = df.nlargest(200, "pagerank").copy()
    df["dev_short"] = df["dev_id"].str[:12]
    (out_dir / "d4_pagerank.json").write_text(
        df[["dev_short", "pagerank", "hub_score", "auth_score",
            "degree", "total_commits"]].to_json(orient="records")
    )


def export_d5(out_dir: Path, bucket: str | None, local_base: str | None) -> None:
    devs = pd.read_parquet(base_url(bucket, local_base, "d5_communities/"))
    # Aggregate community-level stats
    comm = devs.groupby("community_id").agg(
        size=("dev_id", "count"),
        avg_pagerank=("pagerank", "mean"),
        max_pagerank=("pagerank", "max"),
        avg_degree=("degree", "mean"),
    ).reset_index()
    comm = comm[comm["size"] >= 5].nlargest(60, "size")

    # Try to attach top orgs if available
    try:
        top_orgs = pd.read_parquet(base_url(bucket, local_base, "d5_top_orgs/"))
        comm = comm.merge(top_orgs, on="community_id", how="left")
    except Exception:
        comm["top_orgs"] = ""

    bridge_devs = devs.nlargest(50, "degree")[
        ["dev_id", "community_id", "pagerank", "degree"]
    ].copy()
    bridge_devs["dev_short"] = bridge_devs["dev_id"].str[:12]
    bridge_devs = bridge_devs.drop(columns=["dev_id"])

    (out_dir / "d5_communities.json").write_text(json.dumps({
        "communities": comm.to_dict(orient="records"),
        "bridge_devs": bridge_devs.to_dict(orient="records"),
    }))


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Spark parquet → JSON for web UI")
    parser.add_argument("--bucket", default="github-tech-trends-data")
    parser.add_argument("--local-base",
                        help="If set, read parquet from this local path instead of GCS")
    parser.add_argument("--top-langs", type=int, default=20,
                        help="Number of top languages to include in D1 (default: 20)")
    parser.add_argument("--out", default="webui/public/data",
                        help="Output directory for JSON files")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    bucket = None if args.local_base else args.bucket
    print(f"Reading from: {'local ' + args.local_base if args.local_base else 'gs://' + bucket}")
    print(f"Writing to:  {out_dir.resolve()}\n")

    print("D1 — language trends + clusters")
    export_d1(out_dir, bucket, args.local_base, args.top_langs)
    print("D2 — forecasts")
    export_d2(out_dir, bucket, args.local_base)
    print("D3 — migration graph")
    export_d3(out_dir, bucket, args.local_base)
    print("D4 — developer PageRank")
    export_d4(out_dir, bucket, args.local_base)
    print("D5 — communities")
    export_d5(out_dir, bucket, args.local_base)

    print("\nDone. Restart the dev server (npm run dev) to pick up new JSON.")


if __name__ == "__main__":
    main()
