#!/usr/bin/env python3
"""
d2_forecasting.py — D2 Technology Adoption Forecasting

For each of the top-N languages (by average monthly repo_count), fits an
ARIMA(2,1,2) model and generates a 24-month forward forecast.

Note: Prophet forecasting is performed in notebooks/D2_forecasting.ipynb
using the local venv where Prophet + cmdstanpy are properly installed.
Prophet cannot run reliably on Dataproc base images because the Stan C++
binary cannot be compiled at runtime; the schema retains prophet_* columns
(filled with NaN) for forward compatibility with the notebook visualisation.

GCS input:  gs://<bucket>/processed/d1_monthly/
GCS output: gs://<bucket>/processed/d2_forecasts/
"""

import argparse
import importlib
import subprocess
import sys
import warnings

import numpy as np
import pandas as pd

# Prophet is intentionally excluded from this Spark job — see module docstring.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType,
)

TOP_N        = 30   # number of languages to forecast
FORECAST_MONTHS = 24
ARIMA_ORDER  = (2, 1, 2)
MIN_HISTORY  = 24   # months; skip languages with less data

FORECAST_SCHEMA = StructType([
    StructField("language",      StringType(),  True),
    StructField("year_month",    StringType(),  True),
    StructField("actual",        DoubleType(),  True),   # NaN for forecast rows
    StructField("prophet_yhat",  DoubleType(),  True),
    StructField("prophet_lower", DoubleType(),  True),
    StructField("prophet_upper", DoubleType(),  True),
    StructField("arima_yhat",    DoubleType(),  True),
    StructField("is_forecast",   BooleanType(), True),
])


def _ensure_module(module_name: str, pip_spec: str):
    """Install module on the Dataproc driver if missing, then import it."""
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        print(f"Installing missing dependency: {pip_spec}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pip_spec, "--quiet"])
        return importlib.import_module(module_name)


def _forecast_one(lang: str, df_lang: pd.DataFrame, ARIMACls) -> pd.DataFrame:
    """
    Fit ARIMA for a single language and return historical + forward forecast rows.
    Prophet columns are included as NaN for notebook compatibility.
    """
    warnings.filterwarnings("ignore")

    df_lang = df_lang.sort_values("year_month").reset_index(drop=True)
    df_lang["ds"] = pd.to_datetime(df_lang["year_month"] + "-01")

    # ── Fill gaps: reindex to a continuous monthly series ────────────────────
    # ARIMA(p,d,q) assumes equally-spaced observations. Sparse data (months
    # with no activity produce no row in d1_monthly) must be filled before
    # fitting or the model treats adjacent rows as consecutive when they are not.
    full_range = pd.date_range(
        start=df_lang["ds"].min(), end=df_lang["ds"].max(), freq="MS"
    )
    n_gaps = len(full_range) - len(df_lang)
    if n_gaps > 0:
        print(f"  {lang}: filling {n_gaps} missing month(s) via linear interpolation")
    df_lang = (
        df_lang.set_index("ds")
        .reindex(full_range)
        .rename_axis("ds")
        .reset_index()
    )
    df_lang["year_month"] = df_lang["ds"].dt.strftime("%Y-%m")
    # Linear interpolation for interior gaps; edge NaNs (shouldn't exist after
    # reindex between min/max) are filled with 0 as a safe fallback.
    df_lang["repo_count"] = (
        df_lang["repo_count"].interpolate(method="linear").fillna(0)
    )

    y = df_lang["repo_count"].astype(float).values

    # ── ARIMA ─────────────────────────────────────────────────────────────────
    try:
        model    = ARIMACls(y, order=ARIMA_ORDER).fit()
        arima_fc = model.forecast(steps=FORECAST_MONTHS)
    except Exception as exc:
        print(f"  ARIMA fit failed for {lang}: {exc}")
        arima_fc = np.full(FORECAST_MONTHS, np.nan)

    # ── Forecast dates ────────────────────────────────────────────────────────
    last_date    = df_lang["ds"].iloc[-1]
    fc_dates     = pd.date_range(start=last_date + pd.offsets.MonthBegin(1),
                                 periods=FORECAST_MONTHS, freq="MS")

    # ── Historical rows (prophet_* stays NaN — filled by notebook) ────────────
    hist_rows = pd.DataFrame({
        "language":      lang,
        "year_month":    df_lang["ds"].dt.strftime("%Y-%m"),
        "actual":        y,
        "prophet_yhat":  np.nan,
        "prophet_lower": np.nan,
        "prophet_upper": np.nan,
        "arima_yhat":    np.nan,
        "is_forecast":   False,
    })

    # ── Forecast rows ─────────────────────────────────────────────────────────
    fc_rows = pd.DataFrame({
        "language":      lang,
        "year_month":    fc_dates.strftime("%Y-%m"),
        "actual":        np.nan,
        "prophet_yhat":  np.nan,
        "prophet_lower": np.nan,
        "prophet_upper": np.nan,
        "arima_yhat":    arima_fc,
        "is_forecast":   True,
    })

    return pd.concat([hist_rows, fc_rows], ignore_index=True)[
        ["language", "year_month", "actual", "prophet_yhat",
         "prophet_lower", "prophet_upper", "arima_yhat", "is_forecast"]
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="D2 forecasting")
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--project", required=False)
    parser.add_argument("--top-n",   type=int, default=TOP_N)
    args   = parser.parse_args()
    bucket = args.bucket
    top_n  = args.top_n

    spark = (
        SparkSession.builder
        .appName("D2-Forecasting")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Install statsmodels if missing (Prophet is not used in this Spark job).
    ARIMACls = _ensure_module("statsmodels.tsa.arima.model", "statsmodels==0.14.2").ARIMA

    df = spark.read.parquet(f"gs://{bucket}/processed/d1_monthly/")

    # ── Select top-N languages by average monthly repo_count ─────────────────
    top_langs = (
        df.groupBy("language")
        .agg(F.avg("repo_count").alias("avg_repos"))
        .orderBy(F.col("avg_repos").desc())
        .limit(top_n)
        .rdd.map(lambda r: r["language"])
        .collect()
    )
    print(f"d2_forecasting: top {len(top_langs)} languages selected")

    # ── Collect time-series data to driver (small — N languages × 120 months) ─
    lang_pd = (
        df.filter(F.col("language").isin(top_langs))
        .select("language", "year_month", "repo_count")
        .toPandas()
    )

    # ── Per-language forecasting on driver ────────────────────────────────────
    results = []
    for lang in top_langs:
        subset = lang_pd[lang_pd["language"] == lang].copy()
        # Check span of the continuous series (after gap-filling) rather than
        # raw row count, so a language with 24 sparse rows spanning 10 years
        # is not mistakenly accepted as having 24 months of dense history.
        if len(subset) >= 2:
            ds_min = pd.to_datetime(subset["year_month"] + "-01").min()
            ds_max = pd.to_datetime(subset["year_month"] + "-01").max()
            span_months = (
                (ds_max.year - ds_min.year) * 12 + (ds_max.month - ds_min.month) + 1
            )
        else:
            span_months = len(subset)
        if span_months < MIN_HISTORY:
            print(f"  Skipping {lang}: {span_months} month span < {MIN_HISTORY} required")
            continue
        try:
            results.append(_forecast_one(lang, subset, ARIMACls))
            print(f"  OK: {lang} ({len(subset)} months history)")
        except Exception as exc:
            print(f"  ERROR: {lang} — {exc}")

    if not results:
        print("d2_forecasting: no results produced — exiting")
        spark.stop()
        return

    combined_pd = pd.concat(results, ignore_index=True)

    result_sdf = spark.createDataFrame(combined_pd, schema=FORECAST_SCHEMA)
    (
        result_sdf
        .repartition(10)
        .write
        .mode("overwrite")
        .parquet(f"gs://{bucket}/processed/d2_forecasts/")
    )

    print(
        f"d2_forecasting: wrote {len(combined_pd):,} rows "
        f"({len(results)} languages) to gs://{bucket}/processed/d2_forecasts/"
    )
    spark.stop()


if __name__ == "__main__":
    main()
