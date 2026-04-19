-- =============================================================================
-- Q2 — Monthly Commit Counts per Repository
-- Deliverables: D1 (Language Growth Trends), D2 (Forecasting)
--
-- Aggregates commit activity to (repo_name, year_month) granularity.
-- Time window: Jan 2015 through the nearest complete month at query runtime.
-- Bot commits are removed in the Spark preprocessing step, not here,
-- to keep the BigQuery scan as lightweight as possible.
--
-- Estimated scan: ~30–50 GB
-- Run once, export result to GCS as Parquet.
-- =============================================================================

SELECT
    repo_name,
    FORMAT_TIMESTAMP(
        '%Y-%m',
        TIMESTAMP_SECONDS(committer.date.seconds)
    )                       AS year_month,
    COUNT(*)                AS commit_count
FROM
    `bigquery-public-data.github_repos.commits`,
    UNNEST(repo_name) AS repo_name          -- repo_name is a repeated field
WHERE
    committer.date.seconds IS NOT NULL
    AND TIMESTAMP_SECONDS(committer.date.seconds)
        BETWEEN TIMESTAMP('2015-01-01')
            AND TIMESTAMP(
                DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
            )
GROUP BY
    repo_name,
    year_month

-- =============================================================================
-- Development / cost-check: append the following LIMIT before the first full run
-- LIMIT 200000
-- =============================================================================
