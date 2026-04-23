-- =============================================================================
-- Q1 — Language Flat Table
-- Deliverables: D1 (Language Growth Trends), D2 (Forecasting)
--
-- Unnests the nested language array so each row is one (repo, language) pair.
-- Filters out null/empty languages and zero-byte entries.
--
-- DATA SOURCE NOTE:
-- github_repos.languages is the only BigQuery public dataset that maps
-- repo → language. GitHub Archive (githubarchive.month.*) has no language
-- column anywhere — it only records events (pushes, stars, etc.).
-- This table is periodically refreshed by Google (last known refresh ~2023).
-- Repos created after the last refresh will not appear here and will be
-- silently dropped by the inner join in d1_preprocess.py.
-- BEFORE RUNNING: check the "Last modified" date on this table in the
-- BigQuery console (table → Details tab) to know the exact coverage cutoff.
--
-- Estimated scan: ~5–15 GB
-- Run once, export result to GCS as Parquet.
-- =============================================================================

SELECT
    repo_name,
    lang.name  AS language,
    lang.bytes AS bytes
FROM
    `bigquery-public-data.github_repos.languages`,
    UNNEST(language) AS lang
WHERE
    lang.name  IS NOT NULL
    AND lang.name  != ''
    AND lang.bytes  > 0

-- =============================================================================
-- Development / cost-check: append the following LIMIT before the first full run
-- LIMIT 100000
-- =============================================================================
