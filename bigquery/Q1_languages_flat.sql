-- =============================================================================
-- Q1 — Language Flat Table
-- Deliverables: D1 (Language Growth Trends), D2 (Forecasting)
--
-- Unnests the nested language array so each row is one (repo, language) pair.
-- Filters out null/empty languages and zero-byte entries.
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
