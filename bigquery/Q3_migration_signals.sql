-- =============================================================================
-- Q3 — Technology Migration Signals from Commit Messages
-- Deliverable: D3 (Technology Migration Graph)
--
-- Extracts directed (from_tech → to_tech) pairs from commit messages that
-- contain explicit migration/replacement language such as:
--   "migrated from Flask to FastAPI"
--   "replaced webpack with vite"
--   "switching from Travis CI to GitHub Actions"
--
-- Regex strategy:
--   Pattern 1 captures the "from" technology in sentences like
--     "migrate from X ..."
--   Pattern 2 captures the "to" technology in sentences like
--     "... to Y" / "... with Y"
--
-- Noise reduction:
--   - Both from_tech and to_tech must be non-null (HAVING clause)
--   - They must differ
--   - Name length bounded to 2–30 characters
--   - Minimum 5 commits per (from, to, year) triple
--
-- The raw output still contains noise; a technology allowlist is applied
-- in the Spark d3_migration_graph.py job.
--
-- Estimated scan: ~80–120 GB (message column is large)
-- Run once, export result to GCS as Parquet.
-- =============================================================================

WITH parsed AS (
    SELECT
        -- Extract the technology being migrated AWAY FROM
        REGEXP_EXTRACT(
            LOWER(message),
            r'(?:migrat\w*|replac\w*|rewrite\w*|switch\w*|mov\w*|upgrad\w*|drop\w*|remov\w*)[^.\n]{0,60}?\bfrom\b\s+([\w][\w\-\.]{1,29})'
        ) AS from_tech,

        -- Extract the technology being migrated TO
        REGEXP_EXTRACT(
            LOWER(message),
            r'(?:migrat\w*|replac\w*|rewrite\w*|switch\w*|mov\w*|upgrad\w*)[^.\n]{0,60}?\b(?:to|with|into)\b\s+([\w][\w\-\.]{1,29})'
        ) AS to_tech,

        FORMAT_TIMESTAMP(
            '%Y',
            TIMESTAMP_SECONDS(committer.date.seconds)
        ) AS year

    FROM
        `bigquery-public-data.github_repos.commits`,
        UNNEST(repo_name) AS repo_name
    WHERE
        message IS NOT NULL
        AND committer.date.seconds IS NOT NULL
        AND TIMESTAMP_SECONDS(committer.date.seconds)
            BETWEEN TIMESTAMP('2015-01-01')
                AND TIMESTAMP(
                    DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
                )
        -- Pre-filter rows: only scan messages that contain migration keywords
        -- This dramatically reduces the amount of regex work BigQuery must do
        AND REGEXP_CONTAINS(
            LOWER(message),
            r'\b(?:migrat|replac|rewrite|switch.*\bto\b|mov.*\bto\b|upgrad)\b'
        )
)

SELECT
    from_tech,
    to_tech,
    year,
    COUNT(*) AS migration_count
FROM
    parsed
WHERE
    from_tech IS NOT NULL
    AND to_tech   IS NOT NULL
    AND from_tech != to_tech
    AND LENGTH(from_tech) BETWEEN 2 AND 30
    AND LENGTH(to_tech)   BETWEEN 2 AND 30
GROUP BY
    from_tech,
    to_tech,
    year
HAVING
    migration_count >= 5
ORDER BY
    migration_count DESC

-- =============================================================================
-- Development / cost-check: append the following LIMIT before the first full run
-- LIMIT 50000
-- =============================================================================
