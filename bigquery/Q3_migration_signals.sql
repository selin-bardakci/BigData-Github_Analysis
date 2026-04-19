-- =============================================================================
-- Q3 — Technology Migration Signals from Commit Messages
-- Deliverable: D3 (Technology Migration Graph)
--
-- Extracts directed (from_tech → to_tech) pairs from commit messages that
-- contain explicit migration/replacement language such as:
--   "migrated from Flask to FastAPI"
--   "replaced webpack with vite"
--   "switching from Travis CI to GitHub Actions"
--   "replace webpack with vite"           ← "replace X with Y" form
--   "moved to Kubernetes"                 ← to_tech only (fallback pattern)
--
-- Regex strategy:
--   from_tech — two independent patterns combined with COALESCE:
--     P1: verb ... "from" X  (e.g. "migrate from Flask ...")
--     P2: "replace/swap X with Y"  — X is the source (e.g. "replace webpack with vite")
--   to_tech — two independent patterns combined with COALESCE:
--     P1: same full verb set as from_tech + "to/with/into Y"
--         (bug fix: old query used a narrower verb set, missing drop/remov/mov)
--     P2: standalone "verb to Y" without an explicit "from" clause
--
-- Noise reduction:
--   - Both from_tech and to_tech must be non-null
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
        -- P1: "verb … from X"
        -- P2: "replace/swap X with Y" — X is the source technology
        COALESCE(
            REGEXP_EXTRACT(
                LOWER(message),
                r'(?:migrat\w*|replac\w*|rewrite\w*|switch\w*|mov\w*|upgrad\w*|drop\w*|remov\w*)[^.\n]{0,60}?\bfrom\b\s+([\w][\w\-\.]{1,29})'
            ),
            REGEXP_EXTRACT(
                LOWER(message),
                r'\b(?:replac\w*|swap\w*)\s+([\w][\w\-\.]{1,29})\s+with\b'
            )
        ) AS from_tech,

        -- Extract the technology being migrated TO
        -- P1: full verb set (was missing drop/remov/mov in the original) + "to/with/into Y"
        -- P2: standalone "verb to Y" — catches "switched to Kubernetes" without explicit "from"
        COALESCE(
            REGEXP_EXTRACT(
                LOWER(message),
                r'(?:migrat\w*|replac\w*|rewrite\w*|switch\w*|mov\w*|upgrad\w*|drop\w*|remov\w*)[^.\n]{0,60}?\b(?:to|with|into)\b\s+([\w][\w\-\.]{1,29})'
            ),
            REGEXP_EXTRACT(
                LOWER(message),
                r'\b(?:switch\w*|mov\w*|migrat\w*|upgrad\w*|rewrite\w*)\s+to\s+([\w][\w\-\.]{1,29})'
            )
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
        -- Pre-filter rows: only scan messages that contain migration keywords.
        -- Extended to cover the "replace X with" pattern added above.
        AND REGEXP_CONTAINS(
            LOWER(message),
            r'\b(?:migrat|replac|rewrite|switch\w*\s+to|mov\w*\s+to|upgrad|drop\w*\s+from|remov\w*\s+from|swap\w*\s+with)\b'
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
