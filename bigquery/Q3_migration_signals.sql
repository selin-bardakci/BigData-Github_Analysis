-- =============================================================================
-- Q3 — Technology Migration Signals from Commit Messages
-- Deliverable: D3 (Technology Migration Graph)
--
-- SOURCE CHANGE: was bigquery-public-data.github_repos.commits (frozen 2015
-- snapshot — missed all modern migrations: GitHub Actions, FastAPI, Kubernetes,
-- TypeScript, pnpm, Vite, Rust ecosystem shifts, etc.)
-- Now uses githubarchive.month.* which has commit messages from 2015 to today.
--
-- Commit messages live inside the PushEvent JSON payload:
--   payload → $.commits[] → $.message
-- Extracted using JSON_EXTRACT_ARRAY + UNNEST + JSON_EXTRACT_SCALAR,
-- confirmed working pattern against githubarchive BigQuery tables.
--
-- Regex strategy (same improved patterns as before):
--   from_tech — COALESCE of two patterns:
--     P1: "verb … from X"
--     P2: "replace/swap X with Y" — X is the source
--   to_tech — COALESCE of two patterns:
--     P1: full verb set + "to/with/into Y"
--     P2: standalone "verb to Y"
--
-- Noise reduction:
--   - payload pre-filter before JSON unnesting (cost saving)
--   - message-level regex filter after unnesting
--   - Both endpoints must be non-null and differ
--   - Name length 2–30 chars
--   - Minimum 5 commits per (from, to, year) triple
--   - Technology allowlist applied in Spark d3_migration_graph.py
--
-- Estimated scan: ~500 GB – 1 TB (payload column across all months)
-- Run once, save as staging.migration_signals, export to GCS.
-- =============================================================================

WITH raw_messages AS (
    SELECT
        JSON_EXTRACT_SCALAR(commit_obj, '$.message')  AS message,
        FORMAT_TIMESTAMP('%Y', created_at)             AS year
    FROM
        `githubarchive.month.*`,
        UNNEST(JSON_EXTRACT_ARRAY(payload, '$.commits')) AS commit_obj
    WHERE
        type    = 'PushEvent'
        AND payload IS NOT NULL
        AND _TABLE_SUFFIX BETWEEN '201501' AND FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        -- Pre-filter on raw payload string BEFORE JSON unnesting to reduce cost.
        -- Drops PushEvents whose entire payload contains no migration keywords.
        AND REGEXP_CONTAINS(
            LOWER(payload),
            r'\b(?:migrat|replac|rewrite|switch|upgrad|drop|remov|swap)\b'
        )
),

parsed AS (
    SELECT
        -- from_tech: technology being replaced / migrated away from
        COALESCE(
            REGEXP_EXTRACT(LOWER(message),
                r'(?:migrat\w*|replac\w*|rewrite\w*|switch\w*|mov\w*|upgrad\w*|drop\w*|remov\w*)[^.\n]{0,60}?\bfrom\b\s+([\w][\w\-\.]{1,29})'
            ),
            -- "replace/swap X with Y" form — X is the source
            REGEXP_EXTRACT(LOWER(message),
                r'\b(?:replac\w*|swap\w*)\s+([\w][\w\-\.]{1,29})\s+with\b'
            )
        ) AS from_tech,

        -- to_tech: technology being adopted
        COALESCE(
            REGEXP_EXTRACT(LOWER(message),
                r'(?:migrat\w*|replac\w*|rewrite\w*|switch\w*|mov\w*|upgrad\w*|drop\w*|remov\w*)[^.\n]{0,60}?\b(?:to|with|into)\b\s+([\w][\w\-\.]{1,29})'
            ),
            -- standalone "verb to Y" — catches "moved to Kubernetes" without "from"
            REGEXP_EXTRACT(LOWER(message),
                r'\b(?:switch\w*|mov\w*|migrat\w*|upgrad\w*|rewrite\w*)\s+to\s+([\w][\w\-\.]{1,29})'
            )
        ) AS to_tech,

        year

    FROM raw_messages
    WHERE
        message IS NOT NULL
        -- Second-pass filter on the message itself after unnesting
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
FROM parsed
WHERE
    from_tech IS NOT NULL
    AND to_tech   IS NOT NULL
    AND from_tech != to_tech
    AND LENGTH(from_tech) BETWEEN 2 AND 30
    AND LENGTH(to_tech)   BETWEEN 2 AND 30
GROUP BY
    from_tech, to_tech, year
HAVING
    migration_count >= 5
ORDER BY
    migration_count DESC

-- =============================================================================
-- Cost-check: uncomment before first full run
-- LIMIT 50000
-- =============================================================================
