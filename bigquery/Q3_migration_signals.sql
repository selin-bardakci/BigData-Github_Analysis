-- =============================================================================
-- Q3 — Technology Migration Signals from Commit Messages
-- Deliverable: D3 (Technology Migration Graph)
--
-- Scans commit messages inside PushEvent payloads for "migrating from X to Y"
-- patterns, extracting (from_tech, to_tech, year, migration_count) rows.
--
-- Cost control — sampled scan:
--   Full scan (all months 2015-2025) costs ~31 TB — exceeds free tier.
--   This version samples April, August and December of each year
--   (3 months × 11 years = 33 table shards instead of 132), reducing scan to
--   ~7.5 TB while preserving year-level trend resolution.
--   Counts are raw per-month; the Spark D3 job aggregates across years.
--
-- Regex strategy (same improved patterns):
--   from_tech — "verb … from X"  OR  "replace/swap X with Y"
--   to_tech   — "verb … to/with/into Y"  OR  standalone "verb to Y"
--
-- Estimated scan: ~7-8 TB  (sampled months only)
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
        -- Sample: April, August, December of each year 2015-2025
        AND _TABLE_SUFFIX IN (
            '201504','201508','201512',
            '201604','201608','201612',
            '201704','201708','201712',
            '201804','201808','201812',
            '201904','201908','201912',
            '202004','202008','202012',
            '202104','202108','202112',
            '202204','202208','202212',
            '202304','202308','202312',
            '202404','202408','202412',
            '202504','202508','202512'
        )
        -- Pre-filter: drop events whose payload has no migration keywords at all
        AND REGEXP_CONTAINS(
            LOWER(payload),
            r'\b(?:migrat|replac|rewrite|switch|upgrad|drop|remov|swap)\b'
        )
),

parsed AS (
    SELECT
        COALESCE(
            REGEXP_EXTRACT(LOWER(message),
                r'(?:migrat\w*|replac\w*|rewrite\w*|switch\w*|mov\w*|upgrad\w*|drop\w*|remov\w*)[^.\n]{0,60}?\bfrom\b\s+([\w][\w\-\.]{1,29})'
            ),
            REGEXP_EXTRACT(LOWER(message),
                r'\b(?:replac\w*|swap\w*)\s+([\w][\w\-\.]{1,29})\s+with\b'
            )
        ) AS from_tech,

        COALESCE(
            REGEXP_EXTRACT(LOWER(message),
                r'(?:migrat\w*|replac\w*|rewrite\w*|switch\w*|mov\w*|upgrad\w*|drop\w*|remov\w*)[^.\n]{0,60}?\b(?:to|with|into)\b\s+([\w][\w\-\.]{1,29})'
            ),
            REGEXP_EXTRACT(LOWER(message),
                r'\b(?:switch\w*|mov\w*|migrat\w*|upgrad\w*|rewrite\w*)\s+to\s+([\w][\w\-\.]{1,29})'
            )
        ) AS to_tech,

        year

    FROM raw_messages
    WHERE
        message IS NOT NULL
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
    migration_count >= 2

ORDER BY
    migration_count DESC
