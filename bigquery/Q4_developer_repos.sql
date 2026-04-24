-- =============================================================================
-- Q4 — Developer–Repository Bipartite Graph Edges
-- Deliverables: D4 (Developer Influence PageRank), D5 (Developer Community Map)
--
-- SOURCE CHANGE: was bigquery-public-data.github_repos.commits (frozen ~2015
-- snapshot — all developer data was from the early GitHub era, missing 10 years
-- of modern contributors).
-- Now uses githubarchive.month.* PushEvent actor.login as developer identity.
--
-- Why actor.login instead of commit author email:
--   Extracting commit author emails via UNNEST(JSON_EXTRACT_ARRAY(payload,...))
--   scans 31 TB (full payload column) — 31× the free tier.
--   actor.login is a top-level column (not in payload), scans ~625 GB total.
--   It identifies who pushed to the repo, which is equivalent to co-contribution
--   for the purpose of building the developer influence graph.
--
-- Bot filtering: drops accounts with bot/automation suffixes.
--
-- Estimated scan: ~625 GB — within the 1 TB/month free tier.
-- Run once, export result to GCS as Parquet.
-- =============================================================================

SELECT
    LOWER(actor.login)                              AS developer_email,
    repo.name                                       AS repo_name,
    CAST(EXTRACT(YEAR FROM created_at) AS STRING)   AS year,
    COUNT(*)                                        AS commit_count
FROM
    `githubarchive.month.*`
WHERE
    type        = 'PushEvent'
    AND actor.login IS NOT NULL
    AND actor.login != ''
    AND repo.name   IS NOT NULL
    AND repo.name   != ''
    AND _TABLE_SUFFIX BETWEEN '201501'
        AND FORMAT_DATE(
                '%Y%m',
                DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
            )
    AND NOT REGEXP_CONTAINS(
            LOWER(actor.login),
            r'bot|renovate|dependabot|github-actions|autobot|no-reply'
        )

GROUP BY
    developer_email,
    repo_name,
    year
HAVING
    commit_count >= 1

-- =============================================================================
-- Cost-check: uncomment before first full run (dry-run shows ~625 GB)
-- LIMIT 500000
-- =============================================================================
