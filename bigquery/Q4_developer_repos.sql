-- =============================================================================
-- Q4 — Developer–Repository Bipartite Graph Edges
-- Deliverables: D4 (Developer Influence PageRank), D5 (Developer Community Map)
--
-- Produces one row per (developer_email, repo_name, year) with the number of
-- commits that developer made to that repo in that year.
--
-- Developer privacy:
--   Raw emails are exported here so that SHA-256 hashing can be applied
--   consistently in the Spark d4d5_developer_graph.py job BEFORE any graph
--   is built or any result is written.  The raw email column is dropped from
--   Spark DataFrames immediately after hashing.  It is never written to any
--   output file or visualisation.
--
-- Bot filtering (email-level):
--   Rows where the email matches known bot patterns are excluded here to
--   avoid inflating the graph with automated activity.
--
-- Estimated scan: ~30–50 GB
-- Run once, export result to GCS as Parquet.
-- =============================================================================

SELECT
    author.email                                                        AS developer_email,
    repo_name,
    FORMAT_TIMESTAMP(
        '%Y',
        TIMESTAMP_SECONDS(author.time_sec)
    )                                                                   AS year,
    COUNT(DISTINCT commit)                                              AS commit_count
FROM
    `bigquery-public-data.github_repos.commits`,
    UNNEST(repo_name) AS repo_name
WHERE
    -- Require a valid email
    author.email IS NOT NULL
    AND author.email != ''

    -- Remove known bot / automation accounts
    AND NOT REGEXP_CONTAINS(
            LOWER(author.email),
            r'\[bot\]|noreply|renovate|dependabot|github-actions|autobot|no-reply'
        )

    -- Time window
    AND author.time_sec IS NOT NULL
    AND TIMESTAMP_SECONDS(author.time_sec)
        BETWEEN TIMESTAMP('2015-01-01')
            AND TIMESTAMP(
                DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
            )

GROUP BY
    developer_email,
    repo_name,
    year
HAVING
    -- Keep only meaningful contributions (at least 1 commit per repo per year)
    commit_count >= 1

-- =============================================================================
-- Development / cost-check: append the following LIMIT before the first full run
-- LIMIT 500000
-- =============================================================================
