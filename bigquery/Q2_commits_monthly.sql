-- =============================================================================
-- Q2 — Monthly Push Activity per Repository
-- Deliverables: D1 (Language Growth Trends), D2 (Forecasting)
--
-- SOURCE CHANGE: was bigquery-public-data.github_repos.commits (frozen 2015
-- snapshot — caused all languages to show artificial universal decline).
-- Now uses githubarchive.month.* which is a continuous live event stream
-- updated daily from 2011 to present.
--
-- Metric: COUNT of PushEvents per (repo, month).
-- One PushEvent = one git push (may contain multiple commits).
-- This is a reliable activity proxy; payload.size (actual commit count) is
-- not used because it is inconsistently populated across schema versions.
--
-- repo.name format: "owner/repo" — matches github_repos.languages.repo_name
-- so the Spark inner join in d1_preprocess.py works without any code changes.
--
-- _TABLE_SUFFIX format for githubarchive.month.*: YYYYMM (e.g. '201501')
--
-- Estimated scan: ~500 GB – 1 TB total across all months.
-- Use the LIMIT below for a cost-check before the first full run.
--
-- Run once, save result as staging.commits_monthly, then export to GCS.
-- =============================================================================

SELECT
    repo.name                                   AS repo_name,
    FORMAT_TIMESTAMP('%Y-%m', created_at)       AS year_month,
    COUNT(*)                                    AS commit_count
FROM
    `githubarchive.month.*`
WHERE
    type = 'PushEvent'
    AND repo.name IS NOT NULL
    AND repo.name != ''
    AND _TABLE_SUFFIX BETWEEN '201501'
        AND FORMAT_DATE(
                '%Y%m',
                DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
            )
GROUP BY
    repo_name,
    year_month
HAVING
    commit_count > 0

-- =============================================================================
-- Cost-check: uncomment the line below before the first full run
-- LIMIT 500000
-- =============================================================================
