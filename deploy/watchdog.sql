-- Health watchdog for the crash ETL.
--
-- Runs hourly as a BigQuery scheduled query with failure email enabled.
-- A healthy check returns one row and succeeds. Any tripped guard RAISEs,
-- which marks the scheduled-query run failed, which sends the mail. The
-- alert *is* the failure: there is no state to keep and nothing to clean up.
--
-- The dataset is written `crashes`. Unlike the pipeline's SQL this file is
-- not passed through Pipeline_Config.render(), so edit the name here if
-- GCP_DATASET ever changes.
--
-- See DEPLOY.md step 8. Guard 1 is the one README.md calls required before
-- anything is scheduled.

DECLARE problems ARRAY<STRING>;

SET problems = ARRAY(
  SELECT msg FROM (

    -- 1. A stuck writer lock. Ownership never expires by design: no elapsed
    -- time proves a paused process won't wake and submit the write it already
    -- reserved. So nothing will ever clear this on its own, and an owner that
    -- died with an unresolved job blocks every later load. This mail is the
    -- only thing that will tell you.
    SELECT FORMAT(
      'STUCK LOCK: etl_lease held by %s since %t (%d minutes), recorded job %s. '
      || 'Follow "A stuck writer lock" in README.md: stop the process, confirm '
      || 'that job is terminal, then clear that owner by name.',
      holder, acquired_at,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), acquired_at, MINUTE),
      IFNULL(current_job_id, '(none recorded)')) AS msg
    FROM crashes.etl_lease
    WHERE holder IS NOT NULL
      AND acquired_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)

    UNION ALL

    -- 2. Loads stopped. A delta runs Mon-Sat and a full on Sun, so a healthy
    -- pipeline writes a 'succeeded' row every day. 36 hours leaves room for
    -- one missed run plus its retry. This also covers a crashed Cloud Run
    -- job, just more slowly than the Monitoring policy in DEPLOY.md step 10.
    --
    -- Silent if the log has never recorded a success: MAX() is NULL and the
    -- HAVING drops the row. That is deliberate, so a half-built project
    -- doesn't mail hourly, and it stops mattering after the first green run.
    SELECT FORMAT(
      'NO RECENT LOAD: newest succeeded load finished %t, %d hours ago. '
      || 'Check Cloud Run job executions for crash-etl-delta / crash-etl-full.',
      MAX(finished_at),
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(finished_at), HOUR))
    FROM crashes.etl_load_log
    WHERE status = 'succeeded'
    HAVING MAX(finished_at) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 36 HOUR)

    UNION ALL

    -- 3. The feed went stale. Deltas keep succeeding against an unchanged
    -- publication, so guard 2 stays quiet while the data silently ages.
    -- Observed cadence is 9-12 days; 21 days is two missed publishes.
    SELECT FORMAT(
      'STALE FEED: socrata_updated_at has read %s for %d days. Loads are '
      || 'succeeding but the city has not published. Check the Socrata dataset.',
      ANY_VALUE(socrata_updated_at HAVING MAX started_at),
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),
                     MAX(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', socrata_updated_at)),
                     DAY))
    FROM crashes.etl_load_log
    WHERE status = 'succeeded' AND socrata_updated_at IS NOT NULL
    HAVING MAX(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', socrata_updated_at))
             < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 21 DAY)

    UNION ALL

    -- 4. The feed republishes but brings no new crash days. Guard 3 cannot
    -- see this: socrata_updated_at changes on every publish, including one
    -- that only re-randomizes coordinates. Observed 2026-10-02 -- a fresh
    -- publish an hour before the first cloud delta, which found 0 new and 0
    -- changed, with MAX(crash_date) unmoved at 2026-08-24. Measuring the
    -- stamp is not measuring the data, so measure the data.
    --
    -- The normal publication lag is large: 39 days at the time of writing.
    -- 60 allows three more weeks of slippage before this counts as abnormal.
    -- Raise the threshold rather than silencing this if the lag grows for a
    -- known reason.
    SELECT FORMAT(
      'DATA NOT ADVANCING: newest crash_date is %t, %d days back. Loads and '
      || 'publishes may both be fine while no new crash days arrive.',
      MAX(crash_date),
      DATE_DIFF(CURRENT_DATE(), MAX(crash_date), DAY))
    FROM crashes.fact_crash_person
    HAVING DATE_DIFF(CURRENT_DATE(), MAX(crash_date), DAY) > 60

  ) WHERE msg IS NOT NULL
);

IF ARRAY_LENGTH(problems) > 0 THEN
  RAISE USING MESSAGE = ARRAY_TO_STRING(problems, '   ||   ');
END IF;

SELECT 'ok' AS health, CURRENT_TIMESTAMP() AS checked_at;
