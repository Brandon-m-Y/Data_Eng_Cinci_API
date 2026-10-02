# Open tasks

Completed work isn't listed here. The reasoning behind decisions already made
lives in README.md under "Design decisions"; the audits are in AUDIT.md.

Production is on the current schema as of 2026-10-01; nothing is pending
there.

## Watch before scheduling anything unattended

- **Crash churn against the deletion cap.** Each publish re-keys a handful of
  `instanceid`s, which a load sees as that many removed and as many added:
  20/20 across the 09-10 → 09-19 publishes, 25/25 across 09-19 → 10-01. The
  cap is `max(100, 1% of the crashes the load could delete)`, so at the
  current rate there is room, but an unattended job starts failing if it
  grows. Check the trend in `etl_load_log.crashes_removed` before scheduling,
  and decide whether the cap or the alerting should change.

## ML

- Complete the crash-likelihood model. The panel and its `crashes_next_7`
  label are built; what remains is in the training code, not the panel:
  - Lag the features to the **forecast issue time**. The panel is event-time
    by design. The feed's ~9-day publish cadence plus the 3.7-day p99
    reporting lag puts the freshest trustworthy crash day around t−13.
  - Use **forecast** weather for the target week, not the observed weather
    the panel carries.
- Load the external feature tables, which are created but empty:
  `ml_weather_daily` (NOAA, CVG station) and `ml_cell_attributes` (OSM road
  miles and intersection counts, ODOT AADT, ACS population). Until AADT and
  road miles arrive, `exposure` falls back to hours alone and
  `exposure_source` records which form each row got.
- Decide on a user interface for the model and for real-time crash analytics.

## Deployment

Plan: a Cloud Run Job triggered by Cloud Scheduler; GitHub Actions only
builds and deploys.

- Decide the periodic `--full` cadence: weekly (recommended, ~2.5 min per
  run) or monthly.
- Deploy two Cloud Run Jobs from the same image, both in `us-east1`, 1
  automatic retry, runtime service account:
  - `crash-etl-delta`: `--args=--delta`, 1Gi memory, 30m timeout (a delta
    peaked at 183 MB)
  - `crash-etl-full`: `--args=--full`, 4Gi memory, 1h timeout (a full load
    peaked at 1.76 GB)
  - Writer ownership never expires. A clean failure releases the lock, so the
    retry is a real second attempt; a killed run or an unknown job outcome
    keeps it, and the retry fails within seconds until owner-specific
    recovery. Follow README.md.
  - Environment: `GCP_PROJECT_ID=cincinnati-open-crash-data` (required);
    `SOCRATA_APP_TOKEN` from Secret Manager. `GCP_DATASET` and `GCP_LOCATION`
    default to `crashes` / `us-east1`.
- Create two Cloud Scheduler triggers (time zone America/New_York), skipping
  the delta on full-load days so they never overlap:
  - Weekly: delta `0 6 * * 1-6`, full `0 6 * * 0`
  - Monthly: delta `0 6 2-31 * *`, full `0 6 1 * *`
- Scheduler calls `POST https://run.googleapis.com/v2/projects/cincinnati-open-crash-data/locations/us-east1/jobs/<job>:run`
  using a scheduler service account with Cloud Run Invoker.
- Alternative if one job is preferred: a single job defaulting to `--delta`,
  with the full trigger sending
  `{"overrides":{"containerOverrides":[{"args":["--full"]}]}}`. Downsides:
  both runs get full-load sizing, and the scheduler account needs
  `run.jobs.runWithOverrides`, which Invoker doesn't include.
- Two triggers fit Cloud Scheduler's free tier (3 jobs per billing account).
- **Required before scheduling:** an alert when `etl_lease` is held for more
  than 2 hours, pointing at the owner-specific recovery runbook.
- Other alerts: failed job executions; no `etl_load_log` row with
  `status = 'succeeded'` in 36 hours; `socrata_updated_at` unchanged for N
  days (stale feed — pick N from the observed ~9-day publish cadence).
- Once Cloud Run is live, delete the local service-account key and use
  `gcloud auth application-default login` for local runs.

## Deferred

- **Unattended recovery of an abandoned writer lock** — only if manual
  recovery proves annoying in practice. An owner that dies with an unresolved
  job holds the lock until a person stops it, confirms its jobs are terminal
  and clears that holder by name. That is deliberate: no amount of elapsed
  time proves a paused process won't wake and submit the write it already
  reserved. The route to automating it would be for a taker to submit a no-op
  job under the dead run's reserved job id, so the sleeper's late submit
  fails with a job-id conflict instead of writing. That needs an integration
  test of BigQuery's conflict behaviour first, and it doesn't cover writes
  carrying no job id, so those would have to move to query jobs (the panel
  label already did).
