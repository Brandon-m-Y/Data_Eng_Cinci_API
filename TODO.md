# Finish the following tasks

- Haversine calculations to get the distance from the center of downtown to each crash
- Possible add Haversine distance to dim_locations table
- Complete ML crash likelyhood project
- Decide on a User interface for ML use and overall real time crash analytics
- Automate API refresh on Google Cloud (plan: Cloud Run Job triggered by Cloud Scheduler; GitHub Actions only builds and deploys)
  - Decide the periodic --full cadence: weekly (recommended, ~2.5 min per run) or monthly
  - Deploy two Cloud Run Jobs from the same image, both in us-east1, 1 retry, runtime service account:
    - crash-etl-delta: --args=--delta, 1Gi memory, 30m timeout (a delta peaked at 183 MB)
    - crash-etl-full: --args=--full, 4Gi memory, 1h timeout (a full load peaked at 1.76 GB)
  - Create two Cloud Scheduler triggers (time zone America/New_York), skipping the delta on full-load days so they never overlap:
    - Weekly: delta `0 6 * * 1-6`, full `0 6 * * 0`
    - Monthly: delta `0 6 2-31 * *`, full `0 6 1 * *`
  - Scheduler calls POST https://run.googleapis.com/v2/projects/cincinnati-open-crash-data/locations/us-east1/jobs/<job>:run using a scheduler service account with Cloud Run Invoker
  - Alternative if one job is preferred: one job defaulting to --delta, with the full trigger sending `{"overrides":{"containerOverrides":[{"args":["--full"]}]}}`. Downsides: both runs get full-load sizing, and the scheduler account needs run.jobs.runWithOverrides, which Invoker doesn't include
  - Two triggers fit in Cloud Scheduler's free tier (3 jobs per billing account)
- Update readme to give a comprehensive overview of the project so far
- Does the ML panel have to be rebuilt on each load or can it have a delta load? LIkely not because it needs fresh aggregations based on the new data.
