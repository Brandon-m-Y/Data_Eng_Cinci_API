# Open tasks

Completed work isn't listed here. The reasoning behind decisions already made
lives in README.md under "Design decisions"; the audits are in AUDIT.md.

Production is on the current schema as of 2026-10-01; nothing is pending
there.

## Watch

- **Crash churn against the deletion cap.** Each publish re-keys a handful of
  `instanceid`s, which a load sees as that many removed and as many added:
  20/20 across the 09-10 → 09-19 publishes, 25/25 across 09-19 → 10-01.

  Measured 2026-10-02, so the headroom is now a number rather than a worry:

  | Load | Deletion-eligible crashes | Cap |
  |---|---:|---:|
  | `--delta` (90-day window) | 3,319 | **100** — 1% is only 34, so the floor governs |
  | `--full` | 221,289 | 2,213 |

  A full load has 88× headroom over the observed 25. A delta is the tighter
  case, and even if every re-key landed inside the 90-day window it would be
  25 against 100.

  **Third data point, 2026-10-02: 0 removed, 0 new.** The feed republished
  (`socrata_updated_at` 10-01T17:37 → 10-02T17:40) and the first Cloud Run
  delta found nothing changed at all. Re-keys therefore are not concentrated
  in recent crashes: 25 spread uniformly over 221,289 would put ~0.4 inside
  the 3,319-crash window, and 0 is consistent with that. The delta cap of 100
  is not close to binding. This no longer blocks scheduling; keep an eye on
  `etl_load_log.crashes_removed` on full loads rather than deltas, since only
  a full load can see re-keys outside the window.

## ML

- **Re-derive the feature lag — the `t−13` figure in the old plan is wrong.**
  Measured 2026-10-02 against the 2026-10-01 publish: the newest crash in the
  data is 2026-08-24, and daily volume runs at full strength (25–50 crashes)
  through 08-23 and then stops dead. That is a **publication lag of about 38
  days**, a cliff rather than a trickle.

  The old estimate added a ~9-day publish cadence to a 3.7-day p99 *reporting*
  lag (crash date → reported date, which is internal to a publication) and got
  13. Those are different quantities, and the binding one is the publication
  lag. Lag the features to roughly **t−40**.

  Confirmed on the next publish, 2026-10-02: `MAX(crash_date)` did **not**
  move — still 2026-08-24, so the lag grew to 39 days. A republish is not
  evidence that new crash days arrived; that one only changed the stamp and
  re-randomized coordinates. Watchdog guard 4 exists because of this.

  This makes the forecasting problem harder, not just different: predicting
  the next 7 days from crash data that ends 40 days ago leans much more on
  weather and on the static cell attributes.
- Complete the crash-likelihood model. The panel and its `crashes_next_7`
  label are built; what remains is in the training code, not the panel:
  - Lag the features to the forecast issue time, per the measurement above.
  - Use **forecast** weather for the target week, not the observed weather
    the panel carries.
- Load the external feature tables, which are created but empty:
  `ml_weather_daily` (NOAA, CVG station) and `ml_cell_attributes` (OSM road
  miles and intersection counts, ODOT AADT, ACS population). Until AADT and
  road miles arrive, `exposure` falls back to hours alone and
  `exposure_source` records which form each row got.
- Decide on a user interface for the model and for real-time crash analytics.

## Deployment

**[DEPLOY.md](DEPLOY.md) has the procedure** — every command in order, each
with a verification. README.md § Deployment has the design. This list is only
what is still undone and what DEPLOY.md does not decide for you.

Decided: `--delta` Monday–Saturday and `--full` Sunday, both 06:00
America/New_York. A full load is ~2.5 minutes, so weekly costs essentially
nothing and bounds how far a missed delta can drift.

Written and validated, not yet installed:

- `deploy/watchdog.sql` — daily health check, 14:00 UTC, a few hours after
  the load. Validated against production on 2026-10-02: the healthy path
  returns `ok`, and all four guards were made to fire against simulated data,
  so the messages are known to be correct.
- `deploy/create_watchdog.py` — manages it. It cannot *create* a user-owned
  config: the API wants an OAuth authorization code that application-default
  credentials do not carry, so creation happens in the BigQuery console. It
  does handle `--update`, `--test-alert` and reporting.
- `.github/workflows/deploy.yml` — build and deploy on push to `main`. Gated
  on the offline suite. Needs Workload Identity Federation (DEPLOY.md Appendix
  A) before it can run; until then it fails at the auth step and nothing else
  is affected.

Remaining, in order:

- [x] APIs, Artifact Registry, Secret Manager, service accounts (steps 1–4)
- [x] Built, pushed and created both Cloud Run Jobs; the delta smoke test
      succeeded against production and released the lease (steps 5–7)
- [x] **Watchdog installed and its email proven** (step 8). Created in the
      BigQuery console, owned by a person, daily at 14:00 UTC. A forced
      failure delivered mail on 2026-10-02.
- [x] Cloud Scheduler triggers live (step 9). Verified by firing one by
      hand: scheduler at 20:10:21, Cloud Run execution at 20:10:22, fact
      committed, panel rebuilt, `succeeded`, lease released. 184s.
- [x] Cloud Monitoring policy for failed executions (step 10). Email channel
      and policy both enabled, installed from `deploy/alert_job_failed.json`.
- [x] Local service-account key retired (step 11). No user-managed keys left
      in the project, no JSON on disk, local runs on ADC.
- [x] Workload Identity Federation (Appendix A). Pool and provider exist,
      the provider only accepts this one repository, and the three repo
      variables are set, so the deploy job is live.

Left over from those, both the same kind of gap: installed but never
exercised.

- [ ] Make the monitoring policy fire once, the way the watchdog's email was
      proved. `gcloud run jobs execute crash-etl-delta --args=--force-a-failure
      --region=us-east1 --wait` fails in argparse before any client is built.
- [x] The deploy job has now run, twice, and both jobs sit on the digest it
      built (`sha256:fb7fbf44...`, tag `7ade673`). Nothing has *executed*
      that image yet — the Saturday 06:00 delta is the first to.

Open questions DEPLOY.md does not answer:

- Whether to prove the watchdog's **email actually arrives**. The guards are
  tested; delivery to your inbox is not. DEPLOY.md step 8 has a safe way to
  force one, and it is worth doing once — an alert nobody has ever received is
  an assumption, not a safeguard.
- Whether `bigquery.dataEditor` should stay project-level. Fine while this
  project holds one dataset; Appendix B tightens it to `crashes` alone.

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
