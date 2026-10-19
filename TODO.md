# Open tasks

Completed work isn't listed here. The reasoning behind decisions already made
lives in README.md under "Design decisions"; the audits are in AUDIT.md.

## Watch

- **The first rows from CPD's new reporting system** (expected after
  2026-10-31). The cleaning view was measured on the history `7iz8-ecuw`
  republishes; new-system rows may bring new labels (Section 6 lists any it
  doesn't recognise), a new report-number format, or a different rate of
  repeated rows. Check `persons_per_crash` for `7iz8-ecuw` in Section 6
  (expected ~1.99).

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

  These are `rvmt-pkmq` figures. Since the source switch only `7iz8-ecuw`
  crashes can be deleted, and that feed keys crashes on the report number,
  which hasn't been seen to re-key. Both caps sit at the floor of 100 until
  the fact holds 10,000 of its crashes.

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
America/New_York. A full load is ~4 minutes, so weekly costs essentially
nothing and bounds how far a missed delta can drift.

Every step is done as of 2026-10-02. Kept as a record of what each one
proved, because "installed" and "working" came apart more than once:

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

Both of the last two were installed before they were proven, which is the
same shape of gap the watchdog email had. Both are now closed:

- [x] The monitoring policy has fired and delivered (2026-10-02). The forced
      failure cost nothing: no lease acquired, no `etl_load_log` row, and the
      metric recorded `1`, confirming it counts executions rather than the
      two task attempts.
- [x] The deploy job has run on every non-docs push since its first build
      (`sha256:fb7fbf44...`, tag `7ade673`), and every scheduled load since
      2026-10-03 has run on an image it built. The first `--full` ran
      2026-10-04, and the 2026-10-11 one was the first to insert and delete
      in the cloud.

Still open:

- Whether `bigquery.dataEditor` should stay project-level. Fine while this
  project holds one dataset; Appendix B tightens it to `crashes` alone.
- Whether to disable `cinci-crash-etl`. It is keyless and unused; disabling
  is the reversible way to find out whether anything still depends on it.
- Whether the deploy job should need a human. One environment, one branch,
  and only the offline suite between a push and production. A GitHub
  Environment with required reviewers would add a click. The first full
  loads have run, so this is ready to decide.

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
