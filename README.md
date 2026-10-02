# Cincinnati Traffic Crash Data Platform

An end-to-end data engineering project built on the Cincinnati Police
**Traffic Crash Reports** open-data feed (Socrata dataset `rvmt-pkmq`). It has
three layers:

1. **Extract and load.** A Python pipeline pages the Socrata API and stages
   the raw feed in BigQuery.
2. **Star schema.** A Kimball dimensional model with one fact table at
   person-per-crash grain, six dimensions and two role-playing views.
3. **ML panel.** A complete neighborhood × day grid of crash counts with
   leakage-safe lag features, for training crash-likelihood count models.

The whole thing runs from one command (`Run_Pipeline.py`). Loads use a delta
watermark and replace the fact one crash at a time, so they are safe to rerun
and survive Socrata republishing the dataset. Every extract is checked
against the source's own row count before it's used, the fact load is one
transaction, only one run can write at a time, and every run is recorded in
an audit log with its outcome.

**Direction:** the pipeline is being moved from manual local runs to a
scheduled batch job on Google Cloud. It is containerized with Docker, will
run as a Cloud Run Job triggered by Cloud Scheduler, and will deploy through
GitHub Actions. See [Deployment](#deployment).

---

## Contents

- [Current state](#current-state)
- [Architecture](#architecture)
- [Setup](#setup)
- [Running the pipeline](#running-the-pipeline)
- [Deployment](#deployment)
- [Load strategy: delta vs full](#load-strategy-delta-vs-full)
- [Data model (star schema)](#data-model-star-schema)
- [ML panel](#ml-panel)
- [Data quality checks](#data-quality-checks)
- [Source data gotchas](#source-data-gotchas)
- [Design decisions](#design-decisions)
- [Files](#files)
- [Operational notes](#operational-notes)
- [Roadmap](#roadmap)
- [Links](#links)

---

## Current state

As of 2026-10-01, BigQuery dataset `crashes` in `us-east1`:

| Object | Type | Rows | Notes |
|---|---|---:|---|
| `stg_crash_person` | table | 433,160 | Raw feed, all STRING; replaced every load (this is the last load's window) |
| `vw_stg_crash_person_clean` | view | | The one place cleaning happens |
| `fact_crash_person` | table | 433,160 | One row per person/unit per crash; 221,289 crashes, 1900-02-06 to 2026-08-24 |
| `dim_date` | table | 9,497 | 2010-01-01 to 2035-12-31, plus the unknown member |
| `dim_time` | table | 1,441 | Minute grain, plus the unknown member |
| `dim_location` | table | 63,021 | Insert-only, so it grows as amended addresses arrive |
| `dim_conditions` | table | 1,977 | Junk dimension |
| `dim_crash_type` | table | 72 | |
| `dim_person_profile` | table | 3,914 | Junk dimension |
| `dim_crash_date`, `dim_reported_date` | views | | Role-playing views over `dim_date` |
| `etl_load_log` | table | 1 row per load | ETL audit, with each load's status |
| `etl_lease` | table | 1 | Which run may write; created by `--setup` |
| `ml_crash_panel` | table | 205,746 | 53 neighborhoods × 3,882 days |
| `ml_weather_daily`, `ml_cell_attributes` | tables | 0 | Schema ready; external data not loaded yet |

**Where things stand:**
- **Pipeline:** complete, and run by hand from a local machine or the Docker
  image.
- **Production matches the repository.** The last migration (`--setup`, then
  `--full --reprocess`) ran 2026-10-01: all 221,289 crashes rewritten in
  4m58s, `distance_to_cbd_m` populated for every row that has coordinates,
  and the 176 single-coordinate rows resolved. A hash recomputation against
  the current staging publish reports 0 new and 0 changed, so the next
  `--delta` is a no-op.
- **Deployment:** the container builds and runs locally. The GCP resources,
  CI/CD and schedule are not set up yet.
- **ML:** the panel is built and validated, and carries its forecast label.
  External features (weather, AADT, population) and model training are not
  started.

**Verified behavior.** The whole suite runs against a fresh clone of
production (`tests/integration_bigquery.py`, 56 checks, last green
2026-10-01), so these are observed, not intended:
- **Rerun:** a second `--delta` or `--full` over unchanged data is a no-op --
  0 crashes new, changed or removed.
- **Republish:** Socrata regenerates every `:id` on each publish, so the fact
  is keyed on `instanceid` instead; a republish that only re-randomizes the
  privacy fuzz on coordinates rewrites nothing.
- **Repair:** after one row is dropped, one duplicated, one crash's hash
  corrupted and a fake crash added, a load rewrites exactly the damaged
  crashes and removes the fake one.
- **Guards:** a half-empty staging table, and one spanning two publishes,
  each fail the load before anything changes. A failure after the delete and
  insert rolls the whole transaction back.
- **Locking:** three simultaneous acquires leave exactly one winner; an old
  heartbeat and a finished job never authorize a takeover; recovery aimed at
  another owner does not release the lock.
- **ML panel:** every build assertion passes, including the leakage and
  target-recomputation checks.

Two episodes worth keeping, because both shaped the design:
- **2026-09-19 — `:id` is not a stable key.** A Socrata republish regenerated
  every `:id`, and the `MERGE` keyed on it duplicated 6,791 rows. The fact
  moved to crash-level replacement on `instanceid` gated by a content hash.
  The same day, a two-model audit (`AUDIT.md`) added extract verification,
  staging validation, the writer lock, a deletion cap, run status, and
  `--reprocess`.
- **2026-10-01 — coordinates are re-randomized per publish.** Including them
  in that content hash made every republish rewrite the entire window. They
  were removed from the hash and every crash re-stamped. See
  [Source data gotchas](#source-data-gotchas).

---

## Architecture

```
 Run_Pipeline.py takes the ETL lease (etl_lease) ─ one writer at a time ─┐
                                                                         │
 Socrata v3 API (rvmt-pkmq)                                              │
        │  POST query, 50K-row pages, ORDER BY :id                       │
        ▼                                                                │
 Get_Data.fetch(since)            raw strings, person-level grain;       │
        │                         source count + publish stamp checked   │
        │                         before and after, schema checked       │
        ▼                                                                │
 Load_to_GBQ.load_staging()       WRITE_TRUNCATE -> crashes.stg_crash_person
        │                                                                │
        ▼                                                                │
 Section 3  staging validation    exact extract, one publish, no NULL    │
        │                         ids, no partial window — before any    │
        │                         change                                 │
        ▼                                                                │
 vw_stg_crash_person_clean        typing, validation, canonical labels,  │
        │                         natural keys, raw row_json for hashing │
        ├──► Section 4  dimension MERGEs + key assertions                │
        └──► Section 5  fact load, one transaction: replace crashes whose│
                 │          content hash changed, delete crashes removed │
                 │          upstream (capped), write the log row, assert,│
                 │          commit only while holding the lease          │
                 ├──► Section 6  sanity checks (printed)                 │
                 ▼                                                       │
 ML_Crash_Panel.sql               neighborhood × day panel, ASSERT-gated │
        │                                                                │
        ├──► Section 9  mark the load 'succeeded'  (Section 11 on failure)
        ▼                                                                │
 crashes.ml_crash_panel  ──►  count models (NB GLM / LightGBM Poisson)   │
                                                            lease released
```

---

## Setup

Create the virtual environment **outside OneDrive**. A venv holds thousands of
files, and OneDrive would sync every one of them.

```powershell
python -m venv $HOME\.venvs\cinci-crash
& $HOME\.venvs\cinci-crash\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

`requirements.txt` pins the pipeline's direct dependencies (tested with
Python 3.13), and `constraints.txt` pins every transitive package to the
tested versions; the Docker image installs the first with the second as
constraints. `requirements-dev.txt` adds the notebook packages on top.

`.env` settings (git-ignored):

| Key | Value |
|---|---|
| `SOCRATA_APP_TOKEN` | Required. App token from the developer settings link below |
| `GCP_PROJECT_ID` | Required. BigQuery project |
| `GCP_DATASET` | Optional, default `crashes`. Moves every table the pipeline reads or writes: the SQL files say `crashes.` and the runner substitutes this name, so a scratch dataset is fully isolated. |
| `GCP_LOCATION` | Optional, default `us-east1` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to the service-account key JSON. Leave unset on Cloud Run, where the job's service account is used instead. |
| `SQL_FILE` | Optional, default `Star_Schema_ETL.sql` |
| `PANEL_SQL_FILE` | Optional, default `ML_Crash_Panel.sql` |
| `DELTA_LOOKBACK_DAYS` | Optional, default `90` |

`GCP_STAGING_TABLE` is no longer configurable (the SQL names the table), and
the pipeline refuses to start if it's set to anything but `stg_crash_person`.

The service account needs to create datasets and tables and run query jobs,
for example with the BigQuery Data Editor and BigQuery Job User roles.

To run the pipeline in a container instead, see [Docker](#docker).

---

## Running the pipeline

```powershell
python Run_Pipeline.py --setup --full       # first time: build schema + load all history
python Run_Pipeline.py --delta              # routine: new and amended recent crashes
python Run_Pipeline.py --full               # regularly: amendments older than the window
python Run_Pipeline.py --full --reprocess   # after changing cleaning logic
python Run_Pipeline.py --panel              # rebuild only the ML panel
```

| Flag | What runs |
|---|---|
| `--setup` | Creates the dataset and the lease table (Section 10), then star-schema Sections 1 (DDL, migrations, cleaning view), 2 (seed calendar, time and unknown members) and 7 (role-playing views). Safe to rerun; run it after pulling schema changes. |
| `--delta` | Fetches only the watermark window, then runs the load sequence below. |
| `--full` | Fetches the whole history (about 9 pages), then runs the load sequence below. |
| `--reprocess` | With `--full` only. Runs `--setup` first (to install the changed view), updates the derived attributes of existing dimension members in place, and rewrites every crash. Use it after changing the cleaning view or a derived column in Section 4. |
| `--allow-deletions` | Lets a load delete more crashes than the cap (100, or 1% of the crashes it could delete, whichever is larger). Use it only after confirming the feed really dropped them. |
| `--panel` | Rebuilds `ml_crash_panel` without loading. |

`--delta` and `--full` can't be combined. `--setup` combines with either.

**Load sequence (every `--delta` / `--full`):**

1. Take the ETL lease. It fails fast if another run holds it.
2. `Get_Data.fetch()`: page the API. It reads the dataset's metadata and the
   source's row count and publish stamp before and after paging. It restarts
   if the publish changed (up to 3 times), and fails unless it got exactly
   the source's rows, each `:id` once.
3. `Load_to_GBQ.load_staging()`: replace staging (`WRITE_TRUNCATE`).
4. Section 3: staging validation. Fails before anything changes.
5. Section 4: dimension merges, then key-uniqueness assertions.
6. Section 5: fact load. Replaces changed crashes, deletes crashes removed
   upstream and inserts the audit row, in one transaction.
7. Section 6: sanity checks, printed to the console.
8. `ML_Crash_Panel.sql`: rebuild the panel and label it with the load id.
9. Section 9: mark the load `succeeded`, then release the lease.

If any step fails, Section 11 marks the load `failed` with the error. The
process exits non-zero either way.

`Run_Pipeline.py` runs the SQL files section by section, splitting them on
the `-- SECTION n:` marker lines, and substitutes `GCP_DATASET` for
`crashes.` as it reads them.

---

## Deployment

The goal is an unattended load on Google Cloud: `--delta` Monday–Saturday and
`--full` on Sunday, both at 06:00 America/New_York. The container is built and
verified against production and the scaffolding is written; the cloud
resources themselves have not been created yet (see [Status](#status)).

**[DEPLOY.md](DEPLOY.md) is the step-by-step procedure** — every command, in
order, each with something to verify before moving on. This section is the
design behind it: what each piece is for and why it is shaped this way.

### Target architecture

```
 GitHub (push to main)
        │  GitHub Actions: build image, push, update the job
        │  (authenticates with Workload Identity Federation, no stored key)
        ▼
 Artifact Registry ──► Cloud Run Job  (us-east1)
                            ▲   runs as the runtime service account
                            │   SOCRATA_APP_TOKEN from Secret Manager
 Cloud Scheduler ───────────┘
   Mon-Sat:  --delta
   Sunday:   --full
                            │
                            ▼
                  BigQuery dataset `crashes`
```

| Decision | Why |
|---|---|
| Cloud Scheduler runs the job; GitHub Actions only builds and deploys | GitHub's `schedule:` cron is best effort. Runs are often 10–60+ minutes late, and it's disabled after 60 days without commits on a public repo. |
| Cloud Run **Job**, not a service | The pipeline runs to completion and exits; it has no HTTP endpoint. |
| Region `us-east1` | Same region as the dataset. |
| No key file on Cloud Run | `Load_to_GBQ.get_client()` uses a key file only when `GOOGLE_APPLICATION_CREDENTIALS` is set. Otherwise it uses the job's attached service account. |
| Workload Identity Federation for GitHub | No long-lived JSON key stored in GitHub secrets. |
| Two service accounts | **Runtime:** BigQuery Data Editor, BigQuery Job User, Secret Manager Secret Accessor. **Deployer:** Artifact Registry Writer, Cloud Run Developer, and Service Account User on the runtime account. |

### Job sizing

Measured in the container on 2026-09-19:

| Run | Rows fetched | Peak memory | Wall time |
|---|---:|---:|---:|
| `--delta` | 6,791 | 183 MB | ~90 s |
| `--full` | 433,160 | 1.76 GB before the BigQuery upload | ~155 s |

Planned job settings:
- **Memory:** 4 GiB, which leaves headroom over the 1.76 GB full-load peak.
- **Task timeout:** 1 hour. The 10-minute default is too tight given the
  600-second page timeouts. BigQuery's 30-minute job timeout is best effort;
  it does not establish that a writer has stopped. Writer ownership never expires.
- **Retries:** 1. A load that fails cleanly — a source error, a failed
  BigQuery job — releases the writer lock on its way out, so the retry is a
  full second attempt and needs nobody. Only an unresolved outcome (SIGKILL, a
  lost response) keeps the lock, and then the retry fails within seconds on the
  held lock instead of writing anything. That fast, loud failure is the point:
  recover that owner before rerunning.
- **Environment:** `GCP_PROJECT_ID` (required) and `SOCRATA_APP_TOKEN` from
  Secret Manager. `GCP_DATASET` and `GCP_LOCATION` default to `crashes` and
  `us-east1`.
- **Alerts.** Three of the four are one hourly BigQuery scheduled query,
  [deploy/watchdog.sql](deploy/watchdog.sql). A tripped guard raises, the
  failed run sends the mail, and there is no extra state to keep: the failure
  *is* the alert.
  - **Required before scheduling:** `etl_lease` held for more than 2 hours.
    An abandoned owner blocks every later load until recovery; see
    [Operational notes](#operational-notes). Nothing else will ever report it,
    because ownership deliberately never expires.
  - No `etl_load_log` row with `status = 'succeeded'` in 36 hours. A delta
    runs six days a week and a full load on the seventh, so a healthy pipeline
    records a success every day; 36 hours allows one miss plus its retry.
  - `socrata_updated_at` unchanged for 21 days, which means the city stopped
    publishing. Deltas keep succeeding against an unchanged publication, so
    the other guards stay quiet while the data ages. The publishes seen so far
    were 9, 12 and 1 days apart, making 21 days a safe two missed publishes.
  - Newest `crash_date` more than 60 days old, which means the feed is
    republishing without adding crash days. Not redundant with the guard
    above: on 2026-10-02 a fresh publish arrived, changed the stamp,
    re-randomized the coordinates and added nothing — `MAX(crash_date)` stayed
    at 2026-08-24, a 39-day publication lag. Measuring the stamp is not
    measuring the data.
  - Job execution failures, as a Cloud Monitoring policy. Redundant with the
    36-hour guard, but minutes instead of hours.

### Docker

The image is based on `python:3.13-slim`, pinned by digest. It installs
`requirements.txt` under `constraints.txt`, runs as a non-root user, and
defaults to `--delta`. Pass other flags as arguments. `.dockerignore` keeps
`.env` and the key JSON out of the image.

To test locally, mount the key and point the variable at the mounted path,
which overrides the Windows path in `.env`:

```powershell
docker build -t cinci-crash-etl .
docker run --rm --env-file .env `
  -v "${PWD}\cincinnati-open-crash-data-<id>.json:/secrets/key.json:ro" `
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/key.json `
  cinci-crash-etl --delta
```

On Windows, Docker Desktop needs WSL 2. If its engine won't start, run
`wsl --install --no-distribution` in an administrator PowerShell and restart.

### Status

- [x] `Dockerfile` and `.dockerignore`, with secrets kept out of the image
- [x] Key-file-or-attached-account auth in `get_client()`
- [x] Runtime and dev requirements split
- [x] Container verified against production (`--setup`, `--full`, `--delta`)
- [x] Fact load made safe for unattended runs (republish-proof, one
      transaction, fails loudly)
- [x] Audit hardening: verified extracts, staging validation, single-writer
      lease, deletion cap, run status, `--reprocess`, configurable dataset,
      locked dependencies and base image, BigQuery integration test
- [x] Production migrated to the current schema (`--setup`, then
      `--full --reprocess`, 2026-10-01): `distance_to_cbd_m` populated and
      coordinates nulled in pairs
- [x] Full-load cadence decided: weekly, Sunday 06:00 America/New_York
- [x] Deployment runbook written and the supporting files with it:
      [DEPLOY.md](DEPLOY.md), [deploy/watchdog.sql](deploy/watchdog.sql),
      `deploy/create_watchdog.py`, `.github/workflows/deploy.yml`
- [x] Watchdog SQL validated against production: the healthy path returns
      `ok`, and all three guards were made to fire against simulated data
- [ ] Enable APIs, Artifact Registry repo, both service accounts, Secret
      Manager secret ([DEPLOY.md](DEPLOY.md) steps 1–4)
- [ ] Build, push and create the two Cloud Run Jobs (steps 5–7)
- [ ] Install the watchdog scheduled query — **required before scheduling**
      (step 8)
- [ ] Cloud Scheduler triggers: `--delta` Mon–Sat, `--full` Sun (step 9)
- [ ] Cloud Monitoring policy for failed executions (step 10)
- [ ] Retire the local service-account key for ADC (step 11)
- [ ] Workload Identity Federation, so the GitHub Actions workflow can run
      (DEPLOY.md Appendix A); the workflow is written but not yet exercised

---

## Load strategy: delta vs full

**There is no usable change stamp in the feed.** Both were measured on the
live API:
- `:updated_at` and `:created_at` each have one distinct value across all
  433,160 rows, because Socrata restamps the dataset on every publish.
- `:version` is unique per row but random (`rv-ei6x_bj7f_buq7`), so it
  can't be ordered.

The delta therefore windows on **crash date**:

- **Watermark:** `MAX(crash_date) − DELTA_LOOKBACK_DAYS` (90), read from the
  fact itself and capped at today. A failed run leaves no state to reset. If
  the fact is empty, a delta runs as a full load.
- **Why 90 days:** 99% of reports are filed within 3.7 days of the crash, and
  only 0.08% take more than 90. A delta pulls about 6,800 rows instead of
  433K.
- **No stable row key.** Socrata regenerates every `:id` and `:version` when
  it republishes the dataset, so neither can key the fact. `instanceid`
  survives republishes, but nothing identifies a person within a crash, so
  Section 5 replaces the fact **one whole crash at a time**.
- **Change detection:** `crash_hash` is an MD5 of the crash's raw staged rows,
  sorted and without the Socrata system columns. The view builds each row's
  JSON (`row_json`) from the raw staging columns, and Section 5 reads staging
  once into a snapshot. The hash and the inserted rows therefore always
  describe the same data. A crash is deleted and reinserted only when it's
  new, its hash changed, or its fact rows disagree with staging. Unchanged
  crashes aren't touched, so a republish with no real changes rewrites
  nothing.
- **Deletes:** fact crashes lying wholly in the loaded window that are
  missing from staging are deleted (`@window_start`; NULL means the whole
  fact on a full load). More than 100 deletions, or 1% of the crashes the
  load could delete if larger, fails it unless `--allow-deletions` is passed.
  The denominator is that deletion-eligible set rather than every crash
  touching the window, since a crash straddling the window start is deferred
  and can never be deleted here. A normal full load removes about 20–25.
- **Delta boundary:** a delta sees only rows dated inside its window. A
  crash whose fact rows lie partly or wholly before the window can't be
  judged from that, so the delta leaves it alone and counts it as
  `crashes_deferred`, and the next `--full` handles it. Two known gaps, both
  repaired by the next `--full`:
  - a crash whose date is amended to before the window looks deleted;
  - a crash whose upstream rows move partly before the window is rewritten
    without them.
- **Checks, before anything commits:**
  - `fetch()`: exactly the source's row count, each `:id` once, one publish
    stamp, and unchanged metadata from start to finish. The expected columns
    must exist.
  - Section 3, before any change:
    - staging is exactly the extract `fetch()` verified;
    - no NULL ids or stamps;
    - one `:id` never carries two different contents;
    - no `|` or `~` in natural-key values (see [Design decisions](#design-decisions));
    - at least 90% of the window's crashes and people.
  - Section 4: every dimension's natural and surrogate keys are unique.
  - Section 5, inside the transaction:
    - the load still holds the lease;
    - no new fact row falls through to an Unknown location, conditions,
      crash type or person profile;
    - every staged crash ends with exactly its staged row count and hash;
    - every crash in the fact has one hash.

  Any failure rolls the transaction back.
- **Audit:** `etl_load_log` records each load's mode, window start, times,
  rows staged, fact rows inserted and deleted, and crashes new, changed,
  removed and deferred. It also records which Socrata publish was read
  (`socrata_updated_at`) and the load's `status`: `fact_committed`, then
  `succeeded` once the panel is built, or `failed` with `error_message`.
  `fact_committed_at` is set only if the fact changed. The log row is
  written inside the fact transaction, so a fact change always has one.

A delta only catches amendments to crashes inside the window. Run `--full`
regularly (the schedule plans weekly or monthly) to catch older ones and
anything a delta deferred.

---

## Data model (star schema)

### Grain

`fact_crash_person` has **one row per person/unit involved in one crash**.
The source is at that grain:

| Column | Distinct values | Role |
|---|---:|---|
| `:id` | 433,160 | Unique per row, but regenerated on every republish |
| `instanceid` | 221,289 | Crash level; degenerate dimension and the load's key |
| `localreportno` | 221,289 | Crash level, 1:1 with `instanceid`; degenerate dimension |

That works out to about 1.96 people per crash.

### Fact table: `fact_crash_person`

| Group | Columns |
|---|---|
| Foreign keys | `crash_date_key`, `reported_date_key` (both role-play `dim_date`), `crash_time_key`, `location_key`, `conditions_key`, `crash_type_key`, `person_profile_key` |
| Degenerate dimensions | `instanceid`, `localreportno` |
| Source row id | `socrata_id` (`:id`); informational only, since it changes on every republish |
| Measures | `person_count` (always 1), `is_injured`, `is_fatal` |
| Other | `age`, `latitude`, `longitude`, `distance_to_cbd_m`, `crash_date`, `crash_datetime`, `reporting_lag_hours` |
| Audit | `socrata_version`, `socrata_updated_at`, `loaded_at`, `crash_hash` (change detection) |

- **Measures** are INT64 0/1 rather than BOOL, so they add up: `SUM(is_fatal)`
  counts fatalities and `AVG(is_injured)` gives an injury rate.
- **`is_injured`** means "possible injury" or worse. **`is_fatal`** means
  fatal.
- **Raw age** stays on the fact. It's validated to 0–110; values like `BB`,
  `NN` and `913` become NULL.
- **Lat/long** is per unit and lives on the fact, not the dimension. Values
  outside Hamilton County's bounds become NULL, **as a pair**: half a
  coordinate is not a location, and leaving one axis behind makes a row read
  as located to anything that checks a single axis. 176 rows of 433K had one
  axis and not the other; pairing cost no usable point, leaving 432,855 rows
  with coordinates and 305 without. Keeping exact coordinates off
  `dim_location` shrank it from about 1:1 with the fact to about 63,000 rows.
- **`distance_to_cbd_m`** is the geodesic distance in metres from Fountain
  Square (39.1011, −84.5125), Cincinnati's central square at Fifth and Vine
  and the conventional centre of the CBD. Computed in Section 5 with
  `ST_DISTANCE`, rounded to 0.1 m, and NULL wherever the coordinates are.
  Because it is derived from the published coordinates it inherits their
  fuzz, so **it is sound in aggregate and meaningless for one crash** — see
  [Source data gotchas](#source-data-gotchas). It is not part of
  `crash_hash`, so a republish never rewrites rows just to update it, and
  the stored distance always matches the row's own stored coordinates.
- **Storage:** partitioned monthly on `crash_date` (daily would create about
  5,000 tiny partitions). Clustered on `crash_type_key`, `person_profile_key`
  and `location_key`.

### Dimensions

| Dimension | Type | Contents |
|---|---|---|
| `dim_date` | Generated calendar, role-playing | Smart key `YYYYMMDD`; day, week, month, quarter, year and weekend attributes. The source `dayofweek` column is dropped because this derives it. |
| `dim_time` | Generated, minute grain | Key `HHMM`; 12- and 24-hour forms, time-of-day band, `is_rush_hour` (07–09, 15–18), `is_overnight` |
| `dim_location` | Conformed | Block address, zip, three neighborhood schemes (community council, CPD, SNA), road class, crash location and `is_intersection`. Natural key is an MD5 of the 8 attributes. |
| `dim_conditions` | Junk | Light, road condition, contour, surface and weather, raw and canonical. Flags `is_dark`, `is_slick`, `is_curve`, `is_grade`, `is_adverse_weather`. |
| `dim_crash_type` | | Manner of crash and crash severity, raw and canonical, with severity rank, `is_injury_crash`, `is_fatal_crash`, `coding_era` |
| `dim_person_profile` | Junk | Person type, unit type and category, gender, age band, injury severity (canonical, KABCO code, rank) |

`dim_crash_date` and `dim_reported_date` are views over `dim_date` with
prefixed column names, so a query joining both dates never has an ambiguous
`year`.

### Modeling patterns

- **One cleaning view.** `vw_stg_crash_person_clean` feeds both the dimension
  merges and the fact lookup. Each natural key is computed once, so a fact
  can't silently fail to match its dimension.
- **Surrogate keys anchored to existing state.** New members get
  `GREATEST(IFNULL(MAX(key), 0), 0) + ROW_NUMBER()`, filtered to genuinely new
  natural keys with `NOT EXISTS`. Reruns are no-ops.
- **Insert-only dimensions.** Every attribute is derived from the natural
  key, so a changed attribute is a new member, not an update. New *logic* is
  the exception: `--full --reprocess` updates existing members' derived
  attributes in place, keeping their surrogate keys. Members nothing points
  at any more (93 locations today, from amended addresses) are harmless and
  kept.
- **Unknown members** (`-1`, or `19000101` for dates). The fact uses
  `LEFT JOIN` plus `IFNULL`, so a failed lookup is counted, never dropped.
- **Two coding eras.** Ohio re-coded crash reports around 2019, and both
  vintages share the same columns. Every coded dimension keeps the raw string
  and a canonical attribute that reconciles the two. Reconciled, injury rates
  line up across the boundary: 15.35% for 2019+ and 14.39% before 2019.
- **Constraints are metadata.** BigQuery doesn't enforce primary or foreign
  keys (`NOT ENFORCED`), so the load asserts them: unique natural and
  surrogate keys after the merges (Section 4), and no unexpected Unknown
  lookups before the fact commits (Section 5).

---

## ML panel

`ML_Crash_Panel.sql` builds `crashes.ml_crash_panel` following
[crash-panel-spec.md](crash-panel-spec.md).

**Why a panel:** the fact only contains crashes, so every row is a positive
event and can't answer "how likely is a crash." A complete grid of
cell × day rows adds the negatives: days with no crash appear as `crashes = 0`.

### Grain and coverage

| Setting | Value | Reason |
|---|---|---|
| Cell | CPD neighborhood (53; `N/A` dropped) | Spec v1. SNA (51) and community council (71) can be switched in. |
| Time | Day | Spec v1 |
| Start | 2016-01-01 | The feed starts Nov 2012. Mid-2013 to mid-2014 runs about 800 crashes a month against 1,300+ either side, which is a reporting shift, and 2015 is still ramping up. From 2016 volume is stable. The Mar–May 2020 COVID dip is real and stays in. |
| End | Newest crash date − 7 days | The newest day in the feed is partial (2026-08-24 had 1 crash against about 38 a day). |
| Crash count | Distinct `instanceid` | Counting person rows would inflate multi-occupant crashes. Neighborhood and date never differ within a crash (0 of 221,289); the panel report prints the current count for each scheme, and a crash that disagrees is placed by its latest date and cell. |

Cell scheme, dates and split boundaries are `DECLARE` settings at the top of
Section 2, so changing the grain doesn't mean rewriting the SQL.

### Columns

| Group | Columns |
|---|---|
| Keys | `cell_scheme`, `cell_id`, `day` |
| Target | `crashes_next_7`: crashes in this cell over days d+1…d+7. NULL for the last 7 days of the panel, where the window would run past the end and return a short sum — train on `WHERE crashes_next_7 IS NOT NULL`. |
| Same-day count | `crashes`: how many happened on `day` itself. This is history, a feature, **not** the label. |
| Lags | `lag_7`, `lag_364` (same weekday last year), `roll_28` (at least 14 days of history), `roll_91` (at least 30). All come from earlier days of the same cell. |
| Calendar | `dow` (1 = Sunday), `month`, `is_weekend`, `is_holiday` (US federal, actual and observed dates), `doy_sin`, `doy_cos` |
| Weather | `tmax_f`, `tmin_f`, `prcp_in`, `snow_in`, `snwd_in`, `awnd_mph`, joined on `day` from `ml_weather_daily` |
| Cell attributes | `road_miles`, `intersection_count`, `aadt`, `population`, joined on cell from `ml_cell_attributes`; `distance_to_cbd_km`, derived in the build |
| Exposure | `exposure`, `log_exposure` (pass as the model offset), `exposure_source` |
| Split | `split`: train before 2024, valid 2024, test 2025 onward |

The rolling windows end at the previous day (`ROWS BETWEEN n PRECEDING AND
1 PRECEDING`), which is the SQL version of the spec's `shift(1)` before
`rolling()`.

### Build checks

The panel is built into a temp table, and `crashes.ml_crash_panel` is
replaced only if every `ASSERT` passes:

- **Sum:** the panel's crash total equals the number of crashes with a cell.
- **Row count:** rows = cells × days, with no duplicate cell-day.
- **Leakage:** every lag is recomputed from strictly earlier days of the same
  cell by self-join and must match exactly. A deliberately leaky version
  (window including the current day) got flagged on 110,822 rows.
- **Exposure:** one exposure form across all rows, always positive.
- **Zero fraction:** below 95%, the spec's "grain too fine" limit.
- **Feature tables:** `ml_weather_daily` has at most one row per day and
  `ml_cell_attributes` one row per scheme and cell, checked before the joins.

**Scope:** the lags are event-time history. They use counts as eventually
reported, not what was known on the day a forecast would be made, and the
feed runs about 2 weeks behind. That's fine for the retrospective panel, but
a deployed forecast needs a defined prediction time and horizon first (see
[Roadmap](#roadmap)). Observed weather for the target day is likewise not a
forecast.

Holiday flags were checked against pandas' `USFederalHolidayCalendar`. All 111
observed holidays match, plus the 14 actual dates that fell on weekends.

### Current results

| Metric | Value | Spec expected |
|---|---:|---|
| Rows | 205,746 | ~180K |
| Cells × days | 53 × 3,882 | |
| Mean crashes per cell-day | 0.84 | ~0.8 |
| Zero fraction | 53.8% | 50–70% |
| Dispersion (variance ÷ mean) | 1.81 | Overdispersed, so Poisson is the floor |
| Rows by split (train / valid / test) | 154,866 / 19,398 / 31,482 | |
| Labelled rows (`crashes_next_7` not NULL) | 205,375 | 371 unlabelled = 53 cells × 7 days |
| Mean `crashes_next_7` | 5.88 | ~6, the reason for the weekly horizon |
| Zero fraction of `crashes_next_7` | 11.6% | Far below the 53.8% of the daily count |
| Distance to downtown, nearest → farthest cell | 0.14 → 15.43 km | C.B.D./Riverfront → Sayler Park |

### Not loaded yet

- **`ml_cell_attributes`** (road miles, AADT, population) is empty, so
  `exposure_source = 'hours_only'`. That's a constant offset, and the model
  learns counts, not traffic-adjusted rates.
- **`ml_weather_daily`** is empty, so the weather columns are NULL. Load
  exactly one station (CVG `GHCND:USW00093814` or Lunken
  `GHCND:USW00093812`): a second row per day fails the row-count check.

Once these tables are loaded, the panel picks them up on the next build.

---

## Data quality checks

The checks a load depends on are assertions that stop it before anything
commits (see [Load strategy](#load-strategy-delta-vs-full)). Section 6 of
`Star_Schema_ETL.sql` then prints these after every load, for watching the
feed:

| Check | Expected |
|---|---|
| Rows routed to unknown members | Crash dates 7 (5 NULL `crashdate` + 2 dated 1900), reported dates 7, times 5. **Location, conditions, crash type and person profile must be 0**; Section 5 fails a load that would add one. |
| Grain | `persons_per_crash` about 1.96 (near 1.0 means staging was deduped on `instanceid`). `multi_date_crashes` 0: crashes whose person rows carry different dates, which deltas defer. `repeated_socrata_ids` 0. |
| Source quality | NULL or invalid ages (about 55K), reported-before-crash (16), reported more than 30 days later (796). These measure the feed, not a bug; watch for jumps. |
| Canonicalization coverage | Values falling into `Unknown` / `Other` while the raw value is present. A growing count means the feed introduced a new label. |
| Coding-era split | Both eras present, with comparable injury rates. |

---

## Source data gotchas

- **Two coding eras.** `1 - FATAL` (old) and `5 - FATAL` (new) are the same
  injury on opposite scales. Crash severity IDs `1`–`3` and `201901`–`201905`
  coexist. Unit type `03` means different vehicles in each era. Group by the
  canonical columns, never by the numeric prefix.
- **Person-level grain.** Don't dedupe on `instanceid`; it throws away every
  passenger and pedestrian. Count crashes with `COUNT(DISTINCT instanceid)`.
- **No usable change stamp.** See [Load strategy](#load-strategy-delta-vs-full).
- **`:id` and `:version` are regenerated on every republish.** Don't key
  anything on them across loads. `instanceid` is stable for almost every
  crash, but between the 2026-09-10 and 2026-09-19 publishes, 20 old crashes
  got new `instanceid`s with the same `localreportno`. The load handles this
  as one crash removed and one added.
- **Coordinates and addresses are deliberately blurred for privacy, and the
  blur is redrawn on every publish.** Addresses are masked to the block
  (`2XX W MITCHELL AV`) and each row's lat/long is independently randomized
  around its true location — the rows of a single crash sit a median 113 m
  apart, and the same row moves between publishes. Measured 2026-10-01:
  - typical offset from the row's own address-block centre: **106 m** (p50),
    396 m (p90)
  - but **3.7% of rows sit over 2 km** from it, and the worst is 36 km, so
    there is a long tail of plainly bad geocodes on top of the fuzz
  - categorical location (neighborhood, road class, intersection flag) is
    perfectly stable across publishes; only the numbers move

  Consequences: exact coordinates are not reproducible, so they are excluded
  from `crash_hash` (otherwise every republish rewrites every crash — see
  [Load strategy](#load-strategy-delta-vs-full)); any per-crash distance or
  point-in-polygon result is unreliable; and anything location-based should
  use the categorical fields or an aggregate over many rows. `ml_crash_panel`
  is unaffected because it is built on `cpd_neighborhood`.
- **The v3 API ignores `$limit` / `$offset` on GET** and returns the full
  ~435 MB. Paging only works through a POSTed query body.
- **Socrata omits NULL fields** from JSON rows, so `fetch()` reindexes to a
  fixed column list.
- **`:` and `_x` in column names.** System fields (`:id`, `:version`, ...) are
  illegal in BigQuery and are renamed to `socrata_*`. The `address_x` /
  `latitude_x` / `longitude_x` suffixes are merge leftovers and are dropped.
- **Junk values:**
  - ages like `BB`, `NN` and `913`
  - zips like `454229`
  - a mis-encoded dash and the misspelling `LIGHTIED` in light conditions
  - doubled spaces in CPD neighborhoods (`MOUNT  AUBURN`)
  - `N/A` as a neighborhood
  - two rows with a 1900 crash date

  The cleaning view nulls or repairs all of these.
- **The feed runs about 2 weeks behind,** and its newest day is partial.

---

## Design decisions

| Decision | Why |
|---|---|
| All-STRING staging | A malformed value lands in staging instead of failing the load. `SAFE_CAST` in the view quarantines it where Section 6 can count it. |
| Fact replaced one crash at a time on `instanceid`, gated by a content hash | `:id` is regenerated on every republish, and a `MERGE` on it duplicated every row in the window. The hash keeps reruns and no-change republishes as no-ops, and amendments land wherever they are. |
| Watermark read from the fact, not a state table | Self-healing: no stored state can drift out of sync with the fact after a failed run. |
| Delete reconciliation limited to the load window | Deltas can remove upstream deletions without treating every row outside the window as deleted. |
| One writer at a time through a non-expiring owner lock | Every writer (staging load, merges, fact, panel) is serialized by one row in `etl_lease`. A timestamp never permits takeover: an old process could be paused before submitting its recorded job. Unknown submissions and interrupted jobs retain ownership. Recovery requires stopping the process, confirming its jobs are terminal, then clearing only that owner. The fact transaction also checks ownership before committing. |
| `--reprocess` for logic changes, not a transformation version | The hash covers raw source values, so logic changes don't trigger rewrites by themselves. An explicit, supervised full reprocess keeps the load simple; a version column would add state for an event that happens rarely and by hand. |
| `\|` / `~` in natural keys rejected, not migrated | The readable natural keys join values with `\|` and write NULL as `~`, so a value containing either could merge two members. None does in the whole history, so Section 3 fails a load that would introduce one rather than migrating every dimension key now. |
| Extract verified against the source, not only against the fact | The fact-window guards can't tell a partial extract from real deletions. Matching the source's own row count and publish stamp before and after paging can. |
| Lat/long on the fact | Exact coordinates are almost unique per row. In the dimension they would make it as big as the fact. |
| Downtown anchored at Fountain Square, verified against the data | Fountain Square (39.1011, −84.5125) is Cincinnati's central public square and the conventional centre of the CBD, so it needs no arbitrary choice to defend. It was checked against this dataset rather than taken on trust: the median coordinate of the 5XX VINE ST block (Fountain Square's own block, n=256) is 70 m away, and the median of the whole `C. B. D. / RIVERFRONT` neighborhood (n=30,766) is 120 m away. Both independent estimates land inside the published coordinate fuzz, which is as close as this data can resolve. |
| Distance kept raw per row, aggregated to a median per cell | The two uses need different treatment. On the fact the honest thing is the row's own distance, left noisy and documented as such. In the panel a feature has to be stable, so the cell's centre is the marginal median of its crash coordinates — the fuzz (~106 m) and the ~3.7% of rows that sit over 2 km from their own address block both wash out of a median over hundreds of crashes, where a mean would chase them. |
| Panel is a table, not a view | Training reads a stable snapshot, reads are cheap, and publishing waits for the assertions to pass. |
| Forecast target is the next 7 days per `cpd_neighborhood` | Daily counts at neighborhood grain are too sparse to model: 0.84 per cell-day with 54% zeros, so a next-day model predicts 0 or 1 and shows no skill over a seasonal baseline. Weekly totals average ~6 per cell. `cpd_neighborhood` is the reporting agency's own geography and the least sparse of the three schemes. The panel carries the label as `crashes_next_7` (NULL for the last 7 days, where the window would return a short sum), checked on every build against a recomputation from the following seven days. The panel stays event-time, so lagging the features to the forecast issue time and swapping observed weather for a forecast belong in the training code, where the issue time is known. See `crash-panel-spec.md`. |
| A neighborhood joins the panel after 100 crashes, not on first sight | One misspelling, or one crash geocoded into a place the city doesn't use, would otherwise add a cell that is almost entirely zeros and drag down every pooled model. The threshold is self-maintaining, where a fixed list would need editing whenever the city changes its geography, and the panel report prints every held-out neighborhood with its count so a real one sitting just under the line stays visible. |
| Panel is fully rebuilt each load, not incrementally | Rolling features and zero-filled cells depend on neighboring days. A late or amended crash changes `lag_7`, `roll_28`, `roll_91` and `lag_364` for up to a year of later rows in its cell. At about 200K rows a rebuild takes seconds, while an incremental version would need to recompute a trailing window anyway. |
| Holidays flag both actual and observed dates | July 4 traffic happens on the 4th even when the day off is the 3rd. |

---

## Files

| File | Role |
|---|---|
| `Get_Data.py` | Extract. `fetch(since=None)` pages the Socrata v3 API via POST, verifies the extract against the source's count, publish stamp and schema, and returns raw strings. |
| `Load_to_GBQ.py` | Load. Renames columns, forces STRING and `WRITE_TRUNCATE`s `crashes.stg_crash_person` (import only; loads run through `Run_Pipeline.py`). |
| `Pipeline_Config.py` | Validated project, dataset and location; rewrites `crashes.` in the SQL to the configured dataset; best-effort job time limit. |
| `Star_Schema_ETL.sql` | Transform: DDL, cleaning view, staging validation, merges, fact load, checks, views, audit log, lease. |
| `ML_Crash_Panel.sql` | ML panel: external feature tables, the ASSERT-gated panel build, and a report. |
| `Run_Pipeline.py` | Orchestration and CLI. Holds the lease, runs the SQL section by section, passes query parameters and records the outcome. |
| `tests/test_regressions.py` | Offline tests (no credentials or network): extraction checks, staging schema, config and rendering, label grammar, CLI rules. `python -m unittest discover -s tests` |
| `tests/test_lock_safety.py` | Offline tests for the writer lock: acquisition, outcome tracking, and the takeover attempts that must fail. Same runner. |
| `tests/integration_bigquery.py` | End-to-end test on a throwaway clone of the dataset: 56 checks, about 30 minutes, needs credentials. Run command in its docstring. |
| `DEPLOY.md` | Step-by-step Cloud Run deployment: every command in order, each with a verification. The design behind it is in [Deployment](#deployment). |
| `deploy/watchdog.sql` | Hourly health check run as a BigQuery scheduled query. Raises on a stuck lease, a stalled pipeline or a stale feed; the failed run is what sends the mail. |
| `deploy/create_watchdog.py` | Installs or updates that scheduled query, running it as the runtime service account. |
| `.github/workflows/deploy.yml` | On push to `main`: offline suite, then build, push and point both jobs at the new digest. Needs Workload Identity Federation; not yet exercised. |
| `crash-panel-spec.md` | Spec for the ML panel and the modeling plan (written as a pandas plan; the panel is built in BigQuery instead). |
| `AUDIT.md` | Two-model audits of the project (2026-09-19 and 2026-10-01) and what each one changed. |
| `TODO.md` | Open tasks. |
| `requirements.txt` | Pinned direct runtime dependencies. |
| `constraints.txt` | Every transitive package pinned to the tested versions; the image installs under it. |
| `requirements-dev.txt` | Runtime dependencies plus notebook packages. |
| `Dockerfile`, `.dockerignore` | Batch image for the Cloud Run Job, base pinned by digest. |
| `Get_Data.ipynb` | Scratch notebook. Uses the old single GET that downloads all ~435 MB. |

### SQL section map

`Star_Schema_ETL.sql`:

| Section | Runs | Contents |
|---|---|---|
| 1 | `--setup` | Staging, dimension, fact and `etl_load_log` DDL; `ALTER` migrations; cleaning view |
| 2 | `--setup` | Generated calendar, `dim_time` and unknown members (guarded) |
| 3 | Every load | Staging validation, before any change; the watermark explanation (`@window_start`, `@expected_rows`, `@publish_stamp`) |
| 4 | Every load | Dimension `MERGE`s and key assertions (`@reprocess`) |
| 5 | Every load | Fact load: crash-level replace, delete reconciliation and the log row, in one transaction fenced by the lease |
| 6 | Every load | Sanity checks (printed) |
| 7 | `--setup` | Role-playing date views |
| 8 | Retired | Folded into Section 5 |
| 9 | Every load, last | Mark the load `succeeded` |
| 10 | `--setup`, first | `etl_lease` table and its one row |
| 11 | When a load fails | Mark the load `failed` (inserts the row if the fact never committed) |

`ML_Crash_Panel.sql`:

| Section | Contents |
|---|---|
| 1 | `ml_weather_daily` and `ml_cell_attributes` DDL (idempotent) |
| 2 | Settings, crash-level rollup, cell threshold, per-cell distance to downtown, holidays, spine, features, the `crashes_next_7` target, `ASSERT`s, publish |
| 3 | Panel report: size, zero fraction, dispersion, target coverage, nearest and farthest cells, split sizes, crashes whose rows disagree on day or cell, neighborhoods held out by the threshold |

---

## Operational notes

- **After changing the cleaning view** (or a derived column in Section 4),
  run `--full --reprocess`. The hash is built from raw values, so an ordinary
  load leaves unchanged crashes and existing dimension members on the old
  logic. The fact stays queryable throughout, since every crash is rewritten
  inside one transaction. Rules that feed a natural key (age bands, location
  cleanup) create new members instead, and the old ones stay behind unused.
  The dimension updates commit before the fact transaction, so if a
  reprocess fails, rerun it.
- **After pulling a schema change,** run `--setup` once. Section 1 carries
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migrations for existing tables,
  and Section 10 creates the lease table. A load against a dataset without
  `etl_lease` stops and says to run `--setup`.
- **Adopting this into a dataset built by an older `:id` MERGE:** run
  `--setup`, then `--full`. Rows without a `crash_hash` are always rewritten,
  so one full load rewrites every crash and removes any duplicates the MERGE
  left; there's no need to truncate first.
- **Two loads can't run at once.** The second one stops with "Load … holds
  the ETL lease". That's expected if two runs overlap; rerun it later.
- **A stuck writer lock.** `etl_lease` is named for a lease, but ownership
  does not expire. `acquired_at` and `heartbeat_at` are diagnostic only.
  First stop the owning local process or terminate its Cloud Run execution,
  and disable any supervisor that could resume it. Only then inspect every
  BigQuery job named `etl_<holder>_…` (or carrying that `load_id` label), including
  control jobs, and confirm all are `DONE`. A missing job ID does not prove a
  delayed submission is safe while its process is alive. If submission status
  cannot be established, keep the lock and investigate; do not force takeover.
  Inspect the fact job and `etl_load_log` to determine whether it committed.
  When recovery is verified, clear exactly the recorded owner in the configured
  project and dataset, replacing the sample value below:

  ```sql
  UPDATE `your-project.crashes.etl_lease`
  SET holder = NULL, acquired_at = NULL, heartbeat_at = NULL, current_job_id = NULL
  WHERE lease_name = 'pipeline' AND holder = 'the-verified-stopped-load-id';
  ```

  Confirm exactly one row changed. Never use an unconditional unlock. Old
  development tables may still have an `expires_at` column; the runner ignores it.

  Then finish the recovered load's audit row. A run whose outcome was never
  resolved deliberately wrote nothing to `etl_load_log`, so its row can sit at
  `fact_committed` forever and keep the 36-hour alert quiet about a pipeline
  that is not actually current. Run `--delta` first: it rebuilds the panel and,
  if that owner's fact work had committed, changes no fact rows. Then close the
  old row by hand:

  ```sql
  UPDATE `your-project.crashes.etl_load_log`
  SET status = 'failed',
      error_message = 'owner lost before completion; recovered manually',
      finished_at = CURRENT_TIMESTAMP()
  WHERE load_id = 'the-verified-stopped-load-id' AND status = 'fact_committed';
  ```
- **What a failed load leaves behind.**
  - A failure before Section 5 commits changes nothing in the fact. Staging
    and any brand-new dimension members may have changed, which is harmless.
  - A confirmed failure after fact commit leaves the fact updated. The panel
    may be old or newly published depending on the failing stage. The log says
    `failed`, with `fact_committed_at` preserved.
  - An unknown server outcome deliberately skips failure-log mutation and keeps
    the writer lock. This prevents a failure MERGE racing the fact transaction's
    audit INSERT. The server may still commit successfully; inspect its job and
    audit row before recovery.

  The process exits non-zero. Once ownership is safely released, rerun: the
  watermark is read from the fact, and a rerun of a committed load is a
  no-op.
- **Mass deletions.** If a load stops with "would delete N crashes", check
  whether the city really removed them. The feed may have been truncated or
  republished partially. Rerun with `--allow-deletions` only if the deletions
  are real.
- **The panel's target column `crashes` has the same name as the dataset.**
  In a query that joins after scanning the panel, write
  `` `crashes.dim_date` `` in backticks. Otherwise BigQuery reads it as a field
  of the column.
- **The panel changes with every load.** For a reproducible training run,
  copy it first, e.g.
  `CREATE TABLE crashes.ml_crash_panel_20260910 COPY crashes.ml_crash_panel`.
- **The service-account key sits in this OneDrive-synced folder.** It's
  git-ignored and excluded from the Docker image, but consider moving it out
  and updating `GOOGLE_APPLICATION_CREDENTIALS`. Once the job runs on Cloud
  Run, the key is only needed for local runs.
- **`APP_ENV` and `GOOGLE_CLOUD_RUN_REGION_ENDPOINT` in `.env`** aren't read
  by any code.
- **Testing.** Run `python -m unittest discover -s tests` after any change.
  The offline suite needs no credentials. Run `tests/integration_bigquery.py`
  before deploying a change to the SQL or the runner: it clones the dataset
  and exercises a real load, the lease, every staging check, the fact
  repairs, a rollback, a post-commit failure and `--reprocess`, then deletes
  the clone. Production is only read.
- **`FutureWarning` about `pandas-gbq`** is printed during the staging load.
  The BigQuery client says future versions will need `pandas-gbq` for
  DataFrame loads. It's harmless for now; add the package if a
  `google-cloud-bigquery` upgrade starts requiring it.

---

## Roadmap

In rough priority order, from `TODO.md` and the panel spec:

- [ ] **Scheduled cloud refresh.** Next up; the steps are tracked under
      [Deployment status](#status).
- [ ] Load `ml_cell_attributes` (OSM road miles, ODOT AADT, ACS) so exposure
      becomes a real rate denominator
- [ ] Load `ml_weather_daily` (NOAA, one station)
- [ ] Before training for forecasting, in the training code rather than the
      panel:
  - lag the features to the forecast issue time — the panel is event-time by
    design, and the ~9-day publish cadence plus the 3.7-day p99 reporting lag
    puts the freshest trustworthy crash day around t−13;
  - use forecast weather for the target week, not the observed weather the
    panel carries.
- [ ] `train.py`:
  - [ ] time-based split
  - [ ] negative binomial GLM baseline
  - [ ] LightGBM Poisson with a `log_exposure` offset
  - [ ] must beat the `roll_28` baseline on Poisson deviance
  - [ ] PAI on the test year
- [ ] Choose a user interface for ML predictions and crash analytics
- [ ] Later grains once neighborhood × day is validated: hex grid, sub-daily
      bins, road segments, severity modeling

---

## Links

- Pagination: [support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset](https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset)
- Data: [data.cincinnati-oh.gov](https://data.cincinnati-oh.gov/)
- Login / app token: [data.cincinnati-oh.gov/profile/edit/developer_settings](https://data.cincinnati-oh.gov/profile/edit/developer_settings)
