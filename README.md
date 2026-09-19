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
and survive Socrata republishing the dataset. Each load is one transaction,
and every run is recorded in an audit log.

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

As of 2026-09-19, BigQuery dataset `crashes` in `us-east1`:

| Object | Type | Rows | Notes |
|---|---|---:|---|
| `stg_crash_person` | table | 6,791 | Raw feed, all STRING; replaced every load (this is the last delta's window) |
| `vw_stg_crash_person_clean` | view | | The one place cleaning happens |
| `fact_crash_person` | table | 433,160 | One row per person/unit per crash; 221,289 crashes |
| `dim_date` | table | 9,497 | 2010-01-01 to 2035-12-31, plus the unknown member |
| `dim_time` | table | 1,441 | Minute grain, plus the unknown member |
| `dim_location` | table | 62,984 | Insert-only, so it grows as amended addresses arrive |
| `dim_conditions` | table | 1,977 | Junk dimension |
| `dim_crash_type` | table | 72 | |
| `dim_person_profile` | table | 3,914 | Junk dimension |
| `dim_crash_date`, `dim_reported_date` | views | | Role-playing views over `dim_date` |
| `etl_load_log` | table | 1 row per load | ETL audit |
| `ml_crash_panel` | table | 205,746 | 53 neighborhoods × 3,882 days |
| `ml_weather_daily`, `ml_cell_attributes` | tables | 0 | Schema ready; external data not loaded yet |

**Where things stand:**
- **Pipeline:** complete and run manually from a local machine or the
  Docker image.
- **Fact load:** rebuilt on 2026-09-19 after a Socrata republish exposed that
  `:id` isn't a stable key (see [Load strategy](#load-strategy-delta-vs-full)).
  Production was migrated the same day with `--setup` then `--full`, which
  removed the 6,791 duplicate rows the old `MERGE` had created.
- **Deployment:** the container is built and tested locally. The GCP
  resources, CI/CD and schedule are not set up yet.
- **ML:** the training panel is built and validated. External features
  (weather, exposure) and model training are not started.

Verified behavior of the crash-level fact load, tested 2026-09-19 on a clone
of `crashes` and then on production:
- **Republish:** a delta after Socrata regenerated every `:id` repaired the
  6,791 duplicate rows the old `:id` MERGE had inserted (3,319 crashes
  rewritten, 13,582 rows deleted, 6,791 inserted). The fact returned to
  433,160 rows and 221,289 crashes.
- **Rerun:** reruns of both `--delta` and `--full` are no-ops (0 crashes
  new, changed or removed), in the clone and in production.
- **Repair:** after one row was dropped, one duplicated, one crash's hash
  was corrupted and a fake crash was added, a load rewrote the 3 damaged
  crashes and removed the fake one.
- **Guards:** a half-empty staging table and a staging table spanning two
  publishes both fail the load before anything changes. A failure after the
  delete and insert rolls the whole transaction back.
- **ML panel:** every build assertion passes.

---

## Architecture

```
 Socrata v3 API (rvmt-pkmq)
        │  POST query, 50K-row pages, ORDER BY :id
        ▼
 Get_Data.fetch(since)            raw strings, 32 columns, person-level grain
        │
        ▼
 Load_to_GBQ.load_staging()       WRITE_TRUNCATE -> crashes.stg_crash_person
        │
        ▼
 vw_stg_crash_person_clean        typing, validation, canonical labels,
        │                         natural keys (one definition, used twice)
        ├──► Section 4  dimension MERGEs (insert-only, surrogate keys)
        └──► Section 5  fact load, one transaction: replace crashes whose
                 │          content hash changed, delete crashes removed
                 │          upstream (in window), assert, commit
                 ├──► Section 6  sanity checks (printed)
                 ├──► Section 9  etl_load_log row
                 ▼
 ML_Crash_Panel.sql               neighborhood × day panel, ASSERT-gated
        │
        ▼
 crashes.ml_crash_panel  ──►  count models (NB GLM / LightGBM Poisson)
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

`requirements.txt` pins the pipeline's runtime dependencies (tested with
Python 3.13) and is all the Docker image installs. `requirements-dev.txt`
adds the notebook packages on top.

`.env` settings (git-ignored):

| Key | Value |
|---|---|
| `SOCRATA_APP_TOKEN` | App token from the developer settings link below |
| `GCP_PROJECT_ID` | BigQuery project |
| `GCP_DATASET` | `crashes` |
| `GCP_LOCATION` | `us-east1` |
| `GCP_STAGING_TABLE` | `stg_crash_person` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to the service-account key JSON. Leave unset on Cloud Run, where the job's service account is used instead. |
| `SQL_FILE` | `Star_Schema_ETL.sql` |
| `PANEL_SQL_FILE` | Optional, default `ML_Crash_Panel.sql` |
| `DELTA_LOOKBACK_DAYS` | Optional, default `90` |

The service account needs to create datasets and tables and run query jobs,
for example with the BigQuery Data Editor and BigQuery Job User roles.

To run the pipeline in a container instead, see [Docker](#docker).

---

## Running the pipeline

```powershell
python Run_Pipeline.py --setup --full   # first time: build schema + load all history
python Run_Pipeline.py --delta          # routine: new and amended recent crashes
python Run_Pipeline.py --full           # occasional: catch amendments older than the window
python Run_Pipeline.py --panel          # rebuild only the ML panel
```

| Flag | What runs |
|---|---|
| `--setup` | Creates the dataset, then star-schema Sections 1 (DDL and cleaning view), 2 (seed calendar, time and unknown members) and 7 (role-playing views). Safe to rerun. |
| `--delta` | Fetches only the watermark window, then runs the load sequence below. |
| `--full` | Fetches the whole history (about 9 pages), then runs the load sequence below. |
| `--panel` | Rebuilds `ml_crash_panel` without loading. |

`--delta` and `--full` can't be combined. `--setup` combines with either.

**Load sequence (every `--delta` / `--full`):**

1. `Get_Data.fetch()`: page the API.
2. `Load_to_GBQ.load_staging()`: replace staging. `WRITE_TRUNCATE` replaces
   SQL Section 3.
3. Section 4: dimension merges.
4. Section 5: fact load. Replaces changed crashes and deletes crashes removed
   upstream, in one transaction.
5. Section 6: sanity checks, printed to the console.
6. Section 9: write an audit log row.
7. `ML_Crash_Panel.sql`: rebuild the panel.

`Run_Pipeline.py` runs the SQL files section by section, splitting them on
the `-- SECTION n:` marker lines.

---

## Deployment

The goal is an unattended daily load on Google Cloud. The container is built
and verified against production. The cloud resources are next (see
[Status](#status)).

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
   daily:     --delta
   periodic:  --full
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
  600-second page timeouts.
- **Retries:** 1. Loads are safe to retry.

### Docker

The image is based on `python:3.13-slim`, installs only `requirements.txt`,
runs as a non-root user, and defaults to `--delta`. Pass other flags as
arguments. `.dockerignore` keeps `.env` and the key JSON out of the image.

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
- [ ] GCP setup script: enable APIs, Artifact Registry repo, both service
      accounts, Secret Manager secret, Workload Identity Federation
- [ ] Create the Cloud Run Job
- [ ] GitHub Actions workflow: on push to `main`, build, push and update the
      job; manual runs with a chosen mode through `workflow_dispatch`
- [ ] Cloud Scheduler triggers: daily `--delta`, periodic `--full` (weekly or
      monthly, still to decide)
- [ ] Alert on failed job executions, plus a freshness check on
      `etl_load_log` (for example, no load in 36 hours)

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
  sorted and without the Socrata system columns. A crash is deleted and
  reinserted only when it's new, its hash changed, or its fact rows disagree
  with staging. Unchanged crashes aren't touched, so a republish with no real
  changes rewrites nothing.
- **Deletes:** fact crashes in the loaded window that are missing from
  staging are deleted (`@window_start`; NULL means the whole fact on a full
  load).
- **One transaction, three guards.** The replace and the deletes commit
  together. The load fails without changing anything if:
  - staging spans two publishes (a republish mid-fetch),
  - staging holds under 90% of the window's crashes, counted before any
    change, or
  - any staged crash doesn't end up with exactly its staged rows.
- **Audit:** `etl_load_log` records each load's mode, window start, start
  and finish times, rows staged, fact rows inserted and deleted, crashes new,
  changed and removed, and which Socrata publish was read
  (`socrata_updated_at`).

A delta only catches amendments to crashes inside the window. Run `--full`
now and then to catch older ones.

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
| Other | `age`, `latitude`, `longitude`, `crash_date`, `crash_datetime`, `reporting_lag_hours` |
| Audit | `socrata_version`, `socrata_updated_at`, `loaded_at`, `crash_hash` (change detection) |

- **Measures** are INT64 0/1 rather than BOOL, so they add up: `SUM(is_fatal)`
  counts fatalities and `AVG(is_injured)` gives an injury rate.
- **`is_injured`** means "possible injury" or worse. **`is_fatal`** means
  fatal.
- **Raw age** stays on the fact. It's validated to 0–110; values like `BB`,
  `NN` and `913` become NULL.
- **Lat/long** is per unit and lives on the fact, not the dimension. Values
  outside Hamilton County's bounds become NULL. Keeping exact coordinates off
  `dim_location` shrank it from about 1:1 with the fact to 62,885 rows.
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
  key, so a changed attribute is a new member, not an update.
- **Unknown members** (`-1`, or `19000101` for dates). The fact uses
  `LEFT JOIN` plus `IFNULL`, so a failed lookup is counted, never dropped.
- **Two coding eras.** Ohio re-coded crash reports around 2019, and both
  vintages share the same columns. Every coded dimension keeps the raw string
  and a canonical attribute that reconciles the two. After the fix, injury
  rates line up: 15.35% for 2019+ and 14.39% before 2019.
- **Constraints are metadata.** BigQuery doesn't enforce primary or foreign
  keys (`NOT ENFORCED`), so the load logic guarantees integrity.

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
| Crash count | Distinct `instanceid` | Counting person rows would inflate multi-occupant crashes. Neighborhood and date never differ within a crash (0 of 221,289). |

Cell scheme, dates and split boundaries are `DECLARE` settings at the top of
Section 2, so changing the grain doesn't mean rewriting the SQL.

### Columns

| Group | Columns |
|---|---|
| Keys | `cell_scheme`, `cell_id`, `day` |
| Target | `crashes` |
| Lags | `lag_7`, `lag_364` (same weekday last year), `roll_28` (at least 14 days of history), `roll_91` (at least 30). All come from earlier days of the same cell. |
| Calendar | `dow` (1 = Sunday), `month`, `is_weekend`, `is_holiday` (US federal, actual and observed dates), `doy_sin`, `doy_cos` |
| Weather | `tmax_f`, `tmin_f`, `prcp_in`, `snow_in`, `snwd_in`, `awnd_mph`, joined on `day` from `ml_weather_daily` |
| Cell attributes | `road_miles`, `intersection_count`, `aadt`, `population`, joined on cell from `ml_cell_attributes` |
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

Holiday flags were checked against pandas' `USFederalHolidayCalendar`. All 111
observed holidays match, plus the 14 actual dates that fell on weekends.

### Current results

| Metric | Value | Spec expected |
|---|---:|---|
| Rows | 205,746 | ~180K |
| Mean crashes per cell-day | 0.84 | ~0.8 |
| Zero fraction | 53.8% | 50–70% |
| Dispersion (variance ÷ mean) | 1.81 | Overdispersed, so Poisson is the floor |
| Rows by split (train / valid / test) | 154,866 / 19,398 / 31,482 | |

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

Section 6 of `Star_Schema_ETL.sql` prints these after every load:

| Check | Expected |
|---|---|
| Rows routed to unknown members | Crash dates 7 (5 NULL `crashdate` + 2 dated 1900), reported dates 7, times 5. **Location, conditions, crash type and person profile must be 0.** Anything else means a natural key is computed differently in two places. |
| Duplicate natural keys per dimension | None |
| Grain | `fact_rows = distinct_socrata_ids`, and `persons_per_crash` about 1.96. A value near 1.0 means staging was deduped on `instanceid`. This check can't catch a republish's duplicates, since those carry new ids; Section 5's per-crash row-count assert catches them before commit. |
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
| Lat/long on the fact | Exact coordinates are almost unique per row. In the dimension they would make it as big as the fact. |
| Panel is a table, not a view | Training reads a stable snapshot, reads are cheap, and publishing waits for the assertions to pass. |
| Panel is fully rebuilt each load, not incrementally | Rolling features and zero-filled cells depend on neighboring days. A late or amended crash changes `lag_7`, `roll_28`, `roll_91` and `lag_364` for up to a year of later rows in its cell. At about 200K rows a rebuild takes seconds, while an incremental version would need to recompute a trailing window anyway. |
| Holidays flag both actual and observed dates | July 4 traffic happens on the 4th even when the day off is the 3rd. |

---

## Files

| File | Role |
|---|---|
| `Get_Data.py` | Extract. `fetch(since=None)` pages the Socrata v3 API via POST and returns raw strings. |
| `Load_to_GBQ.py` | Load. Renames columns, forces STRING and `WRITE_TRUNCATE`s `crashes.stg_crash_person`. |
| `Star_Schema_ETL.sql` | Transform: DDL, cleaning view, merges, checks, views, delete reconciliation, audit log. |
| `ML_Crash_Panel.sql` | ML panel: external feature tables, the ASSERT-gated panel build, and a report. |
| `Run_Pipeline.py` | Orchestration and CLI. Runs the SQL section by section and passes query parameters. |
| `crash-panel-spec.md` | Spec for the ML panel and the modeling plan. |
| `TODO.md` | Open tasks. |
| `requirements.txt` | Pinned runtime dependencies (what the image installs). |
| `requirements-dev.txt` | Runtime dependencies plus notebook packages. |
| `Dockerfile`, `.dockerignore` | Batch image for the Cloud Run Job. |
| `Get_Data.ipynb` | Scratch notebook. Uses the old single GET that downloads all ~435 MB. |

### SQL section map

`Star_Schema_ETL.sql`:

| Section | Runs | Contents |
|---|---|---|
| 1 | `--setup` | Staging, dimension, fact and `etl_load_log` DDL; `ALTER` migrations; cleaning view |
| 2 | `--setup` | Generated calendar, `dim_time` and unknown members (guarded) |
| 3 | Reference only | `TRUNCATE` (the Python loader does this) and the watermark explanation |
| 4 | Every load | Dimension `MERGE`s |
| 5 | Every load | Fact load: crash-level replace and delete reconciliation, in one transaction (`@window_start`) |
| 6 | Every load | Sanity checks |
| 7 | `--setup` | Role-playing date views |
| 8 | Retired | Folded into Section 5 |
| 9 | Every load | Audit log insert |

`ML_Crash_Panel.sql`:

| Section | Contents |
|---|---|
| 1 | `ml_weather_daily` and `ml_cell_attributes` DDL (idempotent) |
| 2 | Settings, crash-level rollup, holidays, spine and features, `ASSERT`s, publish |
| 3 | Panel report: size, zero fraction, dispersion, split sizes |

---

## Operational notes

- **After changing the cleaning view,** `TRUNCATE crashes.fact_crash_person`
  and run `--full`. The hash is built from raw values, so crashes whose
  source rows haven't changed would keep the old logic.
- **After pulling a schema change,** run `--setup` once. Section 1 carries
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migrations for existing tables.
- **Upgrading a dataset from the old `:id` MERGE:** run `--setup`, then
  `--full`. Rows without a `crash_hash` are always rewritten, so the full load
  rewrites every crash once and removes any duplicates; there's no need to
  truncate first. Production went through this on 2026-09-19.
- **Don't start two loads at once.** Both write the same staging table.
  Cloud Run doesn't prevent overlapping executions. Section 5's guards and
  transaction will usually fail the second load rather than corrupt the fact,
  but avoid manual runs near the scheduled time.
- **A failed load changes nothing.** The fact load is one transaction, and the
  watermark is read from the fact, so rerunning is the fix. Failures raise, so
  the process exits non-zero, which is what the planned Cloud Run alert
  watches for.
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
- **`FutureWarning` about `pandas-gbq`** is printed during the staging load.
  The BigQuery client says future versions will need `pandas-gbq` for
  DataFrame loads. It's harmless for now; add the package if a
  `google-cloud-bigquery` upgrade starts requiring it.

---

## Roadmap

In rough priority order, from `TODO.md` and the panel spec:

- [ ] **Scheduled cloud refresh.** Next up; the steps are tracked under
      [Deployment status](#status).
- [ ] Haversine distance from downtown to each crash; possibly as an
      attribute on `dim_location`
- [ ] Load `ml_cell_attributes` (OSM road miles, ODOT AADT, ACS) so exposure
      becomes a real rate denominator
- [ ] Load `ml_weather_daily` (NOAA, one station)
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
