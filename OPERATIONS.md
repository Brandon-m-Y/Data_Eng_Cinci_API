# How the deployed pipeline runs

What happens in the cloud, what triggers it, and what to check when something
looks wrong. [DEPLOY.md](DEPLOY.md) is how it was built; this is how it
behaves now that it exists. [README.md](README.md) covers the data model and
the ETL logic itself.

Deployed and verified end to end on 2026-10-02.

---

## Summary: what triggers what

**GitHub does not run the pipeline.** This is the single most common wrong
assumption about this setup, so it is worth stating plainly. GitHub Actions
only builds the container image. **Cloud Scheduler** is what runs the
pipeline, on a clock, inside Google Cloud, with no involvement from GitHub at
all.

The two paths are completely separate, and only the second one runs on a
schedule:

```
 PATH 1 - code changes (manual today)
 ─────────────────────────────────────
 push to main
     │
     ├─► GitHub Actions: `test` job — 51 offline tests on Ubuntu/Python 3.13
     │
     └─► GitHub Actions: `deploy` job — SKIPPED (see note below)
             would build the image, push it, and repoint both jobs

 Today the image is built and pushed by hand (DEPLOY.md step 5).


 PATH 2 - the pipeline actually running (live, unattended)
 ──────────────────────────────────────────────────────────
 Cloud Scheduler                       06:00 America/New_York
     │  POST .../jobs/<job>:run        OAuth token, crash-etl-scheduler
     ▼
 Cloud Run Job                         crash-etl-delta  (Mon-Sat)
     │                                 crash-etl-full   (Sun)
     │  runs as crash-etl-runtime, no key file
     │  SOCRATA_APP_TOKEN injected from Secret Manager
     ▼
 Run_Pipeline.py
     │  acquire writer lease → extract from Socrata → stage → validate
     │  → merge dimensions → fact transaction → rebuild ML panel
     │  → mark succeeded → release lease
     ▼
 BigQuery dataset `crashes`
     ▲
     │  reads etl_lease, etl_load_log, fact_crash_person
 BigQuery scheduled query "watchdog"   14:00 UTC daily
     │  raises if anything is wrong; the failed run sends the email
     ▼
 your inbox
```

### Why the deploy job is skipped

The `deploy` job is gated on a repository variable:

```yaml
if: ${{ vars.GCP_WIF_PROVIDER != '' }}
```

That variable is not set, because Workload Identity Federation has not been
configured ([DEPLOY.md](DEPLOY.md) Appendix A). Until it is, the job is
skipped and the workflow stays green.

It was not always gated. For three pushes it ran and failed at the auth step,
emailing a failure each time. Gating it was the fix: skipped is honest, failed
is noise. Setting the three repository variables in Appendix A turns
deployment on with no further change to the workflow.

**This repository is public, so a service-account JSON key must never be used
in CI.** That is why the path is Workload Identity Federation rather than a
stored secret.

---

## What runs, and when

| Trigger | Cron | Job | Mode |
|---|---|---|---|
| `crash-etl-delta-mon-sat` | `0 6 * * 1-6` | `crash-etl-delta` | `--delta` |
| `crash-etl-full-sun` | `0 6 * * 0` | `crash-etl-full` | `--full` |

Both in `America/New_York`, so they follow daylight saving. They never
overlap: Sunday is the full load, Monday–Saturday is the delta.

A delta covers crashes within 90 days of the newest loaded crash
(`DELTA_LOOKBACK_DAYS`, default 90). A full load covers all history. The
weekly full load exists to bound how far a missed or partial delta can drift,
and costs about 2.5 minutes.

### Job sizing

Both jobs run the same image; only the argument and the sizing differ.

| | `crash-etl-delta` | `crash-etl-full` |
|---|---|---|
| Args | `--delta` | `--full` |
| Memory | 1 GiB | 4 GiB |
| CPU | 1 | 2 |
| Task timeout | 30 min | 1 hour |
| Retries | 1 (two attempts) | 1 (two attempts) |
| Measured peak memory | 183 MB | 1.76 GB |
| Measured wall time | 184 s in Cloud Run | ~155 s locally |

The delta took ~90 s locally and ~190 s in Cloud Run. Cloud Run is slower,
and the sizing has room for that.

### Why one retry, and what it means

A load that fails *cleanly* — a source error, a rejected BigQuery job —
releases the writer lease on its way out, so the retry is a genuine second
attempt and needs nobody.

A load whose outcome is *unresolved* — SIGKILL, a lost response — keeps the
lease deliberately. The retry then fails within seconds against the held lock
instead of writing anything. That fast, loud failure is the intended
behaviour, not a bug: see [README.md](README.md) § Operational notes for the
recovery procedure.

---

## Inside a single run

`Run_Pipeline.py` orchestrates; the SQL does the work, section by section.

1. **Acquire the writer lease.** One row in `crashes.etl_lease`. Ownership
   never expires — no elapsed time proves a paused process will not wake and
   submit a write it already reserved.
2. **Extract** from the Socrata v3 API, paged, verified against the source's
   own row count, publish stamp and schema both before and after paging.
3. **Stage** into `stg_crash_person` with `WRITE_TRUNCATE`, every column a
   string, bound to the verified extract's manifest.
4. **Validate staging** before anything downstream changes.
5. **Merge dimensions**, then the **fact transaction**: crash-level replace
   keyed on `instanceid`, gated by an MD5 content hash, with upstream
   deletions reconciled inside the same transaction behind a deletion cap.
6. **Rebuild the ML panel** (`ML_Crash_Panel.sql`).
7. **Mark `succeeded`** in `etl_load_log` — only after the panel is published.
8. **Release the lease.**

Status in `etl_load_log` moves `fact_committed` → `succeeded`. Seeing
`fact_committed` with the lease held usually means the run is still building
the panel, not that it failed. On the verified run the fact committed at 41 s
and the panel finished at 184 s.

### Authentication, and the absence of a key

The jobs carry **no** `GOOGLE_APPLICATION_CREDENTIALS`, and that is
deliberate. `Load_to_GBQ.get_client()` uses a key file only when that variable
is set; with it unset the client falls back to the attached service account.
Setting it on the job would break the run looking for a file that is not in
the image.

`SOCRATA_APP_TOKEN` is injected from Secret Manager by reference, so it never
appears in `gcloud run jobs describe` output or in the image.

---

## Monitoring

### The watchdog

A BigQuery scheduled query, `deploy/watchdog.sql`, running daily at **14:00
UTC** — 09:00 EST / 10:00 EDT, a few hours after the 06:00 load. It checks the
load that just ran rather than polling the clock.

A healthy check returns one row, `ok`. Any tripped guard raises, which marks
the scheduled-query run failed, which sends the email. **The failure is the
alert** — there is no extra state to maintain.

| Guard | Trips when | Catches |
|---|---|---|
| Stuck lock | `etl_lease` held over 2 hours | An owner that died with an unresolved job |
| No recent load | No `succeeded` row in 24 hours | Crashed job, bad deploy, revoked permission |
| Stale feed | `socrata_updated_at` unchanged 21 days | The city stopped publishing |
| Data not advancing | Newest `crash_date` over 60 days old | The feed republishes but adds no crash days |

The threshold is **24** hours, not 36, because the check runs once a day after
the load. Normally the newest success is ~4 hours old; after a failed load it
is ~28. At 36 hours a single failed load would not be caught until the
following day.

The last two guards are not redundant. A publish can change the stamp while
adding nothing — observed 2026-10-02.

**The watchdog must be owned by a person, not the runtime service account.**
This is the thing most likely to be got wrong, because the broken version
looks correct in every readout. Details in the gotchas below.

### Cloud Run job failures

Watchdog guard 2 catches a crashed job within ~24 hours. A Cloud Monitoring
policy on `run.googleapis.com/job/completed_execution_count` with
`result = failed` makes that minutes instead. **Not yet configured** —
[DEPLOY.md](DEPLOY.md) step 10.

---

## Resource inventory

Project `cincinnati-open-crash-data`, region `us-east1` throughout — the same
region as the BigQuery dataset.

| Resource | Name | Notes |
|---|---|---|
| Artifact Registry | `crash-etl` | Docker, `us-east1` |
| Image | `us-east1-docker.pkg.dev/cincinnati-open-crash-data/crash-etl/crash-etl` | Deployed by digest `sha256:cfd0f854…` |
| Cloud Run Job | `crash-etl-delta` | `--delta`, 1 GiB, 1 CPU, 30 min |
| Cloud Run Job | `crash-etl-full` | `--full`, 4 GiB, 2 CPU, 1 h |
| Scheduler | `crash-etl-delta-mon-sat` | `0 6 * * 1-6` America/New_York |
| Scheduler | `crash-etl-full-sun` | `0 6 * * 0` America/New_York |
| Secret | `socrata-app-token` | 25 bytes, no trailing newline |
| Scheduled query | `crash-etl_watchdog` | Daily 14:00 UTC, owned by a person |
| Dataset | `crashes` | `us-east1` |

### Identities

| Service account | Has | Why |
|---|---|---|
| `crash-etl-runtime` | `bigquery.jobUser`, `bigquery.dataEditor` (project); `secretmanager.secretAccessor` (that one secret) | Runs both jobs |
| `crash-etl-scheduler` | `run.invoker` on each job, **nothing at project level** | Can start those two jobs and do nothing else |
| `cinci-crash-etl` | `bigquery.jobUser`, `bigquery.dataEditor` | **Legacy.** Owns the local JSON key. To be retired — DEPLOY.md step 11 |

`bigquery.jobUser` has to be project-level; that is where the right to start a
job lives. `bigquery.dataEditor` is project-level because this project holds
one dataset. DEPLOY.md Appendix B scopes it to `crashes` alone if that stops
being true.

### Cost

Everything sits inside the always-free tiers. Cloud Run uses roughly 3,700
vCPU-seconds and 5,000 GiB-seconds a month against free allowances of 180,000
and 360,000. Two scheduler triggers fit the free three. The watchdog scans 3.5
MB per run, ~0.1 GB a month against 1 TB. Artifact Registry storage is the
only real line item, at a few cents. **The weekly full load is not what costs
money** — accumulating old images slowly is.

---

## The scripts

| File | Role |
|---|---|
| `Run_Pipeline.py` | Orchestration and CLI. Holds the lease, runs the SQL section by section, records the outcome. The container's entrypoint. |
| `Get_Data.py` | Extract. Pages the Socrata v3 API and verifies the extract against the source's own count, publish stamp and schema. |
| `Load_to_GBQ.py` | Staging load. `get_client()` is what makes the keyless Cloud Run auth work. |
| `Pipeline_Config.py` | Validated project/dataset/location; rewrites `crashes.` in the SQL to the configured dataset. |
| `Star_Schema_ETL.sql` | The ETL itself: DDL, cleaning view, validation, merges, fact transaction, checks, audit log, lease. |
| `ML_Crash_Panel.sql` | The ML panel build, rebuilt after every load. |
| `deploy/watchdog.sql` | The four health guards. Deployed as a scheduled query. |
| `deploy/create_watchdog.py` | Manages that scheduled query — see below. |
| `.github/workflows/deploy.yml` | Offline tests on every push; build-and-deploy when WIF is configured. |
| `Dockerfile` | Base pinned by digest, dependencies under `constraints.txt`, non-root user. |

### `deploy/create_watchdog.py`

It **cannot create** a watchdog owned by a person — only the BigQuery console
can (see gotchas). Everything after creation it handles:

```powershell
# Report: schedule, whether email is on, WHO the mail reaches,
# and whether the deployed query still matches the repo
python deploy/create_watchdog.py

# Push a changed watchdog.sql
python deploy/create_watchdog.py --update --as-me

# Prove the failure email still arrives, then restore
python deploy/create_watchdog.py --update --test-alert --run-now --as-me
python deploy/create_watchdog.py --update --as-me
```

`--test-alert` installs a watchdog that always fails, so the notification path
gets exercised for real. Forgetting to restore it sends a daily email you
cannot miss — loud rather than silent, which is the right direction for a
safeguard to fail.

---

## What is verified, and what is not

Being specific about this matters more than a green checklist.

**Verified against production:**

- A Cloud Run delta end to end: keyless auth, the secret resolving to a
  working Socrata token, extract verification, lease acquire *and release*,
  staging validation, hash comparison, panel rebuild, audit row.
- The scheduler → Cloud Run chain: trigger at 20:10:21, execution at
  20:10:22, `succeeded` at 184 s, lease released.
- The watchdog's healthy path returns `ok`, and **all four guards fire** with
  correct messages when forced against simulated data.
- The failure email actually reaches a human inbox.
- 51 offline tests pass on clean Ubuntu with Python 3.13 in CI.

**Not yet exercised:**

- **The full write path in Cloud Run.** Every load so far has found zero
  changes, because the feed has added no new crash days since 2026-08-24.
  Inserts, deletions and the deletion cap have not run in the cloud. The first
  Sunday full load is the real test.
- The `crash-etl-full` job has never run at all — only `crash-etl-delta`.
- The GitHub Actions deploy job, which needs WIF.
- Recovery from a genuinely stuck lease, which has only been reasoned about.

---

## Gotchas learned the hard way

Each of these cost real time on 2026-10-02 and is not obvious from any
documentation.

**A watchdog owned by a service account emails nobody.** It installs
correctly, runs correctly, reports `failure email: True`, and silently has
nowhere to send. A service account has no mailbox. The config must be owned by
a person.

**Only the BigQuery console can create a user-owned scheduled query.** The API
refuses: `400 Failed to find a valid credential. The field 'version_info' or
'service_account_name' must be specified.` `version_info` is an OAuth
authorization code from a consent flow the console runs and
`gcloud auth application-default login` does not provide. `--update` on an
existing user-owned config works fine; only creation is blocked.

**`list_transfer_configs` never populates `owner_info`.** Only
`get_transfer_config` does. Reading ownership from a list response reports
every config as ownerless, which made a perfectly good watchdog look broken.

**The console renames and reformats.** It stores `crash-etl watchdog` as
`crash-etl_watchdog`, and pasting SQL drops a space after `--` on comment
lines. Both are harmless; both break naive exact-match checks.

**`bq` does not work in Git Bash on Windows.** It fails with
`ERROR: (bq) python3.13: command not found` because the `.cmd` wrapper's
interpreter lookup does not survive MSYS2 path translation. `gcloud` is fine;
`bq` is not. Use PowerShell. (`export CLOUDSDK_PYTHON=.../bundledpython/python.exe`
fixes it if you must.)

**A republish does not mean new data.** On 2026-10-02 the feed published, the
stamp moved, every coordinate was re-randomized, and `MAX(crash_date)` did not
budge from 2026-08-24. Guard 4 exists because of this. The publication lag is
~39 days and growing while the data does not advance.

**Secrets and trailing newlines.** `Out-File`, `Set-Content` and piping into
`--data-file=-` all append a newline, producing a token that looks right and
authenticates wrong. `[IO.File]::WriteAllText` does not. Check the byte count
before uploading, not after — PowerShell strips a trailing newline when it
captures output, so reading the secret back cannot detect the problem.

**Paste one command at a time.** Pasting several lines into PowerShell buffers
them behind `>>` and the second can run before the first finishes. This
silently skipped a service-account creation and half an API enablement.

---

## What this run proved about the data model

The first Cloud Run delta landed an hour after a genuine republish, which made
it an unplanned test of the coordinate-free content hash.

Socrata re-randomizes every row's latitude and longitude on each publish for
privacy. Before that fix, a load against a new publish reported **all ~3,317**
in-window crashes as changed. This one reported **0 new, 0 changed, 0
removed** — correct, because nothing substantive changed. The fix is now
proven in production under the exact conditions it was written for.

It also gave the third churn data point: **0 `instanceid` re-keys inside the
90-day window.** Earlier full loads saw 20 and 25 across all history, so
re-keys are spread rather than clustered in recent crashes. The delta deletion
cap is 100 (1% of the 3,319 in-window crashes is 34, so the floor governs) and
the full cap is 2,213. Neither is close to binding.

---

## Still to do

- **Retire the local service-account key** (DEPLOY.md step 11). The cloud no
  longer needs it, and it is now the weakest thing in the setup.
- **Cloud Monitoring alert** on failed job executions (step 10).
- **Workload Identity Federation**, which turns the GitHub deploy job on
  (Appendix A).
- **Watch Sunday's full load** — the first exercise of the full write path in
  the cloud.

Open work beyond deployment is in [TODO.md](TODO.md).

---

## Quick reference

```powershell
# Run a job by hand
gcloud run jobs execute crash-etl-delta --region=us-east1 --wait

# Recent executions and logs
gcloud run jobs executions list --job=crash-etl-delta --region=us-east1 --limit=5
gcloud beta run jobs logs read crash-etl-delta --region=us-east1 --limit=100

# Is the lease free?
bq query --use_legacy_sql=false "SELECT holder, acquired_at FROM crashes.etl_lease"

# Last few loads
bq query --use_legacy_sql=false "SELECT load_mode, status, started_at, crashes_new, crashes_changed, crashes_removed FROM crashes.etl_load_log ORDER BY started_at DESC LIMIT 5"

# Watchdog health, ownership and drift
python deploy/create_watchdog.py

# Stop everything without deleting anything
gcloud scheduler jobs pause crash-etl-delta-mon-sat --location=us-east1
gcloud scheduler jobs pause crash-etl-full-sun --location=us-east1
```

A rollback does not undo writes, and does not need to. The model is
rebuildable: `--full --reprocess` re-derives every crash from the feed, so a
bad load is recovered by fixing the code and reloading, not by restoring a
backup.
