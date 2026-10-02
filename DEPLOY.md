# Deployment runbook

Exact steps to put the pipeline on Cloud Run with a weekly full load. Run
these yourself, in order, in PowerShell from the repo root. Every step ends
with a **Verify** you should see pass before moving on.

This is the *procedure*. The *design* — why a Job and not a service, why the
lock never expires, how the sizing was measured — is in
[README.md § Deployment](README.md#deployment). Recovery from a stuck lock is
in [README.md § Operational notes](README.md#operational-notes).

Commands are written as single lines on purpose. They are wide, but they
paste correctly into any shell, which a continuation character does not.

**Cadence:** `--delta` Monday–Saturday, `--full` Sunday, both 06:00
America/New_York. A full load takes about 2.5 minutes, so running one weekly
costs almost nothing and bounds how far a missed delta can drift.

---

## Before you start

Checked on this machine 2026-10-02: the Google Cloud SDK was **not
installed**, and Docker Desktop was installed but **not running**. That is not
an oversight — the pipeline has only ever reached BigQuery through the Python
client and a service-account key, so `gcloud` and `bq` were never needed until
this runbook. Both are needed now.

**Use PowerShell, not Git Bash.** Two reasons, both found the hard way on
2026-10-02. `bq` fails under Git Bash on this install with
`ERROR: (bq) python3.13: command not found`, because the `.cmd` wrapper's
interpreter lookup does not survive MSYS2's path translation; the same
command works untouched in PowerShell. And the variables below are PowerShell
syntax, which bash will not parse. (If you ever do need `bq` from Git Bash,
`export CLOUDSDK_PYTHON=".../google-cloud-sdk/platform/bundledpython/python.exe"`
fixes it.)

**1. Install the Cloud SDK** (this is what provides `gcloud` and `bq`), then
open a *new* PowerShell window so the PATH refreshes. The installer updates
the persisted user PATH, but a shell that was already open keeps the old one —
that is why `gcloud` reports "command not found" right after installing:

```powershell
winget install --id Google.CloudSDK --exact
```

**2. Sign in as yourself** — not with the pipeline's service-account key.
Steps 1–4 enable APIs and create service accounts, which that key has no
permission to do:

```powershell
gcloud auth login
gcloud config set project cincinnati-open-crash-data
```

Verify with `gcloud auth list` and `bq query --use_legacy_sql=false "SELECT 1"`.

**3. Start Docker Desktop** and let it finish starting. Only step 5 needs it,
so you can leave this until then:

```powershell
Start-Process "$env:LOCALAPPDATA\Programs\DockerDesktop\Docker Desktop.exe"
```

Verify with `docker version` — the **Server** line must be present, not just
the client. WSL 2 is already configured on this machine.

Also true before you begin:

- You are Owner on `cincinnati-open-crash-data`.
- The offline suite passes: `python -m unittest discover -s tests` (51 tests).
  Deploying a red build just moves the failure somewhere harder to see.
- Nothing else is mid-load. Step 7 takes the writer lock; a local run holding
  it will make the cloud run fail fast on a held lock, which is correct
  behaviour but a confusing first result.

---

## Step 0 — Variables

Set these once per terminal. Every later step reuses them.

```powershell
$PROJECT      = "cincinnati-open-crash-data"
$REGION       = "us-east1"
$REPO         = "crash-etl"
$IMAGE        = "$REGION-docker.pkg.dev/$PROJECT/$REPO/crash-etl"
$RUNTIME_SA   = "crash-etl-runtime@$PROJECT.iam.gserviceaccount.com"
$SCHEDULER_SA = "crash-etl-scheduler@$PROJECT.iam.gserviceaccount.com"
gcloud config set project $PROJECT
```

**Verify:** `gcloud config get-value project` prints the project id.

---

## Step 1 — Enable the APIs

```powershell
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudscheduler.googleapis.com secretmanager.googleapis.com bigquerydatatransfer.googleapis.com monitoring.googleapis.com
```

**Verify:** `gcloud services list --enabled --filter="name:run.googleapis.com OR name:artifactregistry.googleapis.com"` lists both. Enabling is slow the first time; it is safe to rerun.

---

## Step 2 — Artifact Registry

A private Docker repository in the same region as the dataset.

```powershell
gcloud artifacts repositories create $REPO --repository-format=docker --location=$REGION --description="Crash ETL container images"
gcloud auth configure-docker "$REGION-docker.pkg.dev"
```

**Verify:** `gcloud artifacts repositories list --location=$REGION` shows `crash-etl`.

---

## Step 3 — The Socrata token in Secret Manager

The token must not be baked into the image or typed into a job's environment,
where it shows up in `gcloud run jobs describe` output.

This reads the token out of your local `.env` and writes it with no trailing
newline. A trailing newline is the classic way to get a secret that looks
right and authenticates wrong.

```powershell
$tok = ((Get-Content .env | Select-String '^SOCRATA_APP_TOKEN=') -replace '^SOCRATA_APP_TOKEN=','').Trim()
[IO.File]::WriteAllText("$env:TEMP\tok.txt", $tok)
gcloud secrets create socrata-app-token --data-file="$env:TEMP\tok.txt" --replication-policy=automatic
Remove-Item "$env:TEMP\tok.txt"
```

**Verify before uploading.** Check the temp file's size on disk, because that
is exactly what becomes the secret:

```powershell
$tok.Length
(Get-Item "$env:TEMP\tok.txt").Length
```

Both must be the same number (25 for the current token). A file one or two
bytes larger means a newline got in — fix it before creating the secret
rather than after.

`[IO.File]::WriteAllText` is what guarantees this: unlike `Out-File`,
`Set-Content` or a pipe into `--data-file=-`, it appends no newline and
writes no BOM. The byte count above confirms it rather than trusting it.

Reading the secret back is a weaker check and should not be relied on alone —
PowerShell strips a trailing newline when it captures native output, so a
newline-corrupted secret still measures correct:

```powershell
(gcloud secrets versions access latest --secret=socrata-app-token).Length
```

---

## Step 4 — Service accounts and IAM

Two identities, each with only what it needs. The runtime account reads and
writes BigQuery; the scheduler account can do exactly one thing, start a job.

```powershell
gcloud iam service-accounts create crash-etl-runtime --display-name="Crash ETL runtime"
gcloud iam service-accounts create crash-etl-scheduler --display-name="Crash ETL scheduler"
```

```powershell
gcloud projects add-iam-policy-binding $PROJECT --member="serviceAccount:$RUNTIME_SA" --role="roles/bigquery.jobUser" --condition=None
gcloud projects add-iam-policy-binding $PROJECT --member="serviceAccount:$RUNTIME_SA" --role="roles/bigquery.dataEditor" --condition=None
gcloud secrets add-iam-policy-binding socrata-app-token --member="serviceAccount:$RUNTIME_SA" --role="roles/secretmanager.secretAccessor"
```

`bigquery.jobUser` has to be project-level — that is where the permission to
run a job lives. `bigquery.dataEditor` is project-level here because this
project holds one dataset and nothing else; see
[Appendix B](#appendix-b--tightening-bigquery-access) to scope it to `crashes`
alone if that ever stops being true.

The scheduler account gets its one permission in step 9, after the jobs it
will invoke exist.

**Verify:** `gcloud projects get-iam-policy $PROJECT --flatten="bindings[].members" --filter="bindings.members:crash-etl-runtime" --format="value(bindings.role)"` lists both BigQuery roles.

---

## Step 5 — Build and push the image

Tag with the git SHA so you can always tell which commit is running, then
resolve that tag to a **digest** and deploy the digest. A tag can be moved
later and silently change what runs; a digest cannot. This is the same reason
the Dockerfile pins its own base image by digest.

```powershell
$SHA = git rev-parse --short HEAD
docker build -t "${IMAGE}:$SHA" -t "${IMAGE}:latest" .
docker push "${IMAGE}:$SHA"
docker push "${IMAGE}:latest"
$DIGEST = (gcloud artifacts docker images describe "${IMAGE}:$SHA" --format="value(image_summary.fully_qualified_digest)")
$DIGEST
```

**Verify:** `$DIGEST` prints a `...@sha256:...` string. Keep it; step 6 uses
it and the rollback section needs the previous one.

> Commit before building. `git rev-parse` tags the image with the last commit,
> so a dirty tree produces an image whose tag is a lie.

---

## Step 6 — Create the two jobs

Same image, different arguments and sizing. Sizing comes from measured peaks
(README § Job sizing): a delta peaked at 183 MB, a full load at 1.76 GB.

```powershell
gcloud run jobs create crash-etl-delta --image=$DIGEST --region=$REGION --service-account=$RUNTIME_SA --args=--delta --memory=1Gi --cpu=1 --task-timeout=30m --max-retries=1 --set-env-vars="GCP_PROJECT_ID=$PROJECT,GCP_DATASET=crashes,GCP_LOCATION=$REGION" --set-secrets="SOCRATA_APP_TOKEN=socrata-app-token:latest"
```

```powershell
gcloud run jobs create crash-etl-full --image=$DIGEST --region=$REGION --service-account=$RUNTIME_SA --args=--full --memory=4Gi --cpu=2 --task-timeout=1h --max-retries=1 --set-env-vars="GCP_PROJECT_ID=$PROJECT,GCP_DATASET=crashes,GCP_LOCATION=$REGION" --set-secrets="SOCRATA_APP_TOKEN=socrata-app-token:latest"
```

Notes on the settings:

- **No `GOOGLE_APPLICATION_CREDENTIALS`.** That is deliberate.
  `Load_to_GBQ.get_client()` uses a key file only when that variable is set,
  and otherwise picks up the attached service account. Setting it here would
  break the job looking for a file that is not in the image.
- **`--max-retries=1`** means one retry, two attempts total. A clean failure
  releases the lock on the way out, so the retry is a real second attempt. An
  unresolved outcome keeps the lock and the retry fails in seconds against it,
  loudly, instead of writing. That is the intended behaviour.
- **`--task-timeout`** is generous because the source pages have 600-second
  read timeouts. Cloud Run sends SIGTERM at the timeout, which
  [Run_Pipeline.py](Run_Pipeline.py) turns into a clean shutdown.
- **2 vCPU on the full job** keeps the 1.76 GB pandas stage short and sits
  safely inside Cloud Run's memory-to-CPU ratio limits.

**Verify:** `gcloud run jobs list --region=$REGION` shows both.

---

## Step 7 — Smoke test

Run the delta by hand before anything is on a schedule. **This writes to
production** — that is the point, it is the real thing.

```powershell
gcloud run jobs execute crash-etl-delta --region=$REGION --wait
```

**Verify** three things:

```powershell
gcloud run jobs executions list --job=crash-etl-delta --region=$REGION --limit=1
bq query --use_legacy_sql=false "SELECT load_mode, status, rows_staged, crashes_new, crashes_changed, crashes_removed, finished_at FROM crashes.etl_load_log ORDER BY started_at DESC LIMIT 1"
bq query --use_legacy_sql=false "SELECT holder, acquired_at FROM crashes.etl_lease"
```

1. The execution shows 1/1 tasks succeeded.
2. The newest `etl_load_log` row has `status = 'succeeded'`.
3. `etl_lease.holder` is **NULL** — the run gave the lock back.

If `holder` is not NULL the run did not finish cleanly. Do not schedule
anything. Go to README § Operational notes and recover that owner by name
first.

Logs if you need them:

```powershell
gcloud beta run jobs logs read crash-etl-delta --region=$REGION --limit=100
```

---

## Step 8 — The watchdog (required before step 9)

Writer ownership never expires, by design: no amount of elapsed time proves a
paused process will not wake and submit the write it already reserved. The
consequence is that a stuck lock blocks every later load **forever** and
nothing in the system will mention it. Unattended scheduling without this
alert turns a silent stall into weeks of stale data.

[deploy/watchdog.sql](deploy/watchdog.sql) checks four things once a day and
raises an error if any trips. It runs at 14:00 UTC — 09:00 EST / 10:00 EDT —
a few hours after the 06:00 load, so it checks the load that just ran rather
than polling the clock. Daily suits a daily pipeline, and a persistent fault
mails once a day instead of twenty-four times. A failed scheduled query sends mail, so the
failure *is* the alert — there is no extra state to keep.

| Guard | Trips when | Catches |
|---|---|---|
| Stuck lock | `etl_lease` held over 2 hours | An owner that died with an unresolved job |
| No recent load | No `succeeded` row in 24 hours | Crashed job, bad deploy, revoked permission |
| Stale feed | `socrata_updated_at` unchanged 21 days | The city stopped publishing |
| Data not advancing | Newest `crash_date` over 60 days old | The feed republishes but adds no crash days |

The last two are not redundant. A publish can change the stamp while adding
nothing — observed on 2026-10-02, when a fresh publish arrived an hour before
the first cloud delta, which found 0 new and 0 changed with `MAX(crash_date)`
unmoved. Guard 3 watches whether the city is publishing; guard 4 watches
whether any of it is new.

Install it **owned by you, from the BigQuery console**. Not from the script,
and not owned by the runtime service account. Both of those were tried here
on 2026-10-02 and neither works:

- **Owned by the service account** — the config installs fine and the query
  runs fine, but the failure notification goes to the config's owner, a
  service account has no mailbox, and `owner_info` is empty. The watchdog
  fails on schedule, correctly, and tells nobody. A forced failure produced
  no mail at all.
- **Owned by you, via this script** — refused by the API:
  `400 Failed to find a valid credential. The field 'version_info' or
  'service_account_name' must be specified.` Creating a user-owned scheduled
  query needs an OAuth authorization code that application-default
  credentials do not carry. The console runs that consent flow; the API does
  not. The script now refuses this up front rather than failing partway.

In the BigQuery console:

1. Open the SQL editor and paste the whole of
   [deploy/watchdog.sql](deploy/watchdog.sql).
2. **Run it once** first. It should return a single row, `ok`.
3. **Schedule** → *Create new scheduled query*.
4. Name: `crash-etl watchdog`.
5. Repeats: *Custom*, with `every day 14:00`. The console reads this as UTC,
   which is 09:00 EST / 10:00 EDT — after the 06:00 load either way, which
   guard 2's 24-hour threshold depends on.
6. Leave the destination table **empty**. This is a script, not a query with
   a result to store.
7. Leave the service-account field on your own credentials. Choosing the
   runtime account here is exactly the thing that silences the mail.
8. Tick **Send email notifications**.
9. Save, and accept the OAuth consent prompt.

Afterwards the script manages it normally — `--update` pushes a changed
`watchdog.sql` without touching ownership.

The objection to owning it yourself is that the schedule then depends on your
credentials. That is real but much smaller than it sounds, because it fails
in the safe direction: if those credentials stop working the run fails, and a
failed run mails you. A watchdog that breaks loudly is recoverable; one that
cannot reach anyone is not.

If you later want it off a personal account entirely, the route is a Pub/Sub
notification topic plus a Cloud Monitoring notification channel, which routes
anywhere and keeps the service-account runner. More moving parts, so it is
not the starting point.

Check who it belongs to at any time — no flags means report, not change:

```powershell
python deploy/create_watchdog.py
```

That prints the schedule, whether failure email is on, **who the mail
reaches**, and whether the deployed query still matches `watchdog.sql`.

Or by hand: BigQuery console → paste `deploy/watchdog.sql` → **Schedule** →
repeat every 1 hour → leave the destination table empty (it is a script) →
run as `crash-etl-runtime` → tick **Send email notifications** → Save.

**Verify:** paste `deploy/watchdog.sql` into the BigQuery console and run it.
A healthy system returns one row, `ok`. Then check the Scheduled Queries page
after the first hour and confirm the run shows Succeeded.

### Prove the mail actually arrives

Do this once, before step 9. The guards are tested and fire correctly, but
that only shows the query raises — it does not show the notification reaches
a person. The query runs as a service account, and a service account has no
inbox, so this is worth confirming rather than assuming.

Install an always-failing watchdog and trigger it immediately:

```powershell
python deploy/create_watchdog.py --update --test-alert --run-now --service-account $RUNTIME_SA
```

Within a minute or two you should get mail about a failed scheduled query,
with a body saying it is a delivery test. Then **restore the real watchdog**:

```powershell
python deploy/create_watchdog.py --update --service-account $RUNTIME_SA
```

**Verify the restore**, because this is the one state you do not want to walk
away from:

```powershell
python deploy/create_watchdog.py
```

With no flags it reports the existing config instead of changing it. If no
mail arrives, the notification is not reaching you — restore the real
watchdog first, then reinstall it with `--as-me`, which ties the alert to
your own account at the cost of depending on your credentials.

Forgetting to restore fails loudly rather than silently: you get a daily
mail you cannot miss. That is the right way round for a safeguard, but fix it
the same day — a watchdog crying wolf is one you will start ignoring, and
then it protects nothing.

---

## Step 9 — Cloud Scheduler

Do not start this until step 8 is green.

Give the scheduler account permission to start each job, and nothing else:

```powershell
gcloud run jobs add-iam-policy-binding crash-etl-delta --region=$REGION --member="serviceAccount:$SCHEDULER_SA" --role="roles/run.invoker"
gcloud run jobs add-iam-policy-binding crash-etl-full --region=$REGION --member="serviceAccount:$SCHEDULER_SA" --role="roles/run.invoker"
```

Then the two triggers. The schedules do not overlap: Sunday is the full load,
Monday–Saturday is the delta.

```powershell
gcloud scheduler jobs create http crash-etl-delta-mon-sat --location=$REGION --schedule="0 6 * * 1-6" --time-zone="America/New_York" --uri="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/crash-etl-delta:run" --http-method=POST --oauth-service-account-email=$SCHEDULER_SA
```

```powershell
gcloud scheduler jobs create http crash-etl-full-sun --location=$REGION --schedule="0 6 * * 0" --time-zone="America/New_York" --uri="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/crash-etl-full:run" --http-method=POST --oauth-service-account-email=$SCHEDULER_SA
```

**Verify:** fire one by hand rather than waiting until 6am.

```powershell
gcloud scheduler jobs run crash-etl-delta-mon-sat --location=$REGION
gcloud scheduler jobs list --location=$REGION
gcloud run jobs executions list --job=crash-etl-delta --region=$REGION --limit=2
```

A new execution should appear. `--oauth-` is correct here, not `--oidc-`:
`run.googleapis.com` is a Google API, so it takes an OAuth token.

Two triggers fit Cloud Scheduler's free tier of three.

---

## Step 10 — Fast failure alert (recommended)

Watchdog guard 2 already catches a crashed job, but it can take up to 36
hours. This makes it minutes.

Monitoring → Alerting → **Create policy** → metric
`run.googleapis.com/job/completed_execution_count` on resource **Cloud Run
Job** → filter `result = failed` → condition: *any time series is above 0*
over a 5-minute window → add an email notification channel → name it
`crash-etl job failed` → Save.

---

## Step 11 — Retire the local key

Once the cloud runs the pipeline, the long-lived JSON key on your laptop is
the weakest thing left. It is already gitignored and has never been committed,
but a key that does not exist cannot leak.

```powershell
gcloud auth application-default login
```

Delete the `GOOGLE_APPLICATION_CREDENTIALS` line from `.env`, then re-run the
offline suite and one integration run to confirm ADC works. Only then:

```powershell
$KEY_SA = "<the account the local key belongs to>"
gcloud iam service-accounts keys list --iam-account=$KEY_SA
gcloud iam service-accounts keys delete <KEY_ID> --iam-account=$KEY_SA
Remove-Item .\cincinnati-open-crash-data-*.json
```

**Verify:** `gcloud iam service-accounts keys list --iam-account=$KEY_SA`
shows only Google-managed keys, and the tests still pass.

---

## What it costs

Roughly nothing, and that is worth knowing before you worry about the weekly
full load.

| Resource | Monthly usage | Cost |
|---|---|---|
| Cloud Run | ~26 deltas (90 s, 1 vCPU, 1 GiB) + ~4.3 full loads (155 s, 2 vCPU, 4 GiB) ≈ 3,700 vCPU-s, 5,000 GiB-s | Inside the always-free tier (180,000 vCPU-s, 360,000 GiB-s) |
| Cloud Scheduler | 2 triggers | Free tier is 3 |
| BigQuery watchdog | ~30 daily queries scanning 3.5 MB each, 10 MB minimum billing ≈ 0.3 GB | Inside the 1 TB free tier |
| Artifact Registry | ~0.5 GB of images | ~$0.05 |
| Secret Manager | 1 secret, few accesses | ~$0.06 |

The weekly full load is not what costs money. Keeping old images is, slowly —
prune them occasionally with `gcloud artifacts docker images list $IMAGE`.

---

## Rolling back

**Stop everything quickly** — pausing the triggers leaves the jobs intact and
lets an in-flight run finish normally:

```powershell
gcloud scheduler jobs pause crash-etl-delta-mon-sat --location=$REGION
gcloud scheduler jobs pause crash-etl-full-sun --location=$REGION
```

**Go back to a previous image** — point the job at the digest you recorded in
step 5 for the last good build:

```powershell
gcloud run jobs update crash-etl-delta --image=<previous digest> --region=$REGION
gcloud run jobs update crash-etl-full --image=<previous digest> --region=$REGION
```

Resume with `gcloud scheduler jobs resume <name> --location=$REGION`.

A rollback does not undo writes. The data model is rebuildable: `--full
--reprocess` re-derives every crash from the feed, so a bad load is recovered
by fixing the code and reloading, not by restoring a backup.

---

## Appendix A — GitHub Actions (phase 2)

Not required to run on a schedule; this only removes the manual build in step
5. [.github/workflows/deploy.yml](.github/workflows/deploy.yml) already runs
the offline suite on every push — confirmed green on Ubuntu with Python 3.13.
Its **deploy** job is skipped until `GCP_WIF_PROVIDER` is set, so the
workflow stays green instead of failing at auth and mailing you about it.
Setting the three variables below is what switches deploying on.

**This repository is public, so a JSON key must never be used here.** Workload
Identity Federation lets GitHub mint short-lived tokens instead, with no
stored secret.

```powershell
$PROJECT_NUMBER = (gcloud projects describe $PROJECT --format="value(projectNumber)")
$DEPLOYER_SA = "crash-etl-deployer@$PROJECT.iam.gserviceaccount.com"
gcloud iam service-accounts create crash-etl-deployer --display-name="Crash ETL deployer"
gcloud projects add-iam-policy-binding $PROJECT --member="serviceAccount:$DEPLOYER_SA" --role="roles/artifactregistry.writer" --condition=None
gcloud projects add-iam-policy-binding $PROJECT --member="serviceAccount:$DEPLOYER_SA" --role="roles/run.developer" --condition=None
gcloud iam service-accounts add-iam-policy-binding $RUNTIME_SA --member="serviceAccount:$DEPLOYER_SA" --role="roles/iam.serviceAccountUser"
```

```powershell
gcloud iam workload-identity-pools create github --location=global --display-name="GitHub Actions"
gcloud iam workload-identity-pools providers create-oidc github --location=global --workload-identity-pool=github --issuer-uri="https://token.actions.githubusercontent.com" --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" --attribute-condition="assertion.repository=='Brandon-m-Y/Data_Eng_Cinci_API'"
gcloud iam service-accounts add-iam-policy-binding $DEPLOYER_SA --role="roles/iam.workloadIdentityUser" --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github/attribute.repository/Brandon-m-Y/Data_Eng_Cinci_API"
```

The `--attribute-condition` is the part that matters. Without it, any
repository on GitHub can request a token for your service account.

Then add these repository variables (Settings → Secrets and variables →
Actions → Variables — they are not secrets):

| Variable | Value |
|---|---|
| `GCP_PROJECT_ID` | `cincinnati-open-crash-data` |
| `GCP_WIF_PROVIDER` | `projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/github/providers/github` |
| `GCP_DEPLOYER_SA` | `crash-etl-deployer@cincinnati-open-crash-data.iam.gserviceaccount.com` |

---

## Appendix B — Tightening BigQuery access

Step 4 grants `bigquery.dataEditor` across the project. That is fine while the
project holds one dataset. To scope it to `crashes` only, drop the
project-level binding and grant it on the dataset instead:

```powershell
gcloud projects remove-iam-policy-binding $PROJECT --member="serviceAccount:$RUNTIME_SA" --role="roles/bigquery.dataEditor" --condition=None
bq show --format=prettyjson "${PROJECT}:crashes" > dataset.json
```

Add this to the `access` array in `dataset.json`, then apply it:

```json
{"role": "WRITER", "userByEmail": "crash-etl-runtime@cincinnati-open-crash-data.iam.gserviceaccount.com"}
```

```powershell
bq update --source=dataset.json "${PROJECT}:crashes"
Remove-Item dataset.json
```

Keep `bigquery.jobUser` at the project level — a dataset grant cannot confer
the right to start a job.
