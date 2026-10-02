**Remediation status ? 2026-09-19**

The findings below describe the original baseline, not the current working tree. Subsequent changes add verified extraction manifests, serialized writer ownership, a single fact/hash input snapshot, deletion guards, configurable datasets, explicit dimension/fact reprocessing, and tracked load outcomes. Existing MD5 keys remain compatible; reserved key delimiters are rejected rather than silently producing ambiguous keys. MD5 is used for change detection, not security.

Claude approved replacing expiring leases with non-expiring ownership. Unknown BigQuery outcomes retain ownership and suppress competing failure-log writes. Recovery requires stopping the owner, confirming its jobs have finished, and clearing only that owner's lock. The README documents this availability tradeoff and required monitoring.

Local regression tests cover extraction, hashing contracts, configuration, and uncertain writer outcomes. The BigQuery integration scenarios have since been executed against a clone of production; see "Verification" at the end of each round. Deployment and monitoring remain unverified because neither exists yet.

---

**Project audit — 2026-09-19**

Reviewed the Python extractor, staging loader and runner; both SQL files; the notebook including saved outputs; Docker configuration; dependency manifests; ignore rules; README, TODO and panel specification. Baseline commit: `bacc1e4`. The working tree was clean before this audit.

The crash-level replacement design is appropriate for the documented source behavior. Its current safeguards do not establish extract completeness, protect overlapping runs, or refresh derived data after transformation changes. Resolve findings 1–4 before relying on unattended refreshes or using configuration to run against a separate dataset.

This is a source and local-behavior audit. No production queries, pipeline loads, deployments, credential changes or data mutations were performed. Live source contents, deployed schema, IAM, job history and the README's previous production test results were not independently verified. Conditional failure cases below are not claims that production is currently corrupted.

**1. High — overlapping executions can commit rows with the wrong hash.**

References: `Load_to_GBQ.py:68–88`; `Star_Schema_ETL.sql:907–970,1009–1020,1035–1049`; `Run_Pipeline.py:174–179`.

Every execution truncates the same staging table. Section 5 snapshots raw rows and computes hashes before starting its transaction, but the insert reads the cleaning view over live staging again. For example, run A hashes a person aged 30; run B replaces staging with that same person's amended age of 31; run A starts its transaction, inserts age 31 and attaches the hash for age 30. Counts and stored hashes match A's temporary tables, so the assertion passes. This requires no overlapping fact transactions. A local equivalence model reproduced the assertion accepting different row content.

The dimension merges independently allocate `MAX(key) + ROW_NUMBER()`. Concurrent insert-only merges can assign duplicate surrogate keys or duplicate natural keys. BigQuery documents that insert-only MERGEs need not conflict, and that declared keys are not enforced. See [DML concurrency](https://docs.cloud.google.com/bigquery/docs/data-manipulation-language#dml_statement_conflicts) and [key constraints](https://docs.cloud.google.com/bigquery/docs/primary-foreign-keys).

Recommendation: enforce a shared execution lease across full, delta and manual runs; use immutable staging identified by run ID; derive hashes, dimensions and fact rows from that same validated input. Calculate fact comparisons within the transaction that mutates the fact. A transaction protects only its own scope; see [BigQuery transactions](https://docs.cloud.google.com/bigquery/docs/transactions). The existing README warning about simultaneous runs acknowledges the risk but does not enforce exclusion.

**2. High — the completeness guard allows destructive partial loads.**

References: `Get_Data.py:61–74`; `Star_Schema_ETL.sql:907–916,958–967,1023–1049`.

The 90% check counts crashes, not people, and compares only against the old fact. A partial extract containing 95 of 100 existing crashes passes and deletes the missing five. More severely, an extract retaining one person from each of 100 two-person crashes passes the crash-count guard, replaces all crashes, and loses 100 person rows. The final assertion passes because it treats incomplete staging as the truth. These conditions were reproduced with synthetic input. New crashes can also offset missing old crashes in the aggregate count.

The single-publish assertion is useful but insufficient: `COUNT(DISTINCT stamp) <= 1` accepts entirely NULL stamps. Rows with missing IDs are silently removed before hashing, and conflicting duplicate source IDs are reduced using a version value the project itself describes as unordered. None establishes a complete extraction.

Recommendation: validate source schema and required IDs, reject conflicting duplicates and missing publish stamps, and reconcile fetched unique row counts with source-side counts for the same filter and verified unchanged publication. Detect publication changes before and after extraction and retry the whole snapshot. Retain a deletion threshold as an additional anomaly alarm, not proof of completeness. Run validation before dimension mutations.

**3. High — cleaning changes leave existing dimensions stale; the documented recovery is incomplete.**

References: `Star_Schema_ETL.sql:539–543,695–696,759–778,839–855,870–877`; `README.md:590–592`.

The fact hash describes raw source content. Updating a CASE expression does not change it, so even `--setup --full` skips unchanged facts. The SQL and README acknowledge that limitation and recommend truncating the fact, but existing dimension members are also insert-only and selected by their raw natural keys. Rebuilding the fact therefore still joins old dimension labels and ranks. A changed injury rule can produce newly calculated fact measures that disagree with the old person-profile dimension.

Recommendation: version transformation logic separately from source content, support explicit reprocessing, and migrate existing derived dimension attributes while retaining their surrogate keys where appropriate. Recalculate affected fact measures as part of the same controlled migration. Avoid making a standalone destructive truncate the routine recovery mechanism: a later extraction failure would leave the fact empty.

**4. High — configured dataset and staging table do not control the SQL destination.**

References: `Load_to_GBQ.py:17–20,75`; `Run_Pipeline.py:101–104,141–147`; `Star_Schema_ETL.sql:56,978`; `ML_Crash_Panel.sql:316`.

Python honors `GCP_DATASET` and `GCP_STAGING_TABLE`; both SQL files hardcode `crashes` and `stg_crash_person`. Setting a test dataset creates/loads that dataset and reads its watermark, while downstream SQL still reads and mutates `crashes` in the configured project. If the old staging table exists, this may use stale data rather than simply fail. This prevents safe isolation through the advertised configuration.

Recommendation: render validated, fully qualified identifiers from one configuration across both SQL files and the Python loader. Alternatively, reject any unsupported dataset/table configuration before creating a client or issuing writes. SQL value parameters cannot substitute for table identifiers.

**5. Medium — dimension natural keys have ambiguous serialization.**

References: `Star_Schema_ETL.sql:542–564`.

The three readable natural keys use pipe-delimited values with `~` for NULL. Location hashes the same encoding. Tuples `('A|B', 'C')` and `('A', 'B|C')` serialize identically; so do `(NULL, 'C')` and `('~', 'C')`. Padding either example with identical remaining fields preserves the collision for the actual key shapes. This was reproduced locally. These are encoding collisions before hashing; replacing MD5 with SHA-256 alone cannot fix them. Occurrence in the live feed was not checked.

If colliding tuples arrive together, `SELECT DISTINCT` can retain different attribute rows with the same natural key, creating multiple members. Subsequent fact joins can choose one arbitrarily because their fan-out is reduced by `QUALIFY`.

Recommendation: serialize an explicitly ordered, named STRUCT using `TO_JSON_STRING`, preserving NULL separately from strings. Hash that representation for location if a compact key is useful. Check natural-key uniqueness before publishing dimensions and migrate existing keys and fact references together.

**6. Medium — the stored crash hash is not a check of actual fact content.**

References: `Star_Schema_ETL.sql:942–956,1035–1049`.

Both change detection and the post-load assertion compare stored hash values and row counts. Changing an existing fact's age, injury measure or foreign key while preserving its hash and count is invisible. Likewise, replacing one person with a duplicate of another preserves the count. A full load will not repair these cases. The overlap example in finding 1 exploits this same limitation.

The code does repair missing/extra rows and missing/inconsistent hash metadata. Its stronger comment that every crash has exactly its staged rows is not established by those checks.

Recommendation: retain the source hash for change detection, but add expected-versus-actual transformed-row reconciliation for integrity checks or provide a force-rebuild mode with validated dimensions. Keep source identity, transformation version and output integrity as separate concepts.

**7. Medium — failed runs can leave committed data and a misleadingly finished audit record.**

References: `Run_Pipeline.py:174–199`; `Star_Schema_ETL.sql:1051,1189–1196`; `README.md:603–607`.

Staging and dimensions commit before the fact transaction. Sanity queries, audit insertion and panel publication run afterward. If the panel assertion fails, the fact and load-log row have already committed while the old panel remains published. If the audit insertion fails, fact changes have no corresponding load-log record. Earlier failures have no record at all. The README statement that a failed load changes nothing is therefore too broad.

Recommendation: create a run ID at startup, record stage/status/error information, include the fact-commit record in its transaction, and mark overall success only after the panel publishes. Record which run produced the panel. Update the operational description to distinguish fact rollback from whole-pipeline failure.

**8. Medium — an empty extraction exits successfully without an audit record.**

Reference: `Run_Pipeline.py:168–171`.

Both modes return normally for an empty DataFrame. A mocked full extraction reproduced a successful return with no staging load, audit entry or panel refresh. An upstream empty response can therefore appear healthy to monitoring based on exit status. A legitimately emptied reconciliation window would also remain unreconciled.

Recommendation: treat unexpected empty history/window responses as explicit failures, or establish verified source emptiness before choosing an audited no-op or reconciliation action. Do not infer successful freshness from a normal exit alone.

**9. Medium — critical dimension integrity checks only print results.**

References: `Run_Pipeline.py:82–93,185–186`; `Star_Schema_ETL.sql:1015–1020,1069–1094`.

Unknown dimension lookups and duplicate natural keys are reported after fact commit without raising errors. Surrogate-key uniqueness and full referential integrity are not asserted. The fact insert's `QUALIFY` can hide a join fan-out, letting row-count validation pass with an arbitrary dimension assignment.

Recommendation: assert unique natural and surrogate keys and required dimension coverage before inserting facts; reserve source-row deduplication for validated raw staging, before joins. Check external weather and cell-attribute keys before panel joins too. BigQuery warns that queries can return incorrect results when declared but unenforced constraints are violated; see [key constraints](https://docs.cloud.google.com/bigquery/docs/primary-foreign-keys).

**10. Medium for forecasting — backward-looking event dates do not guarantee feature availability.**

References: `ML_Crash_Panel.sql:34–39,209–212,222–223,256–257,272–300`.

The SQL correctly excludes the target day and partitions its lags by cell. However, rolling counts through day D-1 use the eventually reported/revised history, which may not have been available when predicting D. The seven-day buffer makes training targets more mature; it does not reconstruct what was known at each historical prediction time. If the planned NOAA join is used for forecasting, observations for target day D likewise are not known before that day occurs. Weather is currently only a schema placeholder.

Recommendation: define prediction time and horizon before training. Build features from as-of snapshots or use an explicitly justified availability lag, evaluate with rolling prediction origins, and use weather forecasts available at issue time. The existing assertion proves event-time window correctness, not operational availability. This limitation does not invalidate the panel as a retrospective analytical table.

**11. Medium, specification only — the pandas rolling example crosses neighborhoods.**

Reference: `crash-panel-spec.md:105–111`.

`g.shift(1)` returns a Series; the subsequent `.rolling(...)` is no longer grouped. With 40 A rows of count 10 followed by 40 B rows of count 100, the first B row receives a rolling mean of 10 instead of NULL. This was reproduced using the installed pandas. The production SQL correctly uses `PARTITION BY` and is not affected.

Recommendation: replace the example with a grouped transform such as `groupby('cell_id')['crashes'].transform(lambda s: s.shift(1).rolling(28, min_periods=14).mean())`, after sorting by cell and day. Keep the longer rolling window grouped in the same way.

**Additional observations**

- `Star_Schema_ETL.sql:342–348`: the `LIGHTIED` spelling correction is applied only to the not-lighted branch. `DARK - LIGHTIED ROADWAY` reaches `Dark - Unknown Lighting`. Normalize the label once before all lighting branches; live frequency was not measured.
- Delta date corrections across the window boundary can delete a crash until the next full load, as the SQL already documents at lines 885–887. Also verify that every crash's person rows share a crash date before relying on date-filtered extraction to return a complete crash. Make periodic full reconciliation part of the eventual deployed schedule.
- `Get_Data.py` checks HTTP status and uses a timeout, but has no bounded retry/backoff, response-shape validation or positive `page_size` validation. Extraction failures currently happen before staging, which is useful. Memory still grows with the full extract and its pandas/Arrow conversions; the README's measurements were not rerun.
- The notebook uses a separate GET-based extractor with no timeout or status check instead of `fetch()`. It can drift from production behavior. Socrata's current [SODA3 documentation](https://dev.socrata.com/docs/queries/) specifies POST queries with a page object, matching the production extractor. The notebook is exploratory and is not invoked by the pipeline.
- The panel assigns conflicting per-person dates/neighborhoods using independent MAX aggregates. The comments cite a previous live consistency measurement; add an assertion if that assumption is needed for exact crash placement.
- The repository contains no automated regression suite or CI workflow. The README describes valuable historical integration tests, but their fixtures and execution scripts are absent. Deployment automation and model training are explicitly unfinished rather than silently missing from the documented implementation.
- Runtime dependencies are pinned and match the local environment; `pip check` passes. Transitive dependencies and the `python:3.13-slim` image are not locked by artifact hash/digest. No vulnerability database scan or fresh container build was performed.
- `.env` and the service-account JSON are ignored and absent from tracked files; Docker excludes them and copies an explicit application-file list. No matches were found in tracked files for the selected private-key, Google API-key and GitHub-token patterns. This limited scan is not proof that every credential format is absent. The key file is present in the OneDrive workspace, as the README documents; move local credentials outside synced project storage as part of deployment cleanup. No key contents were printed or changed.
- The Docker image uses a non-root user and separates runtime from notebook dependencies. Documentation is unusually explicit about source grain, coding eras and known delta limitations, but the atomicity and repair claims need the qualifications above.

**Assessment of the current hashing setup**

There are two distinct uses of MD5: the location natural key and the crash change detector. Other dimension natural keys are readable serialized tuples. Surrogate dimension keys are integers allocated from existing maxima.

The crash digest uses explicit named JSON fields, sorts serialized rows, preserves repeated identical person rows, and excludes all four volatile Socrata system fields. Those choices make it insensitive to row order and regenerated IDs while detecting raw content and multiplicity changes. NULLs and strings remain distinct in its JSON serialization. The location digest does not share that serialization safety.

MD5's cryptographic weaknesses are documented by [BigQuery](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#md5). For non-adversarial change detection at this project's scale, accidental digest collision is a much smaller concern than the concrete serialization, snapshot and invalidation issues above. SHA-256 is a reasonable future choice, but switching algorithms alone does not repair those issues. Neither hash verifies complete extraction or correct transformation.

Recommended sequence: first isolate and validate each input snapshot; then fix dimension serialization and integrity assertions; then introduce a transformation version and controlled reprocessing; finally migrate digest algorithms if desired. Preserve dimension surrogate keys through the migration or rebuild all dependent references deliberately. A crash-hash algorithm change makes existing crashes compare as changed only when they are next staged, so use a validated full reprocess to complete that migration across history.

**Validation performed**

- Parsed all three Python modules successfully with `ast.parse`.
- Confirmed installed runtime versions match requirements and `python -m pip check` reports no broken requirements.
- Ran 15 local checks covering mocked pagination, filtering, invalid-date rejection, person-grain preservation, NULL-safe staging, staging DDL alignment, SQL section splitting, hash invariants and selected failure reproductions. All assertions matched the described behavior; some intentionally confirm defects, so this is not a claim that the pipeline passes a correctness suite.
- Ran additional synthetic models showing staging overwrite can pass the hash/count assertion, 50% person loss can pass both completeness checks, and an insert-only dimension retains its old mapping after a fact rebuild.
- Confirmed the grouped-rolling specification bug with real pandas and the empty-extract behavior with the actual runner under mocks.
- Inspected tracked files and ignore rules with no credential contents emitted.

The hashing and transaction models are local equivalents of the relevant logic, not execution of GoogleSQL. SQL syntax/engine behavior, current data distributions, collision occurrence and concurrency timing still require integration checks in an isolated BigQuery dataset after the configuration issue is fixed. Application source and production state were left unchanged; this report is the audit artifact.

**Resolution — 2026-09-19 (Claude, with the final say; ChatGPT implemented the extractor and panel changes)**

Verified in production first (read-only): every crash in the fact had exactly one non-NULL hash, one load time, one publish and one crash date; recomputing the hash from staging matched 3,319 of 3,319 crashes; the API returns every value as a string; no natural-key value in any dimension contains `|` or `~`; `LIGHTIED` occurs only as `DARK – ROADWAY NOT LIGHTIED`, already classified correctly. The fixes below were then tested end to end on a clone of the production dataset (`tests/integration_bigquery.py`).

| Finding | Outcome |
|---|---|
| 1. Overlapping executions | **Fixed.** One-row writer lock (`etl_lease`, SQL Section 10): conditional-UPDATE acquire on `holder IS NULL`, stamped with the id of each write job before it is submitted. The lock never expires and is never taken over automatically, because no elapsed time proves a paused process won't wake and submit its reserved job; an abandoned lock is cleared by an operator, by holder name, after that process is stopped and its jobs confirmed terminal. The fact transaction rechecks ownership before committing. Section 5 reads staging once, so the hash and the inserted rows come from the same snapshot. Tested: three simultaneous acquires, one winner, three times. Per-run staging tables were not adopted (the lease already serializes every writer). |
| 2. Completeness guard | **Fixed.** `fetch()` matches the source's row count, publish stamp and schema before and after paging, restarts on a republish, and rejects repeated or missing ids. Section 3 binds staging to that manifest, rejects NULL ids and conflicting duplicates, and checks both crashes and people against the window before any change. Section 5 caps deletions at max(100, 1% of the window) unless `--allow-deletions`. |
| 3. Cleaning changes | **Fixed.** `--full --reprocess` reinstalls the view, updates existing members' derived attributes in place (surrogate keys kept) and rewrites every crash in one transaction. The TRUNCATE advice is gone. A transformation-version column was not adopted. |
| 4. Dataset configuration | **Fixed.** `crashes.` in both SQL files is rewritten to the validated `GCP_DATASET`; the staging table name is fixed and a different `GCP_STAGING_TABLE` is rejected. |
| 5. Key serialization | **Contained.** Section 3 fails any load that would bring `\|` or `~` into a natural key; none exists in the history. Migrating every key was not worth it for zero occurrences. |
| 6. Stored hash vs content | **Partly.** With one staging snapshot the inserted rows always match their hash, and `--reprocess` is the forced rebuild. Independent per-row reconciliation was not adopted. |
| 7. Failed runs and the log | **Fixed.** The log row is written inside the fact transaction (`fact_committed`), marked `succeeded` after the panel, or `failed` with the error and `fact_committed_at` kept. The panel carries the load id as a label. |
| 8. Empty extraction | **Fixed.** `fetch()` and the runner both raise. |
| 9. Printed-only integrity checks | **Fixed.** Section 4 asserts unique natural and surrogate keys; Section 5 asserts no Unknown lookups for the four non-date dimensions and one hash per crash, and the insert's QUALIFY is gone so a fan-out fails the row-count check. |
| 10. Feature availability | **Deferred** to TODO: needs a prediction time and horizon, a modeling decision. Documented in the panel SQL, spec and README. |
| 11. Spec pandas example | **Fixed.** Grouped transform. |
| Additional | LIGHTIED normalized once (no data changes). Deltas defer crashes straddling the window (`crashes_deferred`). Extraction retries with backoff. Panel report counts crashes whose rows disagree on day or cell. Offline tests plus the BigQuery integration test. `constraints.txt` and a digest-pinned base image. Credential relocation remains a user action (README, TODO). |

---

**Second round — 2026-09-19, evening (three reviewers: Claude with the final say, ChatGPT/Codex, and a read-only Claude reviewer)**

The first round's expiring lease did not survive review. Two reviewers, working
separately, found the same defect: a run reserves a job id, records it in the
lock row and then submits the job as a second step. A process paused between
those two statements is indistinguishable from a dead one, so a takeover gated
on "the recorded job is finished or was never submitted" can hand the lock to a
second run while the first is still able to submit its write. A related hole sat
next to it: the takeover's wait for the previous job swallowed every exception,
so failing to *observe* a job counted as proof it had stopped.

No timestamp can close that, because no elapsed time proves a process is dead.
The lock is now non-expiring and is never taken over automatically. Acquire
matches `holder IS NULL` only; an abandoned lock is cleared by an operator, by
holder name, after that owner is stopped and every one of its jobs is confirmed
terminal. A run that cannot resolve its own last write keeps the lock and
writes nothing to the log, so an ambiguous outcome can never be recorded as a
definite one. The cost is availability: a killed container blocks the next load
until someone intervenes. For a daily feed with a loud failure, that is the
right trade, and it is documented with a required "lock held over 2 hours"
alert. Automatic self-healing has a known route — the taker submits a no-op job
under the dead run's reserved id, so the sleeper's late submit hits a
job-id conflict — and it is deferred in TODO.md because it does not cover
writes that carry no job id.

Three defects were found by running the pipeline, not by reading it:

| Defect | Why it mattered | Outcome |
|---|---|---|
| Panel label written as `SET OPTIONS (labels=[(@key, @value)])` | `OPTIONS` is evaluated at parse time and never sees query parameters. Every load would have failed after the fact committed. The change itself was right — a query job has an outcome the lock can resolve, which `tables.patch` never did. | **Fixed.** `panel_label_sql()` inlines the pairs as literals after checking each against BigQuery's label grammar, and refuses anything else. Four offline tests, including that the statement contains no `@`. |
| `ALTER TABLE … ADD COLUMN IF NOT EXISTS` on `etl_lease` in Section 10 | It consumes a per-table metadata quota unit even when the column already exists, so several `--setup` runs close together failed with `Exceeded rate limits: too many table update operations`. | **Fixed.** Guarded by an `INFORMATION_SCHEMA.COLUMNS` check. Section 1's four `etl_load_log` migrations were left as they are: one setup's worth stays inside the limit. |
| Deletion cap measured against `COUNTIF(in_window)` | That population includes crashes straddling the window start, which Section 5 defers and can never delete, so the cap sat above what a load could reach: with 100,000 in-window crashes and 10,000 deletion-eligible, all 10,000 could go. | **Fixed.** The denominator is `COUNTIF(in_window AND NOT before_window)`, the deletion-eligible population. |

Three further fixes came from the read-only review and were applied: a missing
`etl_lease` now raises "run `--setup` first" instead of a raw `NotFound`;
Section 11 no longer overwrites a `succeeded` row, so a lost response on the
final status UPDATE cannot turn a finished load into a failed one; and the
recovery runbook now closes the stale `fact_committed` audit row an abandoned
owner leaves behind, which would otherwise keep the staleness alert quiet.

**Accepted limitations, recorded rather than fixed**

- Section 3 binds staging to the extract by row count and publish stamp. A
  stale table with the same count and stamp but one row duplicated and another
  missing would pass. Reaching that state needs an operator to clear the lock
  without stopping the owning process, which the runbook forbids in that order;
  a content checksum would have to be reproduced in SQL exactly, which is the
  fragility the `row_json` hash design exists to avoid.
- An interrupted upload to staging (a local Arrow or memory error, a network
  blip) retains the lock even though nothing was submitted. Loud and
  recoverable, and narrowing it would mean trusting client-side evidence about
  what the server did.

**Verification of the second round**

51 offline tests pass. The BigQuery integration suite passes 56 of 56 checks
against a fresh clone of production (`crashes_it_…`, deleted afterwards;
production was only read). It covers setup and the log migrations, a no-op
delta proving every stored `crash_hash` still matches what the cleaning view
computes, five lock scenarios (a held lock blocks a second run; a 30-day-old
holder with a finished job is still not taken over; recovery aimed at another
holder frees nothing; a new owner acquires only after explicit recovery; three
simultaneous acquires produce one winner, three times), five Section 3
rejections plus the accepted page-overlap duplicate, Section 4's duplicate-key
assert, the Section 5 repair, deferral, fence, deletion cap (101 refused, then
allowed with `--allow-deletions`) and two rollbacks, a panel failure logged
with its fact commit preserved, a late Section 11 write leaving a succeeded
load alone, `--full --reprocess` rewriting all 221,289 crashes with the row
count unchanged and the next delta a no-op, and three racing bootstraps leaving
exactly one lock row.

**Third round — 2026-10-01, coordinates**

Two defects found by running the pipeline rather than reading it:

- **Fuzzed coordinates in the content hash.** The city re-randomizes every
  row's latitude and longitude on each publish, so hashing them reported all
  3,317 crashes in the delta window as changed when 3 had actually changed.
  Every republish rewrote the whole window and `crashes_changed` meant
  nothing. The coordinates were removed from `row_json` and all 221,289
  crashes re-stamped. The regression test perturbs every staged coordinate
  and asserts the load rewrites nothing.
- **Half coordinates.** The Hamilton County bounds check was applied to each
  axis independently, leaving 176 rows with one coordinate and not the other
  — useless for any spatial work, yet present to any code testing a single
  axis. It was this that made a distance assertion fail. Both axes are now
  nulled together, which cost no usable point (432,855 before and after).

Also added in this round: `fact_crash_person.distance_to_cbd_m` and the
panel's per-cell `distance_to_cbd_km`, both anchored on Fountain Square and
cross-checked against an independent Haversine to within 0.5 m.
