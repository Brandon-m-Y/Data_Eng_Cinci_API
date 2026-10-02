"""One-command pipeline: Socrata crash feed -> BigQuery star schema.

Usage:
  python Run_Pipeline.py --setup            first time, and after pulling schema
                                            changes: dataset, lease, tables,
                                            migrations, seed dims, views
                                            (safe to rerun)
  python Run_Pipeline.py --delta            routine load: crashes within
                                            DELTA_LOOKBACK_DAYS (default 90) of
                                            the newest crash already loaded
  python Run_Pipeline.py --full             reload the whole history
  python Run_Pipeline.py --full --reprocess after changing the cleaning view or a
                                            derived column: reinstalls the view,
                                            updates existing dimension members
                                            in place and rewrites every crash
  python Run_Pipeline.py --setup --full     both in one go
  python Run_Pipeline.py --panel            rebuild only the ML panel
                                            (every load rebuilds it anyway)

Delta vs full:
  The feed has no usable change stamp (:updated_at is one value for every row,
  :version is random), so a delta windows on crash date. The watermark is read
  from the fact itself, so a failed run leaves nothing to reset. Socrata
  regenerates every :id on a republish, so the fact is replaced crash by
  crash on instanceid: only crashes whose content hash changed are rewritten,
  and crashes removed upstream within the window are deleted, all in one
  transaction. --full does the same over all history; run it regularly to
  catch amendments older than the window and crashes a delta deferred.
  A delta against an empty fact falls back to a full load.

One writer at a time:
  Every writer owns the non-expiring lock in etl_lease (SQL Section 10).
  Each write records its job ID and outcome. A second run fails fast. An
  interrupted run with an unknown job outcome keeps the lock. Stop that
  owner process, confirm its jobs are terminal, then recover its lock using
  the owner-specific procedure in README.md. Timestamps never permit takeover.

Load sequence:
  1. Get_Data.fetch()             - page the Socrata API; checked against the
                                    source's own row count and publish stamp
  2. Load_to_GBQ.load_staging()   - replace crashes.stg_crash_person
  3. SQL Section 3                - staging validation, before any change
  4. SQL Section 4                - dimension merges + key assertions
  5. SQL Section 5                - fact load and its etl_load_log row, in
                                    one transaction
  6. SQL Section 6                - sanity checks, printed below
  7. ML_Crash_Panel.sql           - rebuild crashes.ml_crash_panel (cell x
                                    day), gated by its ASSERTs
  8. SQL Section 9                - mark the load 'succeeded'
  If any step fails, SQL Section 11 marks the load 'failed' (keeping whether
  the fact had already committed) and the error is re-raised, so the process
  exits non-zero.
"""

import argparse
import os
import re
import signal
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

import Load_to_GBQ as gbq
from Get_Data import fetch
from Pipeline_Config import JOB_TIMEOUT_MINUTES, Config

load_dotenv()

SQL_FILE = os.getenv('SQL_FILE', 'Star_Schema_ETL.sql')
PANEL_SQL_FILE = os.getenv('PANEL_SQL_FILE', 'ML_Crash_Panel.sql')
LOOKBACK_DAYS = int(os.getenv('DELTA_LOOKBACK_DAYS', '90'))


def load_sql_sections(path=SQL_FILE, config=None):
    """Split a SQL file into {section_number: sql_text}.

    Splits at each '-- SECTION n:' marker line, keeping the marker with its
    chunk so every chunk starts on a comment line. With a config, every
    `crashes.` table reference is pointed at the configured dataset.
    """
    with open(path, encoding='utf-8') as f:
        text = f.read()
    if config:
        text = config.render(text)

    sections = {}
    for chunk in re.split(r'(?m)^(?=-- SECTION \d+:)', text):
        m = re.match(r'-- SECTION (\d+):', chunk)
        if m:
            sections[int(m.group(1))] = chunk
    return sections


def job_config(params=None, load_id=None):
    """Query settings for every job: parameters plus the job time limit."""
    return bigquery.QueryJobConfig(query_parameters=params or [],
                                   job_timeout_ms=JOB_TIMEOUT_MINUTES * 60 * 1000,
                                   labels={'load_id': load_id} if load_id else {})


def param(name, type_, value):
    return bigquery.ScalarQueryParameter(name, type_, value)


def section3_params(window_start, expected_rows, publish_stamp):
    """Staging validation: the window, and the manifest fetch() verified."""
    return [param('window_start', 'DATE', window_start),
            param('expected_rows', 'INT64', expected_rows),
            param('publish_stamp', 'TIMESTAMP', publish_stamp)]


def section5_params(window_start, reprocess, allow_deletions, load_id, load_mode,
                    started_at, rows_staged):
    """Fact load. load_id doubles as the lease fence inside the transaction."""
    return [param('window_start', 'DATE', window_start),
            param('reprocess', 'BOOL', reprocess),
            param('allow_deletions', 'BOOL', allow_deletions),
            param('load_id', 'STRING', load_id),
            param('load_mode', 'STRING', load_mode),
            param('started_at', 'TIMESTAMP', started_at),
            param('rows_staged', 'INT64', rows_staged)]


class LeaseError(RuntimeError):
    """Another run holds the ETL lease, or this run lost it."""


class Lease:
    """Non-expiring writer ownership, retaining ambiguous operations for recovery.

    Ownership ends only when this owner releases it or someone recovers it by
    name; no timestamp grants it to anyone else. renew() records a heartbeat
    and reserves the next job ID, both diagnostic -- neither extends a claim.
    """

    NEVER_SUBMITTED = 'never_submitted'
    UNKNOWN = 'unknown'
    TERMINAL = 'terminal'

    def __init__(self, client, config, load_id):
        self.client = client
        self.load_id = load_id
        self.table = f'`{config.table("etl_lease")}`'
        self.held = False
        self.job_id = None
        self.seq = 0
        self.outcome = self.NEVER_SUBMITTED
        self.last_job = None
        self.control_uncertain = False

    def _update(self, sql, *params):
        control_id = f'etl_{self.load_id}_control_{uuid.uuid4().hex}'
        job = None
        try:
            job = self.client.query(
                sql, job_id=control_id, job_retry=None,
                job_config=job_config([param('load_id', 'STRING', self.load_id), *params],
                                      self.load_id))
            job.result()
            return job.num_dml_affected_rows
        except BaseException:
            if job is None or job.state != 'DONE':
                self.control_uncertain = True
                print(f'Lock operation outcome unknown: {control_id}. Ownership requires recovery.')
            raise

    def acquire(self):
        # Only NULL is free. An arbitrarily old owner may still resume a write.
        try:
            taken = self._update(f"""
                UPDATE {self.table}
                SET holder = @load_id, acquired_at = CURRENT_TIMESTAMP(),
                    heartbeat_at = CURRENT_TIMESTAMP(), current_job_id = NULL
                WHERE lease_name = 'pipeline' AND holder IS NULL
                  AND (SELECT COUNT(*) FROM {self.table}) = 1
            """)
        except NotFound:
            # A dataset that has never been set up, not a lock we lost
            raise LeaseError('etl_lease does not exist in this dataset; '
                             'run Run_Pipeline.py --setup first.') from None
        if taken != 1:
            rows = list(self.client.query(f"""
                SELECT holder, acquired_at, current_job_id FROM {self.table}
                WHERE lease_name = 'pipeline'
            """, job_config=job_config()).result())
            if len(rows) != 1:
                raise LeaseError(f'etl_lease must hold exactly one pipeline row; found {len(rows)}.')
            row = rows[0]
            raise LeaseError(
                f'Load {row[0]} holds the ETL lease (non-expiring writer lock); '
                f'acquired_at={row[1]}, current_job_id={row[2]}. '
                'See README.md: A stuck writer lock. Never take over a live process.')
        self.held = True
        print(f'Writer lock acquired by load {self.load_id}.')

    def renew(self, stage):
        """Reserve the next job. Validation must finish before submission starts."""
        if not self.held or not self.resolve_outcome():
            raise LeaseError('Cannot start another write without known, exclusive ownership.')
        self.seq += 1
        job_id = f'etl_{self.load_id}_{self.seq:02d}_{stage}'
        renewed = self._update(f"""
            UPDATE {self.table}
            SET heartbeat_at = CURRENT_TIMESTAMP(), current_job_id = @job_id
            WHERE lease_name = 'pipeline' AND holder = @load_id
        """, param('job_id', 'STRING', job_id))
        if renewed != 1:
            self.held = False
            raise LeaseError('This load no longer holds the ETL lease; stopping before its next write.')
        self.job_id = job_id
        self.outcome = self.NEVER_SUBMITTED
        self.last_job = None
        return job_id

    def submitting(self):
        # Set BEFORE invoking the SDK: losing its response leaves an unknown job.
        self.outcome = self.UNKNOWN

    def terminal(self, job):
        if job.state != 'DONE':
            raise LeaseError('Cannot mark a nonterminal BigQuery job as complete.')
        self.last_job = job
        self.outcome = self.TERMINAL

    def resolve_outcome(self):
        if self.control_uncertain:
            return False
        if self.outcome != self.UNKNOWN:
            return True
        try:
            job = self.client.get_job(self.job_id)
            if job.state == 'DONE':
                self.terminal(job)
                return True
        except Exception:
            # NotFound is ambiguous too: a reserved request can arrive later.
            pass
        return False

    def release(self):
        if not self.held:
            return
        if not self.resolve_outcome():
            print(f'Writer lock retained: outcome of {self.job_id} is unknown or still running. '
                  'Follow the owner-specific recovery procedure in README.md.')
            return
        try:
            released = self._update(f"""
                UPDATE {self.table}
                SET holder = NULL, acquired_at = NULL, heartbeat_at = NULL, current_job_id = NULL
                WHERE lease_name = 'pipeline' AND holder = @load_id
            """)
            if released != 1:
                raise LeaseError('Writer lock release did not match exactly this owner.')
            self.held = False
        except Exception as exc:
            print(f'warning: writer lock release could not be confirmed ({exc}); inspect ownership.')


def run_section(client, lease, sections, n, label, params=None):
    """Run one writing section under the lease; params are ScalarQueryParameters."""
    print(f'-- Section {n}: {label}')
    # job_retry=None: a job id can't be reused, so a failed job isn't
    # resubmitted; retry the whole run after resolving any retained ownership.
    job_id = lease.renew(f'section_{n}')
    lease.submitting()
    job = client.query(sections[n], job_config=job_config(params, lease.load_id),
                       job_id=job_id, job_retry=None)
    job.result()
    lease.terminal(job)
    return job


def split_statements(sql):
    """Split a read-only script into statements, ignoring comments.

    Comments are removed before the split. A plain sql.split(';') cuts a
    comment that contains a semicolon in half and hands the second half to
    BigQuery as SQL. Quoted and backticked literals are tracked so a ';' or a
    '--' inside one is left alone.
    """
    out, stmt, quote, i = [], [], None, 0
    while i < len(sql):
        ch = sql[i]
        if quote:
            stmt.append(ch)
            if ch == '\\' and quote != '`' and i + 1 < len(sql):
                stmt.append(sql[i + 1])        # escaped char, never a closer
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in '\'"`':
            quote = ch
            stmt.append(ch)
        elif sql.startswith('--', i):
            end = sql.find('\n', i)            # drop through end of line
            if end == -1:
                break
            i = end
            stmt.append('\n')
        elif ch == ';':
            out.append(''.join(stmt))
            stmt = []
        else:
            stmt.append(ch)
        i += 1
    out.append(''.join(stmt))
    return [s for s in (part.strip() for part in out) if s]


def run_checks(client, sql):
    """Read-only SELECTs (Section 6, panel report): run each and print rows."""
    for stmt in split_statements(sql):
        for row in client.query(stmt, job_config=job_config()).result():
            print('  ', dict(row))


def delta_watermark(client, config):
    """First crash date a delta load pulls, or None if the fact is empty.

    Capped at today so a mistyped future crash date can't push the window
    past the present and stall every later delta.
    """
    sql = f"""
        SELECT DATE_SUB(MAX(crash_date), INTERVAL {LOOKBACK_DAYS} DAY) AS window_start
        FROM `{config.table('fact_crash_person')}`
        WHERE crash_date <= CURRENT_DATE()
    """
    return next(iter(client.query(sql, job_config=job_config()).result())).window_start


# BigQuery label grammar: lowercase letters, digits, dashes and underscores,
# up to 63 characters. Keys can't be empty; values can.
LABEL_KEY = re.compile(r'^[a-z0-9_-]{1,63}$')
LABEL_VALUE = re.compile(r'^[a-z0-9_-]{0,63}$')


def panel_label_sql(table, labels):
    """ALTER that stamps the panel with this load's id, as a tracked query job.

    A query job is used rather than tables.patch so the writer lock can resolve
    this write's outcome like every other one. OPTIONS accepts literals only --
    query parameters there fail with "unsupported function call 'ARRAY[...]'" --
    so the pairs are inlined, and every key and value is checked against the
    label grammar first. Nothing that matches it needs quoting.
    """
    bad = [f'{k}={v}' for k, v in labels.items()
           if not (LABEL_KEY.match(k) and LABEL_VALUE.match(str(v)))]
    if bad:
        raise ValueError(f'Not valid BigQuery labels, refusing to inline them: {bad}')
    pairs = ', '.join(f'("{k}", "{v}")' for k, v in labels.items())
    return f'ALTER TABLE `{table}` SET OPTIONS (labels=[{pairs}]);'


def build_panel(client, lease, config):
    """Rebuild crashes.ml_crash_panel and label it with this load's id. A
    failed ASSERT raises and leaves the previous panel in place."""
    sections = load_sql_sections(PANEL_SQL_FILE, config)
    run_section(client, lease, sections, 1, 'ML panel: external feature tables')
    run_section(client, lease, sections, 2, 'ML panel: build + assertions')
    panel = client.get_table(config.table('ml_crash_panel'))
    labels = {**panel.labels, 'load_id': lease.load_id}
    run_section(client, lease, {0: panel_label_sql(config.table('ml_crash_panel'), labels)},
                0, 'panel publication label')
    print('-- ML panel report')
    run_checks(client, sections[3])


def record_failure(client, lease, sections, error, **params):
    """Mark the load failed (SQL Section 11). Best effort: the original error
    is what the caller re-raises, whatever happens here."""
    # Google API errors carry the clean text in .message; str() adds the request URL
    text = getattr(error, 'message', None) or str(error)
    message = (text.strip().splitlines() or [type(error).__name__])[0][:1000]
    try:
        run_section(client, lease, sections, 11, 'record failed load', [
            param('load_id', 'STRING', params['load_id']),
            param('load_mode', 'STRING', params['load_mode']),
            param('window_start', 'DATE', params['window_start']),
            param('started_at', 'TIMESTAMP', params['started_at']),
            param('error_message', 'STRING', f"{params['stage']}: {message}"),
        ])
    except Exception as e:
        print(f'warning: could not record the failure in etl_load_log ({e})')


def run_load(client, config, lease, sections, args, started_at):
    window_start = None
    load_mode = 'reprocess' if args.reprocess else ('delta' if args.delta else 'full')
    stage = 'watermark'

    try:
        if args.delta:
            window_start = delta_watermark(client, config)
            if window_start is None:
                print('Fact table is empty; running a full load instead of a delta.')
                load_mode = 'full'

        stage = 'fetch'
        if window_start:
            print(f'Delta: fetching crashes on/after {window_start} '
                  f'({LOOKBACK_DAYS}-day lookback from the newest loaded crash) ...')
        else:
            print('Full: fetching entire history ...')
        df = fetch(since=window_start.isoformat() if window_start else None)
        if df.empty:
            # A 90-day window, let alone the full history, is never really empty
            raise RuntimeError('The API returned no rows; refusing to treat an empty extract as a load.')
        print(f'{len(df):,} person-rows across {df["instanceid"].nunique():,} crashes.')

        stage = 'staging load'
        rows_staged = gbq.load_staging(df, client, config, lease=lease)
        stage = 'section 3'
        manifest = df.attrs['snapshot']
        run_section(client, lease, sections, 3, 'staging validation', section3_params(
            window_start, manifest['expected_rows'],
            datetime.fromisoformat(manifest['publish_stamp'])))
        stage = 'section 4'
        run_section(client, lease, sections, 4, 'dimension merges',
                    [param('reprocess', 'BOOL', args.reprocess)])

        stage = 'section 5'
        # Section 5 is a script; its final SELECT returns the load counts
        fact = run_section(client, lease, sections, 5, 'fact load (crash-level replace)',
                           section5_params(window_start, args.reprocess, args.allow_deletions,
                                           lease.load_id, load_mode, started_at, rows_staged))
        counts = dict(next(iter(fact.result())))
        print(f"   crashes: {counts['crashes_new']} new, {counts['crashes_changed']} changed, "
              f"{counts['crashes_removed']} removed upstream, "
              f"{counts['crashes_deferred']} deferred to the next full load")
        print(f"   fact rows: {counts['rows_inserted']} inserted, {counts['rows_deleted']} deleted")

        stage = 'section 6'
        print('-- Section 6: sanity checks')
        run_checks(client, sections[6])

        stage = 'ML panel'
        build_panel(client, lease, config)

        stage = 'section 9'
        done = run_section(client, lease, sections, 9, 'mark load succeeded',
                           [param('load_id', 'STRING', lease.load_id)])
        if done.num_dml_affected_rows != 1:
            raise RuntimeError(f'Expected one etl_load_log row for load {lease.load_id} '
                               f'to mark succeeded, found {done.num_dml_affected_rows}.')
    except BaseException as e:   # also SIGTERM (see main) and Ctrl+C
        if lease.resolve_outcome() and lease.held:
            if stage == 'section 5' and lease.last_job is not None and lease.last_job.error_result is None:
                print('The fact job succeeded on the server; its committed audit row will be preserved.')
                # The fact is in; what failed is this run's grip on the response
                stage = 'section 5 (fact committed)'
            record_failure(client, lease, sections, e, load_id=lease.load_id, load_mode=load_mode,
                           window_start=window_start, started_at=started_at, stage=stage)
        else:
            print(f'Load {lease.load_id}: no failure-log mutation while the server outcome '
                  'or lock ownership is unresolved. Inspect the recorded job before recovery.')
        raise


def _terminated(signum, frame):
    raise SystemExit('Stopped by SIGTERM (Cloud Run task timeout or cancellation).')


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--setup', action='store_true',
                        help='create dataset, lease, tables, seed dimensions, and views (safe to rerun)')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--delta', action='store_true',
                      help=f'load crashes within {LOOKBACK_DAYS} days of the newest loaded crash')
    mode.add_argument('--full', action='store_true',
                      help='load the entire history')
    parser.add_argument('--reprocess', action='store_true',
                        help='with --full: apply changed cleaning logic to existing '
                             'dimension members and rewrite every crash (implies --setup)')
    parser.add_argument('--allow-deletions', action='store_true',
                        help='let a load delete more crashes than the cap (100, or 1%% of the '
                             'crashes it could delete) after confirming they are gone upstream')
    parser.add_argument('--panel', action='store_true',
                        help='rebuild the ML panel (also runs after every load)')
    args = parser.parse_args()

    if args.reprocess and not args.full:
        parser.error('--reprocess needs --full: only a full extract covers every crash')
    if args.allow_deletions and not (args.delta or args.full):
        parser.error('--allow-deletions only applies to a --delta or --full load')
    if not (args.setup or args.delta or args.full or args.panel):
        parser.error('nothing to do: pass --setup, --delta, --full, or --panel')
    setup = args.setup or args.reprocess   # --reprocess must install the new view first

    config = Config.from_env()
    client = gbq.get_client(config)
    sections = load_sql_sections(SQL_FILE, config)
    started_at = datetime.now(timezone.utc)

    # Cloud Run stops a timed-out task with SIGTERM. Turn it into an exception
    # so the failure is logged and the lease released (or kept, if a job is
    # still running) before the SIGKILL that follows.
    signal.signal(signal.SIGTERM, _terminated)

    if setup:
        dataset = bigquery.Dataset(f'{config.project}.{config.dataset}')
        dataset.location = config.location
        client.create_dataset(dataset, exists_ok=True)
        print(f'Dataset {config.dataset} ready in {config.location}.')
        # Only creates and seeds the lease table, so it's safe before the lease
        print('-- Section 10: ETL lease table')
        client.query(sections[10], job_config=job_config()).result()

    lease = Lease(client, config, uuid.uuid4().hex)
    lease.acquire()
    try:
        if setup:
            run_section(client, lease, sections, 1, 'table DDL, migrations, cleaning view')
            run_section(client, lease, sections, 2, 'seed dimensions')
            run_section(client, lease, sections, 7, 'role-playing date views')
        if args.delta or args.full:
            run_load(client, config, lease, sections, args, started_at)
        elif args.panel:
            build_panel(client, lease, config)
    finally:
        lease.release()
    print('Done.')


if __name__ == '__main__':
    main()
