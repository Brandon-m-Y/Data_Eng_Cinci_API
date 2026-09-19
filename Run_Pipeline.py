"""One-command pipeline: Socrata crash feed -> BigQuery star schema.

Usage:
  python Run_Pipeline.py --setup            first time only: dataset, tables,
                                            seed dims, views (safe to rerun)
  python Run_Pipeline.py --delta            routine load: crashes within
                                            DELTA_LOOKBACK_DAYS (default 90) of
                                            the newest crash already loaded
  python Run_Pipeline.py --full             reload the whole history
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
  transaction. --full does the same over all history; run it now and then to
  catch amendments older than the window. A delta against an empty fact falls
  back to a full load.

Load sequence:
  1. Get_Data.fetch()             - page the Socrata API (raw strings)
  2. Load_to_GBQ.load_staging()   - replace crashes.stg_crash_person
     (WRITE_TRUNCATE stands in for SQL Section 3)
  3. SQL Section 4                - dimension merges (idempotent)
  4. SQL Section 5                - fact load: replace changed crashes, delete
                                    crashes removed upstream (one transaction)
  5. SQL Section 6                - sanity checks, printed below
  6. SQL Section 9                - append a row to crashes.etl_load_log
  7. ML_Crash_Panel.sql           - rebuild crashes.ml_crash_panel (cell x
                                    day), gated by its ASSERTs
"""

import argparse
import os
import re
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

import Load_to_GBQ as gbq
from Get_Data import fetch

load_dotenv()

SQL_FILE = os.getenv('SQL_FILE', 'Star_Schema_ETL.sql')
PANEL_SQL_FILE = os.getenv('PANEL_SQL_FILE', 'ML_Crash_Panel.sql')
LOOKBACK_DAYS = int(os.getenv('DELTA_LOOKBACK_DAYS', '90'))


def load_sql_sections(path=SQL_FILE):
    """Split a SQL file into {section_number: sql_text}.

    Splits at each '-- SECTION n:' marker line, keeping the marker
    with its chunk so every chunk starts on a comment line.
    """
    with open(path, encoding='utf-8') as f:
        text = f.read()

    sections = {}
    for chunk in re.split(r'(?m)^(?=-- SECTION \d+:)', text):
        m = re.match(r'-- SECTION (\d+):', chunk)
        if m:
            sections[int(m.group(1))] = chunk
    return sections


def run_section(client, sections, n, label, params=None):
    """Run one section; params is a list of bigquery.ScalarQueryParameter."""
    print(f'-- Section {n}: {label}')
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    job = client.query(sections[n], job_config=job_config)
    job.result()
    return job


def run_checks(client, sql):
    """Section 6 is several standalone SELECTs; run each and print rows."""
    for stmt in sql.split(';'):
        # Skip fragments that are only comments/whitespace
        body = '\n'.join(
            line for line in stmt.splitlines()
            if line.strip() and not line.strip().startswith('--')
        )
        if not body.strip():
            continue
        for row in client.query(stmt).result():
            print('  ', dict(row))


def delta_watermark(client):
    """First crash date a delta load pulls, or None if the fact is empty.

    Capped at today so a mistyped future crash date can't push the window
    past the present and stall every later delta.
    """
    sql = f"""
        SELECT DATE_SUB(MAX(crash_date), INTERVAL {LOOKBACK_DAYS} DAY) AS window_start
        FROM {gbq.DATASET}.fact_crash_person
        WHERE crash_date <= CURRENT_DATE()
    """
    return next(iter(client.query(sql).result())).window_start


def build_panel(client):
    """Rebuild crashes.ml_crash_panel. A failed ASSERT raises and leaves the
    previous panel in place."""
    sections = load_sql_sections(PANEL_SQL_FILE)
    run_section(client, sections, 1, 'ML panel: external feature tables')
    run_section(client, sections, 2, 'ML panel: build + assertions')
    print('-- ML panel report')
    run_checks(client, sections[3])


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--setup', action='store_true',
                        help='create dataset, tables, seed dimensions, and views (safe to rerun)')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--delta', action='store_true',
                      help=f'load crashes within {LOOKBACK_DAYS} days of the newest loaded crash')
    mode.add_argument('--full', action='store_true',
                      help='load the entire history')
    parser.add_argument('--panel', action='store_true',
                        help='rebuild the ML panel (also runs after every load)')
    args = parser.parse_args()

    if not (args.setup or args.delta or args.full or args.panel):
        parser.error('nothing to do: pass --setup, --delta, --full, or --panel')

    client = gbq.get_client()
    sections = load_sql_sections()

    if args.setup:
        dataset = bigquery.Dataset(f'{gbq.PROJECT_ID}.{gbq.DATASET}')
        dataset.location = gbq.LOCATION
        client.create_dataset(dataset, exists_ok=True)
        print(f'Dataset {gbq.DATASET} ready in {gbq.LOCATION}.')
        run_section(client, sections, 1, 'table DDL + cleaning view')
        run_section(client, sections, 2, 'seed dimensions')
        run_section(client, sections, 7, 'role-playing date views')

    if not (args.delta or args.full):
        if args.panel:
            build_panel(client)
        print('Done.')
        return

    started_at = datetime.now(timezone.utc)
    window_start = None
    if args.delta:
        window_start = delta_watermark(client)
        if window_start is None:
            print('Fact table is empty; running a full load instead of a delta.')
    load_mode = 'delta' if window_start else 'full'

    if window_start:
        print(f'Delta: fetching crashes on/after {window_start} '
              f'({LOOKBACK_DAYS}-day lookback from the newest loaded crash) ...')
    else:
        print('Full: fetching entire history ...')
    df = fetch(since=window_start.isoformat() if window_start else None)
    if df.empty:
        print('No rows returned; nothing to load.')
        return
    print(f'{len(df):,} person-rows across {df["instanceid"].nunique():,} crashes.')

    rows_staged = gbq.load_staging(df, client)
    run_section(client, sections, 4, 'dimension merges')

    window_param = bigquery.ScalarQueryParameter('window_start', 'DATE', window_start)
    # Section 5 is a script; its final SELECT returns the load counts
    fact = run_section(client, sections, 5, 'fact load (crash-level replace)', [window_param])
    counts = dict(next(iter(fact.result())))
    print(f"   crashes: {counts['crashes_new']} new, {counts['crashes_changed']} changed, "
          f"{counts['crashes_removed']} removed upstream")
    print(f"   fact rows: {counts['rows_inserted']} inserted, {counts['rows_deleted']} deleted")

    print('-- Section 6: sanity checks')
    run_checks(client, sections[6])

    run_section(client, sections, 9, 'ETL load log', [
        bigquery.ScalarQueryParameter('load_mode', 'STRING', load_mode),
        window_param,
        bigquery.ScalarQueryParameter('started_at', 'TIMESTAMP', started_at),
        bigquery.ScalarQueryParameter('rows_staged', 'INT64', rows_staged),
        bigquery.ScalarQueryParameter('fact_inserted', 'INT64', counts['rows_inserted']),
        bigquery.ScalarQueryParameter('fact_deleted', 'INT64', counts['rows_deleted']),
        bigquery.ScalarQueryParameter('crashes_new', 'INT64', counts['crashes_new']),
        bigquery.ScalarQueryParameter('crashes_changed', 'INT64', counts['crashes_changed']),
        bigquery.ScalarQueryParameter('crashes_removed', 'INT64', counts['crashes_removed']),
    ])
    build_panel(client)
    print('Done.')


if __name__ == '__main__':
    main()
