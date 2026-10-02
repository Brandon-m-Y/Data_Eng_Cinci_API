"""BigQuery integration test: runs the real pipeline against a throwaway clone.

Clones the `crashes` tables into a new dataset `crashes_it_<random>`, points
the pipeline at it with GCP_DATASET, exercises the load end to end and its
failure paths, then deletes the clone (also on failure). Production is only
read (CLONE). Needs credentials and network, so it is not part of the
offline suite:

  docker run --rm --env-file .env -v "${PWD}:/app:ro" -w /app \
    -v "$env:APPDATA\gcloud:/home/etl/.config/gcloud:ro" \
    --entrypoint python cinci-crash-etl tests/integration_bigquery.py

Takes about 30 minutes and runs 56 checks: most of the time is the five
Socrata extracts (four delta windows and one full history). It also runs
outside the image (python tests/integration_bigquery.py) when the
credentials and .env are in place.

Every check prints [PASS] or [FAIL]; the process exits non-zero if any
failed, and the clone is dropped either way.
"""

import os
import sys
import tempfile
import threading
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

TEST_DATASET = f'crashes_it_{uuid.uuid4().hex[:8]}'
os.environ['GCP_DATASET'] = TEST_DATASET          # before any Config.from_env()
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.cloud import bigquery  # noqa: E402

import Load_to_GBQ as gbq  # noqa: E402
import Run_Pipeline as rp  # noqa: E402
from Pipeline_Config import Config  # noqa: E402

SOURCE = 'crashes'
CLONED = ['stg_crash_person', 'dim_date', 'dim_time', 'dim_location', 'dim_conditions',
          'dim_crash_type', 'dim_person_profile', 'fact_crash_person', 'etl_load_log']

config = Config.from_env()
assert config.dataset == TEST_DATASET and config.dataset != SOURCE
client = gbq.get_client(config)
T = f'`{config.project}.{TEST_DATASET}`'
results = []


def q(sql, params=None):
    return [dict(r) for r in client.query(sql, job_config=rp.job_config(params)).result()]


def check(label, ok, detail=''):
    results.append((label, bool(ok)))
    print(f'[{"PASS" if ok else "FAIL"}] {label}' + (f' — {detail}' if detail else ''))


def expect_error(label, fn, contains):
    try:
        fn()
    except Exception as e:
        first = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
        check(label, contains in str(e), first[:200])
        return
    check(label, False, 'did not raise')


def run_main(*argv):
    old = sys.argv
    sys.argv = ['Run_Pipeline.py', *argv]
    try:
        rp.main()
    finally:
        sys.argv = old


def fact_state():
    return q(f"""
        SELECT COUNT(*) AS n_rows, COUNT(DISTINCT instanceid) AS n_crashes,
               COUNTIF(crash_hash IS NULL) AS no_hash
        FROM {T}.fact_crash_person""")[0]


def lease_row():
    return q(f"SELECT holder, current_job_id, acquired_at, heartbeat_at FROM {T}.etl_lease")[0]


def latest_log():
    return q(f"SELECT * FROM {T}.etl_load_log ORDER BY started_at DESC LIMIT 1")[0]


def set_lease(holder, minutes, job_id=None):
    q(f"""UPDATE {T}.etl_lease
          SET holder = @h, current_job_id = @j,
              acquired_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @m MINUTE),
              heartbeat_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @m MINUTE)
          WHERE TRUE""",
      [rp.param('h', 'STRING', holder), rp.param('j', 'STRING', job_id), rp.param('m', 'INT64', minutes)])


def section5(lease, sections, window_start, sql=None, load_id=None, allow_deletions=False):
    """Run Section 5 as the lease holder (or as load_id, to test the fence)."""
    if sql:
        sections = {**sections, 5: sql}
    if load_id is None:
        # Each simulated load gets a new owner/audit ID, just like main().
        lease.release()
        if lease.held:
            raise RuntimeError('Prior test job is unresolved; refusing to start another load')
        lease.load_id = uuid.uuid4().hex
        lease.acquire()
    job = rp.run_section(client, lease, sections, 5, 'fact load (test)', rp.section5_params(
        window_start=window_start, reprocess=False, allow_deletions=allow_deletions,
        load_id=load_id or lease.load_id, load_mode='delta',
        started_at=datetime.now(timezone.utc), rows_staged=0))
    return dict(next(iter(job.result())))


def section3(lease, sections, window_start, publish_stamp, expected_rows=None):
    """Run Section 3 with a manifest; by default the manifest matches staging's
    current row count, so each case reaches the check it's aimed at."""
    if expected_rows is None:
        expected_rows = q(f'SELECT COUNT(*) AS n FROM {T}.stg_crash_person')[0]['n']
    return rp.run_section(client, lease, sections, 3, 'staging validation (test)', rp.section3_params(
        window_start=window_start, expected_rows=expected_rows, publish_stamp=publish_stamp))


def main():
    print(f'Cloning {SOURCE} into {TEST_DATASET} ...')
    ds = bigquery.Dataset(f'{config.project}.{TEST_DATASET}')
    ds.location = config.location
    ds.description = 'Integration test clone; safe to delete.'
    client.create_dataset(ds)
    for t in CLONED:
        q(f'CREATE TABLE {T}.{t} CLONE `{config.project}.{SOURCE}.{t}`')

    # 1. Setup migrates a production-shaped dataset
    run_main('--setup')
    cols = {r['column_name'] for r in q(
        f"SELECT column_name FROM {T}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'etl_load_log'")}
    check('setup: log migrations applied',
          {'status', 'error_message', 'fact_committed_at', 'crashes_deferred'} <= cols)
    check('setup: lease seeded and free', lease_row()['holder'] is None)

    # 2. First delta re-stamps anything whose stored hash predates the current
    # row_json (the clone carries production's hashes, which may have been
    # computed by an older formula, and the feed may have republished since).
    # What must hold is that it succeeds; the no-op proof is the second delta.
    run_main('--delta')
    first = latest_log()
    check('delta: first load against a production clone succeeds',
          first['status'] == 'succeeded' and first['fact_committed_at'] is not None,
          f"new {first['crashes_new']}, changed {first['crashes_changed']}, "
          f"removed {first['crashes_removed']}, publish {first['socrata_updated_at']}")

    # Now the fact and the feed agree, so a second delta must change nothing.
    # This is the real hash check: the view's row_json reproduces the hashes
    # Section 5 just stored.
    before = fact_state()
    run_main('--delta')
    log = latest_log()
    check('delta: no-op on unchanged data (view row_json hashes = stored hashes)',
          (log['crashes_new'], log['crashes_changed'], log['crashes_removed']) == (0, 0, 0),
          f"new {log['crashes_new']}, changed {log['crashes_changed']}, removed {log['crashes_removed']}, "
          f"publish {log['socrata_updated_at']}")
    check('delta: log row succeeded with fact_committed_at',
          log['status'] == 'succeeded' and log['fact_committed_at'] is not None)
    check('delta: fact unchanged', fact_state() == before, str(fact_state()))
    check('delta: lease released', lease_row()['holder'] is None)
    labels = client.get_table(config.table('ml_crash_panel')).labels
    check('delta: panel labeled with the load id', labels.get('load_id') == log['load_id'])

    # The forecast label: present, and unlabelled only where the horizon runs
    # off the end of the panel. The build's own ASSERTs prove the arithmetic.
    tgt = q(f"""SELECT COUNTIF(crashes_next_7 IS NULL)     AS unlabelled,
                       COUNTIF(crashes_next_7 IS NOT NULL) AS labelled,
                       COUNT(DISTINCT cell_id)             AS cells
                FROM {T}.ml_crash_panel""")[0]
    check('delta: 7-day target present, only the last week per cell unlabelled',
          tgt['unlabelled'] == tgt['cells'] * 7 and tgt['labelled'] > 0, str(tgt))

    # Distance to downtown, as far as a delta can show it. The clone starts
    # without the column, and a delta only rewrites the crashes that changed,
    # so most rows are still NULL here -- the full coverage check runs after
    # the reprocess below. What must hold already: a distance never appears
    # without coordinates (0.0 for a missing coordinate would read as "at
    # Fountain Square"), and wherever one was written it matches the
    # coordinates stored beside it.
    dist = q(f"""SELECT COUNTIF((latitude IS NULL OR longitude IS NULL)
                                AND distance_to_cbd_m IS NOT NULL) AS spurious,
                        COUNTIF(ABS(distance_to_cbd_m
                          - ST_DISTANCE(ST_GEOGPOINT(longitude, latitude),
                                        ST_GEOGPOINT(-84.5125, 39.1011))) > 0.1) AS disagreeing
                 FROM {T}.fact_crash_person""")[0]
    check('fact: no distance without coordinates, and written distances match them',
          dist['spurious'] == 0 and dist['disagreeing'] == 0, str(dist))

    # The panel feature is static per cell: one distinct value per cell_id,
    # never varying by day, and never NULL (the build asserts this too).
    cellwise = q(f"""SELECT COUNTIF(n_values != 1) AS cells_not_constant,
                            COUNTIF(dist IS NULL)  AS cells_without_distance
                     FROM (SELECT cell_id, COUNT(DISTINCT distance_to_cbd_km) AS n_values,
                                  MAX(distance_to_cbd_km) AS dist
                           FROM {T}.ml_crash_panel GROUP BY cell_id)""")[0]
    check('panel: distance_to_cbd_km is one non-null constant per cell',
          cellwise['cells_not_constant'] == 0 and cellwise['cells_without_distance'] == 0,
          str(cellwise))

    sections = rp.load_sql_sections(rp.SQL_FILE, config)
    window_start = rp.delta_watermark(client, config)

    # 3. Lease
    set_lease('someone-else', 10)
    expect_error('lease: live holder blocks a second run',
                 lambda: rp.Lease(client, config, uuid.uuid4().hex).acquire(), 'holds the ETL lease')

    done_id = f'it_finished_{uuid.uuid4().hex}'
    client.query('SELECT 1', job_id=done_id, job_config=rp.job_config()).result()
    set_lease('stopped-test-owner', -120, done_id)
    expect_error('lock: old heartbeat and DONE job never authorize takeover',
                 lambda: rp.Lease(client, config, uuid.uuid4().hex).acquire(), 'holds the ETL lease')
    q(f"UPDATE {T}.etl_lease SET holder = NULL WHERE holder = 'wrong-owner'")
    check('lock: recovery for another owner does not release it',
          lease_row()['holder'] == 'stopped-test-owner')
    # This holder is a test fixture, not a live process, and its job is DONE.
    q(f"""UPDATE {T}.etl_lease SET holder = NULL, acquired_at = NULL,
           heartbeat_at = NULL, current_job_id = NULL WHERE holder = 'stopped-test-owner'""")
    taker = rp.Lease(client, config, uuid.uuid4().hex)
    taker.acquire()
    check('lock: a new owner can acquire after explicit recovery', lease_row()['holder'] == taker.load_id)
    taker.release()
    check('lease: release frees it', lease_row()['holder'] is None)

    for trial in range(3):
        wins, errors = [], []

        def contender():
            lease = rp.Lease(client, config, uuid.uuid4().hex)
            try:
                lease.acquire()
                wins.append(lease)
            except rp.LeaseError as e:
                errors.append(e)
            except Exception as e:   # a DML conflict error also means "lost"
                errors.append(e)
        threads = [threading.Thread(target=contender) for _ in range(3)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        check(f'lease: 3 simultaneous acquires, exactly one wins (trial {trial + 1})',
              len(wins) == 1, f'{len(wins)} won; losers: {[str(e)[:60] for e in errors]}')
        for lease in wins:
            lease.release()

    # Every section test below runs under one held lease, like a real load
    lease = rp.Lease(client, config, uuid.uuid4().hex)
    lease.acquire()
    try:
        # 4. Section 3 staging validation (fails before any change)
        q(f'CREATE OR REPLACE TABLE {T}.stg_backup AS SELECT * FROM {T}.stg_crash_person')
        restore = f'CREATE OR REPLACE TABLE {T}.stg_crash_person AS SELECT * FROM {T}.stg_backup'
        stamp = q(f'SELECT SAFE_CAST(ANY_VALUE(socrata_updated_at) AS TIMESTAMP) AS s FROM {T}.stg_backup')[0]['s']
        s3 = lambda: section3(lease, sections, window_start, stamp)  # noqa: E731
        staged = q(f'SELECT COUNT(*) AS n FROM {T}.stg_crash_person')[0]['n']
        expect_error('section 3: rejects staging that is not the verified extract (row count)',
                     lambda: section3(lease, sections, window_start, stamp, expected_rows=staged + 1),
                     'row count fetch() verified')
        expect_error('section 3: rejects staging from a different publish than the verified one',
                     lambda: section3(lease, sections, window_start, stamp + timedelta(seconds=1)),
                     'publish fetch() verified')
        cases = [
            ('one person per crash kept (crash count intact)',
             f'DELETE FROM {T}.stg_crash_person WHERE socrata_id NOT IN '
             f'(SELECT MIN(socrata_id) FROM {T}.stg_crash_person GROUP BY instanceid)', 'Refusing to load'),
            ('NULL publish stamp',
             f'UPDATE {T}.stg_crash_person SET socrata_updated_at = NULL WHERE socrata_id = '
             f'(SELECT MIN(socrata_id) FROM {T}.stg_crash_person)', 'without :id'),
            ('two publishes',
             f"UPDATE {T}.stg_crash_person SET socrata_updated_at = '2099-01-01T00:00:00.000Z' "
             f'WHERE socrata_id = (SELECT MIN(socrata_id) FROM {T}.stg_crash_person)', 'more than one'),
            ('same :id, different content',
             f"INSERT INTO {T}.stg_crash_person SELECT * REPLACE ('999' AS age) FROM {T}.stg_crash_person "
             f'WHERE socrata_id = (SELECT MIN(socrata_id) FROM {T}.stg_crash_person)', 'two different contents'),
            ("'|' in a natural-key value",
             f"UPDATE {T}.stg_crash_person SET address = CONCAT(IFNULL(address, ''), ' | X') "
             f'WHERE socrata_id = (SELECT MIN(socrata_id) FROM {T}.stg_crash_person)', 'natural-key value'),
        ]
        for label, tamper, message in cases:
            q(tamper)
            expect_error(f'section 3: rejects {label}', s3, message)
            q(restore)
        q(f'INSERT INTO {T}.stg_crash_person SELECT * FROM {T}.stg_crash_person '
          f'WHERE socrata_id = (SELECT MIN(socrata_id) FROM {T}.stg_crash_person)')
        try:
            s3()
            check('section 3: accepts an exact duplicate row (page overlap)', True)
        except Exception as e:
            check('section 3: accepts an exact duplicate row (page overlap)', False, str(e)[:200])
        r = section5(lease, sections, window_start)
        check('section 5: exact duplicate staged row changes nothing', not any(r.values()), str(r))

        # The decisive hash test. The city fuzzes each row's coordinates
        # independently and redraws them on every publish, so a republish that
        # changes nothing else must still be a no-op. With latitude/longitude
        # in the hash this reported every crash in the window as changed.
        q(f"""UPDATE {T}.stg_crash_person
              SET latitude  = CAST(SAFE_CAST(latitude  AS FLOAT64) + 0.0009 AS STRING),
                  longitude = CAST(SAFE_CAST(longitude AS FLOAT64) - 0.0007 AS STRING)
              WHERE latitude IS NOT NULL AND longitude IS NOT NULL""")
        r = section5(lease, sections, window_start)
        check('section 5: a republish that only re-fuzzes coordinates changes nothing',
              not any(r.values()), str(r))

        q(restore)
        q(f'DROP TABLE {T}.stg_backup')

        # 5. Section 4 key assertion
        q(f"""INSERT INTO {T}.dim_conditions
              SELECT * REPLACE (conditions_key + 1000000 AS conditions_key) FROM {T}.dim_conditions
              WHERE conditions_key = (SELECT MIN(conditions_key) FROM {T}.dim_conditions WHERE conditions_key > 0)""")
        expect_error('section 4: duplicate natural key fails before the fact load',
                     lambda: rp.run_section(client, lease, sections, 4, 'dims (test)',
                                            [rp.param('reprocess', 'BOOL', False)]),
                     'dim_conditions has a duplicate')
        q(f'DELETE FROM {T}.dim_conditions WHERE conditions_key > 1000000')

        # 6. Section 5: repair, deferral, rollback
        staged = q(f"""
            SELECT instanceid, COUNT(*) AS n FROM {T}.stg_crash_person
            GROUP BY instanceid HAVING n >= 2 ORDER BY instanceid LIMIT 6""")
        a, b, c, e_, f_, g = [s['instanceid'] for s in staged]
        q(f"DELETE FROM {T}.fact_crash_person WHERE socrata_id = "
          f"(SELECT MIN(socrata_id) FROM {T}.fact_crash_person WHERE instanceid = '{a}')")
        q(f"INSERT INTO {T}.fact_crash_person SELECT * FROM {T}.fact_crash_person "
          f"WHERE instanceid = '{b}' LIMIT 1")
        q(f"UPDATE {T}.fact_crash_person SET crash_hash = 'tampered' WHERE instanceid = '{c}'")
        q(f"INSERT INTO {T}.fact_crash_person SELECT * REPLACE ('FAKE-CRASH' AS instanceid, 'fake-row' AS socrata_id) "
          f"FROM {T}.fact_crash_person WHERE instanceid = '{a}' LIMIT 1")
        r = section5(lease, sections, window_start)
        check('section 5: repairs dropped, duplicated, re-hashed crashes and removes a fake one',
              (r['crashes_new'], r['crashes_changed'], r['crashes_removed'], r['crashes_deferred']) == (0, 3, 1, 0),
              str(r))
        r = section5(lease, sections, window_start)
        check('section 5: rerun after repair is a no-op', not any(r.values()), str(r))

        # Deletion cap: 101 crashes gone from the feed stops the load ...
        q(f"""INSERT INTO {T}.fact_crash_person
              SELECT f.* REPLACE (CONCAT('FAKE-', CAST(n AS STRING)) AS instanceid,
                                  CONCAT('fake-row-', CAST(n AS STRING)) AS socrata_id)
              FROM (SELECT * FROM {T}.fact_crash_person WHERE instanceid = '{a}' LIMIT 1) f,
                   UNNEST(GENERATE_ARRAY(1, 101)) AS n""")
        before = fact_state()
        expect_error('section 5: deleting more than the cap fails the load',
                     lambda: section5(lease, sections, window_start), 'would delete 101')
        check('section 5: the capped load changed nothing', fact_state() == before)
        # ... and goes through when the deletions are confirmed real
        r = section5(lease, sections, window_start, allow_deletions=True)
        check('section 5: --allow-deletions removes them', r['crashes_removed'] == 101, str(r))

        # Fence: a load that isn't the lease holder can't commit
        before = fact_state()
        q(f"UPDATE {T}.fact_crash_person SET crash_hash = 'tampered-g' WHERE instanceid = '{g}'")
        expect_error('section 5: a load that lost the lease rolls back',
                     lambda: section5(lease, sections, window_start, load_id=uuid.uuid4().hex),
                     'no longer holds the ETL lease')
        kept = q(f"SELECT COUNT(*) AS n FROM {T}.fact_crash_person WHERE crash_hash = 'tampered-g'")[0]['n']
        check('section 5: after the fence failure the fact is untouched',
              fact_state() == before and kept > 0, f'tampered kept {kept}')
        r = section5(lease, sections, window_start)
        check('section 5: the lease holder then repairs it', r['crashes_changed'] == 1, str(r))

        # A changed crash with a fact row before the window is deferred, untouched
        q(f"""UPDATE {T}.fact_crash_person
              SET crash_hash = 'tampered-e',
                  crash_date = IF(socrata_id = (SELECT MIN(socrata_id) FROM {T}.fact_crash_person
                                                WHERE instanceid = '{e_}'),
                                  DATE_SUB(@w, INTERVAL 10 DAY), crash_date)
              WHERE instanceid = '{e_}'""", [rp.param('w', 'DATE', window_start)])
        r = section5(lease, sections, window_start)
        kept = q(f"SELECT COUNTIF(crash_hash = 'tampered-e') AS n FROM {T}.fact_crash_person "
                 f"WHERE instanceid = '{e_}'")[0]['n']
        check('section 5: delta defers a crash straddling the window and leaves it untouched',
              r['crashes_deferred'] == 1 and r['crashes_changed'] == 0 and r['crashes_removed'] == 0 and kept > 0,
              f'{r}; tampered rows kept {kept}')
        q(f"DELETE FROM {T}.fact_crash_person WHERE instanceid = '{e_}'")
        r = section5(lease, sections, window_start)
        check('section 5: the deferred crash reloads cleanly once its rows are gone',
              (r['crashes_new'], r['crashes_changed'], r['crashes_deferred']) == (1, 0, 0), str(r))

        # Rollback: a failing in-transaction assert undoes the delete and insert
        q(f"UPDATE {T}.fact_crash_person SET crash_hash = 'tampered-f' WHERE instanceid = '{f_}'")
        forced = sections[5].replace('  ASSERT NOT EXISTS (\n    SELECT 1\n    FROM stg_crash s',
                                     '  ASSERT FALSE AND NOT EXISTS (\n    SELECT 1\n    FROM stg_crash s', 1)
        assert forced != sections[5]
        log_rows = lambda: q(f'SELECT COUNT(*) AS n FROM {T}.etl_load_log')[0]['n']  # noqa: E731
        before, logged_before = fact_state(), log_rows()
        expect_error('section 5: failing check rolls the transaction back',
                     lambda: section5(lease, sections, window_start, sql=forced), 'rolled back')
        kept = q(f"SELECT COUNT(*) AS n FROM {T}.fact_crash_person WHERE crash_hash = 'tampered-f'")[0]['n']
        check('section 5: after rollback the fact and log are untouched',
              fact_state() == before and kept > 0 and log_rows() == logged_before,
              f'tampered kept {kept}, log rows {logged_before} -> {log_rows()}')
        r = section5(lease, sections, window_start)
        check('section 5: the next load repairs it', r['crashes_changed'] == 1, str(r))
        r = section5(lease, sections, window_start)
        check('section 5: final state is a no-op', not any(r.values()), str(r))
    finally:
        lease.release()

    # 7. A failure after the fact commit is logged as failed, fact commit kept
    # gettempdir(), not '/tmp': this suite also runs outside the Linux image
    broken = Path(tempfile.gettempdir()) / 'ML_Crash_Panel_broken.sql'
    text = Path(rp.PANEL_SQL_FILE).read_text(encoding='utf-8')
    marker = 'CREATE OR REPLACE TABLE crashes.ml_crash_panel'
    assert marker in text
    broken.write_text(text.replace(marker, "ASSERT FALSE AS 'forced panel failure';\n" + marker, 1),
                      encoding='utf-8')
    rp.PANEL_SQL_FILE, original = str(broken), rp.PANEL_SQL_FILE
    try:
        expect_error('pipeline: a panel failure fails the run', lambda: run_main('--delta'), 'forced panel failure')
    finally:
        rp.PANEL_SQL_FILE = original
        broken.unlink(missing_ok=True)
    log = latest_log()
    check('pipeline: failed run logged with its fact commit kept',
          log['status'] == 'failed' and log['fact_committed_at'] is not None
          and (log['error_message'] or '').startswith('ML panel'), f"{log['status']}: {log['error_message']}")
    check('pipeline: lease released after the failure', lease_row()['holder'] is None)

    # A lost response on Section 9 must not turn a finished load into a failed
    # one: Section 11 skips rows already marked succeeded.
    done = q(f"SELECT load_id FROM {T}.etl_load_log WHERE status = 'succeeded' "
             f"ORDER BY started_at DESC LIMIT 1")[0]['load_id']
    q(sections[11], [rp.param('load_id', 'STRING', done),
                     rp.param('load_mode', 'STRING', 'delta'),
                     rp.param('window_start', 'DATE', window_start),
                     rp.param('started_at', 'TIMESTAMP', datetime.now(timezone.utc)),
                     rp.param('error_message', 'STRING', 'simulated lost response')])
    rows = q(f"SELECT status FROM {T}.etl_load_log WHERE load_id = '{done}'")
    check('section 11: a succeeded load survives a late failure write, without a second row',
          len(rows) == 1 and rows[0]['status'] == 'succeeded', str(rows))

    # 8. --full --reprocess rewrites every crash, then a delta is a no-op again
    before = fact_state()
    run_main('--full', '--reprocess')
    log = latest_log()
    check('reprocess: rewrites every crash and succeeds',
          log['status'] == 'succeeded' and log['load_mode'] == 'reprocess'
          and log['crashes_changed'] + log['crashes_new'] == fact_state()['n_crashes'],
          f"changed {log['crashes_changed']}, new {log['crashes_new']}, removed {log['crashes_removed']}; "
          f"fact before {before}, after {fact_state()}")

    # Every crash has now been rewritten, so distance_to_cbd_m must be fully
    # populated -- this is the state production reaches after --setup plus
    # --full --reprocess.
    dist = q(f"""SELECT COUNTIF(latitude IS NOT NULL AND longitude IS NOT NULL
                                AND distance_to_cbd_m IS NULL)        AS missing,
                        COUNTIF((latitude IS NULL OR longitude IS NULL)
                                AND distance_to_cbd_m IS NOT NULL)    AS spurious,
                        COUNTIF((latitude IS NULL) != (longitude IS NULL)) AS half_coords,
                        ROUND(MIN(distance_to_cbd_m)) AS min_m,
                        ROUND(MAX(distance_to_cbd_m)) AS max_m
                 FROM {T}.fact_crash_person""")[0]
    check('reprocess: distance_to_cbd_m set for every row that has coordinates',
          dist['missing'] == 0 and dist['spurious'] == 0, str(dist))
    # The cleaning view nulls latitude and longitude as a pair, so no row can
    # carry half a point and read as located to a one-axis check.
    check('reprocess: no row carries only one of the two coordinates',
          dist['half_coords'] == 0, str(dist))
    # Hamilton County is ~40 km across, so nothing legitimate lands past 50 km.
    check('reprocess: distances are within Hamilton County scale',
          dist['min_m'] >= 0 and dist['max_m'] < 50000, str(dist))

    # Orientation: the measure has to actually point at downtown, or a
    # transposed ST_GEOGPOINT would go unnoticed. Medians, so the fuzz and
    # the bad geocodes stay out of it.
    near_far = q(f"""
        SELECT APPROX_QUANTILES(IF(dl.cpd_neighborhood = 'C. B. D. / RIVERFRONT',
                                   f.distance_to_cbd_m, NULL), 2)[OFFSET(1)] AS cbd_median_m,
               APPROX_QUANTILES(IF(dl.cpd_neighborhood = 'WESTWOOD',
                                   f.distance_to_cbd_m, NULL), 2)[OFFSET(1)] AS westwood_median_m
        FROM {T}.fact_crash_person f JOIN {T}.dim_location dl USING (location_key)""")[0]
    check('reprocess: downtown crashes sit nearer the anchor than Westwood ones',
          near_far['cbd_median_m'] < 2000 < near_far['westwood_median_m'], str(near_far))

    # The same orientation at cell grain: the nearest cell to the anchor has
    # to be the central business district itself.
    nearest = q(f"""SELECT cell_id, ANY_VALUE(distance_to_cbd_km) AS km
                    FROM {T}.ml_crash_panel GROUP BY cell_id ORDER BY km LIMIT 1""")[0]
    check('reprocess: the panel cell nearest the anchor is the CBD',
          nearest['cell_id'] == 'C. B. D. / RIVERFRONT' and nearest['km'] < 1.0, str(nearest))

    run_main('--delta')
    log = latest_log()
    check('reprocess: the next delta is a no-op',
          (log['crashes_new'], log['crashes_changed'], log['crashes_removed']) == (0, 0, 0)
          and log['status'] == 'succeeded', f"{log['crashes_new']}/{log['crashes_changed']}/{log['crashes_removed']}")

    # 9. Last, because it drops and re-creates the lock table
    concurrent_bootstrap()


def concurrent_bootstrap():
    """Three first-time setups racing must still leave one lock row.

    Runs last, and drops the table first: CREATE and ALTER are metadata
    operations, and BigQuery rate-limits those per table, so racing them
    ahead of the real setup made the next --setup fail on quota.
    """
    q(f'DROP TABLE {T}.etl_lease')
    section10 = rp.load_sql_sections(rp.SQL_FILE, config)[10]
    errors = []

    def contender():
        try:
            q(section10)
        except Exception as exc:
            errors.append(str(exc).strip().splitlines()[0])

    threads = [threading.Thread(target=contender) for _ in range(3)]
    [thread.start() for thread in threads]
    [thread.join() for thread in threads]
    rows = q(f'SELECT COUNT(*) AS n FROM {T}.etl_lease')[0]['n']
    # A loser may fail on the metadata rate limit or on "already exists"; both
    # are fine. Two rows, or a tripped singleton ASSERT, are not.
    fatal = [e for e in errors if 'exactly one pipeline row' in e]
    check('setup: three racing bootstraps leave exactly one lock row',
          rows == 1 and not fatal and len(errors) < 3, f'{rows} rows; {errors}')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        traceback.print_exc()
        results.append(('unexpected error', False))
    finally:
        client.delete_dataset(f'{config.project}.{TEST_DATASET}', delete_contents=True, not_found_ok=True)
        print(f'Deleted {TEST_DATASET}.')
    failed = [label for label, ok in results if not ok]
    print(f'\n{len(results) - len(failed)}/{len(results)} checks passed.')
    sys.exit(1 if failed else 0)
