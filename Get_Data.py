"""Extract: pull Traffic Crash Reports (CPD) from the Cincinnati Socrata API.

Grain is one row per PERSON/UNIT per crash (~1.96 rows per crash). Do not
dedupe on instanceid — that collapses the feed to one row per crash and
drops every passenger and pedestrian the star schema is built around.

Usage:
  from Get_Data import fetch
  df = fetch()                    # full history (~433K rows, ~9 pages)
  df = fetch(since='2026-06-01')  # crashes on/after a date (delta window)
"""

import os
import warnings
from datetime import date

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

ENDPOINT = 'https://data.cincinnati-oh.gov/api/v3/views/rvmt-pkmq/query.json'
METADATA_ENDPOINT = 'https://data.cincinnati-oh.gov/api/views/rvmt-pkmq.json'
PAGE_SIZE = 50000

# The v3 endpoint ignores $limit/$offset on a GET and returns the whole
# dataset (~435 MB) in one response. Paging only works through a POSTed SoQL
# body, which is what fetch() uses.

# Every column the feed returns. Socrata omits null fields from each JSON row,
# so without this list a page where a column is entirely null would drop it.
COLUMNS = [
    ':id', ':version', ':created_at', ':updated_at',
    'instanceid', 'localreportno', 'crashdate', 'datecrashreported', 'dayofweek',
    'address_x', 'latitude_x', 'longitude_x', 'zip',
    'community_council_neighborhood', 'cpd_neighborhood', 'sna_neighborhood',
    'roadclass', 'roadclassdesc', 'crashlocation',
    'lightconditionsprimary', 'roadconditionsprimary', 'roadcontour', 'roadsurface',
    'weather', 'mannerofcrash', 'crashseverity', 'crashseverityid',
    'typeofperson', 'unittype', 'gender', 'age', 'injuries',
]


class InvalidSnapshot(ValueError):
    """The extract cannot safely be used for replacement or deletion."""


class SourceChanged(InvalidSnapshot):
    """Retry the whole extraction because its publication changed."""


def _new_session():
    session = requests.Session()
    session.headers['X-App-Token'] = os.environ['SOCRATA_APP_TOKEN']
    # POSTs here are read-only queries, so retrying a transient response is safe.
    retry = Retry(total=2, backoff_factor=1, allowed_methods={'GET', 'POST'},
                  status_forcelist=(429, 500, 502, 503, 504), raise_on_status=False)
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session


def _query(query, *, session, page=1, page_size=PAGE_SIZE):
    response = session.post(
        ENDPOINT, timeout=(30, 600),
        json={'query': query, 'page': {'pageNumber': page, 'pageSize': page_size}},
    )
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, list) or any(not isinstance(row, dict) for row in result):
        raise InvalidSnapshot('Socrata returned an unexpected response shape')
    return result


def _metadata(session):
    response = session.get(METADATA_ENDPOINT, timeout=(30, 60))
    response.raise_for_status()
    metadata = response.json()
    if not isinstance(metadata, dict) or not isinstance(metadata.get('columns'), list):
        raise InvalidSnapshot('Socrata metadata is missing its column definitions')
    fields = {c.get('fieldName') for c in metadata['columns'] if isinstance(c, dict)}
    expected = {c for c in COLUMNS if not c.startswith(':')}
    missing = expected - fields
    if missing:
        raise InvalidSnapshot(f'Source schema is missing expected columns: {sorted(missing)}')
    unknown = {c for c in fields - expected if isinstance(c, str) and not c.startswith(':')}
    if unknown:
        warnings.warn(f'Source has additional unmodeled columns: {sorted(unknown)}', stacklevel=2)
    stamp = metadata.get('rowsUpdatedAt')
    if isinstance(stamp, bool) or not isinstance(stamp, int) or stamp <= 0:
        raise InvalidSnapshot('Socrata metadata has no valid rowsUpdatedAt stamp')
    return stamp, frozenset(c for c in fields if isinstance(c, str))


def _probe(where, session):
    result = _query(
        'SELECT count(*) AS row_count, count(:updated_at) AS stamped_rows, '
        'min(:updated_at) AS min_stamp, max(:updated_at) AS max_stamp' + where,
        page_size=1, session=session,
    )
    try:
        if len(result) != 1:
            raise ValueError('expected one aggregate row')
        row = result[0]
        count = int(row['row_count'])
        stamped = int(row['stamped_rows'])
        minimum = pd.Timestamp(row['min_stamp'])
        maximum = pd.Timestamp(row['max_stamp'])
        if count <= 0 or stamped != count or pd.isna(minimum) or minimum != maximum:
            raise ValueError('empty window or missing/mixed publication stamps')
        return count, minimum.isoformat()
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidSnapshot('Cannot verify a nonempty, single-publication source snapshot') from exc


def validate_frame(frame, expected_rows, publish_stamp):
    """Reject incomplete rows and duplicate IDs; never silently deduplicate people."""
    if len(frame) != expected_rows or frame.empty:
        raise InvalidSnapshot(f'Expected {expected_rows} source rows, received {len(frame)}')
    for column in (':id', 'instanceid', ':updated_at'):
        if column not in frame or frame[column].isna().any():
            raise InvalidSnapshot(f'Missing required source field: {column}')
        if frame[column].astype('string').str.strip().eq('').any():
            raise InvalidSnapshot(f'Blank required source field: {column}')
    if frame[':id'].duplicated().any():
        raise InvalidSnapshot('Repeated source IDs: extraction pages overlap or conflict')
    try:
        stamps = pd.to_datetime(frame[':updated_at'], utc=True, errors='raise')
        expected_stamp = pd.to_datetime(publish_stamp, utc=True, errors='raise')
    except (ValueError, TypeError) as exc:
        raise InvalidSnapshot('Invalid publication timestamp') from exc
    if not stamps.eq(expected_stamp).all():
        raise InvalidSnapshot('Extract contains rows from a different publication')


def fetch(since=None, page_size=PAGE_SIZE):
    """Return verified raw person rows. Empty or incomplete snapshots raise.

    The count and single-publication timestamp must agree before and after
    pagination, and every returned source ID must be unique. This relies on
    this feed's publisher changing the stamp whenever it republishes content.
    """
    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1:
        raise ValueError('page_size must be a positive integer')
    where = ''
    if since:
        # fromisoformat rejects anything that isn't a date, so no SoQL injection
        where = f" WHERE crashdate >= '{date.fromisoformat(since).isoformat()}T00:00:00'"
    with _new_session() as session:
        for attempt in range(3):
            try:
                return _fetch_once(session, where, since, page_size)
            except SourceChanged:
                if attempt == 2:
                    raise
                print('Source publication changed; restarting the entire extraction.')


def _fetch_once(session, where, since, page_size):
    metadata_before = _metadata(session)
    # Explicit projection makes a removed/renamed source column an API error.
    query = f'SELECT {", ".join(COLUMNS)}{where} ORDER BY :id'
    before = _probe(where, session)
    rows, page = [], 1
    while True:
        batch = _query(query, session=session, page=page, page_size=page_size)
        rows.extend(batch)
        if len(rows) > before[0]:
            break  # Verify publication metadata, then reject or retry this extract.
        print(f'  page {page}: {len(batch):,} rows')
        if len(batch) < page_size:
            break
        page += 1

    after = _probe(where, session)
    metadata_after = _metadata(session)  # Must follow BOTH paging and the final count.
    if before != after or metadata_before != metadata_after:
        raise SourceChanged('Source changed during extraction; retry the entire run')
    frame = pd.DataFrame(rows).reindex(columns=COLUMNS)
    validate_frame(frame, *before)
    frame.attrs['snapshot'] = {
        'expected_rows': before[0], 'publish_stamp': before[1], 'since': since,
        'rows_updated_at': metadata_before[0],
    }
    frame.attrs['rows_updated_at'] = metadata_before[0]
    return frame


if __name__ == '__main__':
    df = fetch()
    print(f'{len(df):,} rows, {df["instanceid"].nunique():,} crashes')
