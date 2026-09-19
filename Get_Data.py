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
from datetime import date

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

ENDPOINT = 'https://data.cincinnati-oh.gov/api/v3/views/rvmt-pkmq/query.json'
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


def fetch(since=None, page_size=PAGE_SIZE):
    """Return the crash feed as a DataFrame of raw strings.

    since: 'YYYY-MM-DD' to pull only crashes on or after that date.
    Run_Pipeline.py --delta passes the watermark it reads from the fact.
    Crash date is the only usable watermark: :updated_at holds a single
    value across every row because Socrata restamps the whole dataset on
    each publish, and :version is random.
    """
    where = ''
    if since:
        # fromisoformat rejects anything that isn't a date, so no SoQL injection
        where = f" WHERE crashdate >= '{date.fromisoformat(since).isoformat()}T00:00:00'"
    # ORDER BY :id gives a stable order, so pages don't overlap or skip rows
    query = f'SELECT *, :id, :version, :created_at, :updated_at{where} ORDER BY :id'
    headers = {'X-App-Token': os.environ['SOCRATA_APP_TOKEN']}

    rows, page = [], 1
    while True:
        r = requests.post(
            ENDPOINT, headers=headers, timeout=600,
            json={'query': query, 'page': {'pageNumber': page, 'pageSize': page_size}},
        )
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        print(f'  page {page}: {len(batch):,} rows')
        if len(batch) < page_size:
            break
        page += 1

    return pd.DataFrame(rows).reindex(columns=COLUMNS)


if __name__ == '__main__':
    df = fetch()
    print(f'{len(df):,} rows, {df["instanceid"].nunique():,} crashes')
