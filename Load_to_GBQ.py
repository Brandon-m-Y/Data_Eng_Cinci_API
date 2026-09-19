"""Load the raw crash feed into the BigQuery staging table.

Run order: Get_Data.fetch() -> this script -> Star_Schema_ETL.sql sections 4, 5, 6, 9.
Staging is fully replaced on every run; the star schema is built from it downstream.
"""

import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

from Get_Data import fetch

load_dotenv()

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET = os.getenv('GCP_DATASET')
LOCATION = os.getenv('GCP_LOCATION')
TABLE = os.getenv('GCP_STAGING_TABLE')
CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# ':' is illegal in BigQuery column names; the '_x' suffixes are leftovers
# from an upstream merge.
RENAME = {
    ':id': 'socrata_id',
    ':version': 'socrata_version',
    ':created_at': 'socrata_created_at',
    ':updated_at': 'socrata_updated_at',
    'address_x': 'address',
    'latitude_x': 'latitude',
    'longitude_x': 'longitude',
}

# Must match the stg_crash_person DDL in Star_Schema_ETL.sql section 1.
# All STRING: typing happens in the SQL cleaning view, so a malformed value
# ('BB' in age, '454229' in zip) lands in staging instead of failing the load.
# dayofweek is deliberately absent — dim_date derives it.
STAGING_COLUMNS = [
    'socrata_id', 'socrata_version', 'socrata_created_at', 'socrata_updated_at',
    'instanceid', 'localreportno', 'crashdate', 'datecrashreported',
    'address', 'latitude', 'longitude', 'zip',
    'community_council_neighborhood', 'cpd_neighborhood', 'sna_neighborhood',
    'roadclass', 'roadclassdesc', 'crashlocation',
    'lightconditionsprimary', 'roadconditionsprimary', 'roadcontour', 'roadsurface',
    'weather', 'mannerofcrash', 'crashseverity', 'crashseverityid',
    'typeofperson', 'unittype', 'gender', 'age', 'injuries',
]
SCHEMA = [bigquery.SchemaField(c, 'STRING') for c in STAGING_COLUMNS]


def get_client():
    """Key file locally; on Cloud Run, GOOGLE_APPLICATION_CREDENTIALS is unset
    and the client falls back to the job's attached service account (ADC)."""
    credentials = None
    if CREDENTIALS_PATH:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID, location=LOCATION)


def build_staging_frame(df):
    """Rename to BigQuery-legal names, fix the column order, and make every value a string."""
    staged = df.rename(columns=RENAME).reindex(columns=STAGING_COLUMNS)
    # StringDtype keeps missing values as NULL rather than the text 'nan'
    return staged.astype('string')


def load_staging(df, client=None):
    """Replace crashes.stg_crash_person with the given raw frame.

    WRITE_TRUNCATE replaces the table contents atomically, which is
    what makes the SQL-side TRUNCATE (Section 3) unnecessary here.
    """
    client = client or get_client()
    table_ref = f'{PROJECT_ID}.{DATASET}.{TABLE}'

    staged = build_staging_frame(df)

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(staged, table_ref, job_config=job_config)
    job.result()

    loaded = client.get_table(table_ref).num_rows
    print(f'Loaded {len(staged):,} rows into {table_ref} ({loaded:,} rows in table).')
    return len(staged)


def main():
    load_staging(fetch())


if __name__ == '__main__':
    main()
