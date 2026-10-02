"""Load the raw crash feed into the BigQuery staging table.

Run order: Get_Data.fetch() -> this module -> Star_Schema_ETL.sql sections 3, 4, 5, 6, 9.
Staging is fully replaced on every run; the star schema is built from it downstream.
Import only: Run_Pipeline.py calls load_staging() while it holds the ETL lease,
so nothing else can replace staging in the middle of another load.
"""

import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

from Get_Data import validate_frame
from Pipeline_Config import JOB_TIMEOUT_MINUTES, STAGING_TABLE, Config

load_dotenv()

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


def get_client(config=None):
    """Key file locally; on Cloud Run, GOOGLE_APPLICATION_CREDENTIALS is unset
    and the client falls back to the job's attached service account (ADC)."""
    config = config or Config.from_env()
    credentials = None
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
    return bigquery.Client(credentials=credentials, project=config.project, location=config.location)


def build_staging_frame(df):
    """Rename to BigQuery-legal names, fix the column order, and make every value a string."""
    staged = df.rename(columns=RENAME).reindex(columns=STAGING_COLUMNS)
    # StringDtype keeps missing values as NULL rather than the text 'nan'
    return staged.astype('string')


def load_staging(df, client, config, *, lease):
    """Replace crashes.stg_crash_person with a frame Get_Data.fetch() verified.

    WRITE_TRUNCATE replaces the table contents atomically. The frame must
    carry fetch()'s snapshot manifest, so an unverified extract can't be staged.
    Validation and conversion precede reserving and submitting the tracked job.
    Returns the number of rows staged.
    """
    manifest = df.attrs.get('snapshot')
    if not manifest:
        raise ValueError('Staging requires the snapshot manifest from Get_Data.fetch()')
    validate_frame(df, manifest['expected_rows'], manifest['publish_stamp'])

    table_ref = config.table(STAGING_TABLE)
    staged = build_staging_frame(df)
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        job_timeout_ms=JOB_TIMEOUT_MINUTES * 60 * 1000,
        labels={'load_id': lease.load_id},
    )
    job_id = lease.renew('staging')
    lease.submitting()
    job = client.load_table_from_dataframe(staged, table_ref, job_config=job_config, job_id=job_id)
    job.result()
    lease.terminal(job)

    # Rows this job wrote, not the table's count (which another writer could change)
    loaded = job.output_rows
    if loaded != len(staged):
        raise ValueError(f'Staging count mismatch: sent {len(staged):,}, the load wrote {loaded:,}')
    print(f'Loaded {loaded:,} rows into {table_ref}.')
    return loaded


def main():
    raise SystemExit('Use Run_Pipeline.py --full or --delta: loads run under the ETL lease.')


if __name__ == '__main__':
    main()
