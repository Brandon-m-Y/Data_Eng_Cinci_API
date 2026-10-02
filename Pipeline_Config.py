"""Validated BigQuery settings shared by the loader and the SQL runner.

The SQL files name the dataset `crashes` so they still run as-is in the
BigQuery console; render() swaps in the configured dataset when the pipeline
loads them, so GCP_DATASET moves every read and write, not just the Python ones.
"""

import os
import re
from dataclasses import dataclass

STAGING_TABLE = 'stg_crash_person'   # fixed: the SQL files refer to it by name

# Best-effort job timeout, not a fencing mechanism. Writer ownership does not
# expire: an abandoned lock requires verified, owner-specific recovery.
JOB_TIMEOUT_MINUTES = 30

# `crashes.<table>`, also inside backticks, but not the panel's `crashes`
# column (never followed by a dot) or prose like 'crashes. Amendments'.
_DATASET_REF = re.compile(r'\bcrashes\.(?=[A-Za-z_])')


@dataclass(frozen=True)
class Config:
    project: str
    dataset: str = 'crashes'
    location: str = 'us-east1'

    def __post_init__(self):
        if not re.fullmatch(r'[a-z][a-z0-9-]{4,28}[a-z0-9]', self.project or ''):
            raise ValueError('GCP_PROJECT_ID must be set to a valid Google Cloud project ID')
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,1023}', self.dataset or ''):
            raise ValueError(f'GCP_DATASET {self.dataset!r} is not a valid BigQuery dataset name')
        if not self.location:
            raise ValueError('GCP_LOCATION must not be empty')

    @classmethod
    def from_env(cls):
        staging = os.getenv('GCP_STAGING_TABLE')
        if staging and staging != STAGING_TABLE:
            raise ValueError(f'GCP_STAGING_TABLE is no longer configurable; the SQL uses {STAGING_TABLE}')
        return cls(
            project=os.getenv('GCP_PROJECT_ID', ''),
            dataset=os.getenv('GCP_DATASET') or 'crashes',
            location=os.getenv('GCP_LOCATION') or 'us-east1',
        )

    def table(self, name):
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,1023}', name):
            raise ValueError(f'Invalid table name {name!r}')
        return f'{self.project}.{self.dataset}.{name}'

    def render(self, sql):
        """Point every `crashes.` table reference at the configured dataset."""
        return _DATASET_REF.sub(f'{self.dataset}.', sql)
