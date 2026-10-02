"""Create or update the hourly health watchdog as a BigQuery scheduled query.

DEPLOY.md step 8. The BigQuery console does the same thing in a few clicks;
this exists so the alert is reproducible and reviewable instead of hand-built,
and so a changed watchdog.sql can be pushed without re-clicking it.

    pip install google-cloud-bigquery-datatransfer
    python deploy/create_watchdog.py --service-account crash-etl-runtime@PROJECT.iam.gserviceaccount.com
    python deploy/create_watchdog.py --update --service-account ...   # after editing watchdog.sql

It runs as the runtime service account so the check does not quietly stop
working when a person's credentials expire.

Where the failure mail lands is worth confirming rather than assuming. The
notification follows the transfer config's owner, and running the query as a
service account makes that worth testing once -- a service account has no
inbox. DEPLOY.md step 8 has a safe way to force a failure and see whether the
mail arrives. If it does not, rerun with --as-me, which keeps the alert tied
to your own account at the cost of depending on your credentials.
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import dotenv_values
from google.cloud import bigquery_datatransfer
from google.protobuf import field_mask_pb2

DISPLAY_NAME = 'crash-etl watchdog'
SCHEDULE = 'every 1 hours'
SQL_PATH = Path(__file__).with_name('watchdog.sql')
REPO_ROOT = Path(__file__).resolve().parent.parent

# --test-alert installs this instead of the real watchdog. It always fails,
# which is the only way to find out whether the failure email reaches a
# person. Forgetting to restore the real one is loud rather than silent --
# an hourly mail you cannot miss -- which is the right way round for a
# safeguard to fail.
TEST_SQL = """-- TEMPORARY. Installed by: create_watchdog.py --test-alert
-- Restore the real watchdog with:
--   python deploy/create_watchdog.py --update --service-account <runtime SA>
RAISE USING MESSAGE =
  'WATCHDOG DELIVERY TEST -- this is not a real alert, nothing is wrong. '
  'Receiving this proves failure email works. Restore the real watchdog: '
  'python deploy/create_watchdog.py --update --service-account <runtime SA>';
"""


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--update', action='store_true',
                        help='replace the SQL and schedule on an existing watchdog')
    parser.add_argument('--service-account',
                        help='runtime service account email to run the query as')
    parser.add_argument('--as-me', action='store_true',
                        help='run as your own credentials instead; use only if the '
                             'service-account notification does not reach your inbox')
    parser.add_argument('--test-alert', action='store_true',
                        help='install an always-failing watchdog to prove the failure '
                             'email arrives, then rerun with --update to restore')
    parser.add_argument('--run-now', action='store_true',
                        help='also trigger a run immediately instead of waiting for '
                             'the next hourly one')
    args = parser.parse_args()

    # dotenv_values, not load_dotenv: .env sets GOOGLE_APPLICATION_CREDENTIALS
    # to the pipeline's own key, and exporting it here would make this script
    # authenticate as the pipeline's service account -- which has no rights to
    # create a scheduled query. Read the two settings needed and leave the
    # environment alone, so the client uses your own ADC.
    env = dotenv_values(REPO_ROOT / '.env')
    project = os.getenv('GCP_PROJECT_ID') or env.get('GCP_PROJECT_ID')
    location = os.getenv('GCP_LOCATION') or env.get('GCP_LOCATION') or 'us-east1'
    if not project:
        sys.exit('GCP_PROJECT_ID must be set (.env or the environment).')

    service_account = None if args.as_me else (
        args.service_account or os.getenv('WATCHDOG_SERVICE_ACCOUNT'))
    if not service_account and not args.as_me:
        sys.exit('Pass --service-account, or set WATCHDOG_SERVICE_ACCOUNT. Running the '
                 'watchdog as a person ties the alert to that person\'s credentials; '
                 'pass --as-me if you have decided that is what you want.')

    sql = TEST_SQL if args.test_alert else SQL_PATH.read_text(encoding='utf-8')
    client = bigquery_datatransfer.DataTransferServiceClient()
    parent = f'projects/{project}/locations/{location}'

    existing = next((t for t in client.list_transfer_configs(parent=parent)
                     if t.display_name == DISPLAY_NAME), None)

    if existing and not args.update:
        print(f'Already exists: {existing.name}\n'
              f'  schedule: {existing.schedule}\n'
              f'  failure email: {existing.email_preferences.enable_failure_email}\n'
              'Rerun with --update to push a changed watchdog.sql.')
        return

    config = bigquery_datatransfer.TransferConfig(
        display_name=DISPLAY_NAME,
        data_source_id='scheduled_query',
        params={'query': sql},
        schedule=SCHEDULE,
        email_preferences=bigquery_datatransfer.EmailPreferences(enable_failure_email=True),
    )

    # service_account_name is a field on the request message, not one of the
    # method's convenience keyword arguments, so both calls build a request.
    if existing:
        config.name = existing.name
        result = client.update_transfer_config(
            request=bigquery_datatransfer.UpdateTransferConfigRequest(
                transfer_config=config,
                update_mask=field_mask_pb2.FieldMask(
                    paths=['params', 'schedule', 'email_preferences']),
                service_account_name=service_account or ''))
        print(f'Updated {result.name}')
    else:
        result = client.create_transfer_config(
            request=bigquery_datatransfer.CreateTransferConfigRequest(
                parent=parent,
                transfer_config=config,
                service_account_name=service_account or ''))
        print(f'Created {result.name}')

    runner = service_account or 'your own credentials'
    print(f'  runs: {result.schedule}  as: {runner}')

    if args.run_now:
        client.start_manual_transfer_runs(
            request=bigquery_datatransfer.StartManualTransferRunsRequest(
                parent=result.name,
                requested_run_time=datetime.now(timezone.utc)))
        print('  triggered a run now; it should finish within a minute or two.')

    if args.test_alert:
        print('\n  *** THIS IS THE ALWAYS-FAILING TEST WATCHDOG, NOT THE REAL ONE ***\n'
              '  It fails every hour on purpose. When the mail arrives, restore the\n'
              '  real watchdog immediately:\n'
              f'    python deploy/create_watchdog.py --update --service-account {runner}')
    else:
        print('  A tripped guard fails the run, and the failed run sends the mail.\n'
              '  Verify with DEPLOY.md step 8.')


if __name__ == '__main__':
    main()
