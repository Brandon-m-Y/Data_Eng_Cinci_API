"""Create or update the daily health watchdog as a BigQuery scheduled query.

DEPLOY.md step 8. The BigQuery console does the same thing in a few clicks;
this exists so the alert is reproducible and reviewable instead of hand-built,
and so a changed watchdog.sql can be pushed without re-clicking it.

    pip install google-cloud-bigquery-datatransfer
    python deploy/create_watchdog.py --service-account crash-etl-runtime@PROJECT.iam.gserviceaccount.com
    python deploy/create_watchdog.py --update --service-account ...   # after editing watchdog.sql

Create the watchdog in the BigQuery console, not here -- see DEPLOY.md
step 8. This script cannot create one owned by a person: the API wants an
OAuth authorization code ('version_info') that application-default
credentials do not carry, and a service-account-owned config can never send
mail because a service account has no mailbox. Both were confirmed on
2026-10-02. What this script is good for is everything after that: --update
to push a changed watchdog.sql, --test-alert to prove the mail arrives, and
no flags at all to report who the mail currently reaches.
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
# App Engine cron syntax, interpreted in UTC. 14:00 UTC is 09:00 EST /
# 10:00 EDT, so it always lands a few hours after the 06:00 America/
# New_York load, in either half of the year. Guard 2's 24-hour
# threshold in watchdog.sql depends on that ordering.
SCHEDULE = 'every day 14:00'
SQL_PATH = Path(__file__).with_name('watchdog.sql')
REPO_ROOT = Path(__file__).resolve().parent.parent

# --test-alert installs this instead of the real watchdog. It always fails,
# which is the only way to find out whether the failure email reaches a
# person. Forgetting to restore the real one is loud rather than silent --
# a daily mail you cannot miss -- which is the right way round for a
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
                             'the next scheduled one')
    parser.add_argument('--recreate', action='store_true',
                        help='delete the existing watchdog and create it again. Needed '
                             'to change who owns it, which is who the failure mail '
                             'reaches; --update cannot move ownership.')
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

    sql = TEST_SQL if args.test_alert else SQL_PATH.read_text(encoding='utf-8')
    client = bigquery_datatransfer.DataTransferServiceClient()
    parent = f'projects/{project}/locations/{location}'

    existing = next((t for t in client.list_transfer_configs(parent=parent)
                     if t.display_name == DISPLAY_NAME), None)

    # Refuse before deleting anything. Creating a config owned by a person
    # requires an OAuth authorization code, which the API calls version_info
    # and application-default credentials do not supply:
    #   400 Failed to find a valid credential. The field 'version_info' or
    #       'service_account_name' must be specified.
    # The BigQuery console runs that consent flow, so it is the only way to
    # create a user-owned watchdog. This check exists because finding out by
    # trying deleted the live watchdog first (2026-10-02).
    if args.as_me and (existing is None or args.recreate):
        sys.exit(
            'Cannot create a watchdog owned by you through this API: it needs an\n'
            "OAuth authorization code ('version_info') that application-default\n"
            'credentials do not provide. Nothing has been changed.\n\n'
            'Create it in the BigQuery console instead (DEPLOY.md step 8), which\n'
            'runs the consent flow for you. This script can --update it afterwards.')

    # --recreate has to delete before creating, because two configs cannot
    # share a display name. If the create then fails you are left with no
    # watchdog at all, which happened on 2026-10-02. Keep the old definition
    # and put it back rather than leaving the project unmonitored.
    replaced = None
    if existing and args.recreate:
        replaced = existing
        client.delete_transfer_config(name=existing.name)
        print(f'Deleted {existing.name}')
        existing = None

    if existing and not args.update:
        owner = existing.owner_info.email if existing.owner_info else ''
        installed = existing.params.get('query', '').strip()
        matches = installed == SQL_PATH.read_text(encoding='utf-8').strip()
        print(f'Already exists: {existing.name}\n'
              f'  schedule      : {existing.schedule}\n'
              f'  failure email : {existing.email_preferences.enable_failure_email}\n'
              f'  mail goes to  : {owner or "NOBODY - owned by a service account"}\n'
              f'  query         : {"matches watchdog.sql" if matches else "DIFFERS from watchdog.sql"}')
        if not owner and existing.email_preferences.enable_failure_email:
            print('  WARNING: failure email is on but there is no owner address, so no\n'
                  '  mail can be sent. The checks run; nobody hears about a failure.\n'
                  '  Fixing this means recreating it owned by a person, which only the\n'
                  '  BigQuery console can do -- see DEPLOY.md step 8.')
        if not matches:
            print('  WARNING: the deployed query is not the one in this repo.\n'
                  '    python deploy/create_watchdog.py --update --service-account <SA>')
        print('Rerun with --update to push a changed watchdog.sql.')
        return

    if existing is None and not args.recreate:
        print('No watchdog is installed in this project right now.')

    # Only creating or updating needs to decide who runs it.
    if not service_account and not args.as_me:
        sys.exit('Pass --service-account, or set WATCHDOG_SERVICE_ACCOUNT. Running the '
                 'watchdog as a person ties the alert to that person\'s credentials; '
                 'pass --as-me if you have decided that is what you want.')

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
        try:
            result = client.create_transfer_config(
                request=bigquery_datatransfer.CreateTransferConfigRequest(
                    parent=parent,
                    transfer_config=config,
                    service_account_name=service_account or ''))
        except Exception as failure:
            if replaced is None:
                raise
            # Put back exactly what was deleted. Restore with the service
            # account if one was named: if creating as a person just failed,
            # restoring as that same person will fail the same way.
            try:
                client.create_transfer_config(
                    request=bigquery_datatransfer.CreateTransferConfigRequest(
                        parent=parent,
                        transfer_config=bigquery_datatransfer.TransferConfig(
                            display_name=replaced.display_name,
                            data_source_id=replaced.data_source_id,
                            params=dict(replaced.params),
                            schedule=replaced.schedule,
                            email_preferences=replaced.email_preferences),
                        service_account_name=args.service_account or ''))
                note = 'The previous watchdog was put back unchanged.'
            except Exception as rollback_failure:
                note = ('COULD NOT PUT THE PREVIOUS WATCHDOG BACK -- there is no\n'
                        'watchdog installed right now, and nothing is watching the\n'
                        'writer lease. Reinstall one before scheduling anything:\n'
                        '  python deploy/create_watchdog.py --service-account <runtime SA>\n'
                        f'  (rollback error: {rollback_failure})')
            sys.exit(f'Create failed. {note}\n\n{type(failure).__name__}: {failure}')
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
