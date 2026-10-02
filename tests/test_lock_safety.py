"""Writer safety when a process pauses or BigQuery's outcome is unknown."""

import unittest
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from types import SimpleNamespace

import pandas as pd

from google.api_core.exceptions import NotFound, ServiceUnavailable

import Run_Pipeline as pipeline
from Pipeline_Config import Config


class LockSafetyTests(unittest.TestCase):
    def test_two_acquirers_have_one_winner(self):
        mutex = threading.Lock()
        barrier = threading.Barrier(2)
        state = {'holder': None}

        def attempt(owner):
            client = Mock()
            lease = pipeline.Lease(client, Config('test-project'), owner)

            def update(sql, *params):
                self.assertIn('holder IS NULL', sql)
                barrier.wait(timeout=5)
                with mutex:
                    if state['holder'] is not None:
                        return 0
                    state['holder'] = owner
                    return 1

            client.query.return_value.result.side_effect = lambda: [
                (state['holder'], datetime.now(timezone.utc), None)]
            with patch.object(lease, '_update', side_effect=update):
                try:
                    lease.acquire()
                except pipeline.LeaseError:
                    return False
                return lease.held

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(attempt, ['owner-a', 'owner-b']))
        self.assertEqual(sum(results), 1)

    def lease(self):
        client = Mock()
        return pipeline.Lease(client, Config('test-project'), 'new-owner'), client

    def test_expired_owner_cannot_be_replaced_while_reserved_job_is_absent(self):
        lease, client = self.lease()
        client.query.return_value.result.return_value = [
            ('old-owner', datetime.now(timezone.utc), 'not-submitted-yet', True)]
        client.get_job.side_effect = NotFound('not submitted yet')
        with patch.object(lease, '_update', return_value=0) as update:
            with self.assertRaises(pipeline.LeaseError):
                lease.acquire()
            self.assertIn('holder IS NULL', update.call_args.args[0])
            self.assertNotIn('expires_at', update.call_args.args[0])
        self.assertFalse(lease.held)

    def test_expired_owner_still_requires_recovery_after_job_is_done(self):
        # The old process may be paused between stages, not dead.
        lease, client = self.lease()
        client.query.return_value.result.return_value = [
            ('old-owner', datetime.now(timezone.utc), 'finished-job', True)]
        client.get_job.return_value.state = 'DONE'
        with patch.object(lease, '_update', return_value=0) as update:
            with self.assertRaises(pipeline.LeaseError):
                lease.acquire()
            self.assertIn('holder IS NULL', update.call_args.args[0])

    def test_unknown_job_visibility_keeps_lock(self):
        lease, client = self.lease()
        lease.held = True
        lease.job_id = 'ambiguous-submission'
        lease.outcome = lease.UNKNOWN
        client.get_job.side_effect = NotFound('job not yet visible')
        with patch.object(lease, '_update', return_value=1) as update:
            lease.release()
            update.assert_not_called()
        self.assertTrue(lease.held)

    def test_network_failure_while_checking_job_keeps_lock(self):
        lease, client = self.lease()
        lease.held = True
        lease.job_id = 'possibly-running'
        lease.outcome = lease.UNKNOWN
        client.get_job.side_effect = ServiceUnavailable('try later')
        with patch.object(lease, '_update', return_value=1) as update:
            lease.release()
            update.assert_not_called()
        self.assertTrue(lease.held)

    def test_running_job_keeps_lock(self):
        lease, client = self.lease()
        lease.held = True
        lease.job_id = 'running'
        lease.outcome = lease.UNKNOWN
        client.get_job.return_value.state = 'RUNNING'
        with patch.object(lease, '_update', return_value=1) as update:
            lease.release()
            update.assert_not_called()
        self.assertTrue(lease.held)

    def test_terminal_job_allows_owner_specific_release(self):
        lease, client = self.lease()
        lease.held = True
        lease.job_id = 'finished'
        lease.outcome = lease.UNKNOWN
        client.get_job.return_value.state = 'DONE'
        with patch.object(lease, '_update', return_value=1) as update:
            lease.release()
            self.assertIn('holder = @load_id', update.call_args.args[0])
        self.assertFalse(lease.held)

    def test_reserved_but_never_submitted_allows_release(self):
        lease, client = self.lease()
        lease.held = True
        lease.job_id = 'reserved-only'
        lease.outcome = lease.NEVER_SUBMITTED
        with patch.object(lease, '_update', return_value=1):
            lease.release()
        client.get_job.assert_not_called()
        self.assertFalse(lease.held)

    def test_ambiguous_control_write_keeps_lock(self):
        lease, client = self.lease()
        lease.held = True
        lease.control_uncertain = True
        with patch.object(lease, '_update') as update:
            lease.release()
            update.assert_not_called()
        self.assertTrue(lease.held)

    def test_control_submission_exception_marks_outcome_unknown(self):
        lease, client = self.lease()
        client.query.side_effect = ConnectionError('lost submit response')
        with self.assertRaises(ConnectionError):
            lease._update('UPDATE lock SET holder = @load_id')
        self.assertTrue(lease.control_uncertain)
        self.assertFalse(lease.resolve_outcome())

    def test_terminal_control_failure_does_not_mark_outcome_unknown(self):
        lease, client = self.lease()
        job = client.query.return_value
        job.state = 'DONE'
        job.result.side_effect = RuntimeError('confirmed query failure')
        with self.assertRaises(RuntimeError):
            lease._update('UPDATE lock SET holder = @load_id')
        self.assertFalse(lease.control_uncertain)

    def test_sigterm_during_submit_keeps_lock(self):
        lease, client = self.lease()
        lease.held = True
        client.query.side_effect = SystemExit('SIGTERM')
        client.get_job.side_effect = NotFound('submission is ambiguous')
        def reserve(stage):
            lease.job_id = 'reserved-before-sigterm'
            return lease.job_id
        with patch.object(lease, 'renew', side_effect=reserve):
            with self.assertRaises(SystemExit):
                pipeline.run_section(client, lease, {1: 'SELECT 1'}, 1, 'test')
        with patch.object(lease, '_update') as update:
            lease.release()
            update.assert_not_called()
        self.assertEqual(lease.outcome, lease.UNKNOWN)

    def test_staging_validation_fails_before_reservation(self):
        lease, client = self.lease()
        lease.held = True
        with patch.object(lease, 'renew') as reserve:
            with self.assertRaisesRegex(ValueError, 'manifest'):
                pipeline.gbq.load_staging(pd.DataFrame(), client, Config('test-project'), lease=lease)
            reserve.assert_not_called()
        client.load_table_from_dataframe.assert_not_called()
        self.assertEqual(lease.outcome, lease.NEVER_SUBMITTED)

    def test_invalid_manifest_frame_is_rejected_before_reservation(self):
        lease, client = self.lease()
        frame = pd.DataFrame([{'instanceid': 'crash-a'}])
        frame.attrs['snapshot'] = {'expected_rows': 1, 'publish_stamp': '2026-09-19T12:00:00Z'}
        with patch.object(lease, 'renew') as reserve:
            with self.assertRaises(ValueError):
                pipeline.gbq.load_staging(frame, client, Config('test-project'), lease=lease)
            reserve.assert_not_called()
        client.load_table_from_dataframe.assert_not_called()
        self.assertEqual(lease.outcome, lease.NEVER_SUBMITTED)

    def test_bootstrap_seeds_atomically_and_fence_has_no_expiry(self):
        sections = pipeline.load_sql_sections('Star_Schema_ETL.sql')
        bootstrap = '\n'.join(line.split('--')[0] for line in sections[10].splitlines())
        self.assertRegex(bootstrap, r'CREATE TABLE IF NOT EXISTS crashes.etl_lease AS\s+SELECT')
        self.assertNotRegex(bootstrap, r'\bINSERT\s+INTO\b')
        transaction = sections[5].split('BEGIN TRANSACTION;')[1].split('COMMIT TRANSACTION;')[0]
        self.assertNotIn('expires_at', transaction)
        self.assertNotIn('@lease_minutes', transaction)


class FailureLogSafetyTests(unittest.TestCase):
    def simulate_fact_result_error(self, server_result):
        client = Mock()
        if isinstance(server_result, Exception):
            client.get_job.side_effect = server_result
        else:
            client.get_job.return_value = server_result
        config = Config('test-project')
        lease = pipeline.Lease(client, config, 'owner')
        lease.held = True
        frame = pd.DataFrame([{'instanceid': 'crash-a'}])
        frame.attrs['snapshot'] = {'expected_rows': 1, 'publish_stamp': '2026-09-19T12:00:00+00:00'}
        args = SimpleNamespace(reprocess=False, delta=False, allow_deletions=False)
        def section(*args, **kwargs):
            if args[3] == 5:
                lease.job_id = 'fact-job'
                lease.submitting()
                raise ConnectionError('lost fact response')
            return Mock()
        with patch.object(pipeline, 'fetch', return_value=frame), \
             patch.object(pipeline.gbq, 'load_staging', return_value=1), \
             patch.object(pipeline, 'run_section', side_effect=section), \
             patch.object(pipeline, 'record_failure') as record:
            with self.assertRaises(ConnectionError):
                pipeline.run_load(client, config, lease, {}, args, datetime.now(timezone.utc))
        return lease, record

    def test_unknown_fact_outcome_never_mutates_log(self):
        lease, record = self.simulate_fact_result_error(NotFound('not yet visible'))
        record.assert_not_called()
        self.assertEqual(lease.outcome, lease.UNKNOWN)

    def test_terminal_failed_fact_can_be_logged(self):
        lease, record = self.simulate_fact_result_error(Mock(state='DONE', error_result={'reason': 'invalidQuery'}))
        record.assert_called_once()
        self.assertEqual(lease.outcome, lease.TERMINAL)
        self.assertIsNotNone(lease.last_job.error_result)

    def test_terminal_successful_fact_is_recognized_as_committed(self):
        lease, record = self.simulate_fact_result_error(Mock(state='DONE', error_result=None))
        record.assert_called_once()
        self.assertEqual(lease.outcome, lease.TERMINAL)
        self.assertIsNone(lease.last_job.error_result)


if __name__ == '__main__':
    unittest.main()
