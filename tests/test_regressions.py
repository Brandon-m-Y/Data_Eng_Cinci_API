"""Offline regression tests. No credentials or network calls are needed."""

import re
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests

import Get_Data as extract
import Load_to_GBQ as staging
import Run_Pipeline as pipeline
from Pipeline_Config import Config


class StagingTests(unittest.TestCase):
    def test_preserves_people_invalid_values_and_nulls(self):
        raw = pd.DataFrame([
            {':id': 'person-a', 'instanceid': 'crash-a', 'age': 'BB'},
            {':id': 'person-b', 'instanceid': 'crash-a', 'age': None},
        ])
        result = staging.build_staging_frame(raw)
        self.assertEqual(list(result.columns), staging.STAGING_COLUMNS)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.loc[0, 'age'], 'BB')
        self.assertTrue(pd.isna(result.loc[1, 'age']))
        self.assertEqual(result.socrata_id.tolist(), ['person-a', 'person-b'])

    def test_python_columns_match_sql_staging_schema(self):
        sql = Path('Star_Schema_ETL.sql').read_text(encoding='utf-8')
        ddl = re.search(
            r'CREATE TABLE IF NOT EXISTS [^\n]+stg_crash_person[^\n]* \((.*?)\n\);',
            sql, re.S,
        )
        self.assertIsNotNone(ddl)
        self.assertEqual(
            re.findall(r'^\s*(\w+)\s+STRING', ddl.group(1), re.M),
            staging.STAGING_COLUMNS,
        )

    def test_fact_ddl_columns_all_appear_in_the_section_5_insert(self):
        """A column added to the fact table but not to Section 5's INSERT is
        silently never populated. Only a load would reveal it, so check here."""
        sql = Path('Star_Schema_ETL.sql').read_text(encoding='utf-8')
        ddl = re.search(
            r'CREATE TABLE IF NOT EXISTS [^\n]+fact_crash_person\s*\((.*?)\n\)\n',
            sql, re.S,
        )
        self.assertIsNotNone(ddl)
        declared = re.findall(
            r'^\s*(\w+)\s+(?:INT64|STRING|FLOAT64|DATE|DATETIME|TIMESTAMP|BOOL)\b',
            ddl.group(1), re.M,
        )
        self.assertGreater(len(declared), 20)

        insert = re.search(
            r'INSERT INTO [^\n]+fact_crash_person\s*\((.*?)\)\s*\n\s*SELECT',
            sql, re.S,
        )
        self.assertIsNotNone(insert)
        inserted = [c.strip() for c in insert.group(1).replace('\n', ' ').split(',') if c.strip()]

        self.assertEqual(sorted(declared), sorted(inserted))
        self.assertEqual(len(inserted), len(set(inserted)), 'duplicate column in the INSERT list')


class ExtractTests(unittest.TestCase):
    stamp = '2026-09-19T12:00:00Z'

    def setUp(self):
        env = patch.dict('os.environ', {'SOCRATA_APP_TOKEN': 'test-token'})
        env.start()
        self.addCleanup(env.stop)
        metadata = patch.object(extract, '_metadata', return_value=(1000, frozenset(extract.COLUMNS)))
        self.metadata = metadata.start()
        self.addCleanup(metadata.stop)

    def row(self, person):
        return {':id': person, 'instanceid': 'crash-a', ':updated_at': self.stamp}

    def probe(self, count=2, stamp=None):
        return [{'row_count': str(count), 'stamped_rows': str(count),
                 'min_stamp': stamp or self.stamp, 'max_stamp': stamp or self.stamp}]

    def test_complete_pagination_retains_both_people(self):
        with patch.object(extract, '_query', side_effect=[
            self.probe(), [self.row('a')], [self.row('b')], [], self.probe(),
        ]) as query:
            result = extract.fetch(since='2026-06-01', page_size=1)
        self.assertEqual(len(result), 2)
        self.assertEqual(result['instanceid'].nunique(), 1)
        self.assertEqual(result.attrs['snapshot']['expected_rows'], 2)
        self.assertIn("crashdate >= '2026-06-01T00:00:00'", query.call_args_list[1].args[0])
        self.assertEqual(query.call_args_list[2].kwargs['page'], 2)

    def test_partial_people_fail_even_when_crash_is_present(self):
        with patch.object(extract, '_query', side_effect=[
            self.probe(), [self.row('a')], self.probe(),
        ]):
            with self.assertRaisesRegex(extract.InvalidSnapshot, 'Expected 2'):
                extract.fetch()

    def test_duplicate_ids_cannot_fill_missing_person(self):
        with patch.object(extract, '_query', side_effect=[
            self.probe(), [self.row('a'), self.row('a')], self.probe(),
        ]):
            with self.assertRaisesRegex(extract.InvalidSnapshot, 'Repeated source IDs'):
                extract.fetch()

    def test_republish_between_probes_fails(self):
        with patch.object(extract, '_query', side_effect=[
            self.probe(), [self.row('a'), self.row('b')],
            self.probe(stamp='2026-09-19T13:00:00Z'),
        ] * 3) as query:
            with self.assertRaisesRegex(extract.InvalidSnapshot, 'changed during extraction'):
                extract.fetch()
        self.assertEqual(query.call_count, 9)

    def test_metadata_change_restarts_whole_extract(self):
        self.metadata.side_effect = [(1000, frozenset()), (1001, frozenset()),
                                     (1001, frozenset()), (1001, frozenset())]
        with patch.object(extract, '_query', side_effect=[
            self.probe(), [self.row('a'), self.row('b')], self.probe(),
        ] * 2) as query:
            result = extract.fetch()
        self.assertEqual(query.call_count, 6)
        self.assertEqual(result.attrs['rows_updated_at'], 1001)

    def test_metadata_after_read_follows_last_count(self):
        events = []
        self.metadata.side_effect = lambda session: (events.append('metadata') or (1000, frozenset()))
        def query(sql, **kwargs):
            if 'count(*)' in sql:
                events.append('count')
                return self.probe()
            events.append('page')
            return [self.row('a'), self.row('b')]
        with patch.object(extract, '_query', side_effect=query):
            extract.fetch()
        self.assertEqual(events, ['metadata', 'count', 'page', 'count', 'metadata'])

    def test_missing_identity_and_wrong_stamp_fail(self):
        for change in ({':id': None}, {'instanceid': ''},
                       {':updated_at': None}, {':updated_at': '2026-09-18T00:00:00Z'}):
            with self.subTest(change=change):
                frame = pd.DataFrame([{**self.row('a'), **change}])
                with self.assertRaises(extract.InvalidSnapshot):
                    extract.validate_frame(frame, 1, self.stamp)

    def test_empty_source_fails(self):
        with patch.object(extract, '_query', return_value=[{'row_count': '0', 'stamped_rows': '0'}]):
            with self.assertRaises(extract.InvalidSnapshot):
                extract.fetch()

    def test_page_size_rejected_before_query(self):
        for size in (0, -1, True, '100', 1.5):
            with self.subTest(size=size), patch.object(extract, '_query') as query:
                with self.assertRaises(ValueError):
                    extract.fetch(page_size=size)
                query.assert_not_called()

    def test_invalid_date_never_contacts_source(self):
        with patch.object(extract.requests, 'post') as post:
            with self.assertRaises(ValueError):
                extract.fetch(since="2026-01-01' OR true")
            post.assert_not_called()

    def test_http_error_is_not_an_empty_success(self):
        response = Mock()
        response.raise_for_status.side_effect = requests.HTTPError('upstream failed')
        session = Mock()
        session.post.return_value = response
        with self.assertRaises(requests.HTTPError):
            extract._query('SELECT *', session=session)

    def test_retries_include_read_only_post(self):
        with extract._new_session() as session:
            retries = session.adapters['https://'].max_retries
            self.assertIn('POST', retries.allowed_methods)
            self.assertEqual(retries.total, 2)
            self.assertIn(429, retries.status_forcelist)
            self.assertNotIn(400, retries.status_forcelist)


class MetadataTests(unittest.TestCase):
    def metadata(self):
        return {'rowsUpdatedAt': 1000,
                'columns': [{'fieldName': c} for c in extract.COLUMNS if not c.startswith(':')]}

    def session(self, payload):
        session = Mock()
        session.get.return_value.json.return_value = payload
        return session

    def test_missing_source_column_fails(self):
        data = self.metadata()
        data['columns'] = [c for c in data['columns'] if c['fieldName'] != 'age']
        with self.assertRaisesRegex(extract.InvalidSnapshot, 'age'):
            extract._metadata(self.session(data))

    def test_new_column_warns_without_discarding_snapshot(self):
        data = self.metadata()
        data['columns'].append({'fieldName': 'new_attribute'})
        with self.assertWarnsRegex(UserWarning, 'new_attribute'):
            stamp, fields = extract._metadata(self.session(data))
        self.assertEqual(stamp, 1000)
        self.assertIn('new_attribute', fields)

    def test_missing_metadata_timestamp_fails(self):
        data = self.metadata()
        del data['rowsUpdatedAt']
        with self.assertRaises(extract.InvalidSnapshot):
            extract._metadata(self.session(data))

    def test_unexpected_query_payload_fails(self):
        for payload in ({'error': 'bad request'}, ['not-a-row'], None):
            with self.subTest(payload=payload):
                session = Mock()
                session.post.return_value.json.return_value = payload
                with self.assertRaises(extract.InvalidSnapshot):
                    extract._query('SELECT *', session=session)


class SqlSectionsTests(unittest.TestCase):
    def test_runner_finds_required_sections(self):
        sections = pipeline.load_sql_sections('Star_Schema_ETL.sql')
        self.assertTrue({1, 2, 3, 4, 5, 6, 7, 9, 10, 11}.issubset(sections))
        panel = pipeline.load_sql_sections('ML_Crash_Panel.sql')
        self.assertTrue({1, 2, 3}.issubset(panel))

    def test_rendering_moves_every_table_reference(self):
        config = Config('test-project', 'crashes_test')
        for path in ('Star_Schema_ETL.sql', 'ML_Crash_Panel.sql'):
            with self.subTest(path=path):
                rendered = '\n'.join(pipeline.load_sql_sections(path, config).values())
                self.assertIsNone(re.search(r'\bcrashes\.[A-Za-z_]', rendered))
                self.assertIn('crashes_test.', rendered)
        # The panel's `crashes` column must survive rendering
        panel = pipeline.load_sql_sections('ML_Crash_Panel.sql', config)[2]
        self.assertIn('AVG(g.crashes)', panel)

    def test_fact_transaction_is_fenced_by_the_lease(self):
        section5 = pipeline.load_sql_sections('Star_Schema_ETL.sql')[5]
        transaction = section5[section5.index('BEGIN TRANSACTION'):section5.index('COMMIT TRANSACTION')]
        self.assertRegex(transaction, r'UPDATE crashes\.etl_lease[^;]*holder = @load_id')
        self.assertIn('INSERT INTO crashes.etl_load_log', transaction)

    def test_section_parameters_match_the_sql(self):
        # Every @name a section uses is supplied, and nothing unused is sent
        sections = pipeline.load_sql_sections('Star_Schema_ETL.sql')
        supplied = {
            3: pipeline.section3_params(None, 1, None),
            5: pipeline.section5_params(None, False, False, 'id', 'delta', None, 0),
        }
        for n, params in supplied.items():
            with self.subTest(section=n):
                code = '\n'.join(line.split('--')[0] for line in sections[n].splitlines())
                used = set(re.findall(r'@(\w+)', code)) - {'row_count', 'error'}
                self.assertEqual(used, {p.name for p in params})


class RunnerTests(unittest.TestCase):
    def parse(self, *argv):
        with patch('sys.argv', ['Run_Pipeline.py', *argv]), \
             patch.object(pipeline, 'Config') as config, \
             patch.object(pipeline.gbq, 'get_client'):
            config.from_env.side_effect = RuntimeError('stop after argument parsing')
            pipeline.main()

    def test_reprocess_requires_full(self):
        for argv in (('--reprocess',), ('--delta', '--reprocess'), ('--setup', '--reprocess')):
            with self.subTest(argv=argv), self.assertRaises(SystemExit):
                self.parse(*argv)

    def test_allow_deletions_requires_a_load(self):
        for argv in (('--allow-deletions',), ('--panel', '--allow-deletions'),
                     ('--setup', '--allow-deletions')):
            with self.subTest(argv=argv), self.assertRaises(SystemExit):
                self.parse(*argv)

    def test_nothing_to_do_is_an_error(self):
        with self.assertRaises(SystemExit):
            self.parse()

    def test_valid_flags_reach_configuration(self):
        for argv in (('--delta',), ('--full', '--reprocess'), ('--setup',), ('--panel',),
                     ('--full', '--allow-deletions')):
            with self.subTest(argv=argv), self.assertRaisesRegex(RuntimeError, 'stop after'):
                self.parse(*argv)


class ConfigTests(unittest.TestCase):
    def test_all_identifiers_use_the_selected_project_and_dataset(self):
        config = Config('test-project', 'test_crashes')
        result = config.render('SELECT * FROM crashes.stg_crash_person JOIN `crashes.fact_crash_person` USING(instanceid)')
        self.assertIn('test_crashes.stg_crash_person', result)
        self.assertIn('`test_crashes.fact_crash_person`', result)
        self.assertEqual(config.table('stg_crash_person'), 'test-project.test_crashes.stg_crash_person')

    def test_rejects_identifier_injection_and_unsupported_staging(self):
        for dataset in ('bad.name', 'x`; DROP TABLE t; --', '', 'a-b'):
            with self.subTest(dataset=dataset), self.assertRaises(ValueError):
                Config('test-project', dataset)
        with patch.dict('os.environ', {'GCP_PROJECT_ID': 'test-project', 'GCP_STAGING_TABLE': 'other'}):
            with self.assertRaisesRegex(ValueError, 'no longer configurable'):
                Config.from_env()


class PanelLabelTests(unittest.TestCase):
    """The panel label goes through a query job, so its SQL is built by hand."""

    def test_labels_are_literals_not_parameters(self):
        # OPTIONS is evaluated at parse time: a @parameter there fails the job
        # with "Found unsupported function call 'ARRAY[...]'".
        sql = pipeline.panel_label_sql('p.d.ml_crash_panel', {'load_id': 'abc123'})
        self.assertIn('("load_id", "abc123")', sql)
        self.assertNotIn('@', sql)

    def test_existing_labels_are_kept(self):
        sql = pipeline.panel_label_sql('p.d.t', {'owner': 'data-eng', 'load_id': 'abc'})
        self.assertIn('("owner", "data-eng")', sql)
        self.assertIn('("load_id", "abc")', sql)

    def test_empty_value_is_allowed(self):
        self.assertIn('("env", "")', pipeline.panel_label_sql('p.d.t', {'env': ''}))

    def test_anything_outside_the_label_grammar_is_refused(self):
        for labels in ({'load_id': 'a") , ("x'}, {'Bad Key': 'v'}, {'k': 'a' * 64}):
            with self.assertRaisesRegex(ValueError, 'refusing to inline'):
                pipeline.panel_label_sql('p.d.t', labels)


if __name__ == '__main__':
    unittest.main()
