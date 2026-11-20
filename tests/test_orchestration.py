import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from psycopg import sql

from orchestration.contract import artifact_id, months_between, scoped_path
from orchestration.commands import execute
from orchestration.recovery import demonstrate, fixture, state
from serving.database import connect, migrate


class OrchestrationContractTests(unittest.TestCase):
    def test_month_window_crosses_year_and_rejects_unbounded_or_invalid_input(self):
        self.assertEqual(months_between('2023-12','2024-02'),['2023-12','2024-01','2024-02'])
        for start,end in [('2024-02','2024-01'),('2024-01','2025-01'),('2024-13','2024-13'),('2024-01; echo secret','2024-02')]:
            with self.subTest(start=start),self.assertRaises(ValueError): months_between(start,end)

    def test_artifact_paths_cannot_escape_orchestrated_outputs(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            self.assertEqual(scoped_path(root,root/'run'),root/'run')
            with self.assertRaises(ValueError): scoped_path(root,root.parent/'other')
        with self.assertRaises(ValueError): artifact_id('../outside')
        self.assertEqual(artifact_id('a'*64),'a'*64)

    def test_changed_verified_identity_is_rejected_before_publication(self):
        with patch('serving.artifacts.prepare',return_value=fixture('mock',1)),patch('serving.publish.publish') as publish:
            with self.assertRaisesRegex(ValueError,'nothing was published'):
                execute('publish-replay',experiment_id='a'*64,expected_run_id='b'*64,activate=True)
            publish.assert_not_called()

    def test_recovery_cannot_target_application_database(self):
        with self.assertRaisesRegex(ValueError,'dedicated database'):
            demonstrate('test',database='fleetpulse')


@unittest.skipUnless(os.environ.get('FLEETPULSE_INTEGRATION')=='1','requires local PostgreSQL')
class RecoveryIntegrationTests(unittest.TestCase):
    def test_failure_retry_idempotent_rollback(self):
        database='fleetpulse_test_'+uuid.uuid4().hex
        with connect('admin','postgres') as conn:
            conn.autocommit=True
            conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        try:
            migrate(database)
            with self.assertRaisesRegex(RuntimeError,'Intentional first-attempt'):
                demonstrate('test-run',1,database=database)
            candidate=fixture('test-run',2)
            self.assertEqual(state(database,candidate.run['run_id'])[:2],(0,0))
            self.assertEqual(demonstrate('test-run',2,database=database)['status'],'recovered')
            self.assertEqual(demonstrate('test-run',2,database=database)['status'],'recovered')
            demonstrate('test-run',rollback=True,database=database)
            before=state(database,candidate.run['run_id'])
            demonstrate('test-run',rollback=True,database=database)
            self.assertEqual(state(database,candidate.run['run_id']),before)
            self.assertEqual(before[:2],(1,3))
        finally:
            with connect('admin','postgres') as conn:
                conn.autocommit=True
                conn.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))
