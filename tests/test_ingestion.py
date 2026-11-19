import json
import shutil
import tempfile
import unittest
from pathlib import Path

import duckdb

from ingestion.pipeline import run


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.root = self.base / 'snapshots'
        self.trips = self.base / 'input.parquet'
        self.zones = self.base / 'input.csv'
        self.zones.write_text('LocationID,Borough,Zone,service_zone\n1,Manhattan,Example,Yellow Zone\n264,Unknown,N/A,N/A\n')
        self.make_trips()

    def make_trips(self, malformed=False):
        with duckdb.connect() as con:
            query = 'SELECT 1 AS wrong_column' if malformed else """SELECT * FROM (VALUES
                (TIMESTAMP '2024-01-01 01:00:00', 1),
                (TIMESTAMP '2024-01-01 01:00:00', 1),
                (TIMESTAMP '2024-02-01 01:00:00', 1),
                (TIMESTAMP '2024-01-01 02:00:00', 264),
                (NULL, 1)) t(tpep_pickup_datetime, PULocationID)"""
            con.sql(query).write_parquet(str(self.trips))

    def fetch(self, url, target):
        shutil.copyfile(self.trips if url.endswith('.parquet') else self.zones, target)
        return {'url': url, 'downloaded_at': 'fixture'}

    def test_rerun_and_partition_conservation(self):
        self.assertEqual(run('2024-01', self.root, self.fetch)['status'], 'created')
        manifest = (self.root / '2024-01/manifest.json').read_bytes()
        self.assertEqual(run('2024-01', self.root, self.fetch)['status'], 'unchanged')
        self.assertEqual(manifest, (self.root / '2024-01/manifest.json').read_bytes())
        report = json.loads((self.root / '2024-01/profile.json').read_text())
        self.assertEqual(report['accepted'], 2)  # Identical pickup pairs retained.
        self.assertEqual(report['rows'], report['accepted'] + sum(report['rejected'].values()))
        self.assertFalse((self.root / '2024-01.lock').exists())

    def test_upstream_change_preserves_snapshot(self):
        run('2024-01', self.root, self.fetch)
        original = (self.root / '2024-01/manifest.json').read_bytes()
        self.zones.write_text(self.zones.read_text() + '2,Queens,Another,Yellow Zone\n')
        with self.assertRaisesRegex(ValueError, 'Upstream source changed'):
            run('2024-01', self.root, self.fetch)
        self.assertEqual(original, (self.root / '2024-01/manifest.json').read_bytes())

    def test_bad_schema_never_publishes_and_can_retry(self):
        self.make_trips(malformed=True)
        with self.assertRaisesRegex(ValueError, 'Missing required column'):
            run('2024-01', self.root, self.fetch)
        self.assertEqual(list(self.root.iterdir()), [])
        self.make_trips()
        self.assertEqual(run('2024-01', self.root, self.fetch)['status'], 'created')

    def test_failed_download_never_publishes(self):
        def fail(url, target):
            target.write_bytes(b'partial')
            raise OSError('injected interruption')
        with self.assertRaisesRegex(OSError, 'injected interruption'):
            run('2024-01', self.root, fail)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_local_corruption_is_detected(self):
        run('2024-01', self.root, self.fetch)
        (self.root / '2024-01/silver.parquet').write_bytes(b'bad')
        with self.assertRaisesRegex(ValueError, 'Local artifact corrupted'):
            run('2024-01', self.root, self.fetch)

    def test_existing_lock_is_not_removed(self):
        self.root.mkdir()
        lock = self.root / '2024-01.lock'
        lock.touch()
        with self.assertRaises(FileExistsError):
            run('2024-01', self.root, self.fetch)
        self.assertTrue(lock.exists())


if __name__ == '__main__':
    unittest.main()
