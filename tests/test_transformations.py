import shutil
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb

from ingestion.pipeline import run
from transformations.build import build
from transformations.calendar import calendar_rows


class CalendarTests(unittest.TestCase):
    def test_both_transition_days_are_masked(self):
        for month, transition_day in [('2024-03', 10), ('2024-11', 3)]:
            rows = list(calendar_rows([month]))
            masked = [t for t, _, flag in rows if flag]
            self.assertEqual(len(masked), 24)
            self.assertEqual({t.day for t in masked}, {transition_day})
        self.assertFalse(any(flag for _, _, flag in calendar_rows(['2024-01'])))

    def test_missing_month_is_explicit(self):
        rows = list(calendar_rows(['2024-01', '2024-03']))
        february = [present for t, present, _ in rows if t.month == 2]
        self.assertEqual(len(february), 29 * 24)
        self.assertFalse(any(february))


class AnalyticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.tmp.cleanup)
        cls.base = Path(cls.tmp.name)
        cls.origin = datetime(2024, 1, 9, 10)
        cls.missing = datetime(2024, 1, 4, 12)
        cls.snapshots = cls.make_snapshot('original', False)
        cls.result = build(['2024-01'], cls.snapshots, cls.base / 'gold')
        cls.output = Path(cls.result['path'])

    @classmethod
    def make_snapshot(cls, name, change_future):
        folder = cls.base / name
        folder.mkdir()
        parquet = folder / 'trips.parquet'
        zones = folder / 'zones.csv'
        zones.write_text('LocationID,Borough,Zone,service_zone\n1,Manhattan,A,Yellow Zone\n2,Queens,B,Yellow Zone\n')
        with duckdb.connect() as con:
            con.execute('''CREATE TABLE trips AS
                WITH hours AS (
                    SELECT h, TIMESTAMP '2024-01-01' + h * INTERVAL '1 hour' AS hour_local
                    FROM range(744) t(h)
                )
                SELECT hour_local + INTERVAL '15 minutes' AS tpep_pickup_datetime,
                       1::INTEGER AS PULocationID
                FROM hours, range(h % 5 + 1 + CASE WHEN ? AND hour_local >= ? THEN 100 ELSE 0 END)
                WHERE hour_local != ?''', [change_future, cls.origin, cls.missing])
            con.table('trips').write_parquet(str(parquet))
        def fetch(url, target):
            shutil.copyfile(parquet if url.endswith('.parquet') else zones, target)
            return {'url': url}
        snapshots = folder / 'snapshots'
        run('2024-01', snapshots, fetch)
        return snapshots

    def connect(self):
        return duckdb.connect(str(self.output / 'analytics.duckdb'), read_only=True)

    def test_dense_grid_zero_and_unknown_are_distinct(self):
        with self.connect() as con:
            self.assertEqual(con.execute('SELECT count(*) FROM zone_hours').fetchone()[0], 2 * 31 * 24)
            self.assertEqual(con.execute("SELECT pickup_count FROM zone_hours WHERE zone_id=2 AND hour_local='2024-01-01' ").fetchone()[0], 0)
            self.assertEqual(con.execute('SELECT count(*) FROM zone_hours WHERE hour_local=? AND pickup_count IS NULL', [self.missing]).fetchone()[0], 2)
            self.assertEqual(con.execute('SELECT sum(recorded_pickups) FROM zone_hours').fetchone()[0],
                             con.execute('SELECT count(*) FROM raw.pickups').fetchone()[0])

    def test_hand_computed_lags_and_complete_rolling_window(self):
        h = int((self.origin - datetime(2024, 1, 1)).total_seconds() / 3600)
        with self.connect() as con:
            got = con.execute('''SELECT lag_1h, lag_24h, lag_168h, mean_previous_24h, history_ready
                FROM hourly_features WHERE zone_id=1 AND forecast_origin_local=?''', [self.origin]).fetchone()
            self.assertEqual(got[:3], tuple((h - lag) % 5 + 1 for lag in (1, 24, 168)))
            self.assertAlmostEqual(got[3], sum(i % 5 + 1 for i in range(h-24, h)) / 24)
            self.assertTrue(got[4])
            incomplete = con.execute('''SELECT lag_1h, mean_previous_24h, observed_hours_24
                FROM hourly_features WHERE zone_id=1 AND forecast_origin_local=?''', [self.missing + timedelta(hours=1)]).fetchone()
            self.assertEqual(incomplete, (None, None, 23))
            first = con.execute("SELECT lag_1h, lag_168h, history_ready FROM hourly_features WHERE zone_id=1 ORDER BY forecast_origin_local LIMIT 1").fetchone()
            self.assertEqual(first, (None, None, False))

    def test_future_mutation_does_not_change_features_at_or_before_origin(self):
        changed_sources = self.make_snapshot('changed', True)
        changed = build(['2024-01'], changed_sources, self.base / 'gold')
        query = 'SELECT * FROM hourly_features WHERE forecast_origin_local <= ? ORDER BY zone_id, forecast_origin_local'
        with self.connect() as original, duckdb.connect(str(Path(changed['path']) / 'analytics.duckdb'), read_only=True) as revised:
            self.assertEqual(original.execute(query, [self.origin]).fetchall(), revised.execute(query, [self.origin]).fetchall())
            query = 'SELECT pickup_count FROM zone_hours WHERE zone_id=1 AND hour_local=?'
            self.assertNotEqual(original.execute(query, [self.origin]).fetchone(), revised.execute(query, [self.origin]).fetchone())

    def test_rerun_preserves_manifest(self):
        before = (self.output / 'manifest.json').read_bytes()
        repeated = build(['2024-01'], self.snapshots, self.base / 'gold')
        self.assertEqual(repeated['status'], 'unchanged')
        self.assertEqual(before, (self.output / 'manifest.json').read_bytes())
        self.assertGreater(self.result['dbt_tests_passed'], 0)

    def test_dbt_failure_does_not_publish(self):
        root = self.base / 'failed-build'
        with patch('transformations.build.run_dbt', side_effect=RuntimeError('injected dbt failure')):
            with self.assertRaisesRegex(RuntimeError, 'injected dbt failure'):
                build(['2024-01'], self.snapshots, root)
        self.assertEqual(list(root.iterdir()), [])

    def test_changed_input_is_rejected_before_dbt(self):
        corrupt = self.base / 'corrupt-input'
        shutil.copytree(self.snapshots, corrupt)
        (corrupt / '2024-01/silver.parquet').write_bytes(b'bad')
        with patch('transformations.build.run_dbt') as mocked:
            with self.assertRaisesRegex(ValueError, 'Artifact checksum mismatch'):
                build(['2024-01'], corrupt, self.base / 'corrupt-output')
            mocked.assert_not_called()


if __name__ == '__main__':
    unittest.main()
