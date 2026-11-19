import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from ingestion.pipeline import checksum, write_json
from ml.baseline import DEFAULT_PLAN, evaluate, load_plan


class BaselineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.gold = self.root / 'gold'
        self.gold.mkdir()
        with duckdb.connect() as con:
            con.execute('''CREATE TABLE actuals AS SELECT * FROM (VALUES
                (1, TIMESTAMP '2024-01-01', 0, 'recorded_hour'),
                (1, TIMESTAMP '2024-01-02', 1, 'recorded_hour'),
                (2, TIMESTAMP '2024-01-01', 20, 'recorded_hour'),
                (3, TIMESTAMP '2024-01-01', 0, 'recorded_hour'),
                (1, TIMESTAMP '2024-05-01', 10, 'recorded_hour'),
                (1, TIMESTAMP '2024-05-01 01:00', 0, 'recorded_hour'),
                (2, TIMESTAMP '2024-05-01', 4, 'recorded_hour'),
                (3, TIMESTAMP '2024-05-01', 0, 'recorded_hour'),
                (1, TIMESTAMP '2024-05-02', NULL, 'no_citywide_pickups'),
                (1, TIMESTAMP '2024-05-03', 99, 'recorded_hour'),
                (1, TIMESTAMP '2024-06-01', 1000000, 'recorded_hour')
            ) t(zone_id, hour_local, pickup_count, coverage_status)''')
            con.execute('''CREATE TABLE features AS SELECT * FROM (VALUES
                (1, TIMESTAMP '2024-05-01', 7, true),
                (1, TIMESTAMP '2024-05-01 01:00', 2, true),
                (2, TIMESTAMP '2024-05-01', 4, true),
                (3, TIMESTAMP '2024-05-01', 0, true),
                (1, TIMESTAMP '2024-05-02', 9, true),
                (1, TIMESTAMP '2024-05-03', NULL, false),
                (1, TIMESTAMP '2024-06-01', 0, true)
            ) t(zone_id, forecast_origin_local, lag_168h, history_ready)''')
            con.table('actuals').write_parquet(str(self.gold / 'zone_hours.parquet'))
            con.table('features').write_parquet(str(self.gold / 'hourly_features.parquet'))
        self.identity = {'policy': 'local-replay-v1', 'sources': {f'2024-{month:02}': 'synthetic' for month in range(1, 7)}}
        self.write_manifest()

    def write_manifest(self):
        write_json(self.gold / 'manifest.json', {
            'run_id': hashlib.sha256(json.dumps(self.identity, sort_keys=True).encode()).hexdigest(),
            'identity': self.identity,
            'outputs': {name: checksum(self.gold / name) for name in ('zone_hours.parquet', 'hourly_features.parquet')},
        })

    def test_metrics_zero_denominator_cutoffs_and_training_only_segments(self):
        result = evaluate(self.gold, output_root=self.root / 'evaluation')
        report = json.loads((Path(result['path']) / 'metrics.json').read_text())
        self.assertEqual(report['overall']['rows'], 4)
        self.assertEqual(report['overall']['mae'], 1.25)
        self.assertAlmostEqual(report['overall']['wape'], 5 / 14)
        self.assertEqual(report['eligibility_rows'], {'eligible': 4, 'unavailable_target': 1, 'missing_history': 1})
        zero_zone = next(r for r in report['breakdowns']['zone'] if r['group_value'] == 3)
        self.assertIsNone(zero_zone['wape'])
        self.assertFalse(report['test_scored'])
        with duckdb.connect() as con:
            path = str(Path(result['path']) / 'validation_predictions.parquet')
            self.assertEqual(con.execute('SELECT max(forecast_origin_local)::VARCHAR FROM read_parquet(?)', [path]).fetchone()[0], '2024-05-01 01:00:00')
            path = str(Path(result['path']) / 'training_zone_segments.parquet')
            self.assertEqual(con.execute('SELECT training_mean, demand_segment FROM read_parquet(?) WHERE zone_id=1', [path]).fetchone(), (0.5, 'low'))

    def test_rerun_and_corrupted_output(self):
        first = evaluate(self.gold, output_root=self.root / 'evaluation')
        manifest = (Path(first['path']) / 'manifest.json').read_bytes()
        again = evaluate(self.gold, output_root=self.root / 'evaluation')
        self.assertEqual(again['status'], 'unchanged')
        self.assertEqual(manifest, (Path(again['path']) / 'manifest.json').read_bytes())
        (Path(first['path']) / 'metrics.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'Evaluation checksum mismatch'):
            evaluate(self.gold, output_root=self.root / 'evaluation')

    def test_corrupt_gold_rejected(self):
        (self.gold / 'hourly_features.parquet').write_bytes(b'bad')
        with self.assertRaisesRegex(ValueError, 'Gold checksum mismatch'):
            evaluate(self.gold, output_root=self.root / 'evaluation')
        self.assertFalse((self.root / 'evaluation').exists())

    def test_missing_month_rejected(self):
        del self.identity['sources']['2024-03']
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, 'missing required monthly snapshots'):
            evaluate(self.gold, output_root=self.root / 'evaluation')

    def test_unordered_split_rejected(self):
        plan = json.loads(DEFAULT_PLAN.read_text())
        plan['validation_start'] = plan['test_start']
        path = self.root / 'plan.json'
        write_json(path, plan)
        with self.assertRaisesRegex(ValueError, 'strictly increasing'):
            load_plan(path)

    def test_duplicate_keys_rejected_without_publication(self):
        path = self.gold / 'hourly_features.parquet'
        with duckdb.connect() as con:
            con.execute('CREATE TABLE duplicate_features AS SELECT * FROM read_parquet(?)', [str(path)])
            con.execute('INSERT INTO duplicate_features SELECT * FROM duplicate_features LIMIT 1')
            con.table('duplicate_features').write_parquet(str(path))
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, 'Duplicate validation keys'):
            evaluate(self.gold, output_root=self.root / 'evaluation')
        self.assertEqual(list((self.root / 'evaluation').iterdir()), [])


if __name__ == '__main__':
    unittest.main()
