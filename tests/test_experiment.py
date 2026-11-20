import hashlib
import json
import tempfile
import unittest
import warnings
from pathlib import Path
from unittest.mock import patch

import duckdb
import numpy as np
from threadpoolctl import threadpool_limits
from sklearn.exceptions import ConvergenceWarning

from ingestion.pipeline import checksum, write_json
from ml.baseline import DEFAULT_PLAN
from ml.experiment import DEFAULT_CANDIDATES, InvalidPrediction, read_split, reserve_holdout, run, score, train_candidate
from ml.models import make_model, passes_gate, select_candidate
from ml.resources import peak_memory_bytes


def metric_report(wape, group_mae):
    return {'overall': {'wape': wape, 'rows': 10, 'actual_sum': 100},
            'breakdowns': {'demand_segment': [{'group_value': 'low', 'rows': 10, 'mae': group_mae}]}}


class NonconvergedModel:
    def fit(self, x, y):
        warnings.warn('Injected nonconvergence', ConvergenceWarning)
        return self

    def predict(self, x):
        raise AssertionError('Nonconverged model must not be evaluated')


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.policy = json.loads(DEFAULT_CANDIDATES.read_text())['selection']

    def test_overall_improvement_cannot_hide_segment_regression(self):
        gate = passes_gate(metric_report(0.5, 1.11), metric_report(1, 1), self.policy)
        self.assertFalse(gate['passed'])
        self.assertIn('segment_mae:low', gate['failures'])
        self.assertTrue(passes_gate(metric_report(0.5, 1.1), metric_report(1, 1), self.policy)['passed'])

    def test_undefined_denominator_and_different_cohorts(self):
        self.assertFalse(passes_gate(metric_report(None, 0), metric_report(None, 0), self.policy)['passed'])
        different = metric_report(0.5, 1)
        different['overall']['rows'] = 9
        with self.assertRaisesRegex(ValueError, 'same evaluation cohort'):
            passes_gate(different, metric_report(1, 1), self.policy)

    def test_selection_ignores_rejected_and_failed_gate_candidates(self):
        records = [
            {'name': 'failed', 'status': 'rejected_nonconvergence'},
            {'name': 'regression', 'status': 'evaluated', 'gate': {'passed': False}, 'metrics': metric_report(0.1, 5)},
            {'name': 'qualified', 'status': 'evaluated', 'gate': {'passed': True}, 'metrics': metric_report(0.5, 1)},
        ]
        self.assertEqual(select_candidate(records), 'qualified')
        self.assertEqual(select_candidate(records[:2]), 'weekly_baseline')

    def test_preprocessing_does_not_refit_during_prediction(self):
        x = np.array([[1, 0, 1, 1, 2, 3, 2], [2, 1, 2, 3, 4, 5, 4]] * 10, dtype=float)
        with threadpool_limits(limits=2):
            model = make_model('poisson', {'alpha': 1, 'max_iter': 100, 'tol': 1e-6})
            model.fit(x, np.ones(len(x)))
            scale = model['preprocess'].named_transformers_['counts']['scale']
            before = scale.mean_.copy()
            future = x[:1].copy()
            future[:, 3:] = 1000
            prediction = model.predict(future)
        np.testing.assert_array_equal(scale.mean_, before)
        self.assertTrue(np.isfinite(prediction).all() and (prediction >= 0).all())

    def test_holdout_rejects_a_different_frozen_experiment(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity = {'gold_run_id': 'synthetic', 'splits': {'test_start': '2024-06-01'}}
            frozen = {'selected': 'weekly_baseline', 'candidates': []}
            reserve_holdout(root, identity, 'first', frozen)
            reserve_holdout(root, identity, 'first', frozen)
            with self.assertRaisesRegex(ValueError, 'already opened'):
                reserve_holdout(root, identity, 'second', frozen)

    def test_peak_memory_is_measured(self):
        self.assertGreater(peak_memory_bytes(), 0)

    def test_nonconvergence_rejects_before_prediction(self):
        with tempfile.TemporaryDirectory() as tmp, patch('ml.experiment.make_model', return_value=NonconvergedModel()):
            record = train_candidate(None, Path(tmp), ('injected', 'poisson', {}),
                np.zeros((2,7)), np.ones(2), np.zeros((2,7)), metric_report(1,1), self.policy)
        self.assertEqual(record['status'], 'rejected_nonconvergence')

    def test_invalid_predictions_cannot_reach_quality_gate(self):
        with duckdb.connect() as con:
            con.execute('CREATE TABLE validation_data AS SELECT * FROM range(2)')
            for predictions in ([1], [1, float('nan')], [1, -1]):
                with self.assertRaises(InvalidPrediction):
                    score(con, 'validation', predictions)


class ExperimentIntegrationTests(unittest.TestCase):
    def test_frozen_selection_precedes_holdout_and_rerun_preserves_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gold = root / 'gold'
            gold.mkdir()
            with duckdb.connect() as con:
                con.execute('''CREATE TABLE examples AS
                    WITH dates AS (
                        SELECT TIMESTAMP '2024-01-01' + h * INTERVAL '1 hour' AS origin, h FROM range(720) t(h)
                        UNION ALL SELECT TIMESTAMP '2024-05-01' + h * INTERVAL '1 hour', h FROM range(48) t(h)
                        UNION ALL SELECT TIMESTAMP '2024-06-01' + h * INTERVAL '1 hour', h FROM range(48) t(h)
                    ) SELECT z::INTEGER AS zone_id, origin, (h % 5 + 1)::DOUBLE AS lag_count
                    FROM dates CROSS JOIN range(1,4) zones(z)''')
                con.execute('''CREATE TABLE actuals AS SELECT zone_id, origin AS hour_local,
                    (lag_count*2)::BIGINT AS pickup_count, 'recorded_hour' AS coverage_status FROM examples''')
                con.execute('''CREATE TABLE features AS SELECT zone_id, origin AS forecast_origin_local,
                    origin AS feature_cutoff_exclusive_local, extract(hour FROM origin)::INTEGER AS hour_of_day,
                    extract(isodow FROM origin)::INTEGER AS day_of_week,
                    lag_count AS lag_1h, lag_count AS lag_24h, lag_count AS lag_168h,
                    lag_count AS mean_previous_24h, true AS history_ready FROM examples''')
                con.table('actuals').write_parquet(str(gold / 'zone_hours.parquet'))
                con.table('features').write_parquet(str(gold / 'hourly_features.parquet'))
            identity = {'policy': 'local-replay-v1', 'sources': {f'2024-{m:02}': 'synthetic' for m in range(1,7)}}
            gold_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
            write_json(gold / 'manifest.json', {'identity': identity, 'run_id': gold_id,
                'outputs': {name: checksum(gold / name) for name in ('zone_hours.parquet', 'hourly_features.parquet')}})
            config = json.loads(DEFAULT_CANDIDATES.read_text())
            config['gold_run_id'] = gold_id
            config['candidates'][1]['n_estimators'] = 20  # Small synthetic smoke experiment only.
            config_path = root / 'approved-test-plan.json'
            write_json(config_path, config)
            output = root / 'experiments'
            observed_test_reads = []

            def guarded_read(con, split, start, end):
                if split == 'test':
                    self.assertEqual(len(list(output.glob('.pending-*/validation_checksums.json'))), 1)
                    self.assertEqual(len(list(output.glob('holdout-*.json'))), 1)
                    observed_test_reads.append((start, end))
                x, y, coverage = read_split(con, split, start, end)
                if split == 'train':
                    self.assertEqual(len(y), 2160)
                    self.assertLess(con.execute('SELECT max(forecast_origin_local)::VARCHAR FROM train_data').fetchone()[0], '2024-05-01')
                return x, y, coverage

            with patch('ml.experiment.read_split', side_effect=guarded_read):
                result = run(gold, output, config_path, DEFAULT_PLAN)
            self.assertEqual(len(observed_test_reads), 1)
            self.assertEqual(result['status'], 'created')
            final = Path(result['path'])
            manifest = (final / 'manifest.json').read_bytes()
            self.assertTrue((final / 'model_selection.json').exists())
            self.assertGreater(result['peak_process_memory_bytes'], 0)
            with patch('ml.experiment.confirm_test', side_effect=AssertionError('Must not rescore holdout')):
                again = run(gold, output, config_path, DEFAULT_PLAN)
            self.assertEqual(again['status'], 'unchanged')
            self.assertEqual(manifest, (final / 'manifest.json').read_bytes())
            (final / 'test_results.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
                run(gold, output, config_path, DEFAULT_PLAN)


if __name__ == '__main__':
    unittest.main()
