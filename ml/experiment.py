"""Train approved candidates, freeze validation selection, then confirm on holdout."""
import argparse
import gc
import hashlib
import json
import os
from pathlib import Path
import time
import warnings
from datetime import datetime, timezone
from importlib.metadata import version

# Set before native numerical libraries are imported; also enforce runtime limits.
for setting in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'):
    os.environ[setting] = '2'

import duckdb
import joblib
import numpy as np
from sklearn.exceptions import ConvergenceWarning
from threadpoolctl import threadpool_limits

from ingestion.pipeline import checksum, exclusive_lock, write_json
from ml.baseline import DEFAULT_PLAN, load_plan, metrics, prepare_scores, verify_gold
from ml.models import FEATURES, candidate_specs, make_model, passes_gate, predict_model, select_candidate
from ml.resources import peak_memory_bytes

REPO = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATES = Path(__file__).with_name('candidate_plan.json')


class InvalidPrediction(ValueError):
    pass


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    write_json(temporary, value)
    temporary.replace(path)


def verify_outputs(folder, outputs):
    for name, expected in outputs.items():
        path = (folder / name).resolve()
        if not path.is_relative_to(folder.resolve()) or checksum(path) != expected:
            raise ValueError(f'Experiment artifact checksum mismatch: {name}')


def load_candidates(path):
    plan = json.loads(Path(path).read_text(encoding='utf-8'))
    if plan['status'] != 'approved' or plan['predictors'] != FEATURES:
        raise ValueError('An approved plan with the supported feature schema is required')
    if plan['cohort'] != 'history_ready_and_recorded_target' or plan['final_test']['refit_on_validation']:
        raise ValueError('Unsupported evaluation or refitting policy')
    if [c['name'] for c in plan['candidates']] != ['regularized_poisson', 'poisson_boosted_trees']:
        raise ValueError('Unsupported candidate families')
    for package, pinned in plan['package_versions'].items():
        if version(package) != pinned:
            raise ValueError(f'Expected {package}=={pinned}')
    specs = list(candidate_specs(plan))
    if not specs or len({s[0] for s in specs}) != len(specs):
        raise ValueError('Candidate identifiers must be nonempty and unique')
    return plan


def read_split(con, split, start, end):
    if split not in ('train', 'validation', 'test'):
        raise ValueError('Unknown split')
    columns = ', '.join(f'f.{name}' for name in FEATURES)
    con.execute(f'''CREATE OR REPLACE TEMP TABLE {split}_pool AS
        SELECT {columns}, a.hour_local AS forecast_origin_local, a.pickup_count AS actual,
               coalesce(s.demand_segment, 'unseen') AS demand_segment,
               f.feature_cutoff_exclusive_local,
               CASE WHEN a.coverage_status != 'recorded_hour' OR a.pickup_count IS NULL THEN 'unavailable_target'
                    WHEN f.zone_id IS NULL OR f.history_ready IS NOT TRUE THEN 'missing_history'
                    ELSE 'eligible' END AS eligibility
        FROM actuals a LEFT JOIN features f
          ON a.zone_id=f.zone_id AND a.hour_local=f.forecast_origin_local
        LEFT JOIN segments s ON a.zone_id=s.zone_id
        WHERE a.hour_local >= ?::TIMESTAMP AND a.hour_local < ?::TIMESTAMP''', [start, end])
    coverage = dict(con.execute(f'SELECT eligibility, count(*) FROM {split}_pool GROUP BY 1').fetchall())
    con.execute(f'''CREATE OR REPLACE TEMP TABLE {split}_data AS
        SELECT row_number() OVER (ORDER BY zone_id, forecast_origin_local) AS row_index, *
        FROM {split}_pool WHERE eligibility='eligible' ''')
    if con.execute(f'''SELECT count(*) FROM (SELECT zone_id, forecast_origin_local FROM {split}_data
        GROUP BY 1,2 HAVING count(*) != 1)''').fetchone()[0]:
        raise ValueError(f'Duplicate {split} keys')
    invalid_cutoffs = con.execute(f'''SELECT count(*) FROM {split}_data
        WHERE feature_cutoff_exclusive_local IS DISTINCT FROM forecast_origin_local''').fetchone()[0]
    if invalid_cutoffs:
        raise ValueError('Feature cutoff does not match prediction origin')
    arrays = con.execute(f"SELECT {', '.join(FEATURES)}, actual FROM {split}_data ORDER BY row_index").fetchnumpy()
    if not len(arrays['actual']):
        raise ValueError(f'No eligible {split} rows')
    for values in arrays.values():
        if np.ma.isMaskedArray(values) and np.any(values.mask):
            raise ValueError('Eligible features/targets contain nulls')
    x = np.column_stack([arrays[key] for key in FEATURES]).astype(np.float64)
    y = np.asarray(arrays['actual'], dtype=np.float64)
    if not np.isfinite(x).all() or not np.isfinite(y).all() or (x < 0).any() or (y < 0).any():
        raise ValueError('Features and counts must be finite and nonnegative')
    return x, y, coverage


def score(con, split, predictions, output=None):
    predictions = np.asarray(predictions, dtype=np.float64)
    expected = con.execute(f'SELECT count(*) FROM {split}_data').fetchone()[0]
    if predictions.shape != (expected,) or not np.isfinite(predictions).all() or (predictions < 0).any():
        raise InvalidPrediction('Predictions must match the cohort and be finite and nonnegative')
    # Direct NumPy scans avoid costly scalar-list parameter binding for full cohorts.
    con.register('prediction_values', {'row_index': np.arange(1, expected + 1, dtype=np.int64),
                                       'prediction': predictions})
    try:
        con.execute(f'''CREATE OR REPLACE TEMP TABLE scores AS
            SELECT d.zone_id, d.forecast_origin_local, d.actual, p.prediction, d.demand_segment,
                   abs(d.actual-p.prediction) AS absolute_error
            FROM {split}_data d JOIN prediction_values p USING (row_index)''')
    finally:
        con.unregister('prediction_values')
    result = {'overall': metrics(con), 'breakdowns': {
        key: metrics(con, key) for key in ('zone', 'hour_of_day', 'day_of_week', 'demand_segment')}}
    if output is not None:
        escaped = str(output).replace("'", "''")
        con.execute(f"COPY (SELECT * FROM scores ORDER BY zone_id, forecast_origin_local) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    return result


def train_candidate(con, stage, spec, x_train, y_train, x_validation, baseline, policy):
    name, kind, params = spec
    print(json.dumps({'event': 'training', 'candidate': name}), flush=True)
    model = make_model(kind, params)
    started = time.perf_counter()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        if kind == 'lightgbm':
            model.fit(x_train, y_train, feature_name=FEATURES, categorical_feature=[0, 1, 2])
        else:
            model.fit(x_train, y_train)
    record = {'name': name, 'kind': kind, 'parameters': params,
              'fit_seconds': time.perf_counter() - started,
              'warnings': [str(w.message) for w in caught],
              'peak_process_memory_bytes_so_far': peak_memory_bytes()}
    model_path = stage / f'{name}.joblib'
    joblib.dump(model, model_path, compress=3)
    record['artifact'] = model_path.name
    record['artifact_sha256'] = checksum(model_path)
    if any(issubclass(w.category, ConvergenceWarning) for w in caught):
        record.update(status='rejected_nonconvergence')
    else:
        started = time.perf_counter()
        predicted = predict_model(model, x_validation)
        record['validation_predict_seconds'] = time.perf_counter() - started
        try:
            record['metrics'] = score(con, 'validation', predicted, stage / f'{name}_validation.parquet')
            # Verify serialization using exactly the artifact that would be served.
            reloaded = joblib.load(model_path)
            np.testing.assert_allclose(predict_model(reloaded, x_validation), predicted, rtol=1e-12, atol=1e-12)
            del reloaded
            record['gate'] = passes_gate(record['metrics'], baseline, policy)
            record['status'] = 'evaluated'
        except InvalidPrediction as error:
            record.update(status='rejected_predictions', error=str(error))
    atomic_json(stage / f'{name}.json', record)
    print(json.dumps({'event': 'candidate_complete', 'candidate': name, 'status': record['status'],
                      'gate': record.get('gate'), 'overall': record.get('metrics', {}).get('overall')}), flush=True)
    del model
    gc.collect()
    return record


def freeze_validation(con, stage, config, splits):
    x_train, y_train, train_coverage = read_split(con, 'train', splits['train_start'], splits['validation_start'])
    x_validation, _, validation_coverage = read_split(con, 'validation', splits['validation_start'], splits['test_start'])
    print(json.dumps({'event': 'cohorts', 'training': train_coverage, 'validation': validation_coverage}), flush=True)
    baseline = score(con, 'validation', x_validation[:, FEATURES.index('lag_168h')], stage / 'baseline_validation.parquet')
    records = [train_candidate(con, stage, spec, x_train, y_train, x_validation, baseline, config['selection'])
               for spec in candidate_specs(config)]
    selected = select_candidate(records)
    frozen = {'selected': selected, 'baseline': baseline, 'candidates': records,
              'training_coverage': train_coverage, 'validation_coverage': validation_coverage,
              'training_start': splits['train_start'], 'training_end_exclusive': splits['validation_start'],
              'frozen_at': datetime.now(timezone.utc).isoformat(), 'feature_schema': FEATURES}
    atomic_json(stage / 'validation_selection.json', frozen)
    files = {p.name: checksum(p) for p in stage.iterdir() if p.is_file() and p.name != 'identity.json'}
    atomic_json(stage / 'validation_checksums.json', files)
    print(json.dumps({'event': 'selection_frozen', 'selected': selected}), flush=True)
    return frozen


def reserve_holdout(root, identity, run_id, frozen):
    key = {'gold_run_id': identity['gold_run_id'], 'splits': identity['splits']}
    holdout_id = hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()
    path = root / f'holdout-{holdout_id}.json'
    reservation = {'experiment_run_id': run_id, 'selected': frozen['selected'],
                   'artifact_sha256': next((r['artifact_sha256'] for r in frozen['candidates'] if r['name'] == frozen['selected']), None)}
    with exclusive_lock(root / f'holdout-{holdout_id}.lock'):
        if path.exists():
            if json.loads(path.read_text()) != reservation:
                raise ValueError('This holdout was already opened by a different frozen experiment; do not retune on it')
        else:
            atomic_json(path, reservation)


def confirm_test(con, stage, frozen, config, splits):
    # This is called only after validation selection is persisted and holdout reserved.
    x_test, _, coverage = read_split(con, 'test', splits['test_start'], splits['test_end'])
    baseline = score(con, 'test', x_test[:, FEATURES.index('lag_168h')], stage / 'baseline_test.parquet')
    result = {'baseline': baseline, 'coverage': coverage, 'selected_on_validation': frozen['selected'],
              'promoted': 'weekly_baseline', 'candidate': None, 'gate': None}
    if frozen['selected'] != 'weekly_baseline':
        record = next(r for r in frozen['candidates'] if r['name'] == frozen['selected'])
        if checksum(stage / record['artifact']) != record['artifact_sha256']:
            raise ValueError('Selected model artifact changed after validation')
        model = joblib.load(stage / record['artifact'])
        started = time.perf_counter()
        predictions = predict_model(model, x_test)
        result['test_predict_seconds'] = time.perf_counter() - started
        try:
            result['candidate'] = score(con, 'test', predictions, stage / 'selected_test.parquet')
            result['gate'] = passes_gate(result['candidate'], baseline, config['selection'])
            if result['gate']['passed']:
                result['promoted'] = frozen['selected']
        except InvalidPrediction as error:
            result['gate'] = {'passed': False, 'failures': ['invalid_test_predictions'], 'error': str(error)}
    return result


def run(gold, output_root=Path('data/experiments'), plan_path=DEFAULT_CANDIDATES, split_path=DEFAULT_PLAN):
    started = time.perf_counter()
    config, splits = load_candidates(plan_path), load_plan(split_path)
    gold = Path(gold).resolve()
    source = verify_gold(gold, splits)
    if source['run_id'] != config['gold_run_id']:
        raise ValueError('Gold dataset differs from the approved experiment')
    code = ['ml/experiment.py', 'ml/models.py', 'ml/resources.py', 'ml/baseline.py', 'ingestion/pipeline.py', 'requirements-lock.txt']
    identity = {'gold_run_id': source['run_id'], 'gold_manifest_sha256': checksum(gold / 'manifest.json'),
                'config': config, 'splits': splits, 'code': {p: checksum(REPO / p) for p in code},
                'versions': {p: version(p) for p in ('duckdb', 'numpy', 'scipy', 'scikit-learn', 'lightgbm', 'joblib', 'threadpoolctl')}}
    run_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    destination = root / run_id
    stage = root / f'.pending-{run_id}'
    with exclusive_lock(root / f'{run_id}.lock'), threadpool_limits(limits=2):
        if destination.exists():
            manifest = json.loads((destination / 'manifest.json').read_text())
            if manifest['identity'] != identity:
                raise ValueError('Experiment identity mismatch')
            verify_outputs(destination, manifest['outputs'])
            return {'status': 'unchanged', 'path': str(destination)}
        stage.mkdir(exist_ok=True)
        if (stage / 'identity.json').exists() and json.loads((stage / 'identity.json').read_text()) != identity:
            raise ValueError('Pending experiment identity mismatch')
        atomic_json(stage / 'identity.json', identity)
        with duckdb.connect(config={'memory_limit': '512MB', 'threads': 2}) as con:
            prepare_scores(con, gold, splits)
            if (stage / 'validation_checksums.json').exists():
                verify_outputs(stage, json.loads((stage / 'validation_checksums.json').read_text()))
                frozen = json.loads((stage / 'validation_selection.json').read_text())
            else:
                frozen = freeze_validation(con, stage, config, splits)
            reserve_holdout(root, identity, run_id, frozen)
            final = confirm_test(con, stage, frozen, config, splits)
        final.update(elapsed_seconds=time.perf_counter() - started, peak_process_memory_bytes=peak_memory_bytes(),
                     memory_scope='OS-reported process lifetime peak resident memory including native libraries',
                     test_start=splits['test_start'], test_end_exclusive=splits['test_end'])
        atomic_json(stage / 'test_results.json', final)
        chosen = next((r for r in frozen['candidates'] if r['name'] == final['promoted']), None)
        atomic_json(stage / 'model_selection.json', {
            'model': final['promoted'], 'artifact': chosen['artifact'] if chosen else None,
            'artifact_sha256': chosen['artifact_sha256'] if chosen else None,
            'fallback': 'weekly_baseline', 'fallback_rule': 'lag_168h',
            'feature_schema': FEATURES, 'gold_run_id': source['run_id'],
            'training_period': [splits['train_start'], splits['validation_start']],
            'mode': 'historical_replay', 'no_refit_on_validation': True,
            'selection_scope': 'local evaluation artifact; serving promotion is a later milestone'})
        outputs = {p.name: checksum(p) for p in stage.iterdir() if p.is_file() and p.name != 'manifest.json'}
        atomic_json(stage / 'manifest.json', {'run_id': run_id, 'identity': identity, 'outputs': outputs,
                    'created_at': datetime.now(timezone.utc).isoformat()})
        stage.rename(destination)
        return {'status': 'created', 'path': str(destination), 'selected_on_validation': frozen['selected'],
                'promoted': final['promoted'], 'test_baseline': final['baseline']['overall'],
                'test_candidate': final['candidate']['overall'] if final['candidate'] else None,
                'test_gate': final['gate'], 'elapsed_seconds': final['elapsed_seconds'],
                'peak_process_memory_bytes': final['peak_process_memory_bytes']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gold', type=Path, required=True)
    parser.add_argument('--output-root', type=Path, default=Path('data/experiments'))
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.gold, args.output_root)), flush=True)
    except Exception as error:
        parser.exit(1, f'Experiment failed: {error}\n')


if __name__ == '__main__':
    main()
