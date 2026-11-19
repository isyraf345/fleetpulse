"""Score the weekly baseline on validation only; final test scoring is unavailable."""
import argparse
import hashlib
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ingestion.pipeline import checksum, exclusive_lock, write_json

DEFAULT_PLAN = Path(__file__).with_name('evaluation_plan.json')


def load_plan(path):
    plan = json.loads(Path(path).read_text(encoding='utf-8'))
    dates = [datetime.fromisoformat(plan[key]) for key in ('train_start', 'validation_start', 'test_start', 'test_end')]
    if any(t.tzinfo is not None for t in dates) or not all(a < b for a, b in zip(dates, dates[1:])):
        raise ValueError('Split boundaries must be strictly increasing local timestamps')
    if any(t.day != 1 or t.hour or t.minute or t.second or t.microsecond for t in dates):
        raise ValueError('This evaluation plan requires month-boundary splits')
    segments = plan['demand_segments']
    low, medium = segments['low_max'], segments['medium_max']
    if not all(math.isfinite(v) for v in (low, medium)) or not 0 <= low < medium:
        raise ValueError('Demand segment thresholds must be finite and ordered')
    if (plan['schema_version'] != 1 or plan['mode'] != 'historical_replay'
            or plan['baseline'] != 'same_zone_same_local_hour_previous_week'
            or plan['comparison_cohort'] != 'history_ready_and_recorded_target'
            or segments['basis'] != 'mean_recorded_pickups_per_usable_training_hour'):
        raise ValueError('Unsupported baseline evaluation policy')
    return plan


def verify_gold(folder, plan):
    folder = Path(folder).resolve()
    manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
    expected_id = hashlib.sha256(json.dumps(manifest['identity'], sort_keys=True).encode()).hexdigest()
    if manifest['run_id'] != expected_id:
        raise ValueError('Gold run identity does not match its manifest')
    if manifest['identity']['policy'] != 'local-replay-v1':
        raise ValueError('Unsupported Gold time/coverage policy')
    for name in ('zone_hours.parquet', 'hourly_features.parquet'):
        if checksum(folder / name) != manifest['outputs'][name]:
            raise ValueError(f'Gold checksum mismatch: {name}')
    required = set()
    month = datetime.fromisoformat(plan['train_start'])
    end = datetime.fromisoformat(plan['test_end'])
    while month < end:
        required.add(month.strftime('%Y-%m'))
        month = datetime(month.year + (month.month == 12), month.month % 12 + 1, 1)
    if not required.issubset(manifest['identity']['sources']):
        raise ValueError('Evaluation window is missing required monthly snapshots')
    return manifest


def prepare_scores(con, gold, plan):
    con.read_parquet(str(Path(gold) / 'zone_hours.parquet')).create_view('actuals')
    con.read_parquet(str(Path(gold) / 'hourly_features.parquet')).create_view('features')
    # Demand groups are fitted only on training actuals, including true recorded zeros.
    con.execute('''CREATE TEMP TABLE segments AS
        SELECT zone_id, avg(pickup_count) AS training_mean,
          CASE WHEN avg(pickup_count) <= ? THEN 'low'
               WHEN avg(pickup_count) <= ? THEN 'medium' ELSE 'high' END AS demand_segment
        FROM actuals WHERE hour_local >= ?::TIMESTAMP AND hour_local < ?::TIMESTAMP
          AND coverage_status = 'recorded_hour' AND pickup_count IS NOT NULL GROUP BY zone_id''',
        [plan['demand_segments']['low_max'], plan['demand_segments']['medium_max'],
         plan['train_start'], plan['validation_start']])
    con.execute('''CREATE TEMP TABLE validation_pool AS
        SELECT a.zone_id, a.hour_local AS forecast_origin_local, a.pickup_count AS actual,
               f.lag_168h AS prediction, coalesce(s.demand_segment, 'unseen') AS demand_segment,
               CASE WHEN a.coverage_status != 'recorded_hour' OR a.pickup_count IS NULL THEN 'unavailable_target'
                    WHEN f.zone_id IS NULL OR f.history_ready IS NOT TRUE THEN 'missing_history'
                    ELSE 'eligible' END AS eligibility
        FROM actuals a LEFT JOIN features f
          ON a.zone_id=f.zone_id AND a.hour_local=f.forecast_origin_local
        LEFT JOIN segments s ON a.zone_id=s.zone_id
        WHERE a.hour_local >= ?::TIMESTAMP AND a.hour_local < ?::TIMESTAMP''',
        [plan['validation_start'], plan['test_start']])
    if con.execute('''SELECT count(*) FROM (
        SELECT zone_id, forecast_origin_local FROM validation_pool GROUP BY 1,2 HAVING count(*) != 1)''').fetchone()[0]:
        raise ValueError('Duplicate validation keys')
    if con.execute('''SELECT count(*) FROM validation_pool WHERE eligibility='eligible'
        AND (actual < 0 OR prediction IS NULL OR prediction < 0 OR NOT isfinite(prediction))''').fetchone()[0]:
        raise ValueError('Invalid eligible actual or baseline prediction')
    con.execute("CREATE TEMP TABLE scores AS SELECT *, abs(actual - prediction) AS absolute_error FROM validation_pool WHERE eligibility='eligible'")
    if con.execute('SELECT count(*) FROM scores').fetchone()[0] == 0:
        raise ValueError('No eligible validation rows')


def metrics(con, group=None):
    expressions = {'zone': 'zone_id', 'hour_of_day': 'extract(hour FROM forecast_origin_local)::INTEGER',
                   'day_of_week': 'extract(isodow FROM forecast_origin_local)::INTEGER',
                   'demand_segment': 'demand_segment'}
    if group is not None and group not in expressions:
        raise ValueError('Unsupported metric grouping')
    prefix = f'{expressions[group]} AS group_value,' if group else ''
    suffix = 'GROUP BY 1 ORDER BY 1' if group else ''
    rows = con.execute(f'''SELECT {prefix} count(*) AS rows, avg(absolute_error) AS mae,
        sum(absolute_error)::DOUBLE / nullif(sum(actual), 0) AS wape,
        sum(actual) AS actual_sum, sum(absolute_error) AS absolute_error_sum FROM scores {suffix}''').fetchall()
    names = (['group_value'] if group else []) + ['rows', 'mae', 'wape', 'actual_sum', 'absolute_error_sum']
    result = [dict(zip(names, row)) for row in rows]
    return result if group else result[0]


def evaluate(gold, plan_path=DEFAULT_PLAN, output_root=Path('data/evaluation')):
    plan = load_plan(plan_path)
    gold = Path(gold).resolve()
    source = verify_gold(gold, plan)
    identity = {'gold_run_id': source['run_id'], 'gold_manifest_sha256': checksum(gold / 'manifest.json'),
                'plan': plan, 'evaluator_sha256': checksum(Path(__file__)),
                'shared_utilities_sha256': checksum(Path(__file__).parents[1] / 'ingestion/pipeline.py'),
                'duckdb_version': duckdb.__version__, 'scored_split': 'validation'}
    run_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    destination = root / run_id
    with exclusive_lock(root / f'{run_id}.lock'):
        if destination.exists():
            old = json.loads((destination / 'manifest.json').read_text())
            if old['identity'] != identity:
                raise ValueError('Evaluation identity mismatch')
            for name, expected in old['outputs'].items():
                path = (destination / name).resolve()
                if not path.is_relative_to(destination) or checksum(path) != expected:
                    raise ValueError(f'Evaluation checksum mismatch: {name}')
            return {'status': 'unchanged', 'path': str(destination)}
        with tempfile.TemporaryDirectory(dir=root, prefix='.baseline-') as temporary:
            stage = Path(temporary)
            with duckdb.connect(config={'memory_limit': '512MB', 'threads': 2}) as con:
                prepare_scores(con, gold, plan)
                report = {'baseline': plan['baseline'], 'mode': plan['mode'], 'scored_split': 'validation',
                          'start_inclusive': plan['validation_start'], 'end_exclusive': plan['test_start'],
                          'wape_unit': 'ratio; null if total actual count is zero',
                          'comparison_cohort': plan['comparison_cohort'],
                          'eligibility_rows': dict(con.execute('SELECT eligibility, count(*) FROM validation_pool GROUP BY 1').fetchall()),
                          'overall': metrics(con),
                          'breakdowns': {key: metrics(con, key) for key in ('zone', 'hour_of_day', 'day_of_week', 'demand_segment')},
                          'test_scored': False}
                for table, filename in [('scores', 'validation_predictions.parquet'), ('segments', 'training_zone_segments.parquet')]:
                    path = str(stage / filename).replace("'", "''")
                    con.execute(f"COPY (SELECT * FROM {table} ORDER BY 1) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            write_json(stage / 'metrics.json', report)
            write_json(stage / 'baseline.json', {'kind': 'seasonal_naive', 'lag_column': 'lag_168h',
                'fitted_parameters': None, 'feature_policy': source['identity']['policy'],
                'missing_history': 'exclude_and_report', 'plan': plan})
            outputs = {p.name: checksum(p) for p in stage.iterdir() if p.is_file()}
            write_json(stage / 'manifest.json', {'run_id': run_id, 'identity': identity, 'outputs': outputs,
                       'created_at': datetime.now(timezone.utc).isoformat()})
            stage.rename(destination)
            return {'status': 'created', 'path': str(destination), 'validation': report['overall'], 'test_scored': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gold', type=Path, required=True)
    parser.add_argument('--plan', type=Path, default=DEFAULT_PLAN)
    parser.add_argument('--output-root', type=Path, default=Path('data/evaluation'))
    args = parser.parse_args()
    try:
        print(json.dumps(evaluate(args.gold, args.plan, args.output_root)))
    except Exception as error:
        parser.exit(1, f'Baseline evaluation failed: {error}\n')


if __name__ == '__main__':
    main()
