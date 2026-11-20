"""Verify saved experiment lineage and build a publication payload without inference."""
from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path

import duckdb

from ingestion.pipeline import checksum
from serving.local import ROOT
from transformations.build import verified_sources


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def identity_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def verify_bundle(folder):
    folder = Path(folder).resolve()
    if folder.name.startswith('.'):
        raise ValueError('A pending artifact directory cannot be published')
    manifest = read_json(folder / 'manifest.json')
    expected = hashlib.sha256(json.dumps(manifest['identity'], sort_keys=True).encode()).hexdigest()
    if manifest['run_id'] != expected:
        raise ValueError('Artifact identity mismatch')
    for name, digest in manifest['outputs'].items():
        path = (folder / name).resolve()
        if not path.is_relative_to(folder) or checksum(path) != digest:
            raise ValueError(f'Artifact checksum mismatch: {name}')
    return manifest


@dataclass
class Payload:
    dataset: dict
    zones: list
    model: dict
    run: dict
    rows: list


def prepare(experiment, gold_root=ROOT / 'data/gold', snapshot_root=ROOT / 'data/snapshots'):
    experiment = Path(experiment).resolve()
    manifest = verify_bundle(experiment)
    selection = read_json(experiment / 'model_selection.json')
    confirmation = read_json(experiment / 'test_results.json')
    frozen = read_json(experiment / 'validation_selection.json')
    if selection['model'] != confirmation['promoted'] or selection['mode'] != 'historical_replay':
        raise ValueError('Inconsistent model selection')
    baseline_only = selection['model'] == 'weekly_baseline'
    if not baseline_only:
        record = next(r for r in frozen['candidates'] if r['name'] == selection['model'])
        if (record['kind'] != 'lightgbm' or not record['gate']['passed'] or not confirmation['gate']['passed']
                or frozen['selected'] != selection['model'] or record['artifact_sha256'] != selection['artifact_sha256']):
            raise ValueError('Selected model did not pass the frozen quality gates')
        if checksum(experiment / selection['artifact']) != selection['artifact_sha256']:
            raise ValueError('Selected model checksum mismatch')
    gold_id = manifest['identity']['gold_run_id']
    if selection['gold_run_id'] != gold_id:
        raise ValueError('Selection dataset mismatch')
    gold = Path(gold_root).resolve() / gold_id
    gold_manifest = verify_bundle(gold)
    if gold_manifest['run_id'] != gold_id or checksum(gold / 'manifest.json') != manifest['identity']['gold_manifest_sha256']:
        raise ValueError('Experiment Gold provenance mismatch')
    if gold_manifest['identity']['policy'] != 'local-replay-v1':
        raise ValueError('Unsupported feature policy')
    sources = verified_sources(snapshot_root, sorted(gold_manifest['identity']['sources']))
    for source in sources:
        if source['manifest_sha256'] != gold_manifest['identity']['sources'][source['month']]:
            raise ValueError('Gold source snapshot mismatch')
    profiles = [read_json(Path(s['path']) / 'profile.json') for s in sources]
    dataset = dict(dataset_id=gold_id, manifest_sha256=checksum(gold / 'manifest.json'),
        zone_reference_sha256=sources[0]['manifest']['sources']['zones.csv']['sha256'],
        event_time_basis='local_wall_clock', calendar_assumption='America/New_York', feature_policy='local-replay-v1',
        first_observed_local=min(datetime.fromisoformat(p['validated_min_timestamp']) for p in profiles),
        last_observed_local=max(datetime.fromisoformat(p['validated_max_timestamp']) for p in profiles),
        source_manifests={s['month']: {'manifest_sha256': s['manifest_sha256'], 'sources': s['manifest']['sources']} for s in sources})
    bounds = manifest['identity']['splits']
    start, end = (datetime.fromisoformat(bounds[key]) for key in ('test_start', 'test_end'))
    if start.tzinfo or end.tzinfo or not start < end:
        raise ValueError('Invalid replay window')
    model_identity = {'experiment_id': manifest['run_id'], 'model_name': selection['model'],
        'model_kind': 'weekly_baseline' if baseline_only else 'lightgbm_poisson',
        'artifact_sha256': selection['artifact_sha256'], 'feature_schema': selection['feature_schema'],
        'feature_policy': 'local-replay-v1', 'training_period': selection['training_period']}
    model = dict(model_id=identity_hash(model_identity), experiment_id=manifest['run_id'],
        experiment_manifest_sha256=checksum(experiment / 'manifest.json'), training_dataset_id=gold_id,
        model_name=selection['model'], model_kind=model_identity['model_kind'], artifact_sha256=selection['artifact_sha256'],
        training_start_local=datetime.fromisoformat(selection['training_period'][0]),
        training_end_exclusive_local=datetime.fromisoformat(selection['training_period'][1]),
        feature_schema=selection['feature_schema'], evaluation_summary=confirmation)
    predicted_file = 'baseline_test.parquet' if baseline_only else 'selected_test.parquet'
    publisher_hash = identity_hash({name: checksum(ROOT / 'serving' / name)
                                   for name in ('artifacts.py', 'publish.py', 'database.py', 'migrations/001_initial.sql')})
    run = dict(dataset_id=gold_id, model_id=model['model_id'], mode='historical_replay', source_split='test',
        prediction_artifact_sha256=manifest['outputs'][predicted_file],
        baseline_artifact_sha256=manifest['outputs']['baseline_test.parquet'],
        publication_contract_version=1, publisher_code_sha256=publisher_hash,
        origins_start_local=start, origins_end_exclusive_local=end)
    run_identity = {**run, 'origins_start_local': start.isoformat(), 'origins_end_exclusive_local': end.isoformat(),
                    'gold_manifest_sha256': dataset['manifest_sha256'], 'experiment_manifest_sha256': model['experiment_manifest_sha256']}
    run['run_id'] = identity_hash(run_identity)
    with duckdb.connect(config={'memory_limit': '512MB', 'threads': 2}) as con:
        con.read_parquet(str(experiment / predicted_file)).create_view('selected')
        con.read_parquet(str(experiment / 'baseline_test.parquet')).create_view('baseline')
        con.read_parquet(str(gold / 'zone_hours.parquet')).create_view('actuals')
        con.read_parquet(str(gold / 'hourly_features.parquet')).create_view('features')
        for table in ('selected', 'baseline'):
            if con.execute(f'''SELECT count(*) FROM (SELECT zone_id, forecast_origin_local
                FROM {table} GROUP BY 1,2 HAVING count(*) != 1)''').fetchone()[0]:
                raise ValueError('Duplicate prediction keys')
        con.execute('''CREATE TEMP TABLE expected AS SELECT a.zone_id, a.hour_local AS forecast_origin_local,
            a.pickup_count AS actual FROM actuals a JOIN features f
            ON a.zone_id=f.zone_id AND a.hour_local=f.forecast_origin_local
            WHERE a.coverage_status='recorded_hour' AND f.history_ready
              AND a.hour_local >= ? AND a.hour_local < ?''', [start, end])
        for table in ('selected', 'baseline'):
            mismatch = con.execute(f'''SELECT count(*) FROM (
                (SELECT zone_id, forecast_origin_local, actual FROM {table} EXCEPT ALL SELECT * FROM expected)
                UNION ALL (SELECT * FROM expected EXCEPT ALL SELECT zone_id, forecast_origin_local, actual FROM {table}))''').fetchone()[0]
            if mismatch:
                raise ValueError(f'{table} keys/actuals differ from the eligible Gold cohort')
        con.execute('''CREATE TEMP TABLE payload AS SELECT s.zone_id, s.forecast_origin_local,
            s.prediction, b.prediction AS baseline, s.actual FROM selected s JOIN baseline b USING(zone_id,forecast_origin_local)''')
        invalid = con.execute('''SELECT count(*) FROM payload WHERE prediction IS NULL OR baseline IS NULL OR actual IS NULL
            OR NOT isfinite(prediction) OR NOT isfinite(baseline) OR prediction<0 OR baseline<0
            OR NOT isfinite(actual) OR actual<0 OR actual!=floor(actual)
            OR forecast_origin_local!=date_trunc('hour', forecast_origin_local)''').fetchone()[0]
        if invalid:
            raise ValueError('Invalid prediction payload')
        con.execute('''CREATE TEMP TABLE lookup AS SELECT cast(LocationID AS INTEGER) AS zone_id, Zone AS zone_name, Borough AS borough
            FROM read_csv(?, header=true, all_varchar=true) WHERE Borough IS NOT NULL AND Borough NOT IN ('Unknown','N/A')
            AND Zone IS NOT NULL AND Zone NOT IN ('Unknown','Outside of NYC')''', [str(Path(sources[0]['path']) / 'zones.csv')])
        zones = [dict(dataset_id=gold_id, zone_id=r[0], zone_name=r[1], borough=r[2])
                 for r in con.execute('SELECT * FROM lookup ORDER BY zone_id').fetchall()]
        if con.execute('SELECT count(*) FROM payload p LEFT JOIN lookup z USING(zone_id) WHERE z.zone_id IS NULL').fetchone()[0]:
            raise ValueError('Unknown geographic zone')
        rows = [(run['run_id'], gold_id, z, origin, origin, origin+timedelta(hours=1), float(p), float(b), int(a))
                for z,origin,p,b,a in con.execute('SELECT * FROM payload ORDER BY forecast_origin_local,zone_id').fetchall()]
    expected_count = confirmation['baseline' if baseline_only else 'candidate']['overall']['rows']
    if not rows or len(rows) != expected_count:
        raise ValueError('Prediction count differs from the evaluated cohort')
    run['prediction_count'] = len(rows)
    return Payload(dataset, zones, model, run, rows)
