"""Build tested, immutable Gold artifacts from verified ingestion snapshots."""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

import duckdb

from ingestion.pipeline import checksum, exclusive_lock, write_json
from transformations.calendar import calendar_rows, month_bounds

PROJECT = Path(__file__).resolve().parent / 'dbt'
REPO = PROJECT.parent.parent


def verified_sources(snapshot_root, months):
    sources = []
    for month in months:
        start, _ = month_bounds(month)
        if start.strftime('%Y-%m') != month:
            raise ValueError('Months must use YYYY-MM format')
        folder = Path(snapshot_root).resolve() / month
        manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
        if manifest['month'] != month or manifest['format_version'] != 1:
            raise ValueError(f'Unexpected manifest format or month: {month}')
        for filename in ('trips.parquet', 'zones.csv'):
            if checksum(folder / filename) != manifest['sources'][filename]['sha256']:
                raise ValueError(f'Source checksum mismatch: {month}/{filename}')
        for filename in ('silver.parquet', 'quarantine.parquet', 'profile.json'):
            if checksum(folder / filename) != manifest['outputs'][filename]:
                raise ValueError(f'Artifact checksum mismatch: {month}/{filename}')
        sources.append({'month': month, 'path': str(folder), 'manifest': manifest,
                        'manifest_sha256': checksum(folder / 'manifest.json')})
    if len({s['manifest']['sources']['zones.csv']['sha256'] for s in sources}) != 1:
        raise ValueError('Zone reference snapshots differ; reconcile them before combining months')
    return sources


def prepare_database(database, sources):
    with duckdb.connect(str(database), config={'memory_limit': '512MB', 'threads': 2}) as con:
        con.execute('CREATE SCHEMA raw')
        con.execute('CREATE TABLE raw.pickups (pickup_local TIMESTAMP, zone_id INTEGER)')
        for source in sources:
            silver = Path(source['path']) / 'silver.parquet'
            start, end = month_bounds(source['month'])
            invalid = con.execute('''SELECT count(*) FROM read_parquet(?) WHERE
                pickup_local IS NULL OR zone_id IS NULL OR rejection_reason IS NOT NULL
                OR pickup_local < ? OR pickup_local >= ?''', [str(silver), start, end]).fetchone()[0]
            if invalid:
                raise ValueError(f"Silver contract violation in {source['month']}")
            expected = json.loads((Path(source['path']) / 'profile.json').read_text())['accepted']
            actual = con.execute('SELECT count(*) FROM read_parquet(?)', [str(silver)]).fetchone()[0]
            if expected != actual:
                raise ValueError('Silver row count disagrees with ingestion profile')
            con.execute('INSERT INTO raw.pickups SELECT pickup_local, zone_id FROM read_parquet(?)', [str(silver)])
        con.execute('''CREATE TABLE raw.zones AS
            SELECT cast(LocationID AS INTEGER) AS zone_id, Borough AS borough, Zone AS zone_name
            FROM read_csv(?, header=true, all_varchar=true)
            WHERE Borough IS NOT NULL AND Borough NOT IN ('Unknown', 'N/A')
              AND Zone IS NOT NULL AND Zone NOT IN ('Unknown', 'Outside of NYC')''',
            [str(Path(sources[0]['path']) / 'zones.csv')])
        if con.execute('SELECT count(*) = 0 OR count(*) != count(DISTINCT zone_id) FROM raw.zones').fetchone()[0]:
            raise ValueError('Empty or duplicate eligible zone lookup')
        con.execute('CREATE TABLE raw.calendar (hour_local TIMESTAMP PRIMARY KEY, snapshot_present BOOLEAN, transition_day BOOLEAN)')
        con.executemany('INSERT INTO raw.calendar VALUES (?, ?, ?)', list(calendar_rows([s['month'] for s in sources])))


def run_dbt(database, artifact_dir):
    env = {**os.environ, 'FLEETPULSE_DB_PATH': str(database),
           'DBT_TARGET_PATH': str(artifact_dir / 'dbt_target'),
           'DBT_LOG_PATH': str(artifact_dir / 'dbt_logs'),
           'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false', 'DBT_USE_COLORS': 'false'}
    command = [sys.executable, '-c', 'from dbt.cli.main import cli; cli()', 'build',
               '--project-dir', str(PROJECT), '--profiles-dir', str(PROJECT), '--no-partial-parse']
    result = subprocess.run(command, env=env, capture_output=True, text=True, encoding='utf-8', errors='replace')
    if result.returncode:
        raise RuntimeError(f'dbt build failed; no Gold snapshot published:\n{result.stdout}\n{result.stderr}')
    return json.loads((artifact_dir / 'dbt_target' / 'run_results.json').read_text())


def code_checksums():
    files = [Path(__file__), PROJECT.parent / 'calendar.py', REPO / 'ingestion/pipeline.py', REPO / 'requirements-lock.txt']
    files += [p for p in PROJECT.rglob('*') if p.is_file() and p.suffix in ('.sql', '.yml')
              and not {'target', 'logs'}.intersection(p.parts) and p.name != '.user.yml']
    return {p.relative_to(REPO).as_posix(): checksum(p) for p in sorted(files)}


def build(months, snapshot_root=Path('data/snapshots'), output_root=Path('data/gold')):
    months = sorted(set(months))
    if not months:
        raise ValueError('At least one month is required')
    sources = verified_sources(snapshot_root, months)
    identity = {'sources': {s['month']: s['manifest_sha256'] for s in sources},
                'code': code_checksums(), 'policy': 'local-replay-v1',
                'versions': {p: version(p) for p in ('duckdb', 'dbt-core', 'dbt-duckdb', 'tzdata')}}
    run_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    destination = root / run_id
    with exclusive_lock(root / f'{run_id}.lock'):
        if destination.exists():
            manifest = json.loads((destination / 'manifest.json').read_text())
            if manifest['identity'] != identity:
                raise ValueError('Gold manifest identity mismatch')
            for name, expected in manifest['outputs'].items():
                # All published artifact paths must remain inside this snapshot.
                path = (destination / name).resolve()
                if not path.is_relative_to(destination) or checksum(path) != expected:
                    raise ValueError(f'Gold checksum mismatch: {name}')
            return {'status': 'unchanged', 'run_id': run_id, 'path': str(destination)}
        with tempfile.TemporaryDirectory(dir=root, prefix='.build-') as temporary:
            stage = Path(temporary)
            started = time.perf_counter()
            database = stage / 'analytics.duckdb'
            prepare_database(database, sources)
            results = run_dbt(database, stage)
            tests = [r for r in results['results'] if r['unique_id'].startswith('test.')]
            if not tests or any(r['status'] != 'pass' for r in tests):
                raise ValueError('Gold publication requires all dbt tests to pass')
            with duckdb.connect(str(database), config={'memory_limit': '512MB', 'threads': 2}) as con:
                summary = {
                    'zones': con.execute('SELECT count(*) FROM raw.zones').fetchone()[0],
                    'zone_hours': con.execute('SELECT count(*) FROM zone_hours').fetchone()[0],
                    'recorded_pickups': con.execute('SELECT sum(recorded_pickups) FROM zone_hours').fetchone()[0],
                    'coverage_rows': dict(con.execute('SELECT coverage_status, count(*) FROM zone_hours GROUP BY 1').fetchall()),
                    'zero_rows': con.execute('SELECT count(*) FROM zone_hours WHERE pickup_count = 0').fetchone()[0],
                    'history_ready_rows': con.execute('SELECT count(*) FROM hourly_features WHERE history_ready').fetchone()[0],
                    'dbt_tests_passed': len(tests),
                }
                for table in ('zone_hours', 'hourly_features'):
                    path = str(stage / f'{table}.parquet').replace("'", "''")
                    con.execute(f"COPY (SELECT * FROM {table} ORDER BY 1, 2) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                con.execute('CHECKPOINT')
            summary['elapsed_seconds'] = round(time.perf_counter() - started, 3)
            write_json(stage / 'summary.json', summary)
            outputs = {p.relative_to(stage).as_posix(): checksum(p) for p in stage.rglob('*') if p.is_file()}
            write_json(stage / 'manifest.json', {'run_id': run_id, 'identity': identity,
                'created_at': datetime.now(timezone.utc).isoformat(), 'outputs': outputs,
                'artifact_bytes': sum(p.stat().st_size for p in stage.rglob('*') if p.is_file()),
                'source_paths': {s['month']: s['path'] for s in sources}})
            stage.rename(destination)
            return {'status': 'created', 'run_id': run_id, 'path': str(destination), **summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--months', nargs='+', default=['2024-01'])
    parser.add_argument('--snapshot-root', type=Path, default=Path('data/snapshots'))
    parser.add_argument('--output-root', type=Path, default=Path('data/gold'))
    args = parser.parse_args()
    try:
        print(json.dumps(build(args.months, args.snapshot_root, args.output_root)))
    except Exception as error:
        parser.exit(1, f'Analytics build failed: {error}\n')


if __name__ == '__main__':
    main()
