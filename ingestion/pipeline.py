"""Single-writer, immutable monthly ingestion. Run with python -m ingestion.pipeline."""
import argparse
import hashlib
import json
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

import duckdb


@contextmanager
def exclusive_lock(path):
    handle = path.open('x')
    try:
        yield
    finally:
        handle.close()
        path.unlink()


def checksum(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str) + '\n', encoding='utf-8')


def download(url, target):
    with urlopen(url, timeout=120) as response, target.open('wb') as output:
        shutil.copyfileobj(response, output, length=1024 * 1024)
        expected = response.headers.get('Content-Length')
        if expected and output.tell() != int(expected):
            raise ValueError('Incomplete source download')
        return {'url': url, 'downloaded_at': datetime.now(timezone.utc).isoformat(),
                'etag': response.headers.get('ETag'),
                'last_modified': response.headers.get('Last-Modified')}


def profile(trips, zones, output, month):
    start = datetime.strptime(month, '%Y-%m')
    end = datetime(start.year + (start.month == 12), start.month % 12 + 1, 1)
    with duckdb.connect(config={'memory_limit': '512MB', 'threads': 2}) as con:
        con.read_parquet(str(trips)).create_view('trips')
        schema = con.execute('DESCRIBE trips').fetchall()
        types = {row[0]: row[1] for row in schema}
        for name in ('tpep_pickup_datetime', 'PULocationID'):
            if name not in types:
                raise ValueError(f'Missing required column: {name}')
        if types['tpep_pickup_datetime'] not in ('TIMESTAMP', 'TIMESTAMP_NS', 'TIMESTAMP_MS'):
            raise ValueError('Pickup timestamp must be a timezone-naive timestamp')
        if types['PULocationID'] not in ('BIGINT', 'INTEGER', 'SMALLINT', 'TINYINT'):
            raise ValueError('Pickup zone must be an integer')
        con.execute('CREATE TABLE zones AS SELECT * FROM read_csv(?, header=true, all_varchar=true)', [str(zones)])
        required = {'LocationID', 'Borough', 'Zone', 'service_zone'}
        if not required.issubset({r[0] for r in con.execute('DESCRIBE zones').fetchall()}):
            raise ValueError('Invalid zone lookup schema')
        bad_zones = con.execute('''SELECT count(*) - count(DISTINCT try_cast(LocationID AS INTEGER)) FROM zones''').fetchone()[0]
        if bad_zones:
            raise ValueError('Null, invalid, or duplicate zone IDs')
        con.execute('''CREATE TEMP TABLE classified AS
            SELECT row_number() OVER () AS source_row_number,
                   tpep_pickup_datetime AS pickup_local, PULocationID AS zone_id,
                   CASE WHEN tpep_pickup_datetime IS NULL THEN 'null_timestamp'
                        WHEN tpep_pickup_datetime < ? OR tpep_pickup_datetime >= ? THEN 'outside_source_month'
                        WHEN PULocationID IS NULL THEN 'null_zone'
                        WHEN z.LocationID IS NULL THEN 'unmapped_zone'
                        WHEN z.Borough IS NULL OR z.Borough IN ('Unknown', 'N/A')
                             OR z.Zone IS NULL OR z.Zone IN ('Unknown', 'Outside of NYC') THEN 'non_geographic_zone'
                        ELSE NULL END AS rejection_reason
            FROM trips t LEFT JOIN zones z ON t.PULocationID = try_cast(z.LocationID AS INTEGER)''', [start, end])
        total, accepted, minimum, maximum = con.execute('''SELECT count(*),
            count(*) FILTER (WHERE rejection_reason IS NULL), min(pickup_local), max(pickup_local)
            FROM classified''').fetchone()
        if not total or not accepted:
            raise ValueError('No usable trips in source month')
        rejected = dict(con.execute('''SELECT rejection_reason, count(*) FROM classified
            WHERE rejection_reason IS NOT NULL GROUP BY 1''').fetchall())
        observed = con.execute('''SELECT min(pickup_local), max(pickup_local),
            count(DISTINCT date_trunc('hour', pickup_local)) FROM classified WHERE rejection_reason IS NULL''').fetchone()
        # A collision here is only a diagnostic: distinct trips may share pickup time and zone.
        collisions = con.execute('''SELECT coalesce(sum(n - 1), 0) FROM (
            SELECT count(*) n FROM classified GROUP BY pickup_local, zone_id HAVING count(*) > 1)''').fetchone()[0]
        for filename, predicate in [('silver.parquet', 'IS NULL'), ('quarantine.parquet', 'IS NOT NULL')]:
            dest = str(output / filename).replace("'", "''")
            con.execute(f"COPY (SELECT * FROM classified WHERE rejection_reason {predicate}) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        return {'schema': [{'name': r[0], 'type': r[1]} for r in schema],
                'rows': total, 'accepted': accepted, 'rejected': rejected,
                'raw_min_timestamp': minimum, 'raw_max_timestamp': maximum,
                'validated_min_timestamp': observed[0], 'validated_max_timestamp': observed[1],
                'observed_hours': observed[2], 'expected_calendar_hours': int((end-start).total_seconds()/3600),
                'pickup_zone_collision_excess': collisions,
                'coverage_note': 'Observed hours do not prove source completeness; no zero filling.',
                'timezone_note': 'Source local timestamps preserved; no UTC conversion or DST resolution.',
                'duplicate_policy': 'Retain all rows; collision counts are not proof of duplicates.'}


def run(month, root, fetch=download):
    if not re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])', month):
        raise ValueError('Month must be YYYY-MM')
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    destination = root / month
    lock = root / f'{month}.lock'
    # Exclusive file prevents concurrent writers to the same monthly snapshot.
    with exclusive_lock(lock):
        try:
            with tempfile.TemporaryDirectory(dir=root, prefix=f'.{month}-') as temporary:
                stage = Path(temporary)
                started = time.perf_counter()
                sources = {}
                for name, url in {
                    'trips.parquet': f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet',
                    'zones.csv': 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
                }.items():
                    metadata = fetch(url, stage / name)
                    sources[name] = {**metadata, 'sha256': checksum(stage / name), 'bytes': (stage / name).stat().st_size}
                if destination.exists():
                    old = json.loads((destination / 'manifest.json').read_text(encoding='utf-8'))
                    for name, info in sources.items():
                        if info['sha256'] != old['sources'][name]['sha256']:
                            raise ValueError(f'Upstream source changed: {name}; existing snapshot preserved. Use a new data root to inspect revision.')
                    for name, expected in old['outputs'].items():
                        if checksum(destination / name) != expected:
                            raise ValueError(f'Local artifact corrupted: {name}')
                    for name, info in old['sources'].items():
                        if checksum(destination / name) != info['sha256']:
                            raise ValueError(f'Local source corrupted: {name}')
                    return {'status': 'unchanged', 'path': str(destination)}
                report = profile(stage / 'trips.parquet', stage / 'zones.csv', stage, month)
                write_json(stage / 'profile.json', report)
                outputs = {name: checksum(stage / name) for name in ('silver.parquet', 'quarantine.parquet', 'profile.json')}
                write_json(stage / 'manifest.json', {'format_version': 1, 'month': month,
                    'sources': sources, 'outputs': outputs,
                    'pipeline_sha256': checksum(Path(__file__)), 'duckdb_version': duckdb.__version__,
                    'elapsed_seconds': round(time.perf_counter() - started, 3),
                    'artifact_bytes': sum(p.stat().st_size for p in stage.iterdir()),
                    'created_at': datetime.now(timezone.utc).isoformat()})
                # Same filesystem directory rename publishes all artifacts together.
                stage.rename(destination)
                return {'status': 'created', 'path': str(destination), 'rows': report['rows'], 'accepted': report['accepted']}
        finally:
            # TemporaryDirectory removes unpublished staging artifacts on failure.
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--month', default='2024-01')
    parser.add_argument('--data-root', type=Path, default=Path('data/snapshots'))
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.month, args.data_root)))
    except Exception as error:
        parser.exit(1, f'Ingestion failed: {error}\n')


if __name__ == '__main__':
    main()
