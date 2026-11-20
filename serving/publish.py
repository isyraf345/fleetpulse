"""Atomically publish immutable replay batches and activate/roll back complete runs."""
import argparse
import json
from pathlib import Path

from psycopg import sql
from psycopg.types.json import Jsonb

from serving.artifacts import prepare
from serving.database import LOCK_ID, connect

PREDICTION_COLUMNS = ('run_id','dataset_id','zone_id','forecast_origin_local','target_start_local',
                      'target_end_local','predicted_pickups','baseline_pickups','actual_pickups')


def ensure_row(conn, table, values, keys):
    columns = list(values)
    query = sql.SQL('INSERT INTO fleetpulse.{} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING').format(
        sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier,columns)),
        sql.SQL(',').join(sql.Placeholder() for _ in columns), sql.SQL(',').join(map(sql.Identifier,keys)))
    adapt = lambda value: Jsonb(value) if isinstance(value,(dict,list)) else value
    conn.execute(query, [adapt(values[c]) for c in columns])
    row = conn.execute(sql.SQL('SELECT {} FROM fleetpulse.{} WHERE {}').format(
        sql.SQL(',').join(map(sql.Identifier,columns)), sql.Identifier(table),
        sql.SQL(' AND ').join(sql.SQL('{}=%s').format(sql.Identifier(k)) for k in keys)), [values[k] for k in keys]).fetchone()
    if row != tuple(values[c] for c in columns):
        raise ValueError(f'Immutable {table} identity conflicts with stored metadata')


def copy_rows(conn, table, rows, fail_after=None, observer=None):
    with conn.cursor().copy(sql.SQL('COPY {} ({}) FROM STDIN').format(
            sql.Identifier(*table.split('.')), sql.SQL(',').join(map(sql.Identifier,PREDICTION_COLUMNS)))) as copy:
        for count, row in enumerate(rows, start=1):
            copy.write_row(row)
            if fail_after is not None and count == fail_after:
                if observer:
                    observer()
                raise RuntimeError('Injected publication failure during COPY')


def check_complete(conn, run_id):
    run = conn.execute('SELECT prediction_count,origins_start_local,origins_end_exclusive_local FROM fleetpulse.forecast_runs WHERE run_id=%s', [run_id]).fetchone()
    if not run:
        raise ValueError('No published run with that ID')
    count, outside = conn.execute('''SELECT count(*), count(*) FILTER (WHERE forecast_origin_local<%s OR forecast_origin_local>=%s)
        FROM fleetpulse.predictions WHERE run_id=%s''', [run[1],run[2],run_id]).fetchone()
    if count != run[0] or outside:
        raise ValueError('Run is incomplete or has invalid bounds')


def activate_in_transaction(conn, run_id, reason):
    if not reason.strip():
        raise ValueError('Activation reason is required')
    check_complete(conn, run_id)
    old = conn.execute("SELECT run_id FROM fleetpulse.active_releases WHERE slot='historical-demo'").fetchone()
    previous = old[0] if old else None
    if previous == run_id:
        return False
    conn.execute('''INSERT INTO fleetpulse.release_history(slot,previous_run_id,next_run_id,reason)
        VALUES ('historical-demo',%s,%s,%s)''', [previous,run_id,reason])
    conn.execute('''INSERT INTO fleetpulse.active_releases(slot,run_id) VALUES ('historical-demo',%s)
        ON CONFLICT(slot) DO UPDATE SET run_id=EXCLUDED.run_id, changed_at_utc=CURRENT_TIMESTAMP''', [run_id])
    return True


def publish(payload, database=None, activate=False, fail_after=None, observer=None):
    with connect(database=database) as conn:
        conn.execute('SELECT pg_advisory_xact_lock(%s)', [LOCK_ID])
        ensure_row(conn, 'dataset_snapshots', payload.dataset, ['dataset_id'])
        for zone in payload.zones:
            ensure_row(conn, 'zones', zone, ['dataset_id','zone_id'])
        stored_zones = conn.execute('SELECT zone_id,zone_name,borough FROM fleetpulse.zones WHERE dataset_id=%s ORDER BY zone_id', [payload.dataset['dataset_id']]).fetchall()
        expected_zones = sorted((z['zone_id'],z['zone_name'],z['borough']) for z in payload.zones)
        if stored_zones != expected_zones:
            raise ValueError('Stored zone reference differs')
        ensure_row(conn, 'model_versions', payload.model, ['model_id'])
        exists = conn.execute('SELECT 1 FROM fleetpulse.forecast_runs WHERE run_id=%s', [payload.run['run_id']]).fetchone()
        ensure_row(conn, 'forecast_runs', payload.run, ['run_id'])
        if exists:
            conn.execute('CREATE TEMP TABLE incoming_predictions (LIKE fleetpulse.predictions) ON COMMIT DROP')
            copy_rows(conn, 'incoming_predictions', payload.rows)
            difference = conn.execute('''SELECT EXISTS (
                (SELECT * FROM fleetpulse.predictions WHERE run_id=%s EXCEPT ALL SELECT * FROM incoming_predictions)
                UNION ALL (SELECT * FROM incoming_predictions EXCEPT ALL SELECT * FROM fleetpulse.predictions WHERE run_id=%s))''',
                [payload.run['run_id'],payload.run['run_id']]).fetchone()[0]
            if difference:
                raise ValueError('Stored predictions differ from the expected immutable payload')
            check_complete(conn, payload.run['run_id'])
            return {'status': 'unchanged', 'run_id': payload.run['run_id'], 'rows': len(payload.rows)}
        copy_rows(conn, 'fleetpulse.predictions', payload.rows, fail_after, observer)
        check_complete(conn, payload.run['run_id'])
        if activate:
            activate_in_transaction(conn, payload.run['run_id'], 'Initial publication activation')
    return {'status': 'published', 'run_id': payload.run['run_id'], 'rows': len(payload.rows), 'activated': activate}


def change_active(run_id=None, database=None, reason='Local operator activation', rollback=False):
    with connect(database=database) as conn:
        conn.execute('SELECT pg_advisory_xact_lock(%s)', [LOCK_ID])
        if rollback:
            row = conn.execute('''SELECT h.previous_run_id FROM fleetpulse.release_history h
                JOIN fleetpulse.active_releases a ON a.slot=h.slot AND a.run_id=h.next_run_id
                ORDER BY h.change_id DESC LIMIT 1''').fetchone()
            if not row or row[0] is None:
                raise ValueError('No previous active run to roll back to')
            run_id = row[0]
        changed = activate_in_transaction(conn, run_id, reason)
    return {'status': 'activated' if changed else 'unchanged', 'run_id': run_id}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    publication = commands.add_parser('publish')
    publication.add_argument('--experiment', type=Path, required=True)
    publication.add_argument('--activate', action='store_true')
    activation = commands.add_parser('activate')
    activation.add_argument('--run-id', required=True)
    activation.add_argument('--reason', required=True)
    rollback = commands.add_parser('rollback')
    rollback.add_argument('--reason', required=True)
    args = parser.parse_args()
    if args.command == 'publish':
        result = publish(prepare(args.experiment), activate=args.activate)
    else:
        result = change_active(getattr(args, 'run_id', None), reason=args.reason, rollback=args.command=='rollback')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
