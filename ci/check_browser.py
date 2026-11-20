"""Run browser checks against an isolated database, then remove only that database."""
from datetime import timedelta
import os
import re
import shutil
import subprocess
import sys
import uuid

from psycopg import sql

from serving.database import connect, migrate
from serving.local import ROOT
from serving.publish import publish

# Reuse the same synthetic publication contract fixture as the database tests.
sys.path.insert(0, str(ROOT / 'tests'))
from test_serving import api, fixture


def browser_fixture():
    payload = fixture('ci-browser-synthetic')
    payload.model['model_name'] = 'synthetic_test_model'
    payload.zones.append(dict(dataset_id=payload.dataset['dataset_id'], zone_id=2,
                              zone_name='Second zone', borough='Manhattan'))
    start = payload.run['origins_start_local']
    payload.run['origins_end_exclusive_local'] = start + timedelta(hours=720)
    payload.rows = []
    for hour in range(720):
        origin = start + timedelta(hours=hour)
        actual = hour % 24
        for zone in (1, 2):
            payload.rows.append((payload.run['run_id'], payload.dataset['dataset_id'], zone,
                origin, origin, origin+timedelta(hours=1), actual+2.25, actual+3.0, actual))
    payload.run['prediction_count'] = len(payload.rows)
    return payload


def main():
    if not (ROOT/'backend/FleetPulse.Api/wwwroot/index.html').exists():
        raise RuntimeError('Build Angular before browser verification')
    database = 'fleetpulse_ci_' + uuid.uuid4().hex
    with connect('admin', 'postgres') as conn:
        conn.autocommit = True
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
    try:
        migrate(database)
        payload = browser_fixture()
        publish(payload, database, activate=True)
        with api(database) as base:
            env = {**os.environ, 'FLEETPULSE_E2E_FIXTURE': '1', 'FLEETPULSE_BASE_URL': base}
            npm = shutil.which('npm.cmd' if os.name == 'nt' else 'npm')
            if npm is None:
                raise RuntimeError('npm is required for browser verification')
            result = subprocess.run([npm, 'test'], cwd=ROOT/'frontend', env=env)
            return result.returncode
    finally:
        # The generated name is the only cleanup target; never use a configured DB name.
        if not re.fullmatch(r'fleetpulse_ci_[0-9a-f]{32}', database):
            raise RuntimeError('Refusing cleanup of a non-CI database')
        with connect('admin', 'postgres') as conn:
            conn.autocommit = True
            conn.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))


if __name__ == '__main__':
    raise SystemExit(main())
