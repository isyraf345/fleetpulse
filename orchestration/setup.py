"""Provision separate local Airflow metadata and recovery-demo databases."""
import secrets

from psycopg import sql

from serving.database import connect, migrate
from serving.local import ROOT, settings


def setup():
    path=ROOT/'.env'
    if not path.exists():
        raise RuntimeError('Run python -m serving.local init first')
    config=settings()
    keys=['FLEETPULSE_AIRFLOW_PASSWORD','FLEETPULSE_AIRFLOW_UI_PASSWORD','FLEETPULSE_AIRFLOW_JWT_SECRET']
    missing={key:secrets.token_hex(32) for key in keys if not config.get(key)}
    if missing:
        with path.open('a',encoding='utf-8') as output:
            output.write('\n'+''.join(f'{key}={value}\n' for key,value in missing.items()))
        config=settings()
    jwt_key='FLEETPULSE_AIRFLOW_JWT_SECRET'
    if len(config[jwt_key])<64:
        # Upgrade an earlier local key without rotating the database or UI credentials.
        lines=path.read_text(encoding='utf-8').splitlines()
        lines=[line for line in lines if not line.startswith(jwt_key+'=')]
        path.write_text('\n'.join(lines)+'\n'+jwt_key+'='+secrets.token_hex(32)+'\n',encoding='utf-8')
    (ROOT/'data/orchestrated').mkdir(parents=True,exist_ok=True)
    with connect('admin','postgres') as conn:
        conn.autocommit=True
        if not conn.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',['fleetpulse_airflow']).fetchone():
            conn.execute(sql.SQL('CREATE ROLE fleetpulse_airflow LOGIN PASSWORD {}').format(sql.Literal(config[keys[0]])))
        for database,owner in [('fleetpulse_airflow','fleetpulse_airflow'),('fleetpulse_recovery','fleetpulse_admin')]:
            if not conn.execute('SELECT 1 FROM pg_database WHERE datname=%s',[database]).fetchone():
                conn.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(sql.Identifier(database),sql.Identifier(owner)))
    migrate('fleetpulse_recovery')
    print('Separate Airflow metadata and recovery databases are ready. UI credentials are in ignored .env.')


if __name__ == '__main__':
    setup()
