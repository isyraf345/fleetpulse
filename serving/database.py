"""Checksummed migrations and least-privilege local database roles."""
import argparse
from pathlib import Path

import psycopg
from psycopg import sql

from ingestion.pipeline import checksum
from serving.local import ROOT, connection_options, settings

LOCK_ID = 747310401


def connect(role='publisher', database=None):
    return psycopg.connect(**connection_options(role, database))


def migrate(database=None):
    with connect('admin', database) as conn:
        conn.execute('SELECT pg_advisory_xact_lock(%s)', [LOCK_ID])
        conn.execute('CREATE SCHEMA IF NOT EXISTS fleetpulse')
        conn.execute('''CREATE TABLE IF NOT EXISTS fleetpulse.schema_migrations (
            version text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
        for path in sorted((ROOT / 'serving/migrations').glob('*.sql')):
            digest = checksum(path)
            row = conn.execute('SELECT sha256 FROM fleetpulse.schema_migrations WHERE version=%s', [path.stem]).fetchone()
            if row:
                if row[0] != digest:
                    raise ValueError(f'Applied migration checksum changed: {path.name}')
                continue
            conn.execute(path.read_text(encoding='utf-8'))
            conn.execute('INSERT INTO fleetpulse.schema_migrations(version,sha256) VALUES (%s,%s)', [path.stem,digest])
        config = settings()
        for role, key in [('fleetpulse_publisher','FLEETPULSE_WRITER_PASSWORD'), ('fleetpulse_reader','FLEETPULSE_READER_PASSWORD')]:
            if not conn.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', [role]).fetchone():
                conn.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(sql.Identifier(role), sql.Literal(config[key])))
        conn.execute('GRANT USAGE ON SCHEMA fleetpulse TO fleetpulse_publisher, fleetpulse_reader')
        conn.execute('GRANT SELECT ON ALL TABLES IN SCHEMA fleetpulse TO fleetpulse_publisher, fleetpulse_reader')
        conn.execute('''GRANT INSERT ON fleetpulse.dataset_snapshots, fleetpulse.zones, fleetpulse.model_versions,
            fleetpulse.forecast_runs, fleetpulse.predictions, fleetpulse.release_history TO fleetpulse_publisher''')
        conn.execute('GRANT INSERT, UPDATE ON fleetpulse.active_releases TO fleetpulse_publisher')
        conn.execute('GRANT USAGE ON ALL SEQUENCES IN SCHEMA fleetpulse TO fleetpulse_publisher')
        conn.execute('ALTER ROLE fleetpulse_reader SET default_transaction_read_only = on')
    return {'status': 'migrated', 'database': database or settings().get('FLEETPULSE_DB_NAME', 'fleetpulse')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database')
    args = parser.parse_args()
    print(migrate(args.database))


if __name__ == '__main__':
    main()
