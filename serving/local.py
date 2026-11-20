"""Local setup commands. Credentials remain in ignored .env, never command output."""
import argparse
import os
from pathlib import Path
import secrets
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def settings():
    result = {}
    path = ROOT / '.env'
    if path.exists():
        for line in path.read_text(encoding='utf-8').splitlines():
            if line.strip() and not line.lstrip().startswith('#'):
                key, value = line.split('=', 1)
                result[key.strip()] = value.strip()
    for key in list(result) + ['FLEETPULSE_DB_HOST', 'FLEETPULSE_DB_PORT', 'FLEETPULSE_DB_NAME',
                               'POSTGRES_PASSWORD', 'FLEETPULSE_WRITER_PASSWORD', 'FLEETPULSE_READER_PASSWORD']:
        if key in os.environ:
            result[key] = os.environ[key]
    return result


def connection_options(role='publisher', database=None):
    config = settings()
    user, key = {'admin': ('fleetpulse_admin', 'POSTGRES_PASSWORD'),
                 'publisher': ('fleetpulse_publisher', 'FLEETPULSE_WRITER_PASSWORD'),
                 'reader': ('fleetpulse_reader', 'FLEETPULSE_READER_PASSWORD')}[role]
    return dict(host=config.get('FLEETPULSE_DB_HOST', '127.0.0.1'),
                port=int(config.get('FLEETPULSE_DB_PORT', '55432')),
                dbname=database or config.get('FLEETPULSE_DB_NAME', 'fleetpulse'),
                user=user, password=config[key], connect_timeout=5)


def dotnet_environment(database=None, port=5080, db_port=None):
    options = connection_options('reader', database)
    # Npgsql quoted values escape embedded double quotes by doubling them.
    quote = lambda value: '"' + str(value).replace('"', '""') + '"'
    parts = {'Host': options['host'], 'Port': db_port or options['port'], 'Database': options['dbname'],
             'Username': options['user'], 'Password': options['password'], 'Timeout': 3,
             'Command Timeout': 15, 'Maximum Pool Size': 8, 'Include Error Detail': 'false'}
    return {**os.environ, 'ConnectionStrings__FleetPulse': ';'.join(f'{k}={quote(v)}' for k,v in parts.items()),
            'ASPNETCORE_URLS': f'http://127.0.0.1:{port}', 'ASPNETCORE_ENVIRONMENT': 'Production',
            'DOTNET_CLI_HOME': str(ROOT / 'data/dotnet'), 'NUGET_PACKAGES': str(ROOT / 'data/nuget-packages'),
            'DOTNET_CLI_TELEMETRY_OPTOUT': '1'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['init', 'api'])
    args = parser.parse_args()
    if args.command == 'init':
        path = ROOT / '.env'
        if path.exists():
            print('.env already exists; credentials preserved.')
            return
        values = {key: secrets.token_hex(24) for key in ('POSTGRES_PASSWORD', 'FLEETPULSE_WRITER_PASSWORD', 'FLEETPULSE_READER_PASSWORD')}
        values.update(FLEETPULSE_DB_HOST='127.0.0.1', FLEETPULSE_DB_PORT='55432', FLEETPULSE_DB_NAME='fleetpulse')
        with path.open('x', encoding='utf-8') as output:
            output.write(''.join(f'{key}={value}\n' for key,value in values.items()))
        print('Created private .env with random credentials.')
    else:
        raise SystemExit(subprocess.call(['dotnet', 'run', '--no-launch-profile', '--project',
            str(ROOT / 'backend/FleetPulse.Api')], env=dotnet_environment(), cwd=ROOT))


if __name__ == '__main__':
    main()
