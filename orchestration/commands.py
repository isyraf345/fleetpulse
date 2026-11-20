"""Thin CLI adapters for existing ingestion, dbt builds, and replay publication."""
import argparse
import json
from pathlib import Path

from orchestration.contract import artifact_id, months_between, scoped_path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'data/orchestrated'


def execute(command, **options):
    if command == 'ingest':
        from ingestion.pipeline import run
        month = months_between(options['month'], options['month'])[0]
        return run(month, OUTPUT/'snapshots')
    if command == 'build':
        from transformations.build import build
        months = months_between(options['start_month'], options['end_month'])
        return build(months, OUTPUT/'snapshots', OUTPUT/'gold')
    if command == 'verify-gold':
        from serving.artifacts import verify_bundle
        path = scoped_path(OUTPUT/'gold', options['path'])
        manifest = verify_bundle(path)
        return {'status': 'verified', 'run_id': manifest['run_id'], 'path': str(path)}
    if command in ('verify-replay', 'publish-replay'):
        from serving.artifacts import prepare
        path = ROOT/'data/experiments'/artifact_id(options['experiment_id'])
        payload = prepare(path)
        if command == 'verify-replay':
            return {'status': 'verified', 'run_id': payload.run['run_id'], 'rows': len(payload.rows)}
        if options.get('expected_run_id') and payload.run['run_id'] != artifact_id(options['expected_run_id']):
            raise ValueError('Artifacts changed after verification; nothing was published')
        from serving.publish import publish
        # Activation is an explicit operator action, never the default for retries/backfills.
        return publish(payload, activate=options.get('activate', False))
    raise ValueError('Unknown orchestration command')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('ingest').add_argument('--month', required=True)
    build = sub.add_parser('build')
    build.add_argument('--start-month', required=True)
    build.add_argument('--end-month', required=True)
    sub.add_parser('verify-gold').add_argument('--path', required=True)
    for command in ('verify-replay', 'publish-replay'):
        item = sub.add_parser(command)
        item.add_argument('--experiment-id', required=True)
        if command == 'publish-replay':
            item.add_argument('--activate', action='store_true')
            item.add_argument('--expected-run-id')
    args = vars(parser.parse_args())
    print(json.dumps(execute(**args)), flush=True)


if __name__ == '__main__':
    main()
