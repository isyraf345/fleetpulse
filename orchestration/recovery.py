"""Synthetic partial-COPY failure, retry and rollback in a dedicated demo database."""
import argparse
from datetime import datetime, timedelta
import json
import os

from serving.artifacts import Payload, identity_hash
from serving.database import connect
from serving.publish import publish, change_active


def fixture(tag, version):
    dataset_id = identity_hash({'demo': tag, 'dataset': 1})
    model_id = identity_hash({'demo': tag, 'model': version})
    run_id = identity_hash({'demo': tag, 'run': version})
    start = datetime(2024,6,1)
    dataset = dict(dataset_id=dataset_id,manifest_sha256='1'*64,zone_reference_sha256='2'*64,
        event_time_basis='local_wall_clock',calendar_assumption='America/New_York',feature_policy='local-replay-v1',
        first_observed_local=datetime(2024,1,1),last_observed_local=datetime(2024,6,30,23,59,57),
        source_manifests={'synthetic_recovery_demo': tag})
    zones = [dict(dataset_id=dataset_id,zone_id=1,zone_name='Synthetic recovery zone',borough='Test fixture')]
    model = dict(model_id=model_id,experiment_id='3'*64,experiment_manifest_sha256='4'*64,
        training_dataset_id=dataset_id,model_name=f'synthetic_recovery_{version}',model_kind='lightgbm_poisson',
        artifact_sha256='5'*64,training_start_local=datetime(2024,1,1),training_end_exclusive_local=datetime(2024,5,1),
        feature_schema=[],evaluation_summary={'synthetic': True})
    run = dict(run_id=run_id,dataset_id=dataset_id,model_id=model_id,mode='historical_replay',source_split='test',
        prediction_artifact_sha256='6'*64,baseline_artifact_sha256='7'*64,publication_contract_version=1,
        publisher_code_sha256='8'*64,origins_start_local=start,origins_end_exclusive_local=start+timedelta(hours=3),prediction_count=3)
    rows = [(run_id,dataset_id,1,start+timedelta(hours=i),start+timedelta(hours=i),start+timedelta(hours=i+1),
             float(i+version),float(i+2),i) for i in range(3)]
    return Payload(dataset,zones,model,run,rows)


def state(database, run_id):
    with connect('reader', database) as conn:
        return conn.execute('''SELECT
            (SELECT count(*) FROM fleetpulse.forecast_runs WHERE run_id=%s),
            (SELECT count(*) FROM fleetpulse.predictions WHERE run_id=%s),
            (SELECT run_id FROM fleetpulse.active_releases WHERE slot='historical-demo'),
            (SELECT count(*) FROM fleetpulse.release_history)''', [run_id,run_id]).fetchone()


def demonstrate(tag, attempt=1, rollback=False, database=None):
    database = database or os.environ.get('FLEETPULSE_RECOVERY_DB', 'fleetpulse_recovery')
    if database != 'fleetpulse_recovery' and not database.startswith('fleetpulse_test_'):
        raise ValueError('Recovery demonstration requires its dedicated database')
    if not tag or len(tag)>250 or attempt<1:
        raise ValueError('Supply a short run tag and positive attempt number')
    previous, candidate = fixture(tag,1), fixture(tag,2)
    publish(previous,database,activate=True)
    if rollback:
        # Explicit target makes re-clearing this task idempotent, instead of toggling history.
        change_active(previous.run['run_id'],database,reason='Recovery demo rollback to complete prior model')
        assert state(database,candidate.run['run_id'])[:3] == (1,3,previous.run['run_id'])
        result = {'status':'rolled_back','active_run_id':previous.run['run_id'],'retained_candidate_rows':3}
    elif attempt == 1:
        before = state(database,candidate.run['run_id'])
        if before[0]:
            result={'status':'already_complete','run_id':candidate.run['run_id']}
            print(json.dumps(result),flush=True)
            return result
        def observer():
            assert state(database,candidate.run['run_id']) == before, 'Partial publication became visible'
        try:
            publish(candidate,database,activate=True,fail_after=2,observer=observer)
        except RuntimeError:
            assert state(database,candidate.run['run_id']) == before, 'Rollback did not preserve prior release'
            print(json.dumps({'event':'failure_verified','candidate_rows':0,'active_run_id':before[2],
                              'history_unchanged':True}),flush=True)
            raise RuntimeError('Intentional first-attempt COPY failure; retry is safe') from None
        raise AssertionError('Failure injection did not execute')
    else:
        publish(candidate,database,activate=True)
        before = state(database,candidate.run['run_id'])
        assert before[:3] == (1,3,candidate.run['run_id'])
        assert publish(candidate,database,activate=True)['status'] == 'unchanged'
        assert state(database,candidate.run['run_id']) == before
        result = {'status':'recovered','run_id':candidate.run['run_id'],'rows':3,'retry_unchanged':True}
    print(json.dumps(result),flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag',required=True)
    parser.add_argument('--attempt',type=int,default=1)
    parser.add_argument('--rollback',action='store_true')
    demonstrate(**vars(parser.parse_args()))


if __name__ == '__main__':
    main()
