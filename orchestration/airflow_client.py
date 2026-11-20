"""Trigger and verify manual Airflow demos without printing credentials or tokens."""
import argparse
from datetime import datetime, timezone
import json
import time
import uuid

import requests

from serving.local import settings


def verify(mode='recovery',timeout=600):
    base='http://127.0.0.1:'+str(int(settings().get('FLEETPULSE_AIRFLOW_PORT','18080')))
    session=requests.Session()
    response=session.post(base+'/auth/token',json={'username':'fleetpulse',
        'password':settings()['FLEETPULSE_AIRFLOW_UI_PASSWORD']},timeout=15)
    response.raise_for_status()
    session.headers['Authorization']='Bearer '+response.json()['access_token']
    dag_id={'recovery':'fleetpulse_recovery_demo','replay':'fleetpulse_publish_replay',
            'pipeline':'fleetpulse_ingest_and_build'}[mode]
    path=base+'/api/v2/dags/'+dag_id
    def call(method,url,**kwargs):
        result=session.request(method,url,timeout=90,**kwargs)
        if not result.ok:
            # API error details are useful here; never include request headers or login responses.
            raise RuntimeError(f'Airflow API returned {result.status_code}: {result.text[:1000]}')
        return result.json()
    original=call('GET',path)
    call('PATCH',path,json={'is_paused':False})
    run_id='manual__verification_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'_'+uuid.uuid4().hex[:8]
    try:
        call('POST',path+'/dagRuns',json={'dag_run_id':run_id,'logical_date':None,'conf':{'activate':False} if mode=='replay' else {}})
        deadline=time.monotonic()+timeout
        last_state=None
        while time.monotonic()<deadline:
            result=call('GET',path+'/dagRuns/'+run_id)
            if result['state']!=last_state:
                print(json.dumps({'dag_id':dag_id,'run_id':run_id,'state':result['state']}),flush=True)
                last_state=result['state']
            if result['state'] in ('success','failed'):
                tasks=call('GET',path+'/dagRuns/'+run_id+'/taskInstances')['task_instances']
                summary=[{'task_id':task['task_id'],'state':task['state'],'try_number':task['try_number']} for task in tasks]
                print(json.dumps({'tasks':summary}),flush=True)
                if result['state']!='success':
                    raise RuntimeError('Airflow verification failed; inspect this run in the local UI')
                if mode=='recovery':
                    retry=next(task for task in summary if task['task_id']=='fail_then_retry')
                    if retry['try_number']!=2:
                        raise RuntimeError('Expected one injected failure followed by one successful retry')
                return {'dag_id':dag_id,'run_id':run_id,'state':'success','tasks':summary}
            time.sleep(2)
        raise TimeoutError('Airflow run did not finish within the verification deadline')
    finally:
        if original['is_paused']:
            call('PATCH',path,json={'is_paused':True})
        session.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['recovery','replay','pipeline'],default='recovery',nargs='?')
    args=parser.parse_args()
    verify(args.mode)
