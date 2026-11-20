"""Manual, serial FleetPulse workflows. No training or holdout rescoring tasks."""
from datetime import datetime, timedelta, timezone
import json
import os
import subprocess

from airflow.sdk import dag, task, Param, get_current_context

from orchestration.contract import APPROVED_EXPERIMENT, months_between

PYTHON = os.environ.get('FLEETPULSE_PIPELINE_PYTHON','/opt/fleetpulse-venv/bin/python')
ROOT = '/opt/project'
DEFAULTS = {'retries':2,'retry_delay':timedelta(seconds=15),'retry_exponential_backoff':True,
            'execution_timeout':timedelta(hours=1)}


def command(module, *arguments):
    # Argument arrays keep run parameters out of shell interpolation.
    result = subprocess.run([PYTHON,'-m',module,*arguments],cwd=ROOT,capture_output=True,text=True)
    if result.stdout:
        print(result.stdout,flush=True)
    if result.returncode:
        if result.stderr:
            print(result.stderr[-12000:],flush=True)
        raise RuntimeError(f'{module} failed with exit code {result.returncode}')
    return json.loads(result.stdout.strip().splitlines()[-1])


COMMON = dict(schedule=None,catchup=False,max_active_runs=1,max_active_tasks=1,
              start_date=datetime(2024,1,1,tzinfo=timezone.utc),default_args=DEFAULTS,
              tags=['fleetpulse','manual','historical'])


@dag(dag_id='fleetpulse_ingest_and_build',params={
    'start_month':Param('2024-01',type='string',pattern=r'^20\d{2}-(0[1-9]|1[0-2])$'),
    'end_month':Param('2024-01',type='string',pattern=r'^20\d{2}-(0[1-9]|1[0-2])$')},**COMMON)
def ingest_and_build():
    @task(retries=0)
    def plan():
        params=get_current_context()['params']
        return months_between(params['start_month'],params['end_month'])
    @task
    def ingest_month(month):
        return command('orchestration.commands','ingest','--month',month)
    @task
    def build_gold(months, ingested):
        return command('orchestration.commands','build','--start-month',months[0],'--end-month',months[-1])
    @task
    def verify_gold(result):
        return command('orchestration.commands','verify-gold','--path',result['path'])
    months=plan()
    ingested=ingest_month.expand(month=months)
    verify_gold(build_gold(months,ingested))


@dag(dag_id='fleetpulse_publish_replay',params={
    'experiment_id':Param(APPROVED_EXPERIMENT,type='string',pattern='^[0-9a-f]{64}$'),
    'activate':Param(False,type='boolean')},**COMMON)
def publish_replay():
    @task
    def verify_artifacts():
        return command('orchestration.commands','verify-replay','--experiment-id',get_current_context()['params']['experiment_id'])
    @task
    def publish_complete_run(verified):
        params=get_current_context()['params']
        args=['publish-replay','--experiment-id',params['experiment_id'],'--expected-run-id',verified['run_id']]
        if params['activate']:
            args.append('--activate')
        result=command('orchestration.commands',*args)
        if result['run_id'] != verified['run_id']:
            raise ValueError('Artifacts changed between verification and publication')
        return result
    publish_complete_run(verify_artifacts())


@dag(dag_id='fleetpulse_recovery_demo',**COMMON)
def recovery_demo():
    @task(retries=1,retry_delay=timedelta(seconds=10),retry_exponential_backoff=False)
    def fail_then_retry():
        context=get_current_context()
        return command('orchestration.recovery','--tag',context['run_id'],'--attempt',str(context['ti'].try_number))
    @task
    def rollback_to_prior_model(recovered):
        return command('orchestration.recovery','--tag',get_current_context()['run_id'],'--rollback')
    rollback_to_prior_model(fail_then_retry())


ingest_and_build()
publish_replay()
recovery_demo()
