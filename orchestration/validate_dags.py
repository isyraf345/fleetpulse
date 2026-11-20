"""Import real Airflow DAGs and enforce the local orchestration contract."""
from airflow.dag_processing.dagbag import DagBag

bag=DagBag(dag_folder='/opt/project/orchestration/dags')
if bag.import_errors:
    raise RuntimeError(bag.import_errors)
expected={
    'fleetpulse_ingest_and_build':{'plan','ingest_month','build_gold','verify_gold'},
    'fleetpulse_publish_replay':{'verify_artifacts','publish_complete_run'},
    'fleetpulse_recovery_demo':{'fail_then_retry','rollback_to_prior_model'},
}
assert set(bag.dags)==set(expected),set(bag.dags)
for name,tasks in expected.items():
    dag=bag.dags[name]
    assert set(dag.task_ids)==tasks
    assert dag.max_active_runs==1 and dag.max_active_tasks==1 and not dag.catchup
    assert dag.schedule is None
    assert all(task.retries>=0 for task in dag.tasks)
assert bag.dags['fleetpulse_recovery_demo'].get_task('fail_then_retry').retries==1
print('Validated three manual, serial DAGs with explicit retry policy and no training tasks.')
