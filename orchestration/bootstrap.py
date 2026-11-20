"""Prepare local-only UI credentials without echoing secrets into container logs."""
import json
import os
from pathlib import Path

path = Path('/opt/airflow/simple_auth_manager_passwords.json.generated')
path.write_text(json.dumps({'fleetpulse':os.environ['FLEETPULSE_AIRFLOW_UI_PASSWORD']}),encoding='utf-8')
path.chmod(0o600)
os.execvp('airflow',['airflow','standalone'])
