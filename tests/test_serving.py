"""Real PostgreSQL/API checks. Enable with FLEETPULSE_INTEGRATION=1 after local setup."""
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import uuid

import psycopg
from psycopg import sql

from serving.artifacts import Payload, identity_hash, verify_bundle
from serving.database import connect, migrate
from serving.local import ROOT, dotnet_environment
from serving.publish import publish, change_active


def fixture(tag='first'):
    digest = identity_hash(tag)
    dataset_id, model_id = identity_hash('dataset'), identity_hash('model')
    start = datetime(2024,6,1)
    dataset = dict(dataset_id=dataset_id,manifest_sha256='1'*64,zone_reference_sha256='2'*64,
        event_time_basis='local_wall_clock',calendar_assumption='America/New_York',feature_policy='local-replay-v1',
        first_observed_local=datetime(2024,1,1),last_observed_local=datetime(2024,6,30,23,59,57),source_manifests={})
    zones = [dict(dataset_id=dataset_id,zone_id=1,zone_name='Test zone',borough='Queens')]
    model = dict(model_id=model_id,experiment_id='3'*64,experiment_manifest_sha256='4'*64,training_dataset_id=dataset_id,
        model_name='test_model',model_kind='lightgbm_poisson',artifact_sha256='5'*64,
        training_start_local=datetime(2024,1,1),training_end_exclusive_local=datetime(2024,5,1),feature_schema=[],evaluation_summary={})
    run = dict(run_id=digest,dataset_id=dataset_id,model_id=model_id,mode='historical_replay',source_split='test',
        prediction_artifact_sha256='6'*64,baseline_artifact_sha256='7'*64,publication_contract_version=1,
        publisher_code_sha256='8'*64,origins_start_local=start,origins_end_exclusive_local=start+timedelta(hours=3),prediction_count=3)
    rows = [(digest,dataset_id,1,start+timedelta(hours=i),start+timedelta(hours=i),start+timedelta(hours=i+1),2.25+i,3.0+i,i) for i in range(3)]
    return Payload(dataset,zones,model,run,rows)


class ArtifactTests(unittest.TestCase):
    def test_bundle_rejects_tampering_and_pending(self):
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'output').write_text('original')
            identity = {'test':1}
            manifest = dict(identity=identity,run_id=hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest(),
                outputs={'output':hashlib.sha256(b'original').hexdigest()})
            (root/'manifest.json').write_text(json.dumps(manifest))
            verify_bundle(root)
            (root/'output').write_text('changed')
            with self.assertRaisesRegex(ValueError,'checksum'): verify_bundle(root)
            pending=root/'.pending-run'
            pending.mkdir()
            with self.assertRaisesRegex(ValueError,'pending'): verify_bundle(pending)


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0))
        return sock.getsockname()[1]


@contextmanager
def api(database, db_port=None):
    port = free_port()
    dll = ROOT/'backend/FleetPulse.Api/bin/Debug/net10.0/FleetPulse.Api.dll'
    if not dll.exists(): raise RuntimeError('Build the API before running integration tests')
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(['dotnet',str(dll)],cwd=ROOT,env=dotnet_environment(database,port,db_port),
            stdout=log,stderr=subprocess.STDOUT,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        base = f'http://127.0.0.1:{port}'
        try:
            for _ in range(100):
                if process.poll() is not None: raise RuntimeError('API exited before becoming reachable')
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.2): pass
                    break
                except OSError: time.sleep(.1)
            else: raise RuntimeError('API did not become reachable')
            yield base
        finally:
            process.terminate()
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def get(base,path):
    try:
        response=urlopen(base+path,timeout=10)
    except HTTPError as error: response=error
    with response:
        return response.status,json.load(response),response.headers.get_content_type()


@unittest.skipUnless(os.environ.get('FLEETPULSE_INTEGRATION')=='1','requires local PostgreSQL and built .NET API')
class ServingIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.database='fleetpulse_test_'+uuid.uuid4().hex
        with connect('admin','postgres') as conn:
            conn.autocommit=True
            conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.database)))
        migrate(self.database)
        self.payload=fixture()

    def tearDown(self):
        # Only databases created by this test instance are eligible for cleanup.
        assert self.database.startswith('fleetpulse_test_') and len(self.database)==48
        with connect('admin','postgres') as conn:
            conn.autocommit=True
            conn.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(self.database)))

    def scalar(self,query,params=None):
        with connect('reader',self.database) as conn:
            return conn.execute(query,params).fetchone()[0]

    def test_idempotence_and_immutable_conflicts(self):
        migrate(self.database)
        self.assertEqual(publish(self.payload,self.database,activate=True)['status'],'published')
        timestamp=self.scalar('SELECT published_at_utc FROM fleetpulse.forecast_runs')
        self.assertEqual(publish(self.payload,self.database,activate=True)['status'],'unchanged')
        self.assertEqual(timestamp,self.scalar('SELECT published_at_utc FROM fleetpulse.forecast_runs'))
        self.assertEqual(self.scalar('SELECT count(*) FROM fleetpulse.release_history'),1)
        changed=copy.deepcopy(self.payload)
        changed.rows[0]=(*changed.rows[0][:6],99.0,*changed.rows[0][7:])
        with self.assertRaisesRegex(ValueError,'predictions differ'): publish(changed,self.database)
        changed=copy.deepcopy(self.payload)
        changed.model['model_name']='changed'
        with self.assertRaisesRegex(ValueError,'Immutable'): publish(changed,self.database)
        self.assertEqual(self.scalar('SELECT count(*) FROM fleetpulse.predictions'),3)

    def test_failed_copy_is_invisible_and_rolls_back(self):
        def observer():
            self.assertEqual(self.scalar('SELECT count(*) FROM fleetpulse.forecast_runs'),0)
            self.assertEqual(self.scalar('SELECT count(*) FROM fleetpulse.dataset_snapshots'),0)
            self.assertEqual(self.scalar('SELECT count(*) FROM fleetpulse.predictions'),0)
        with self.assertRaisesRegex(RuntimeError,'Injected'):
            publish(self.payload,self.database,activate=True,fail_after=2,observer=observer)
        observer()
        self.assertEqual(self.scalar('SELECT count(*) FROM fleetpulse.active_releases'),0)

    def test_concurrent_publication_and_activation_rollback(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:publish(self.payload,self.database,activate=True),range(2)))
        self.assertCountEqual([r['status'] for r in results],['published','unchanged'])
        other=fixture('second')
        other.model['model_id']=identity_hash('second model')
        other.model['model_name']='second_model'
        other.run['model_id']=other.model['model_id']
        publish(other,self.database,activate=True)
        publish(self.payload,self.database,activate=True)
        self.assertEqual(self.scalar('SELECT run_id FROM fleetpulse.active_releases'),other.run['run_id'])
        self.assertEqual(self.scalar('SELECT r.model_id FROM fleetpulse.forecast_runs r JOIN fleetpulse.active_releases a USING(run_id)'),other.model['model_id'])
        change_active(database=self.database,rollback=True,reason='Integration rollback')
        self.assertEqual(self.scalar('SELECT run_id FROM fleetpulse.active_releases'),self.payload.run['run_id'])
        self.assertEqual(self.scalar('SELECT r.model_id FROM fleetpulse.forecast_runs r JOIN fleetpulse.active_releases a USING(run_id)'),self.payload.model['model_id'])
        self.assertEqual(self.scalar('SELECT count(*) FROM fleetpulse.release_history'),3)
        self.assertEqual(self.scalar('SELECT count(*) FROM fleetpulse.predictions'),6)

    def test_constraints_reject_bad_rows_and_incomplete_runs(self):
        for bad in (float('nan'),float('inf'),-1.0):
            payload=copy.deepcopy(self.payload)
            payload.rows[0]=(*payload.rows[0][:6],bad,*payload.rows[0][7:])
            with self.subTest(value=bad),self.assertRaises(psycopg.errors.CheckViolation): publish(payload,self.database)
        duplicate=copy.deepcopy(self.payload)
        duplicate.rows[1]=duplicate.rows[0]
        with self.assertRaises(psycopg.errors.UniqueViolation): publish(duplicate,self.database)
        unknown=copy.deepcopy(self.payload)
        unknown.rows[0]=(*unknown.rows[0][:2],999,*unknown.rows[0][3:])
        with self.assertRaises(psycopg.errors.ForeignKeyViolation): publish(unknown,self.database)
        incomplete=copy.deepcopy(self.payload)
        incomplete.rows.pop()
        with self.assertRaisesRegex(ValueError,'incomplete'): publish(incomplete,self.database)
        self.assertEqual(self.scalar('SELECT count(*) FROM fleetpulse.forecast_runs'),0)

    def test_database_privileges(self):
        publish(self.payload,self.database)
        with self.assertRaises(psycopg.Error),connect('reader',self.database) as conn:
            conn.execute('DELETE FROM fleetpulse.predictions')
        with self.assertRaises(psycopg.errors.InsufficientPrivilege),connect(database=self.database) as conn:
            conn.execute('UPDATE fleetpulse.predictions SET predicted_pickups=0')

    def test_http_contract(self):
        with api(self.database) as base:
            self.assertEqual(get(base,'/health')[:2],(200,{'status':'ready'}))
            status,empty,_=get(base,'/api/forecast-runs')
            self.assertEqual((status,empty['totalCount'],empty['items'],empty['activeRunId']),(200,0,[],None))
            self.payload.rows[2]=(*self.payload.rows[2][:-1],None)
            publish(self.payload,self.database,activate=True)
            run_id=self.payload.run['run_id']
            path='/api/forecast-runs/'+run_id
            status,run,_=get(base,path)
            self.assertEqual(status,200)
            self.assertEqual(run['freshnessStatus'],'historical_only')
            self.assertEqual(run['originsStartLocal'],'2024-06-01T00:00:00')
            self.assertTrue(run['publishedAtUtc'].endswith('Z'))
            self.assertTrue(run['isActive'])
            status,page,_=get(base,path+'/predictions?limit=1&offset=1')
            self.assertEqual(status,200,page)
            self.assertEqual((status,page['totalCount'],len(page['items'])),(200,3,1))
            self.assertEqual(page['items'][0]['forecastOriginLocal'],'2024-06-01T01:00:00')
            self.assertEqual(page['items'][0]['predictedPickups'],3.25)
            self.assertIsNone(get(base,path+'/predictions?offset=2')[1]['items'][0]['actualPickups'])
            _,filtered,_=get(base,path+'/predictions?fromLocal=2024-06-01T01:00:00&toLocal=2024-06-01T02:00:00&zoneId=1')
            self.assertEqual(filtered['totalCount'],1)
            for query in ('zoneId=999','fromLocal=2025-01-01T00:00:00&toLocal=2025-01-02T00:00:00'):
                self.assertEqual(get(base,path+'/predictions?'+query)[1]['totalCount'],0)
            for query in ('limit=0','limit=501','offset=-1','limit=1&limit=2','zoneId=0','fromLocal=2024-02-30T00:00:00',
                          'fromLocal=2024-06-01T00:00:00Z','fromLocal=2024-06-01T00:30:00',
                          'fromLocal=2024-06-02T00:00:00&toLocal=2024-06-01T00:00:00'):
                status,problem,content=get(base,path+'/predictions?'+query)
                with self.subTest(query=query):
                    self.assertEqual(status,400)
                    self.assertEqual(content,'application/problem+json')
                    self.assertIn('traceId',problem)
            self.assertEqual(get(base,'/api/forecast-runs/bad')[0],400)
            self.assertEqual(get(base,'/api/forecast-runs/'+'f'*64)[0],404)
            self.assertEqual(get(base,'/api/forecast-runs/'+'f'*64+'/predictions')[0],404)
            _,runs,_=get(base,'/api/forecast-runs?limit=1')
            self.assertEqual((runs['totalCount'],runs['activeRunId']),(1,run_id))
            second=fixture('newest')
            publish(second,self.database,activate=True)
            _,runs,_=get(base,'/api/forecast-runs?limit=1')
            self.assertEqual((runs['totalCount'],runs['items'][0]['runId'],runs['activeRunId']),(2,second.run['run_id'],second.run['run_id']))
            self.assertEqual(get(base,'/api/forecast-runs?limit=1&offset=1')[1]['items'][0]['runId'],run_id)

    def test_unavailable_database_is_sanitized(self):
        with api(self.database,free_port()) as base:
            for path in ('/health','/api/forecast-runs'):
                status,body,content=get(base,path)
                self.assertEqual((status,content),(503,'application/problem+json'))
                self.assertEqual(body['title'],'Database unavailable')
                self.assertIn('traceId',body)
                self.assertNotIn('Password',json.dumps(body))


if __name__=='__main__': unittest.main()
