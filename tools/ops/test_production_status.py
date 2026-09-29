import importlib.util,json,os,sys,tempfile,types,unittest
from unittest.mock import patch
from collections import namedtuple
from datetime import datetime,timezone,timedelta
from pathlib import Path
spec=importlib.util.spec_from_file_location('status',Path(__file__).with_name('production-status.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Test(unittest.TestCase):
 def fixture(self):
  manifest={'sourceSha':'a'*40,'releaseId':'r1','images':{'query':'digest'}};units={'target':{'ok':True},**{name:{'ok':True} for name in m.REQUIRED}};rows=[{'Service':name,'State':'running','Health':'healthy'} for name in m.REQUIRED];return manifest,units,{'ok':True,'output':json.dumps(rows)}
 def test_all_required_healthy_and_matching_receipt_is_green(self):
  manifest,units,compose=self.fixture();self.assertTrue(m.evaluate(manifest,dict(manifest),units,compose)['ok'])
 def test_cloudflared_running_without_container_healthcheck_is_green(self):
  manifest,units,compose=self.fixture();rows=json.loads(compose['output']);next(row for row in rows if row['Service']=='cloudflared')['Health']='';compose['output']=json.dumps(rows);self.assertTrue(m.evaluate(manifest,dict(manifest),units,compose)['ok'])
 def test_empty_receipt_and_manifest_are_never_a_match(self):
  _,units,compose=self.fixture();self.assertFalse(m.evaluate({}, {}, units,compose)['receiptMatchesCurrent'])
 def test_compose_output_is_not_truncated_before_json_parsing(self):
  manifest,units,compose=self.fixture();rows=json.loads(compose['output']);rows[0]['Padding']='x'*9000;compose['output']=json.dumps(rows);self.assertTrue(m.evaluate(manifest,dict(manifest),units,compose)['ok'])
 def test_empty_dead_missing_unhealthy_or_stale_receipt_is_never_green(self):
  manifest,units,compose=self.fixture();self.assertFalse(m.evaluate(manifest,dict(manifest),units,{'ok':True,'output':'[]'})['ok']);rows=json.loads(compose['output']);rows[0]['State']='exited';self.assertFalse(m.evaluate(manifest,dict(manifest),units,{'ok':True,'output':json.dumps(rows)})['ok']);rows[0]['State']='running';rows[0]['Health']='unhealthy';self.assertFalse(m.evaluate(manifest,dict(manifest),units,{'ok':True,'output':json.dumps(rows)})['ok']);stale={**manifest,'sourceSha':'b'*40};self.assertFalse(m.evaluate(manifest,stale,units,compose)['ok']);units['query']={'ok':False};self.assertFalse(m.evaluate(manifest,dict(manifest),units,compose)['ok'])
 def test_data_disk_reserve(self):
  Usage=namedtuple('Usage','total used free')
  self.assertTrue(m.data_disk_healthy(Usage(40*1024**3,35*1024**3,5*1024**3)))
  self.assertFalse(m.data_disk_healthy(Usage(40*1024**3,37*1024**3,3*1024**3)))
  self.assertFalse(m.data_disk_healthy(Usage(20*1024**3,19*1024**3,1*1024**3)))
 def test_public_collection_only_alerts_for_live_disconnection(self):
  now=datetime.now(timezone.utc);payload={'channelId':'h66rogi','checkedAt':now.isoformat(),'collection':{'active':False,'state':'waiting'},'state':'offline','live':False}
  self.assertEqual(m.public_collection_health(payload,now),(True,True))
  payload.update(state='live',live=True)
  self.assertEqual(m.public_collection_health(payload,now),(True,False))
  payload['collection'].update(active=True,state='connected')
  self.assertEqual(m.public_collection_health(payload,now),(True,True))
  payload['checkedAt']=(now-timedelta(minutes=3)).isoformat()
  self.assertEqual(m.public_collection_health(payload,now),(False,False))
 def test_metric_data_exposes_disk_backup_and_cleanup_failure(self):
  status={'ok':False,'rootDisk':{'free':5*1024**3},'dataDisk':{'free':30*1024**3},'latestBackup':{'ageSeconds':100}}
  metrics={x['MetricName']:x['Value'] for x in m.metric_data(status,False,True,False,False)}
  self.assertEqual(metrics['MetricHeartbeat'],1)
  self.assertEqual(metrics['HostHealthy'],0)
  self.assertEqual(metrics['RootFreeBytes'],5*1024**3)
  self.assertEqual(metrics['ImagePruneHealthy'],0)
  self.assertEqual(metrics['CollectionHealthy'],0)
  self.assertEqual(metrics['BackupStored'],0)
  self.assertEqual(metrics['BackupAgeSeconds'],7*86400)
 def test_backup_metric_requires_confirmed_s3_object(self):
  class Client:
   def __init__(self,length):self.length=length;self.key=None
   def head_object(self,*,Bucket,Key):self.key=(Bucket,Key);return {'ContentLength':self.length}
  with tempfile.TemporaryDirectory() as temporary:
   metadata=Path(temporary)/'backup-s3.json';metadata.write_text(json.dumps({'bucket':'collector-backup','prefix':'prod/backups','region':'ap-northeast-2'}))
   status={'latestBackup':{'path':'/srv/rogi-collector/backups/postgres-20260929T154855Z.dump.gz','ageSeconds':60}}
   client=Client(100)
   self.assertTrue(m.backup_stored(status,client,metadata))
   self.assertEqual(client.key,('collector-backup','prod/backups/postgres-20260929T154855Z.dump.gz'))
   self.assertFalse(m.backup_stored(status,Client(0),metadata))
 def test_deployment_result_tracks_systemd_outcome(self):
  published=[]
  client=types.SimpleNamespace(put_metric_data=lambda **kwargs:published.append(kwargs))
  with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *args,**kwargs:client)}):
   for result,expected in [('success',1),('exit-code',0)]:
    with patch.dict(os.environ,{'SERVICE_RESULT':result}):self.assertEqual(m.publish_deployment_result(),0)
  self.assertEqual([x['MetricData'][0]['Value'] for x in published],[1,0])
if __name__=='__main__':unittest.main()
