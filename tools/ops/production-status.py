#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,os,shutil,subprocess,time,urllib.request
from datetime import datetime,timezone
from pathlib import Path
REQUIRED=('postgres','redis','discover','coordinator','worker','query','data-api','archive-exporter','cloudflared','cookie-auth')
METRIC_NAMESPACE='Rogi/rogi-collector'
METRIC_DIMENSIONS=[{'Name':'Environment','Value':'prod'}]
CURRENT_BROADCAST_URL='https://data-api.rogi.chat/v1/broadcasts/current'
def command(argv):
 r=subprocess.run(argv,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE);return {'ok':r.returncode==0,'output':(r.stdout or r.stderr).strip()}
def parse_containers(raw):
 try:
  value=json.loads(raw);rows=value if isinstance(value,list) else [value]
 except json.JSONDecodeError:
  try:rows=[json.loads(line) for line in raw.splitlines() if line.strip()]
  except json.JSONDecodeError:return {}
 return {row.get('Service'):row for row in rows if isinstance(row,dict) and isinstance(row.get('Service'),str)}
def evaluate(current_manifest,receipt,units,compose):
 containers=parse_containers(compose.get('output','')) if compose.get('ok') else {}
 container_state={name:{'running':containers.get(name,{}).get('State')=='running','healthy':containers.get(name,{}).get('Health')=='healthy' or (name=='cloudflared' and not containers.get(name,{}).get('Health'))} for name in REQUIRED}
 required_receipt=('sourceSha','releaseId','images')
 receipt_complete=isinstance(receipt,dict) and all(receipt.get(key) for key in required_receipt)
 manifest_complete=isinstance(current_manifest,dict) and all(current_manifest.get(key) for key in required_receipt)
 receipt_matches=receipt_complete and manifest_complete and all(receipt[key]==current_manifest[key] for key in required_receipt)
 units_ok=all(units.get(name,{}).get('ok') for name in ('target',)+REQUIRED)
 containers_ok=set(containers)==set(REQUIRED) and all(value['running'] and value['healthy'] for value in container_state.values())
 return {'ok':receipt_matches and units_ok and containers_ok,'receiptMatchesCurrent':receipt_matches,'unitStatuses':units,'containerStatuses':container_state,'requiredServices':list(REQUIRED)}
def data_disk_healthy(usage):
 return usage.free>=max(4*1024**3,usage.total//10)
def collect_status():
 current=Path('/opt/rogi-collector/app/current');receipt_path=Path('/etc/rogi-collector/deployed-release.json');backup_root=Path('/srv/rogi-collector/backups');latest=max(backup_root.glob('postgres-*.dump.gz'),key=lambda p:p.stat().st_mtime,default=None)
 manifest_path=current/'manifest.json';manifest=json.loads(manifest_path.read_text()) if manifest_path.is_file() else None;receipt=json.loads(receipt_path.read_text()) if receipt_path.is_file() else None
 units={'target':command(['systemctl','is-active','rogi-collector.target']),**{name:command(['systemctl','is-active',f'rogi-collector-role@{name}.service']) for name in REQUIRED}}
 compose=command(['docker','compose','--env-file','/etc/rogi-collector/runtime.env','-f',str(current/'deploy/compose.production.yaml'),'ps','--format','json'])
 assessment=evaluate(manifest,receipt,units,compose)
 disk=shutil.disk_usage('/srv/rogi-collector');root_disk=shutil.disk_usage('/');assessment['dataDiskHealthy']=data_disk_healthy(disk);assessment['rootDiskHealthy']=data_disk_healthy(root_disk);assessment['ok']=assessment['ok'] and assessment['dataDiskHealthy'] and assessment['rootDiskHealthy']
 status={'checkedAt':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'sourceSha':manifest.get('sourceSha') if isinstance(manifest,dict) else None,'receipt':receipt,**assessment,'dataDisk':disk._asdict(),'rootDisk':root_disk._asdict(),'latestBackup':{'path':str(latest),'ageSeconds':int(time.time()-latest.stat().st_mtime)} if latest else None,'capabilities':{'grpc7443':'mtls-collector-v1','soopCollector':'single-configured-channel; inspect channel status separately'}}
 return status
def public_collection_health(payload,now=None):
 if not isinstance(payload,dict) or payload.get('channelId')!='h66rogi':return False,False
 try:checked=datetime.fromisoformat(payload['checkedAt'])
 except (KeyError,TypeError,ValueError):return False,False
 if checked.tzinfo is None:return False,False
 age=((now or datetime.now(timezone.utc))-checked).total_seconds()
 if age< -30 or age>120:return False,False
 collection=payload.get('collection')
 if not isinstance(collection,dict):return False,False
 if payload.get('state')=='offline' and payload.get('live') is False:return True,True
 if payload.get('state')=='live' and payload.get('live') is True:
  return True,collection.get('active') is True and collection.get('state')=='connected'
 return False,False
def probe_public_collection():
 request=urllib.request.Request(CURRENT_BROADCAST_URL,headers={'User-Agent':'rogi-collector-health/1'})
 with urllib.request.urlopen(request,timeout=8) as response:
  if response.status!=200:raise ValueError('public collection status is unavailable')
  return public_collection_health(json.load(response))
def backup_stored(status,client,metadata=Path('/etc/rogi-collector/backup-s3.json')):
 backup=status['latestBackup']
 if backup is None:return False
 config=json.loads(metadata.read_text(encoding='utf-8'))
 key=f"{config['prefix'].rstrip('/')}/{Path(backup['path']).name}"
 result=client.head_object(Bucket=config['bucket'],Key=key)
 return result.get('ContentLength',0)>0
def metric_data(status,prune_healthy,public_healthy,collection_healthy,backup_healthy):
 values={
  'MetricHeartbeat':(1,'Count'),
  'HostHealthy':(int(status['ok']),'Count'),
  'RootFreeBytes':(status['rootDisk']['free'],'Bytes'),
  'DataFreeBytes':(status['dataDisk']['free'],'Bytes'),
  'BackupAgeSeconds':(status['latestBackup']['ageSeconds'] if backup_healthy else 7*86400,'Seconds'),
  'BackupStored':(int(backup_healthy),'Count'),
  'ImagePruneHealthy':(int(prune_healthy),'Count'),
  'PublicStatusHealthy':(int(public_healthy),'Count'),
  'CollectionHealthy':(int(collection_healthy),'Count'),
 }
 return [{'MetricName':name,'Dimensions':METRIC_DIMENSIONS,'Value':value,'Unit':unit} for name,(value,unit) in values.items()]
def maintain_and_publish():
 try:
  prune=subprocess.run(['/usr/local/lib/rogi-collector/fetch-release.py','--prune-only'],text=True,capture_output=True,timeout=90)
  prune_healthy=prune.returncode==0
 except (OSError,subprocess.TimeoutExpired):prune_healthy=False
 status=collect_status()
 try:public_healthy,collection_healthy=probe_public_collection()
 except (OSError,ValueError,KeyError,TypeError):public_healthy,collection_healthy=False,False
 import boto3
 try:backup_healthy=backup_stored(status,boto3.client('s3',region_name='ap-northeast-2'))
 except Exception:backup_healthy=False
 boto3.client('cloudwatch',region_name='ap-northeast-2').put_metric_data(Namespace=METRIC_NAMESPACE,MetricData=metric_data(status,prune_healthy,public_healthy,collection_healthy,backup_healthy))
 print(json.dumps({'metricsPublished':True,'hostHealthy':status['ok'],'imagePruneHealthy':prune_healthy,'publicStatusHealthy':public_healthy,'collectionHealthy':collection_healthy,'backupStored':backup_healthy},sort_keys=True))
 return 0
def publish_deployment_result():
 import boto3
 healthy=os.environ.get('SERVICE_RESULT')=='success'
 boto3.client('cloudwatch',region_name='ap-northeast-2').put_metric_data(Namespace=METRIC_NAMESPACE,MetricData=[{'MetricName':'DeploymentHealthy','Dimensions':METRIC_DIMENSIONS,'Value':int(healthy),'Unit':'Count'}])
 print(json.dumps({'deploymentHealthy':healthy},sort_keys=True))
 return 0
def main():
 parser=argparse.ArgumentParser();modes=parser.add_mutually_exclusive_group();modes.add_argument('--maintenance',action='store_true');modes.add_argument('--deployment-result',action='store_true');args=parser.parse_args()
 if args.maintenance:return maintain_and_publish()
 if args.deployment_result:return publish_deployment_result()
 status=collect_status();print(json.dumps(status,sort_keys=True,indent=2));return 0 if status['ok'] else 1
if __name__=='__main__':raise SystemExit(main())
