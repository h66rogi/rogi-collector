#!/usr/bin/env python3
from __future__ import annotations
import json,shutil,subprocess,time
from pathlib import Path
REQUIRED=('postgres','redis','discover','coordinator','worker','query')
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
 container_state={name:{'running':containers.get(name,{}).get('State')=='running','healthy':containers.get(name,{}).get('Health')=='healthy'} for name in REQUIRED}
 required_receipt=('sourceSha','releaseId','images')
 receipt_complete=isinstance(receipt,dict) and all(receipt.get(key) for key in required_receipt)
 manifest_complete=isinstance(current_manifest,dict) and all(current_manifest.get(key) for key in required_receipt)
 receipt_matches=receipt_complete and manifest_complete and all(receipt[key]==current_manifest[key] for key in required_receipt)
 units_ok=all(units.get(name,{}).get('ok') for name in ('target',)+REQUIRED)
 containers_ok=set(containers)==set(REQUIRED) and all(value['running'] and value['healthy'] for value in container_state.values())
 return {'ok':receipt_matches and units_ok and containers_ok,'receiptMatchesCurrent':receipt_matches,'unitStatuses':units,'containerStatuses':container_state,'requiredServices':list(REQUIRED)}
def main():
 current=Path('/opt/rogi-collector/app/current');receipt_path=Path('/run/rogi-collector/deployed-release.json');backup_root=Path('/srv/rogi-collector/backups');latest=max(backup_root.glob('postgres-*.dump.gz'),key=lambda p:p.stat().st_mtime,default=None)
 manifest_path=current/'manifest.json';manifest=json.loads(manifest_path.read_text()) if manifest_path.is_file() else None;receipt=json.loads(receipt_path.read_text()) if receipt_path.is_file() else None
 units={'target':command(['systemctl','is-active','rogi-collector.target']),**{name:command(['systemctl','is-active',f'rogi-collector-role@{name}.service']) for name in REQUIRED}}
 compose=command(['docker','compose','--env-file','/etc/rogi-collector/runtime.env','-f',str(current/'deploy/compose.production.yaml'),'ps','--format','json'])
 assessment=evaluate(manifest,receipt,units,compose)
 status={'checkedAt':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'sourceSha':manifest.get('sourceSha') if isinstance(manifest,dict) else None,'receipt':receipt,**assessment,'dataDisk':shutil.disk_usage('/srv/rogi-collector')._asdict(),'latestBackup':{'path':str(latest),'ageSeconds':int(time.time()-latest.stat().st_mtime)} if latest else None,'capabilities':{'grpc7443':'unavailable-health-only','soopCollector':'not-claimed-by-health-check'}}
 print(json.dumps(status,sort_keys=True,indent=2));return 0 if status['ok'] else 1
if __name__=='__main__':raise SystemExit(main())
