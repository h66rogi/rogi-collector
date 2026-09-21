#!/usr/bin/env python3
import json,shutil,subprocess,time
from pathlib import Path
def command(argv):
 r=subprocess.run(argv,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE);return {'ok':r.returncode==0,'output':(r.stdout or r.stderr).strip()[:8192]}
current=Path('/opt/rogi-collector/app/current');receipt=Path('/run/rogi-collector/deployed-release.json');backup_root=Path('/srv/rogi-collector/backups');latest=max(backup_root.glob('postgres-*.dump.gz'),key=lambda p:p.stat().st_mtime,default=None)
status={'checkedAt':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'receipt':json.loads(receipt.read_text()) if receipt.is_file() else None,'target':command(['systemctl','is-active','rogi-collector.target']),'containers':command(['docker','compose','--env-file','/etc/rogi-collector/runtime.env','-f',str(current/'deploy/compose.production.yaml'),'ps','--format','json']),'dataDisk':shutil.disk_usage('/srv/rogi-collector')._asdict(),'latestBackup':{'path':str(latest),'ageSeconds':int(time.time()-latest.stat().st_mtime)} if latest else None,'capabilities':{'grpc7443':'unavailable-health-only'}}
print(json.dumps(status,sort_keys=True,indent=2));raise SystemExit(0 if status['target']['ok'] and status['containers']['ok'] else 1)
