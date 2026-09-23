#!/usr/bin/env python3
import argparse,json,os,re,shutil,stat
from pathlib import Path
LEGACY_KEYS={'postgres-admin-password','postgres-migrate-password','migrate.pgpass','redis-password','discover.env','coordinator.env','worker.env','query.env','cookie-auth.env','app-migrate.env','tls-ca.pem','tls-ca.key'}
API_KEYS=LEGACY_KEYS|{'data-api.env'}
ARCHIVE_KEYS=API_KEYS|{'archive-exporter.env'}
KEYS=ARCHIVE_KEYS|{'tunnel-token'}
ARN=re.compile(r'^arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]{1,512}$')
def load(metadata:Path,target:Path,*,require_root=True,client=None):
 st=metadata.stat()
 if require_root and (st.st_uid!=0 or stat.S_IMODE(st.st_mode) not in (0o400,0o600)):raise RuntimeError('secrets-manager metadata must be root-owned mode 0400/0600')
 cfg=json.loads(metadata.read_text())
 if set(cfg)!={'runtimeSecretArn','region'} or not ARN.fullmatch(cfg.get('runtimeSecretArn','')) or not re.fullmatch(r'[a-z]{2}-[a-z]+-[0-9]',cfg.get('region','')):raise RuntimeError('invalid secrets-manager metadata')
 if client is None:
  import boto3
  client=boto3.client('secretsmanager',region_name=cfg['region'])
 response=client.get_secret_value(SecretId=cfg['runtimeSecretArn'],VersionStage='AWSCURRENT')
 raw=response.get('SecretString')
 if not isinstance(raw,str) or len(raw.encode('utf-8'))>65536 or '\0' in raw:raise RuntimeError('secret value must be a bounded JSON SecretString')
 values=json.loads(raw)
 if set(values) not in (LEGACY_KEYS,API_KEYS,ARCHIVE_KEYS,KEYS) or any(not isinstance(v,str) or not v or len(v.encode('utf-8'))>8192 or '\0' in v for v in values.values()):raise RuntimeError('secret JSON must contain an admitted bounded non-empty key set')
 target.mkdir(parents=True,exist_ok=True);os.chmod(target,0o700);generation=target/f'source-secrets.{os.getpid()}'
 if generation.exists():shutil.rmtree(generation)
 generation.mkdir(mode=0o700)
 for name,value in values.items():
  path=generation/name;path.write_text(value+'\n');path.chmod(0o400)
 temporary=target/f'.source-secrets.{os.getpid()}';temporary.symlink_to(generation.name);os.replace(temporary,target/'source-secrets')
 for name in KEYS-set(values):
  link=target/name
  if link.exists() or link.is_symlink():link.unlink()
 for name in values:
  link=target/name
  if link.exists() or link.is_symlink():link.unlink()
  link.symlink_to(f'source-secrets/{name}')
 for old in target.glob('source-secrets.*'):
  if old!=generation and old.is_dir():shutil.rmtree(old)
def cleanup(target:Path):
 for name in KEYS|{'source-secrets'}:
  path=target/name
  if path.is_symlink() or path.is_file():path.unlink(missing_ok=True)
 for old in target.glob('source-secrets.*'):
  if old.is_dir():shutil.rmtree(old)
def main():
 p=argparse.ArgumentParser();p.add_argument('target',type=Path);p.add_argument('--cleanup',action='store_true');p.add_argument('--metadata',type=Path,default=Path('/etc/rogi-collector/secrets-manager.json'));a=p.parse_args();cleanup(a.target) if a.cleanup else load(a.metadata,a.target)
if __name__=='__main__':main()
