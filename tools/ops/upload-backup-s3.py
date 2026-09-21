#!/usr/bin/env python3
import argparse,json,re,stat
from pathlib import Path
BUCKET=re.compile(r'^(?=.{3,63}$)[a-z0-9](?:[a-z0-9.-]*[a-z0-9])$');PREFIX=re.compile(r'^[A-Za-z0-9!_.*()/=-]{1,256}$')
def upload(path:Path,metadata:Path,*,require_root=True,client=None):
 st=metadata.stat()
 if require_root and (st.st_uid!=0 or stat.S_IMODE(st.st_mode) not in (0o400,0o600)):raise RuntimeError('backup S3 metadata must be root-owned mode 0400/0600')
 cfg=json.loads(metadata.read_text())
 if set(cfg)!={'bucket','prefix','region'} or not BUCKET.fullmatch(cfg.get('bucket','')) or not PREFIX.fullmatch(cfg.get('prefix','')) or '..' in cfg['prefix'].split('/') or not re.fullmatch(r'[a-z]{2}-[a-z]+-[0-9]',cfg.get('region','')):raise RuntimeError('invalid backup S3 metadata')
 if not path.is_file() or path.stat().st_size<1:raise RuntimeError('backup file is missing or empty')
 if client is None:
  import boto3
  client=boto3.client('s3',region_name=cfg['region'])
 key=f"{cfg['prefix'].rstrip('/')}/{path.name}";client.upload_file(str(path),cfg['bucket'],key,ExtraArgs={'ServerSideEncryption':'AES256'});return key
def main():
 p=argparse.ArgumentParser();p.add_argument('backup',type=Path);p.add_argument('--metadata',type=Path,default=Path('/etc/rogi-collector/backup-s3.json'));a=p.parse_args();upload(a.backup,a.metadata)
if __name__=='__main__':main()
