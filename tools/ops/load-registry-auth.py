#!/usr/bin/env python3
from __future__ import annotations
import argparse,base64,json,os,re,shutil
from pathlib import Path
def exact(value,keys,label):
 if not isinstance(value,dict) or set(value)!=set(keys):raise ValueError(f'invalid {label} fields')
 return value
def load(metadata_path:Path,output:Path,client=None):
 metadata=exact(json.loads(metadata_path.read_text()),{'region','secretArn'},'registry metadata')
 if not re.fullmatch(r'[a-z]{2}-[a-z]+-[0-9]',metadata['region']) or not re.fullmatch(r'arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+',metadata['secretArn']):raise ValueError('invalid registry metadata')
 if client is None:
  import boto3;client=boto3.client('secretsmanager',region_name=metadata['region'])
 value=exact(json.loads(client.get_secret_value(SecretId=metadata['secretArn'],VersionStage='AWSCURRENT').get('SecretString','')),{'username','token'},'registry secret')
 if not isinstance(value['username'],str) or not re.fullmatch(r'[A-Za-z0-9-]{1,64}',value['username']) or not isinstance(value['token'],str) or not 20<=len(value['token'])<=4096 or any(c in value['token'] for c in '\r\n\0'):raise ValueError('invalid registry credential')
 temporary=output.with_name(f'.{output.name}.{os.getpid()}');shutil.rmtree(temporary,ignore_errors=True);temporary.mkdir(mode=0o700,parents=True);config=temporary/'config.json';config.write_text(json.dumps({'auths':{'ghcr.io':{'auth':base64.b64encode(f"{value['username']}:{value['token']}".encode()).decode()}}},separators=(',',':'))+'\n');config.chmod(0o600);shutil.rmtree(output,ignore_errors=True);temporary.replace(output);return output
def main():
 p=argparse.ArgumentParser();p.add_argument('--metadata',type=Path,default=Path('/etc/rogi-collector/registry.json'));p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 try:load(a.metadata,a.output);return 0
 except Exception as e:print(f'registry authentication failed: {e}',file=__import__('sys').stderr);return 1
if __name__=='__main__':raise SystemExit(main())
