#!/usr/bin/env python3
from __future__ import annotations
import argparse,hashlib,json,os,re,tarfile,tempfile,urllib.request
from pathlib import Path,PurePosixPath
API='https://api.github.com'; MAX_ARCHIVE=100*1024*1024
class FetchError(RuntimeError):pass
def read_json(path):return json.loads(Path(path).read_text(encoding='utf-8'))
def exact(value,keys,label):
 if not isinstance(value,dict) or set(value)!=set(keys):raise FetchError(f'invalid {label} fields')
 return value
def download(url,limit):
 req=urllib.request.Request(url,headers={'Accept':'application/vnd.github+json','User-Agent':'rogi-collector-release-host/1'})
 with urllib.request.urlopen(req,timeout=20) as response:
  length=int(response.headers.get('Content-Length','0'))
  if length>limit:raise FetchError('release asset exceeds size limit')
  data=response.read(limit+1)
 if len(data)>limit:raise FetchError('release asset exceeds size limit')
 return data
def safe_extract(archive,target):
 with tarfile.open(archive,'r:gz') as bundle:
  for member in bundle.getmembers():
   path=PurePosixPath(member.name)
   if path.is_absolute() or '..' in path.parts or member.issym() or member.islnk() or not(member.isdir() or member.isfile()):raise FetchError('unsafe release archive entry')
  bundle.extractall(target,filter='data')
def stage(source_path,overlay_path,releases_root,run_root):
 source=exact(read_json(source_path),{'repository','workflowPath'},'release source');repo=source['repository']
 if not isinstance(repo,str) or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repo):raise FetchError('invalid public GitHub repository')
 metadata=json.loads(download(f'{API}/repos/{repo}/releases/latest',1024*1024))
 if metadata.get('draft') or metadata.get('prerelease'):raise FetchError('latest release is not production')
 assets={x.get('name'):x.get('browser_download_url') for x in metadata.get('assets',[]) if isinstance(x,dict)}
 if {'release-bundle.tar.gz','release-bundle.tar.gz.sha256'}-assets.keys():raise FetchError('release assets are incomplete')
 archive_bytes=download(assets['release-bundle.tar.gz'],MAX_ARCHIVE);checksum=download(assets['release-bundle.tar.gz.sha256'],256).decode().strip().split()[0]
 if not re.fullmatch(r'[0-9a-f]{64}',checksum) or hashlib.sha256(archive_bytes).hexdigest()!=checksum:raise FetchError('release archive checksum mismatch')
 releases_root.mkdir(parents=True,exist_ok=True)
 with tempfile.TemporaryDirectory(dir=releases_root) as temporary:
  temp=Path(temporary); archive=temp/'bundle.tar.gz';archive.write_bytes(archive_bytes);tree=temp/'tree';tree.mkdir();safe_extract(archive,tree)
  build=exact(read_json(tree/'manifest.build.json'),{'schemaVersion','product','profile','sourceSha','releaseId','contractVersion','composeSha256','runtimeFiles','images','migrations'},'build manifest')
  sha=build.get('sourceSha')
  if metadata.get('tag_name')!=f'production-{sha}':raise FetchError('release tag does not bind source SHA')
  if source['workflowPath']!='.github/workflows/release.yml':raise FetchError('unapproved workflow path')
  runs=json.loads(download(f'{API}/repos/{repo}/actions/runs?head_sha={sha}&per_page=100',4*1024*1024))
  if not any(x.get('path')==source['workflowPath'] and x.get('conclusion')=='success' and x.get('head_sha')==sha and x.get('head_branch')=='main' and x.get('event')=='push' for x in runs.get('workflow_runs',[]) if isinstance(x,dict)):raise FetchError('successful main release workflow is absent')
  release_id=build.get('releaseId')
  if not isinstance(release_id,str) or not re.fullmatch(r'[A-Za-z0-9._-]{1,96}',release_id):raise FetchError('invalid release id')
  manifest={**build,'runtimeNonSecret':read_json(overlay_path)}; destination=releases_root/release_id
  if not destination.exists():
   (tree/'manifest.build.json').unlink(); (tree/'manifest.json').write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n'); os.rename(tree,destination)
  else:
   existing=read_json(destination/'manifest.json')
   if existing!=manifest:raise FetchError('existing release id has different content')
 candidate=run_root/'candidate';candidate.mkdir(parents=True,exist_ok=True); pointer=candidate/'release.json';pointer.write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n');pointer.chmod(0o600)
 return destination,destination/'manifest.json'
def main():
 p=argparse.ArgumentParser();p.add_argument('--source',type=Path,default=Path('/etc/rogi-collector/release-source.json'));p.add_argument('--overlay',type=Path,default=Path('/etc/rogi-collector/runtime-overlay.json'));p.add_argument('--releases-root',type=Path,default=Path('/opt/rogi-collector/app/releases'));p.add_argument('--run-root',type=Path,default=Path('/run/rogi-collector'));a=p.parse_args()
 try:
  app,manifest=stage(a.source,a.overlay,a.releases_root,a.run_root)
  current=Path('/opt/rogi-collector/app/current')
  if current.exists() and current.resolve()==app.resolve():return 0
  os.execv('/usr/local/lib/rogi-collector/deploy.sh',['deploy.sh','--manifest',str(manifest)])
 except (FetchError,OSError,ValueError,json.JSONDecodeError) as e:print(f'release fetch failed: {e}',file=__import__('sys').stderr);return 1
if __name__=='__main__':raise SystemExit(main())
