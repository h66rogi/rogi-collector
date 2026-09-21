from __future__ import annotations
import hashlib,importlib.util,io,json,tarfile,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('fetch_release',Path(__file__).with_name('fetch-release.py'));module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class FetchReleaseTest(unittest.TestCase):
 def fixture(self,tag_ok=True,run_ok=True):
  temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);root=Path(temporary.name);releases=root/'releases';run=root/'run';source=root/'source.json';overlay=root/'overlay.json';source.write_text(json.dumps({'repository':'owner/repo','workflowPath':'.github/workflows/release.yml'}));runtime={'composeProjectName':'rogi-collector'};overlay.write_text(json.dumps(runtime));sha='b'*40;build={'schemaVersion':1,'product':'rogi-collector','profile':'feedback','sourceSha':sha,'releaseId':'r1','contractVersion':'v1','composeSha256':'a'*64,'runtimeFiles':[],'images':{},'migrations':[]};stream=io.BytesIO()
  with tarfile.open(fileobj=stream,mode='w:gz') as archive:
   body=json.dumps(build).encode();entry=tarfile.TarInfo('manifest.build.json');entry.size=len(body);archive.addfile(entry,io.BytesIO(body));body=b'source';entry=tarfile.TarInfo('README.md');entry.size=len(body);archive.addfile(entry,io.BytesIO(body))
  bundle=stream.getvalue();checksum=hashlib.sha256(bundle).hexdigest().encode();metadata=json.dumps({'draft':False,'prerelease':False,'tag_name':f"production-{sha if tag_ok else '0'*40}",'assets':[{'name':'release-bundle.tar.gz','browser_download_url':'bundle'},{'name':'release-bundle.tar.gz.sha256','browser_download_url':'checksum'}]}).encode();runs=json.dumps({'workflow_runs':[{'path':'.github/workflows/release.yml','conclusion':'success' if run_ok else 'failure','head_sha':sha,'head_branch':'main','event':'push'}]}).encode();prior=module.download;module.download=lambda url,limit:{f'{module.API}/repos/owner/repo/releases/latest':metadata,f'{module.API}/repos/owner/repo/actions/runs?head_sha={sha}&per_page=100':runs,'bundle':bundle,'checksum':checksum}[url];self.addCleanup(setattr,module,'download',prior);return source,overlay,releases,run,runtime
 def test_public_release_checksum_workflow_and_overlay_bound(self):
  source,overlay,releases,run,runtime=self.fixture();app,manifest=module.stage(source,overlay,releases,run);self.assertEqual(app,releases/'r1');self.assertEqual(json.loads(manifest.read_text())['runtimeNonSecret'],runtime)
 def test_wrong_tag_and_failed_workflow_are_rejected(self):
  for tag_ok,run_ok in [(False,True),(True,False)]:
   with self.subTest(tag_ok=tag_ok,run_ok=run_ok):
    args=self.fixture(tag_ok,run_ok)
    with self.assertRaises(module.FetchError):module.stage(*args[:4])
 def test_descendant_and_health_guards(self):
  prior='a'*40;candidate='b'*40;old=module.download
  module.download=lambda url,limit:json.dumps({'status':'ahead'}).encode()
  try:module.require_descendant('owner/repo',prior,candidate)
  finally:module.download=old
  module.download=lambda url,limit:json.dumps({'status':'behind'}).encode()
  try:
   with self.assertRaises(module.FetchError):module.require_descendant('owner/repo',prior,candidate)
  finally:module.download=old
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);receipt=root/'receipt';app=root/'app';app.mkdir();manifest={'sourceSha':candidate,'images':{'x':'y'}};receipt.write_text(json.dumps(manifest));oldrun=module.subprocess.run
   module.subprocess.run=lambda *a,**k:type('R',(),{'returncode':0})()
   try:self.assertTrue(module.deployed_healthy(receipt,app,manifest));self.assertFalse(module.deployed_healthy(receipt,app,{'sourceSha':prior,'images':{'x':'y'}}))
   finally:module.subprocess.run=oldrun
 def test_archive_rejects_parent_traversal(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);archive=root/'bad.tar.gz'
   with tarfile.open(archive,'w:gz') as bundle:entry=tarfile.TarInfo('../escape');entry.size=1;bundle.addfile(entry,io.BytesIO(b'x'))
   with self.assertRaises(module.FetchError):module.safe_extract(archive,root/'out')
if __name__=='__main__':unittest.main()
