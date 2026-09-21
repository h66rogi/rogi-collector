import importlib.util,json,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('status',Path(__file__).with_name('production-status.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Test(unittest.TestCase):
 def fixture(self):
  manifest={'sourceSha':'a'*40,'releaseId':'r1','images':{'query':'digest'}};units={'target':{'ok':True},**{name:{'ok':True} for name in m.REQUIRED}};rows=[{'Service':name,'State':'running','Health':'healthy'} for name in m.REQUIRED];return manifest,units,{'ok':True,'output':json.dumps(rows)}
 def test_all_required_healthy_and_matching_receipt_is_green(self):
  manifest,units,compose=self.fixture();self.assertTrue(m.evaluate(manifest,dict(manifest),units,compose)['ok'])
 def test_empty_receipt_and_manifest_are_never_a_match(self):
  _,units,compose=self.fixture();self.assertFalse(m.evaluate({}, {}, units,compose)['receiptMatchesCurrent'])
 def test_compose_output_is_not_truncated_before_json_parsing(self):
  manifest,units,compose=self.fixture();rows=json.loads(compose['output']);rows[0]['Padding']='x'*9000;compose['output']=json.dumps(rows);self.assertTrue(m.evaluate(manifest,dict(manifest),units,compose)['ok'])
 def test_empty_dead_missing_unhealthy_or_stale_receipt_is_never_green(self):
  manifest,units,compose=self.fixture();self.assertFalse(m.evaluate(manifest,dict(manifest),units,{'ok':True,'output':'[]'})['ok']);rows=json.loads(compose['output']);rows[0]['State']='exited';self.assertFalse(m.evaluate(manifest,dict(manifest),units,{'ok':True,'output':json.dumps(rows)})['ok']);rows[0]['State']='running';rows[0]['Health']='unhealthy';self.assertFalse(m.evaluate(manifest,dict(manifest),units,{'ok':True,'output':json.dumps(rows)})['ok']);stale={**manifest,'sourceSha':'b'*40};self.assertFalse(m.evaluate(manifest,stale,units,compose)['ok']);units['query']={'ok':False};self.assertFalse(m.evaluate(manifest,dict(manifest),units,compose)['ok'])
if __name__=='__main__':unittest.main()
