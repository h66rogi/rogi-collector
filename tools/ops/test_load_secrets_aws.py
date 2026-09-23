import importlib.util,json,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('loader',Path(__file__).with_name('load-secrets-aws.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class TestLoader(unittest.TestCase):
 def test_exact_json_is_written_to_individual_files(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);meta=root/'meta';target=root/'run';meta.write_text(json.dumps({'runtimeSecretArn':'arn:aws:secretsmanager:us-east-1:123456789012:secret:rogi-collector/prod-AbC','region':'us-east-1'}));values={k:f'value-{i}' for i,k in enumerate(sorted(m.KEYS))}
   class Client:
    def get_secret_value(self,**kwargs):self.kwargs=kwargs;return {'SecretString':json.dumps(values)}
   client=Client();m.load(meta,target,require_root=False,client=client);self.assertEqual(client.kwargs['SecretId'],json.loads(meta.read_text())['runtimeSecretArn']);self.assertEqual(client.kwargs['VersionStage'],'AWSCURRENT');self.assertTrue(m.KEYS.issubset({p.name for p in target.iterdir()}));self.assertTrue(all(p.is_symlink() and (p.resolve().stat().st_mode&0o777)==0o400 for p in (target/name for name in m.KEYS)))
 def test_extra_or_empty_secret_is_rejected(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);meta=root/'meta';meta.write_text(json.dumps({'runtimeSecretArn':'arn:aws:secretsmanager:us-east-1:123456789012:secret:x-AbC','region':'us-east-1'}));values={k:'x' for k in m.KEYS};values['extra']='bad'
   with self.assertRaises(RuntimeError):m.load(meta,root/'run',require_root=False,client=type('Client',(),{'get_secret_value':lambda self,**kwargs:{'SecretString':json.dumps(values)}})())
 def test_legacy_generation_is_admitted_during_secret_rollout(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);meta=root/'meta';meta.write_text(json.dumps({'runtimeSecretArn':'arn:aws:secretsmanager:us-east-1:123456789012:secret:x-AbC','region':'us-east-1'}));values={k:'x' for k in m.LEGACY_KEYS}
   client=type('Client',(),{'get_secret_value':lambda self,**kwargs:{'SecretString':json.dumps(values)}})()
   m.load(meta,root/'run',require_root=False,client=client)
   self.assertFalse((root/'run'/'data-api.env').exists())
   self.assertFalse((root/'run'/'archive-exporter.env').exists())
   self.assertFalse((root/'run'/'tunnel-token').exists())
if __name__=='__main__':unittest.main()
