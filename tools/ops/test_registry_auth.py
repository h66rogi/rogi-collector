import base64,importlib.util,json,tempfile,unittest
from pathlib import Path
spec=importlib.util.spec_from_file_location('registry',Path(__file__).with_name('load-registry-auth.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Client:
 def get_secret_value(self,**kwargs):return {'SecretString':json.dumps({'username':'github-actions','token':'x'*32})}
class Test(unittest.TestCase):
 def test_scoped_config(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);meta=root/'m';meta.write_text(json.dumps({'region':'ap-northeast-2','secretArn':'arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:registry-AbCd'}));out=m.load(meta,root/'auth',Client());self.assertEqual(out.stat().st_mode&0o777,0o700);self.assertEqual((out/'config.json').stat().st_mode&0o777,0o600);self.assertEqual(base64.b64decode(json.loads((out/'config.json').read_text())['auths']['ghcr.io']['auth']).decode(),'github-actions:'+('x'*32))
if __name__=='__main__':unittest.main()
