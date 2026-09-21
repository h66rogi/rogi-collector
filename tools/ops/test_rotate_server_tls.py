import importlib.util, pathlib, tempfile, unittest, subprocess
spec=importlib.util.spec_from_file_location('rotation',pathlib.Path(__file__).with_name('rotate-server-tls.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Rotation(unittest.TestCase):
 def test_initial_noop_and_rotation_keep_a_verifiable_identity(self):
  with tempfile.TemporaryDirectory() as td:
   root=pathlib.Path(td);source=root/'secrets';source.mkdir();dest=root/'tls'
   subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(source/'tls-ca.key'),'-out',str(source/'tls-ca.pem'),'-days','365','-subj','/CN=synthetic-test-issuer'],check=True,capture_output=True)
   self.assertTrue(m.renew(source,dest));old=(dest/'current/server.pem').read_bytes()
   self.assertFalse(m.renew(source,dest));self.assertTrue(m.renew(source,dest,force=True))
   self.assertNotEqual(old,(dest/'current/server.pem').read_bytes())
   subprocess.run(['openssl','verify','-verify_hostname','collector.internal','-CAfile',str(source/'tls-ca.pem'),str(dest/'current/server.pem')],check=True,capture_output=True)
   self.assertEqual((dest/'current/server.key').stat().st_mode&0o777,0o400)
if __name__=='__main__':unittest.main()
