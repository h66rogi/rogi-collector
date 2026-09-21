#!/usr/bin/env python3
"""Renew the private gRPC server certificate using the root-only mounted issuer.

The issuer never enters an application container. Leaf files live on encrypted EBS;
renewal publishes a complete directory generation and restarts only query.
"""
import argparse, os, pathlib, shutil, subprocess, tempfile, fcntl

def run(*args):
    return subprocess.run(['openssl', *args], check=True, capture_output=True).stdout

def renew(source, destination, *, force=False, owner=None):
    source, destination = pathlib.Path(source), pathlib.Path(destination)
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (destination/'.rotate.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        current=destination/'current'
        if not force and (current/'server.pem').exists():
            valid=subprocess.run(['openssl','x509','-checkend',str(30*86400),'-noout','-in',str(current/'server.pem')],capture_output=True).returncode==0
            trusted=subprocess.run(['openssl','verify','-CAfile',str(source/'tls-ca.pem'),str(current/'server.pem')],capture_output=True).returncode==0
            if valid and trusted:return False
        generation=pathlib.Path(tempfile.mkdtemp(prefix='generation-',dir=destination))
        try:
            key=generation/'server.key';csr=generation/'server.csr';cert=generation/'server.pem'
            run('req','-new','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(csr),'-subj','/CN=collector.internal')
            ext=generation/'extensions';ext.write_text('subjectAltName=DNS:collector.internal,DNS:query\nextendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n')
            run('x509','-req','-in',str(csr),'-CA',str(source/'tls-ca.pem'),'-CAkey',str(source/'tls-ca.key'),'-set_serial','0x'+os.urandom(16).hex(),'-days','90','-sha256','-extfile',str(ext),'-out',str(cert))
            run('verify','-CAfile',str(source/'tls-ca.pem'),str(cert))
            shutil.copyfile(source/'tls-ca.pem',generation/'ca.pem');csr.unlink();ext.unlink()
            for p in generation.iterdir():
                p.chmod(0o400)
                if owner is not None:os.chown(p,owner,owner)
            if owner is not None:os.chown(generation,owner,owner)
            link=destination/'.current-next';link.unlink(missing_ok=True);link.symlink_to(generation.name);os.replace(link,current)
            for old in destination.glob('generation-*'):
                if old!=generation:shutil.rmtree(old)
            return True
        except Exception:
            shutil.rmtree(generation,ignore_errors=True);raise

def main():
    p=argparse.ArgumentParser();p.add_argument('--restart',action='store_true');p.add_argument('--force',action='store_true');a=p.parse_args()
    # Serialize concurrent certificate renewal requests.
    with open('/run/rogi-collector/tls.lock','w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        changed=renew('/run/rogi-collector','/srv/rogi-collector/tls',force=a.force,owner=65532)
        if changed and a.restart:subprocess.run(['systemctl','restart','rogi-collector-role@query.service'],check=True)
        print('server certificate renewed' if changed else 'server certificate remains valid')
if __name__=='__main__':
    try:main()
    except Exception:raise SystemExit('server certificate preparation failed')
