#!/usr/bin/env node
import {createHash} from 'node:crypto'; import {chmod, cp, lstat, mkdir, readFile, readdir, rm, writeFile} from 'node:fs/promises'; import {spawnSync} from 'node:child_process'; import {dirname,join,resolve} from 'node:path'; import {fileURLToPath} from 'node:url';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'../..'), sha=process.env.SOURCE_SHA, release=process.env.RELEASE_ID??`main-${sha?.slice(0,12)}`;
if(!/^[0-9a-f]{40}$/.test(sha??''))throw new Error('SOURCE_SHA must be lowercase 40 hex');
// Local ignored/untracked runtime files must never enter public release assets.
const inventory=spawnSync('git',['ls-files','-z','--','deploy','tools/ops'],{cwd:root,encoding:'utf8'});
if(inventory.status!==0)throw new Error('A Git checkout is required to select public release files');
const tracked=new Set(inventory.stdout.split('\0').filter(Boolean));

const roles=['postgres','redis','discover','coordinator','worker','query','cloudflared','cookie-auth']; const images=Object.fromEntries(roles.map(k=>[k,process.env[`IMAGE_${k.toUpperCase().replaceAll("-","_")}`]]));
for(const [k,v] of Object.entries(images))if(!/^[^\s@]+@sha256:[0-9a-f]{64}$/.test(v??''))throw new Error(`IMAGE_${k.toUpperCase().replaceAll("-","_")} is not immutable`);
const digest=async p=>createHash('sha256').update(await readFile(p)).digest('hex');
const init=(await readdir(join(root,'deploy/initdb'),{withFileTypes:true})).filter(x=>x.isFile()&&tracked.has(`deploy/initdb/${x.name}`)).map(x=>`deploy/initdb/${x.name}`).sort(); const runtime=['deploy/cookie-auth.apparmor','deploy/systemd/rogi-collector-tls.service','deploy/systemd/rogi-collector-tls.timer','deploy/run-migrations.sh','deploy/install-runtime.sh',...['host-ready','migrate','role@','update','backup'].flatMap(name=>name==='role@'?[`deploy/systemd/rogi-collector-${name}.service`]:name==='host-ready'||name==='migrate'?[`deploy/systemd/rogi-collector-${name}.service`]:[`deploy/systemd/rogi-collector-${name}.service`,`deploy/systemd/rogi-collector-${name}.timer`]),'deploy/systemd/rogi-collector.target',...['deploy.sh','status.sh','prepare-host.sh','validate-manifest.mjs','render-runtime-env.mjs','fetch-release.py','production-status.py','backup-postgres.sh','load-secrets-aws.py','upload-backup-s3.py','validate-runtime-secrets.sh','load-registry-auth.py','rotate-server-tls.py'].map(name=>`tools/ops/${name}`),...init];
const migrations=(await readdir(join(root,'deploy/migrations'),{withFileTypes:true})).filter(x=>x.isFile()&&x.name.endsWith('.sql')&&tracked.has(`deploy/migrations/${x.name}`)).map(x=>`deploy/migrations/${x.name}`).sort();
const manifest={schemaVersion:1,product:'rogi-collector',profile:'soop-single-channel',sourceSha:sha,releaseId:release,contractVersion:'v1',composeSha256:await digest(join(root,'deploy/compose.production.yaml')),runtimeFiles:await Promise.all(runtime.map(async path=>({path,sha256:await digest(join(root,path))}))),images,migrations:await Promise.all(migrations.map(async path=>({path,sha256:await digest(join(root,path))})))};
const out=join(root,'dist/release'), stage=join(root,'dist/.release-stage'); await rm(join(root,'dist'),{recursive:true,force:true}); await mkdir(stage,{recursive:true}); await mkdir(out,{recursive:true}); await writeFile(join(stage,'manifest.build.json'),JSON.stringify(manifest,null,2)+'\n');
for(const path of ['deploy/compose.production.yaml',...runtime,...migrations])if(!tracked.has(path))throw new Error(`Release input is not tracked: ${path}`);
for(const path of tracked){
  if(/(^|\/)(\.env|__pycache__)(\/|$)/.test(path))continue;
  const source=join(root,path);
  if(!(await lstat(source)).isFile())throw new Error(`Release input is not a regular file: ${path}`);
  await mkdir(dirname(join(stage,path)),{recursive:true});
  await cp(source,join(stage,path));
}
async function normalize(dir){for(const item of await readdir(dir,{withFileTypes:true})){const p=join(dir,item.name);if(item.isDirectory()){await chmod(p,0o755);await normalize(p);}else if(item.isFile()){const head=(await readFile(p)).subarray(0,2).toString();await chmod(p,head==='#!'?0o755:0o644);}}}await normalize(stage);
let run=spawnSync('tar',['--mode=u+rwX,go+rX,go-w','-C',stage,'-czf',join(out,'release-bundle.tar.gz'),'.'],{stdio:'inherit'}); if(run.status)process.exit(run.status??1); const sum=await digest(join(out,'release-bundle.tar.gz')); await writeFile(join(out,'release-bundle.tar.gz.sha256'),`${sum}  release-bundle.tar.gz\n`);
