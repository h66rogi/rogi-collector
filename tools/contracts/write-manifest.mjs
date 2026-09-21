import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {readdirSync,readFileSync,writeFileSync} from 'node:fs';
import {join,relative,resolve} from 'node:path';
const root=resolve(process.env.CONTRACT_ROOT??new URL('../..',import.meta.url).pathname);
const hash=path=>createHash('sha256').update(readFileSync(path)).digest('hex');
const files=[];const walk=dir=>{for(const e of readdirSync(dir,{withFileTypes:true})){const p=join(dir,e.name);e.isDirectory()?walk(p):files.push(p)}};
for(const dir of ['proto/gen','contracts/typescript/src'])walk(join(root,dir));files.sort((a,b)=>a.localeCompare(b));
let sourceSha='uncommitted';
try{const head=execFileSync('git',['-C',root,'rev-parse','HEAD'],{encoding:'utf8',stdio:['ignore','pipe','ignore']}).trim();const dirty=execFileSync('git',['-C',root,'status','--porcelain=v1','--untracked-files=all','--','.',':(exclude)contracts/manifest.json'],{encoding:'utf8',stdio:['ignore','pipe','ignore']}).trim();if(!dirty)sourceSha=head;}catch{}
const manifest={contractVersion:'1.0.0',schemaSha256:hash(join(root,'proto/collector/v1/collector.proto')),sourceSha,tools:{protoc:'33.0','protoc-gen-go':'1.36.10','protoc-gen-es':'2.10.1',typescript:'5.9.3'},bundleSha256:Object.fromEntries(files.map(p=>[relative(root,p).replaceAll('\\','/'),hash(p)]))};
writeFileSync(join(root,'contracts/manifest.json'),JSON.stringify(manifest,null,2)+'\n');
