"""Actual two-stage acceptance using an owner-permitted Kinescope video.
The configured real profile is required; private links and credentials are never
written to the report. The numeric example must still permit downloads today.
"""
import argparse
import hashlib
import http.cookiejar
import json
import secrets
import ssl
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

p=argparse.ArgumentParser()
p.add_argument('--config',default='.local-v2-kinescope/client.json')
p.add_argument('--id',default='203613411')
p.add_argument('--output',default='artifacts/v2/kinescope')
p.add_argument('--max-file-mib',type=int,default=128)
args=p.parse_args()
if not args.id.isdecimal() or int(args.id)>2**64-1:raise SystemExit('Unsigned numeric identifier required')
cfg=json.loads(Path(args.config).read_text())
client=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=cfg['ca'])),urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
csrf=''
report={'profile':'real-kinescope-v2','started_at':datetime.now(timezone.utc).isoformat(),'external_id':args.id,'checks':[]}
out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
def api(path,method='GET',body=None):
    headers={'API-Version':'2','Content-Type':'application/json','X-CSRF-Token':csrf,'Idempotency-Key':secrets.token_hex(16)}
    req=urllib.request.Request(cfg['url']+'/api'+path,method=method,data=None if body is None else json.dumps(body).encode(),headers=headers)
    with client.open(req,timeout=90) as r:return json.load(r)
def check(name,ok,observed=None):
    report['checks'].append({'name':name,'status':'passed' if ok else 'failed','observed':observed})
    if not ok:raise AssertionError(name)
def completed(job,seconds=600):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        v=api('/jobs/'+job)
        if v['state']=='completed':return v
        if v['state'] in ['failed','stopped']:raise AssertionError('job_'+v['state'])
        time.sleep(.5)
    raise TimeoutError('job did not finish')
try:
    csrf=api('/auth/login','POST',{'username':cfg['username'],'password':Path(cfg['password_file']).read_text().strip()})['csrf_token']
    snapshot=api('/snapshot')
    check('real_donor',snapshot['source_profile']['source_id']=='kinescope' and not snapshot['source_profile']['mock'])
    nodes=[w['worker_id'] for w in snapshot['workers'] if w['online']]
    check('three_agents',len(nodes)==3)
    config={'worker_ids':nodes,'threads_default':1,'block_size':1,'max_file_bytes':args.max_file_mib<<20,'http_timeout_ms':60000,'retry_policy':{'max_retries':4},'limits':{'rps':1,'connections':1}}
    scan=api('/jobs','POST',{'kind':'discovery','config':dict(config,range_text='['+args.id+','+args.id+']')})['job_id']
    report['discovery_job_id']=scan;api('/jobs/'+scan+'/start','POST',{});completed(scan,180)
    results=api('/jobs/'+scan+'/items')['items']
    check('discovery_available_without_file',len(results)==1 and results[0]['status']=='available' and not results[0].get('file_id'),[{'status':i['status'],'error_code':i.get('error_code')} for i in results])
    catalog=api('/catalog?discovery_job_id='+scan)['items']
    check('catalog_has_one_permitted_material',len(catalog)==1)
    job=api('/jobs','POST',{'kind':'download','config':config,'media_ids':[catalog[0]['media_id']]})['job_id']
    report['download_job_id']=job;api('/jobs/'+job+'/start','POST',{});completed(job)
    results=api('/jobs/'+job+'/items')['items'];check('download_completed',len(results)==1 and results[0]['status']=='completed',{'status':results[0]['status'],'error_code':results[0].get('error_code')})
    item=results[0];file=api('/files/'+item['file_id']);report['file']={k:file[k] for k in ['file_id','external_id','size_bytes','checksum','mime_type']};report['file']['title']=file['metadata']['title']
    digest=hashlib.sha256();size=0
    with client.open(cfg['url']+'/api/files/'+file['file_id']+'/content',timeout=120) as response:
        while block:=response.read(1<<20):digest.update(block);size+=len(block)
    check('delivered_size_and_SHA256',size==int(file['size_bytes']) and digest.hexdigest()==file['checksum'],{'bytes':size})
    req=urllib.request.Request(cfg['url']+'/api/files/'+file['file_id']+'/content',headers={'Range':'bytes=0-31'})
    with client.open(req,timeout=30) as response:check('range_206',response.status==206 and len(response.read())==32)
    deadline=time.monotonic()+60
    while time.monotonic()<deadline and file['preview_state'] not in ['ready','failed']:time.sleep(.5);file=api('/files/'+file['file_id'])
    check('preview_and_duration',file['preview_state']=='ready' and file['duration_ms']>0,{'state':file['preview_state'],'duration_ms':file['duration_ms']})
    report['status']='passed'
except Exception as error:
    report['status']='failed';report['error_type']=type(error).__name__;raise
finally:
    report['finished_at']=datetime.now(timezone.utc).isoformat();(out/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
