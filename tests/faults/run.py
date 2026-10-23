"""Failure injections for the disposable Compose mock stand only.
Restores every stopped service and the volume marker even when assertions fail.
No production hosts, real donors or pre-existing jobs are modified.
"""
import argparse
import http.cookiejar
import json
import os
import secrets
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--config', default='.local/client.json')
parser.add_argument('--output', default='artifacts/faults')
args = parser.parse_args()
cfg = json.loads(Path(args.config).read_text())
jar = http.cookiejar.CookieJar()
client = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=cfg['ca'])), urllib.request.HTTPCookieProcessor(jar))
csrf = ''
report = {'profile': 'compose-mock-failures', 'started_at': datetime.now(timezone.utc).isoformat(), 'checks': []}
out = Path(args.output)
out.mkdir(parents=True, exist_ok=True)

def request(path, method='GET', body=None):
    data = None if body is None else json.dumps(body).encode()
    headers = {'API-Version': '1', 'Content-Type': 'application/json', 'X-CSRF-Token': csrf, 'Idempotency-Key': secrets.token_hex(16)}
    req = urllib.request.Request(cfg['url']+'/api'+path, data=data, method=method, headers=headers)
    with client.open(req, timeout=8) as response:
        return json.load(response)

def compose(*args):
    subprocess.run(['docker', 'compose', *args], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)

def check(name, ok, observed=None):
    report['checks'].append({'name': name, 'status': 'passed' if ok else 'failed', 'observed': observed})
    if not ok:
        raise AssertionError(name)

def wait(predicate, seconds=60):
    until = time.monotonic()+seconds
    while time.monotonic() < until:
        try:
            if predicate():
                return True
        except (OSError, ValueError):
            pass
        time.sleep(.25)
    return False

def stats():
    with urllib.request.urlopen(cfg['mock_url']+'/stats', timeout=5) as r:
        return json.load(r)

def ready():
    with client.open(cfg['url']+'/readyz', timeout=5) as response:
        return response.status == 200

def stable_requests(seconds):
    start = stats()['requests']
    time.sleep(seconds)
    return start == stats()['requests']

job = None
stopped = set()
marker_offline = False
try:
    response = request('/auth/login', 'POST', {'username': cfg['username'], 'password': Path(cfg['password_file']).read_text().strip()})
    csrf = response['csrf_token']
    snapshot = request('/snapshot')
    check('mock_only', snapshot.get('source_profile', {}).get('mock') is True)
    check('no_other_active_jobs', not [j for j in snapshot['jobs'] if j['state'] in ['running', 'pausing', 'stopping']])
    nodes = [w['worker_id'] for w in snapshot['workers'] if w['online']][:3]
    check('three_agents', len(nodes) == 3)
    body = {'range_text': '[4000,6999]', 'worker_ids': nodes, 'threads_default': 40, 'block_size': 1000, 'retry_policy': {'max_retries': 4}, 'http_timeout_ms': 20000}
    job = request('/jobs', 'POST', body)['job_id']
    request('/jobs/'+job+'/start', 'POST', {})
    check('load_started', wait(lambda: stats()['active'] >= 40, 30))

    compose('stop', '--timeout', '0', 'postgres')
    stopped.add('postgres')
    time.sleep(3)
    check('SQL_loss_stops_new_HTTP', stable_requests(3))
    compose('start', 'postgres')
    stopped.remove('postgres')
    check('SQL_recovery', wait(ready, 90))
    check('processing_resumes_after_SQL_recovery', wait(lambda: stats()['active'] > 0, 45))

    # Root helper has access only to this generated development profile.
    compose('run', '--rm', '--no-deps', '--entrypoint', '/bin/mv', 'permissions', '/bootstrap/storage/.begom-volume', '/bootstrap/storage/.begom-volume.offline-test')
    marker_offline = True
    time.sleep(2)
    check('missing_volume_identity_stops_new_HTTP', stable_requests(3))
    compose('run', '--rm', '--no-deps', '--entrypoint', '/bin/mv', 'permissions', '/bootstrap/storage/.begom-volume.offline-test', '/bootstrap/storage/.begom-volume')
    marker_offline = False
    check('mount_identity_recovery', wait(ready, 45))

    compose('stop', '--timeout', '0', 'openbao')
    stopped.add('openbao')
    # The documented bounded secret cache is one minute, not indefinite.
    time.sleep(65)
    check('OpenBao_loss_stops_new_HTTP_after_cache_expiry', stable_requests(3))
    compose('start', 'openbao')
    stopped.remove('openbao')
    compose('run', '--rm', '--no-deps', 'bootstrap')
    check('OpenBao_unseal_and_recovery', wait(ready, 60))
    request('/jobs/'+job+'/stop', 'POST', {})
    check('job_stops_after_dependency_recovery', wait(lambda: request('/jobs/'+job)['state'] == 'stopped', 15))
finally:
    if marker_offline:
        compose('run', '--rm', '--no-deps', '--entrypoint', '/bin/mv', 'permissions', '/bootstrap/storage/.begom-volume.offline-test', '/bootstrap/storage/.begom-volume')
    for service in stopped:
        compose('start', service)
    if 'openbao' in stopped:
        compose('run', '--rm', '--no-deps', 'bootstrap')
    if job:
        try:
            request('/jobs/'+job+'/stop', 'POST', {})
        except Exception:
            pass
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    (out/'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
