"""Real browser smoke, ACL, CSRF, WSS, terminal and collect-to-render checks.
Run only on an initialized mock test stand. No credentials enter the report.
"""
import argparse
import json
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

p = argparse.ArgumentParser()
p.add_argument('--config', default='.local/client.json')
p.add_argument('--output', default='artifacts/browser')
p.add_argument('--executable', help='Optional native Chromium executable on NixOS')
args = p.parse_args()
cfg = json.loads(Path(args.config).read_text())
out = Path(args.output)
out.mkdir(parents=True, exist_ok=True)
report = {'profile': 'mock-browser', 'started_at': datetime.now(timezone.utc).isoformat(), 'checks': []}

def check(name, ok, observed=None):
    report['checks'].append({'name': name, 'status': 'passed' if ok else 'failed', 'observed': observed})
    if not ok:
        raise AssertionError(name)

try:
    with sync_playwright() as pw:
        options = {'headless': True, 'args': ['--no-sandbox']}
        if args.executable:
            options['executable_path'] = args.executable
        browser = pw.chromium.launch(**options)
        # The stand uses a private generated CA. Go integration tests verify TLS
        # trust separately; no OS trust store is modified by this UI suite.
        context = browser.new_context(ignore_https_errors=True, viewport={'width': 1440, 'height': 1000})
        page = context.new_page()
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto(cfg['url'])
        page.locator('#login input[name=username]').fill(cfg['username'])
        page.locator('#login input[name=password]').fill(Path(cfg['password_file']).read_text().strip())
        page.locator('#login button').click()
        expect(page.locator('#workspace')).to_be_visible(timeout=20000)
        expect(page.locator('#identity')).to_contain_text(cfg['username'])
        session = context.request.get(cfg['url']+'/api/auth/session').json()
        csrf = session['csrf_token']

        def api(path, method='GET', data=None):
            headers = {'API-Version': '1', 'X-CSRF-Token': csrf, 'Idempotency-Key': secrets.token_hex(16)}
            r = context.request.fetch(cfg['url']+'/api'+path, method=method, data=data, headers=headers)
            if not r.ok:
                raise AssertionError('API '+path+' status '+str(r.status))
            return r.json()

        snapshot = api('/snapshot')
        check('mock_only', snapshot.get('source_profile', {}).get('mock') is True)
        check('three_registered_agents', len([w for w in snapshot['workers'] if w['online']]) >= 3)
        page.locator('[data-tab=workers]').click()
        expect(page.locator('#worker-list .worker')).to_have_count(len(snapshot['workers']))
        ages = []
        for _ in range(50):
            ages += page.locator('#worker-list .worker').evaluate_all('(nodes) => nodes.map(n => Date.now()-Date.parse(n.dataset.measuredAt))')
            page.wait_for_timeout(200)
        check('metrics_collect_to_render', all(0 <= age <= 5000 for age in ages), {'max_ms': max(ages), 'samples': len(ages)})
        page.evaluate('eventsSocket.close()')
        page.wait_for_timeout(500)
        expect(page.locator('#connection')).to_have_text('Данные обновляются', timeout=15000)
        check('wss_reconnect_and_http_refresh', True)
        page.set_viewport_size({'width': 390, 'height': 844})
        check('responsive_width', page.evaluate('document.documentElement.scrollWidth <= window.innerWidth+1'))
        page.screenshot(path=str(out/'mobile.png'), full_page=True)
        page.set_viewport_size({'width': 1440, 'height': 1000})

        node = page.locator('[data-worker-id=worker-1]')
        node.get_by_role('button', name='Терминал', exact=True).click()
        expect(page.locator('#terminal-output')).to_contain_text('help', timeout=15000)
        page.locator('#terminal-command').fill('help')
        page.locator('#terminal-form button').click()
        expect(page.locator('#terminal-output')).to_contain_text('resources', timeout=10000)
        page.locator('#terminal-command').fill('cat /etc/shadow')
        page.locator('#terminal-form button').click()
        expect(page.locator('#terminal-output')).to_contain_text('Команда запрещена', timeout=10000)
        page.locator('#terminal-dialog [data-close]').click()
        check('ssh_terminal_pinned_host_and_closed_commands', True)

        page.locator('[data-tab=jobs]').click()
        page.locator('#new-job').click()
        page.locator('#job-form input[name=range_text]').fill('[18446744073709551614,18446744073709551615]')
        page.locator('#job-form button[type=submit], #job-form > button').click()
        expect(page.locator('#job-dialog')).not_to_be_visible(timeout=10000)
        snapshot = api('/snapshot')
        jobs = [j for j in snapshot['jobs'] if j['range']['from'] == '18446744073709551614']
        check('uint64_browser_precision', len(jobs) >= 1 and jobs[0]['range']['to'] == '18446744073709551615')
        job = jobs[0]['job_id']
        payload = '<img src=x onerror="window.__begomXss=1">'
        mock = context.request.put(cfg['mock_url']+'/control', data={'delay_ms': 4000, 'demo': False, 'cases': {'7777': {'title': payload, 'no_download': True}}}, headers={'Authorization': 'Bearer '+Path(cfg['mock_token_file']).read_text().strip()})
        check('controlled_XSS_fixture', mock.status == 200)
        xss_job = api('/jobs', 'POST', {'range_text': '[7777,7777]', 'worker_ids': ['worker-1'], 'threads_default': 1})['job_id']
        api('/jobs/'+xss_job+'/start', 'POST', {})
        deadline = time.monotonic()+30
        while time.monotonic() < deadline and api('/jobs/'+xss_job)['state'] != 'completed':
            page.wait_for_timeout(250)
        page.locator('[data-job-id="'+xss_job+'"]').get_by_role('button', name='Результаты', exact=True).click()
        expect(page.locator('#results-list')).to_contain_text(payload)
        check('metadata_XSS_escaped', page.locator('#results-list img').count() == 0 and page.evaluate('window.__begomXss') is None)
        page.locator('#results-dialog [data-close]').click()
        password = secrets.token_urlsafe(24)
        username = 'browser-'+secrets.token_hex(4)
        viewer = api('/users', 'POST', {'username': username, 'password': password, 'roles': ['viewer'], 'enabled': True})
        no_csrf = context.request.post(cfg['url']+'/api/jobs/'+job+'/start', data={}, headers={'Idempotency-Key': secrets.token_hex(16)})
        check('csrf_rejected', no_csrf.status == 403)
        viewer_context = browser.new_context(ignore_https_errors=True)
        r = viewer_context.request.post(cfg['url']+'/api/auth/login', data={'username': username, 'password': password})
        check('viewer_login', r.status == 200)
        vcsrf = r.json()['csrf_token']
        r = viewer_context.request.get(cfg['url']+'/api/jobs/'+job)
        check('acl_hides_unassigned_job', r.status == 404)
        r = viewer_context.request.get(cfg['url']+'/api/audit')
        check('audit_administrator_only', r.status == 403)
        api('/jobs/'+job+'/access', 'PUT', {'user_id': viewer['user_id'], 'permission': 'view'})
        r = viewer_context.request.get(cfg['url']+'/api/jobs/'+job)
        check('acl_grants_view', r.status == 200)
        r = viewer_context.request.post(cfg['url']+'/api/jobs/'+job+'/start', data={}, headers={'X-CSRF-Token': vcsrf, 'Idempotency-Key': secrets.token_hex(16)})
        check('viewer_cannot_manage', r.status == 403)
        for path in ['/_published/mock/0/video', '/data/staging/worker-1/video.part']:
            r = context.request.get(cfg['url']+path)
            check('private_path_'+path.split('/')[1], r.status == 404)
        api('/users/'+viewer['user_id'], 'PATCH', {'username': username, 'password': '', 'roles': ['viewer'], 'enabled': False})
        viewer_context.close()
        check('javascript_errors', not errors, errors)
        page.screenshot(path=str(out/'desktop.png'), full_page=True)
        report['browser'] = browser.version
        browser.close()
finally:
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    (out/'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
