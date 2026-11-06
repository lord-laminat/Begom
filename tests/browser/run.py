"""Real browser smoke, ACL, CSRF, WSS, terminal and collect-to-render checks.
Run only on an initialized mock test stand. No credentials enter the report.
"""
import argparse
import hashlib
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
        # Headless Shell can inherit en-US@posix from a CI host. Grafana uses
        # Intl.NumberFormat, which requires a valid BCP 47 language tag.
        context = browser.new_context(ignore_https_errors=True, locale='ru-RU', viewport={'width': 1440, 'height': 1000})
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
            headers = {'API-Version': '2', 'X-CSRF-Token': csrf, 'Idempotency-Key': secrets.token_hex(16)}
            if path == '/jobs' and method == 'POST' and 'kind' not in data:
                data = {'kind': 'discovery', 'config': data}
            r = context.request.fetch(cfg['url']+'/api'+path, method=method, data=data, headers=headers)
            if not r.ok:
                raise AssertionError('API '+path+' status '+str(r.status))
            return r.json()

        snapshot = api('/snapshot')
        check('mock_only', snapshot.get('source_profile', {}).get('mock') is True)
        check('three_registered_agents', len([w for w in snapshot['workers'] if w['online']]) >= 3)
        expect(page.get_by_role('link', name='Grafana ↗')).to_have_attribute('href', '/grafana/d/begom/begom')
        home = context.request.get(cfg['url']+'/grafana/api/dashboards/home')
        check('grafana_home_redirect', home.status == 200 and home.json().get('redirectUri', '').startswith('/grafana/d/default-home-dashboard/'))
        grafana = context.new_page()
        grafana_errors = []
        grafana.on('pageerror', lambda error: grafana_errors.append(str(error)))
        grafana.on('console', lambda message: grafana_errors.append(message.text) if message.type == 'error' else None)
        try:
            grafana.goto(cfg['url']+'/grafana/')
            expect(grafana.get_by_text('Доступность сервисов', exact=True)).to_be_visible(timeout=30000)
            check('grafana_home_is_begom', True)
            grafana.goto(cfg['url']+'/grafana/d/begom/begom')
            expect(grafana.get_by_text('Доступность сервисов', exact=True)).to_be_visible(timeout=30000)
            check('grafana_dashboard_visible', True)
        except Exception:
            grafana.screenshot(path=str(out/'grafana-failure.png'), full_page=True)
            report['grafana_failure'] = {'url': grafana.url, 'text': grafana.locator('body').inner_text(), 'javascript_errors': grafana_errors}
            raise
        grafana.close()
        r = context.request.get(cfg['url']+'/grafana/api/datasources/proxy/uid/prometheus/api/v1/query', params={'query': 'begom_worker_cpu_percent'})
        series = r.json().get('data', {}).get('result', [])
        check('grafana_prometheus_worker_metrics', r.status == 200 and len(series) == 3)
        r = context.request.get(cfg['url']+'/grafana/api/datasources/proxy/uid/loki/loki/api/v1/query_range', params={'query': '{service=~".+"} | json', 'limit': '1'})
        check('grafana_loki_logs', r.status == 200 and bool(r.json().get('data', {}).get('result')))
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
        expect(page.locator('#terminal-status')).to_have_text('Подключено', timeout=20000)
        def terminal_text():
            return page.evaluate("Array.from({length:terminal.buffer.active.length},(_,i)=>terminal.buffer.active.getLine(i)?.translateToString(true)||'').join('\\n')")
        def terminal_command(command):
            page.evaluate('terminal.focus()')
            page.keyboard.type(command)
            page.keyboard.press('Enter')
        def terminal_wait(text, timeout=15):
            until = time.monotonic()+timeout
            while time.monotonic()<until:
                if text in terminal_text(): return
                page.wait_for_timeout(100)
            raise AssertionError('terminal output missing: '+text)
        terminal_command("test -t 0 && printf 'PTY_%s\\n' OK; id -u; whoami")
        terminal_wait('PTY_OK')
        terminal_wait('begom-shell')
        check('terminal_real_unprivileged_PTY', '10050' in terminal_text())
        page.set_viewport_size({'width': 1200, 'height': 850})
        page.wait_for_timeout(300)
        size = page.evaluate('({rows:terminal.rows,cols:terminal.cols})')
        terminal_command('stty size')
        terminal_wait(str(size['rows'])+' '+str(size['cols']))
        check('terminal_resize', True, size)
        terminal_command('sleep 30')
        page.wait_for_timeout(300)
        page.keyboard.press('Control+c')
        terminal_command("printf 'INTERRUPT_%s\\n' OK")
        terminal_wait('INTERRUPT_OK')
        check('terminal_ctrl_c', True)
        terminal_command('top -d 1')
        terminal_wait('load average')
        page.keyboard.press('q')
        editor_file='begom-browser-'+secrets.token_hex(4)+'.txt'
        terminal_command('nano '+editor_file)
        page.wait_for_timeout(500)
        page.keyboard.type('begom-interactive-editor')
        page.keyboard.press('Control+o')
        page.keyboard.press('Enter')
        page.keyboard.press('Control+x')
        terminal_command("grep -qx begom-interactive-editor "+editor_file+" && printf 'EDITOR_%s\\n' SAVED")
        terminal_wait('EDITOR_SAVED')
        terminal_command('rm -- '+editor_file)
        terminal_command("printf 'TAB_%s\\n' OK > begom-autocomplete-test")
        page.evaluate('terminal.focus()')
        page.keyboard.type('cat begom-auto')
        page.keyboard.press('Tab')
        page.keyboard.press('Enter')
        terminal_wait('TAB_OK')
        terminal_command("printf 'HISTORY_%s\\n' REPLAY")
        terminal_wait('HISTORY_REPLAY')
        history_before=terminal_text().count('HISTORY_REPLAY')
        page.keyboard.press('ArrowUp')
        page.keyboard.press('Enter')
        until=time.monotonic()+10
        while time.monotonic()<until and terminal_text().count('HISTORY_REPLAY')<=history_before:page.wait_for_timeout(100)
        check('terminal_history_and_autocomplete',terminal_text().count('HISTORY_REPLAY')>history_before)
        terminal_command('rm -- begom-autocomplete-test')
        terminal_command("test ! -r /run/secrets/agent.key && printf 'SECRETS_%s\\n' CLOSED; sudo -n true || printf 'SUDO_%s\\n' DENIED")
        terminal_wait('SECRETS_CLOSED')
        terminal_wait('SUDO_DENIED')
        check('terminal_editor_and_no_sudo_or_agent_secrets', True)
        terminal_command("head -c 3145728 /dev/zero | tr '\\0' '.'; printf 'FLOW_%s\\n' OK")
        terminal_wait('FLOW_OK',timeout=30)
        check('terminal_backpressure_over_two_MiB',True,{'output_bytes':3145728})
        page.locator('#terminal-disconnect').click()
        page.goto(cfg['url'])
        expect(page.locator('#workspace')).to_be_visible(timeout=20000)
        page.set_viewport_size({'width': 1440, 'height': 1000})

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
        scans = []
        for number in ['7778', '7780']:
            scan = api('/jobs', 'POST', {'range_text': '['+number+','+number+']', 'worker_ids': ['worker-1'], 'threads_default': 1})['job_id']
            api('/jobs/'+scan+'/start', 'POST', {})
            scans.append(scan)
        deadline = time.monotonic()+60
        while time.monotonic()<deadline and any(api('/jobs/'+scan)['state']!='completed' for scan in scans):
            page.wait_for_timeout(250)
        check('parsing_finishes_without_files', all(api('/jobs/'+scan+'/items')['items'][0]['status']=='available' and not api('/jobs/'+scan+'/items')['items'][0].get('file_id') for scan in scans))
        check('forbidden_download_excluded_from_catalog', not api('/catalog?q=7777')['items'])
        old_jobs = {j['job_id'] for j in api('/snapshot')['jobs']}
        page.goto(cfg['url']+'/catalog')
        expect(page.locator('#catalog-list input[data-media-id]').first).to_be_visible()
        first_catalog_id=page.locator('#catalog-list input[data-media-id]').first.get_attribute('data-media-id')
        page.locator('#catalog-list input[data-media-id]').first.check()
        expect(page.locator('#catalog-next')).to_be_enabled()
        page.locator('#catalog-next').click()
        expect(page.locator('#catalog-list input[data-media-id="'+first_catalog_id+'"]').first).to_have_count(0)
        page.locator('#catalog-list input[data-media-id]').first.check()
        expect(page.locator('#selection-count')).to_contain_text('Выбрано: 2')
        page.locator('#catalog-filter button').first.click()
        expect(page.locator('#catalog-list input[data-media-id="'+first_catalog_id+'"]').first).to_be_checked()
        check('catalog_selection_survives_pagination',True)
        page.locator('#clear-selection').click()
        expect(page.locator('#selection-count')).to_contain_text('Выбрано: 0')
        for number in ['7778','7780']:
            page.locator('#catalog-filter input[name=q]').fill(number)
            page.locator('#catalog-filter button').first.click()
            expect(page.locator('#catalog-list input[data-media-id]')).to_have_count(1)
            expect(page.locator('#catalog-list')).to_contain_text(number)
            page.locator('#catalog-list input[data-media-id]').check()
        expect(page.locator('#selection-count')).to_contain_text('Выбрано: 2')
        page.locator('#download-selection').click()
        expect(page.locator('#job-form-title')).to_contain_text('Новое скачивание')
        page.locator('#job-form > button').click()
        expect(page).to_have_url(cfg['url']+'/')
        expect(page.locator('#workspace')).to_be_visible(timeout=20000)
        downloads = [j for j in api('/snapshot')['jobs'] if j['job_id'] not in old_jobs and j['kind']=='download']
        check('browser_creates_immutable_cross_discovery_selection', len(downloads)==1 and downloads[0]['selection_count']==2)
        download_job = downloads[0]['job_id']
        page.locator('[data-job-id="'+download_job+'"]').get_by_role('button', name='Запустить', exact=True).click()
        deadline=time.monotonic()+90
        while time.monotonic()<deadline and api('/jobs/'+download_job)['state']!='completed':page.wait_for_timeout(250)
        results=api('/jobs/'+download_job+'/items')['items']
        check('published_video_for_client',len(results)==2 and all(i['status']=='completed' and i.get('file_id') for i in results))
        video=next(i for i in results if i['external_id']=='7778')
        deadline=time.monotonic()+60
        while time.monotonic()<deadline and api('/files/'+video['file_id'])['preview_state'] not in ['ready','failed']:page.wait_for_timeout(250)
        first_page=api('/files?job_id='+download_job+'&quality=720p&sort=title&limit=1')
        second_page=api('/files?job_id='+download_job+'&quality=720p&sort=title&limit=1&after='+first_page['next_after'])
        check('gallery_filter_sort_and_cursor',len(first_page['files'])==1 and len(second_page['files'])==1 and first_page['files'][0]['file_id']!=second_page['files'][0]['file_id'])
        file=api('/files/'+video['file_id'])
        check('thumbnail_and_duration_generated',file['preview_state']=='ready' and file['duration_ms']>0,file['preview_state'])
        thumb=context.request.get(cfg['url']+'/api/files/'+video['file_id']+'/thumbnail')
        check('published_JPEG_preview',thumb.status==200 and thumb.body().startswith(bytes.fromhex('ffd8')))
        page.goto(cfg['url']+'/gallery')
        page.locator('#gallery-filter input[name=q]').fill('7778')
        page.locator('#gallery-filter button').click()
        expect(page.locator('#gallery-list .video-card')).to_have_count(1)
        page.reload()
        expect(page.locator('#gallery-list .video-card')).to_have_count(1)
        page.locator('#gallery-list .video-link').click()
        expect(page.locator('#watch-content h1')).to_contain_text('7778')
        deadline=time.monotonic()+15
        while time.monotonic()<deadline and page.evaluate('document.querySelector("#watch-player").readyState')<1:page.wait_for_timeout(100)
        check('watch_player_metadata_loaded',page.evaluate('document.querySelector("#watch-player").readyState')>=1)
        check('gallery_search_deep_links_and_video_playback',True)
        page.goto(cfg['url']+'/gallery')
        page.set_viewport_size({'width':390,'height':844})
        expect(page.locator('#gallery-list .video-card').first).to_be_visible()
        check('gallery_responsive_width',page.evaluate('document.documentElement.scrollWidth<=innerWidth+1'))
        page.screenshot(path=str(out/'gallery-mobile.png'),full_page=True)
        page.set_viewport_size({'width':1440,'height':1000})
        page.goto(cfg['url'])
        for number,changed_case,expected_status,expected_error in [('7782',{'no_download':True},'skipped','download_not_allowed'),('7784',{'canonical_id':'replaced-canonical'},'failed','canonical_changed')]:
            scan=api('/jobs','POST',{'range_text':'['+number+','+number+']','worker_ids':['worker-1'],'threads_default':1})['job_id']
            api('/jobs/'+scan+'/start','POST',{})
            deadline=time.monotonic()+60
            while time.monotonic()<deadline and api('/jobs/'+scan)['state']!='completed':page.wait_for_timeout(250)
            original=api('/catalog?discovery_job_id='+scan)['items'][0]
            pending=api('/jobs','POST',{'kind':'download','config':{'worker_ids':['worker-1'],'threads_default':1},'media_ids':[original['media_id']]})['job_id']
            fixture=context.request.put(cfg['mock_url']+'/control',data={'delay_ms':0,'demo':False,'cases':{number:changed_case}},headers={'Authorization':'Bearer '+Path(cfg['mock_token_file']).read_text().strip()})
            check('changed_donor_fixture_'+number,fixture.status==200)
            api('/jobs/'+pending+'/start','POST',{})
            deadline=time.monotonic()+60
            while time.monotonic()<deadline and api('/jobs/'+pending)['state']!='completed':page.wait_for_timeout(250)
            outcome=api('/jobs/'+pending+'/items')['items'][0]
            check('revalidation_'+expected_error,outcome['status']==expected_status and outcome['error_code']==expected_error and not outcome.get('file_id'))
            check('withdraw_unavailable_catalog_'+number,not api('/catalog?q='+number)['items'])
            if expected_error=='canonical_changed':
                again=api('/jobs','POST',{'range_text':'['+number+','+number+']','worker_ids':['worker-1'],'threads_default':1})['job_id']
                api('/jobs/'+again+'/start','POST',{})
                deadline=time.monotonic()+60
                while time.monotonic()<deadline and api('/jobs/'+again)['state']!='completed':page.wait_for_timeout(250)
                replacement=api('/catalog?discovery_job_id='+again)['items']
                check('canonical_replacement_discovered_without_losing_history',len(replacement)==1 and replacement[0]['media_id']!=original['media_id'])
        content_url = cfg['url']+'/api/files/'+video['file_id']+'/content'
        password = secrets.token_urlsafe(24)
        username = 'browser-'+secrets.token_hex(4)
        viewer = api('/users', 'POST', {'username': username, 'password': password, 'roles': ['viewer'], 'enabled': True})
        no_csrf = context.request.post(cfg['url']+'/api/jobs/'+job+'/start', data={}, headers={'Idempotency-Key': secrets.token_hex(16)})
        check('csrf_rejected', no_csrf.status == 403)
        viewer_context = browser.new_context(ignore_https_errors=True, locale='ru-RU')
        r = viewer_context.request.post(cfg['url']+'/api/auth/login', data={'username': username, 'password': password})
        check('viewer_login', r.status == 200)
        vcsrf = r.json()['csrf_token']
        r = viewer_context.request.get(cfg['url']+'/api/jobs/'+job)
        check('acl_hides_unassigned_job', r.status == 404)
        r = viewer_context.request.get(cfg['url']+'/api/audit')
        check('audit_administrator_only', r.status == 403)
        r = viewer_context.request.get(content_url)
        check('video_acl_hides_unassigned_file', r.status == 404)
        check('gallery_ACL_hides_files_and_previews', not viewer_context.request.get(cfg['url']+'/api/files').json()['files'] and viewer_context.request.get(cfg['url']+'/api/files/'+video['file_id']+'/thumbnail').status==404)
        api('/jobs/'+download_job+'/access', 'PUT', {'user_id': viewer['user_id'], 'permission': 'view'})
        viewer_page = viewer_context.new_page()
        viewer_page.goto(cfg['url'])
        expect(viewer_page.locator('#workspace')).to_be_visible(timeout=20000)
        viewer_page.goto(cfg['url']+'/watch/'+video['file_id'])
        expect(viewer_page.locator('#watch-content h1')).to_contain_text('7778')
        check('viewer_direct_watch_link',True)
        viewer_page.goto(cfg['url'])
        viewer_page.locator('[data-job-id="'+download_job+'"]').get_by_role('button', name='Результаты', exact=True).click()
        expect(viewer_page.locator('#results-list').get_by_role('button', name='Смотреть', exact=True).first).to_be_visible()
        with viewer_page.expect_download(timeout=20000) as pending:
            viewer_page.locator('#results-list').get_by_role('link', name='Скачать', exact=True).first.click()
        download = pending.value
        data = Path(download.path()).read_bytes()
        check('viewer_downloads_published_video', download.failure() is None and download.suggested_filename == 'begom-7778.mp4' and len(data) == int(video['size_bytes']) and hashlib.sha256(data).hexdigest() == video['checksum'], {'bytes': len(data), 'filename': download.suggested_filename})
        r = viewer_context.request.get(content_url, headers={'Range': 'bytes=0-31'})
        check('viewer_video_range', r.status == 206 and r.body() == data[:32])
        anonymous = browser.new_context(ignore_https_errors=True, locale='ru-RU')
        r = anonymous.request.get(content_url)
        check('video_requires_login', r.status == 401)
        anonymous.close()
        r = viewer_context.request.get(cfg['url']+'/grafana/api/search')
        check('grafana_administrator_only', r.status == 403)
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
        report['status']='passed'
except Exception as error:
    report['status']='failed'
    report['error_type']=type(error).__name__
    raise
finally:
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    (out/'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
