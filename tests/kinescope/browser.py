"""Two-stage UI acceptance against the official, owner-permitted Kinescope.

No donor replacement or fault injection. The numeric example must still permit
downloads. Reports exclude credentials, signed URLs and terminal contents.
"""
import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

parser = argparse.ArgumentParser()
parser.add_argument('--config', default='.local-v2-kinescope/client.json')
parser.add_argument('--id', default='203613411')
parser.add_argument('--output', default='artifacts/v2/kinescope-browser')
browser_choice = parser.add_mutually_exclusive_group()
browser_choice.add_argument('--executable', help='Native Chromium executable on NixOS')
browser_choice.add_argument('--channel', choices=['chrome', 'chromium', 'msedge'],
                            help='Use an installed browser channel; CI uses Chrome for media codecs')
args = parser.parse_args()
if not args.id.isdecimal() or int(args.id) > 2**64-1:
    raise SystemExit('Unsigned numeric identifier required')
cfg = json.loads(Path(args.config).read_text())
output = Path(args.output)
output.mkdir(parents=True, exist_ok=True)
report = {'profile': 'real-kinescope-browser-v2', 'external_id': args.id,
          'started_at': datetime.now(timezone.utc).isoformat(), 'checks': []}


def check(name, ok, observed=None):
    report['checks'].append({'name': name, 'status': 'passed' if ok else 'failed',
                             'observed': observed})
    if not ok:
        raise AssertionError(name)


try:
    with sync_playwright() as pw:
        options = {'headless': True, 'args': ['--no-sandbox']}
        if args.executable:
            options['executable_path'] = args.executable
        if args.channel:
            options['channel'] = args.channel
        browser = pw.chromium.launch(**options)
        context = browser.new_context(ignore_https_errors=True, locale='ru-RU',
                                      viewport={'width': 1440, 'height': 1000})
        page = context.new_page()
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto(cfg['url'])
        page.locator('#login input[name=username]').fill(cfg['username'])
        page.locator('#login input[name=password]').fill(Path(cfg['password_file']).read_text().strip())
        page.locator('#login button').click()
        expect(page.locator('#workspace')).to_be_visible(timeout=20000)

        def api(path):
            response = context.request.get(cfg['url']+'/api'+path, headers={'API-Version': '2'})
            if not response.ok:
                raise AssertionError('API status '+str(response.status))
            return response.json()

        def job_finished(job):
            deadline = time.monotonic()+180
            while time.monotonic() < deadline:
                state = api('/jobs/'+job)['state']
                if state == 'completed':
                    return
                if state in ['stopped', 'failed']:
                    raise AssertionError('job_'+state)
                page.wait_for_timeout(250)
            raise TimeoutError('job did not finish')

        def new_job(before, kind):
            jobs = [j for j in api('/snapshot')['jobs']
                    if j['job_id'] not in before and j['kind'] == kind]
            check('browser_created_'+kind, len(jobs) == 1)
            return jobs[0]['job_id']

        def submit():
            page.locator('#job-form input[name=threads_default]').fill('1')
            page.locator('#job-form input[name=block_size]').fill('1')
            page.locator('#job-form input[name=max_file_mib]').fill('128')
            page.locator('#job-form > button').click()
            expect(page.locator('#job-dialog')).not_to_be_visible(timeout=10000)

        snapshot = api('/snapshot')
        check('official_kinescope', snapshot['source_profile']['source_id'] == 'kinescope'
              and not snapshot['source_profile']['mock'])
        check('three_live_agents', len([w for w in snapshot['workers'] if w['online']]) == 3)
        before = {j['job_id'] for j in snapshot['jobs']}
        page.locator('#new-job').click()
        page.locator('#job-form input[name=range_text]').fill('['+args.id+','+args.id+']')
        submit()
        discovery = new_job(before, 'discovery')
        report['discovery_job_id'] = discovery
        page.locator('[data-job-id="'+discovery+'"]').get_by_role('button', name='Запустить', exact=True).click()
        job_finished(discovery)
        items = api('/jobs/'+discovery+'/items')['items']
        check('discovery_available_without_file', len(items) == 1
              and items[0]['status'] == 'available' and not items[0].get('file_id'))

        page.goto(cfg['url']+'/catalog')
        page.locator('#catalog-filter input[name=q]').fill(args.id)
        page.locator('#catalog-filter input[name=discovery_job_id]').fill(discovery)
        page.locator('#catalog-filter button').first.click()
        expect(page.locator('#catalog-list input[data-media-id]')).to_have_count(1)
        page.locator('#catalog-list input[data-media-id]').check()
        expect(page.locator('#selection-count')).to_contain_text('Выбрано: 1')
        before = {j['job_id'] for j in api('/snapshot')['jobs']}
        page.locator('#download-selection').click()
        expect(page.locator('#job-form-title')).to_contain_text('Новое скачивание')
        expect(page.locator('#job-form input[name=range_text]')).not_to_be_visible()
        submit()
        expect(page).to_have_url(cfg['url']+'/')
        download = new_job(before, 'download')
        report['download_job_id'] = download
        page.locator('[data-job-id="'+download+'"]').get_by_role('button', name='Запустить', exact=True).click()
        job_finished(download)
        items = api('/jobs/'+download+'/items')['items']
        check('separate_selected_download_completed', len(items) == 1
              and items[0]['status'] == 'completed' and bool(items[0].get('file_id')))
        file = api('/files/'+items[0]['file_id'])
        report['file'] = {k: file[k] for k in ['file_id', 'size_bytes', 'checksum', 'duration_ms']}
        check('real_thumbnail_and_duration', file['preview_state'] == 'ready'
              and file['duration_ms'] > 0)

        page.goto(cfg['url']+'/gallery')
        page.locator('#gallery-filter input[name=q]').fill(args.id)
        page.locator('#gallery-filter input[name=job_id]').fill(download)
        page.locator('#gallery-filter button').click()
        expect(page.locator('#gallery-list .video-card')).to_have_count(1)
        expect(page.locator('#gallery-list .video-card img')).to_be_visible()
        page.reload()
        expect(page.locator('#gallery-list .video-card')).to_have_count(1)
        check('gallery_search_job_filter_and_reload', True)
        page.screenshot(path=str(output/'gallery-desktop.png'), full_page=True)
        page.set_viewport_size({'width': 390, 'height': 844})
        check('mobile_gallery_width', page.evaluate('document.documentElement.scrollWidth<=innerWidth+1'))
        page.screenshot(path=str(output/'gallery-mobile.png'), full_page=True)
        page.set_viewport_size({'width': 1440, 'height': 1000})
        page.locator('#gallery-list .video-link').click()
        expect(page).to_have_url(cfg['url']+'/watch/'+file['file_id'])
        expect(page.locator('#watch-content h1')).to_have_text(file['metadata']['title'])
        page.reload()
        expect(page.locator('#watch-player')).to_be_visible()
        page.evaluate('const v=document.querySelector("#watch-player");v.muted=true;v.play()')
        deadline = time.monotonic()+20
        while time.monotonic() < deadline and page.evaluate('document.querySelector("#watch-player").currentTime') < .5:
            page.wait_for_timeout(100)
        check('real_video_playback_after_direct_reload',
              page.evaluate('document.querySelector("#watch-player").currentTime') >= .5)
        page.evaluate('document.querySelector("#watch-player").pause()')
        page.screenshot(path=str(output/'watch.png'), full_page=True)
        with page.expect_download(timeout=30000) as pending:
            page.locator('#watch-download').click()
        saved = pending.value
        path = Path(saved.path())
        with path.open('rb') as contents:
            digest = hashlib.file_digest(contents, 'sha256').hexdigest()
        check('client_download_size_and_SHA256', saved.failure() is None
              and path.stat().st_size == int(file['size_bytes']) and digest == file['checksum'],
              {'bytes': path.stat().st_size, 'filename': saved.suggested_filename})

        page.goto(cfg['url'])
        page.locator('[data-tab=workers]').click()
        page.locator('[data-worker-id=worker-1]').get_by_role('button', name='Терминал', exact=True).click()
        expect(page.locator('#terminal-status')).to_have_text('Подключено', timeout=20000)

        def terminal_text():
            return page.evaluate("Array.from({length:terminal.buffer.active.length},(_,i)=>terminal.buffer.active.getLine(i)?.translateToString(true)||'').join('\\n')")

        def command(value):
            page.evaluate('terminal.focus()')
            page.keyboard.type(value)
            page.keyboard.press('Enter')

        def terminal_wait(value):
            deadline = time.monotonic()+15
            while time.monotonic() < deadline:
                if value in terminal_text():
                    return
                page.wait_for_timeout(100)
            raise AssertionError('expected terminal test marker missing')

        command("test -t 0 && printf 'REAL_TTY_%s\\n' OK; id -u; whoami")
        terminal_wait('REAL_TTY_OK')
        terminal_wait('begom-shell')
        check('real_profile_unprivileged_SSH_PTY', '10050' in terminal_text())
        page.set_viewport_size({'width': 1100, 'height': 800})
        page.wait_for_timeout(300)
        size = page.evaluate('({rows:terminal.rows,cols:terminal.cols})')
        command('stty size')
        terminal_wait(str(size['rows'])+' '+str(size['cols']))
        check('real_profile_terminal_resize', True, size)
        command('sleep 30')
        page.wait_for_timeout(250)
        page.keyboard.press('Control+c')
        command("printf 'REAL_INTERRUPT_%s\\n' OK")
        terminal_wait('REAL_INTERRUPT_OK')
        check('real_profile_terminal_CtrlC', True)
        command("test ! -r /run/secrets/agent.key && printf 'AGENT_SECRETS_%s\\n' CLOSED; sudo -n true || printf 'SUDO_%s\\n' DENIED")
        terminal_wait('AGENT_SECRETS_CLOSED')
        terminal_wait('SUDO_DENIED')
        check('real_profile_no_sudo_or_agent_secrets', True)
        page.locator('#terminal-disconnect').click()
        check('javascript_errors', not errors, errors)
        report['browser'] = browser.version
        report['status'] = 'passed'
        browser.close()
except Exception as error:
    report['status'] = 'failed'
    report['error_type'] = type(error).__name__
    raise
finally:
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    (output/'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
