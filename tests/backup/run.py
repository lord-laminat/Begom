"""Snapshot and isolated restore on a disposable local stand.
Only new begom_restore_* databases/directories are created; none are removed.
The local repository is a development volume, not an independent backup host.
"""
import argparse
import json
import os
import secrets
import subprocess
from datetime import datetime, timezone
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--state', default='.local')
parser.add_argument('--output', default='artifacts/backup')
parser.add_argument('--isolated-kinescope', action='store_true',
                    help='Explicitly permit a snapshot and isolated restore on a disposable Kinescope stand')
args = parser.parse_args()
state = Path(args.state).resolve()
output = Path(args.output).resolve()
output.mkdir(parents=True, exist_ok=True)
cfg = json.loads((state/'config/backup.json').read_text())
source = json.loads((state/'config/coordinator.json').read_text())
mock = source['source'].get('mock') is True
if not mock and (not args.isolated_kinescope or source['source'].get('source_id') != 'kinescope'):
    raise SystemExit('A real source requires an explicit --isolated-kinescope test profile')
if Path(os.environ.get('BEGOM_STATE_DIR', './.local')).resolve() != state:
    raise SystemExit('BEGOM_STATE_DIR must select the same generated state directory')
report = {'profile': 'compose-'+('mock' if mock else 'kinescope')+'-isolated-restore', 'started_at': datetime.now(timezone.utc).isoformat(), 'checks': []}

def compose(*args):
    r = subprocess.run(['docker', 'compose', *args], capture_output=True, text=True, timeout=600)
    if r.returncode:
        raise RuntimeError('backup stand command failed (credentials suppressed)')
    return r.stdout

def check(name, ok):
    report['checks'].append({'name': name, 'status': 'passed' if ok else 'failed'})
    if not ok:
        raise AssertionError(name)

try:
    # A present but damaged repository must fail rather than be reinitialized.
    compose('run', '--rm', '--no-deps', '--entrypoint', '/bin/sh', 'backup', '-c',
            'test -f /repository/config || exec /usr/local/bin/begomctl backup init --config /config/backup.json')
    result = json.loads(compose('run', '--rm', '--no-deps', 'backup', 'backup', 'run', '--config', '/config/backup.json'))
    report['backup'] = result
    check('consistent_SQL_and_published_snapshot', result['state'] == 'completed' and result['files'] > 0 and bool(result['snapshot']))
    suffix = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_')+secrets.token_hex(3)
    restore_cfg = dict(cfg, database_url_file='/restore-state/secrets/restore-db-url')
    cfg_file = output/'restore-config.json'
    cfg_file.write_text(json.dumps(restore_cfg))
    cfg_file.chmod(0o600)
    compose('run', '--rm', '--no-deps', '--entrypoint', '/bin/mkdir', 'permissions', '-p', '/bootstrap/restores')
    restored = json.loads(compose('run', '--rm', '--no-deps', '--user', '0:0',
        '-v', str(state)+':/restore-state', '-v', str(cfg_file)+':/config/restore.json:ro', 'backup',
        'restore', '--config', '/config/restore.json', '--snapshot', result['snapshot'],
        '--target', '/restore-state/restores/'+suffix, '--database', 'begom_restore_'+suffix))
    report['restore'] = restored
    check('new_isolated_database_and_directory', restored['state'] == 'completed' and restored['database'].startswith('begom_restore_'))
    check('all_manifest_file_and_SQL_hashes_verified', restored['files'] == result['files'])
    check('measured_RPO_and_RTO', restored['rto_seconds'] > 0 and restored['rpo_seconds'] >= 0)
    report['status'] = 'passed'
except Exception as error:
    report['status'] = 'failed'
    report['error_type'] = type(error).__name__
    raise
finally:
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    (output/'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
    # Configuration holds references only, but need not enter the CI artifacts.
    (output/'restore-config.json').unlink(missing_ok=True)
