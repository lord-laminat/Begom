"""Render deployment templates with non-secret example facts; never connects to hosts."""
from pathlib import Path
import json
import jinja2
import yaml

inventory = yaml.safe_load(Path('deploy/ansible/inventory.example.yml').read_text())['all']
values = dict(inventory['vars'])
groups = {name: list(group['hosts']) for name, group in inventory['children'].items()}
groups['all'] = [host for members in groups.values() for host in members]
hostvars = {host: config for group in inventory['children'].values() for host, config in group['hosts'].items()}
values.update(groups=groups, hostvars=hostvars, inventory_hostname='worker-1', begom_uid=10001,
              ansible_default_ipv4={'address': '10.20.0.10'},
              begom_tickets=[{'user':'begom-worker-1','uid':10001,'file':'nfs.keytab','principal':'begom-worker-1'}])
env = jinja2.Environment(undefined=jinja2.StrictUndefined, keep_trailing_newline=True)
count = 0
for path in sorted(Path('deploy/ansible/templates').glob('*.j2')):
    rendered = env.from_string(path.read_text()).render(**values)
    if path.name.endswith('.yml.j2'):
        value = yaml.safe_load(rendered)
        if path.name == 'compose.yml.j2':
            assert value['services']['fluent-bit']['logging']['driver'] == 'json-file'
            assert value['x-service']['logging']['options']['tag'] == 'begom.{{.Name}}'
        if path.name == 'prometheus.yml.j2':
            assert len(value['scrape_configs']) >= 8
    assert '{{' not in rendered.replace('begom.{{.Name}}', '')
    count += 1
print(json.dumps({'templates_rendered': count, 'profile': 'example-facts-only', 'status': 'passed'}))
