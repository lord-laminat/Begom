function enrich(tag, ts, r)
  local service = string.match(tag, 'worker%-%d+') or string.match(tag, '%-([%w]+)%-%d+$')
  r.component = r.component or service or 'infrastructure'
  r.severity = string.upper(r.severity or r.level or r['@level'] or 'INFO')
  r.timestamp = r.timestamp or r['@timestamp'] or r.time or ts
  r.operation = r.operation or 'service.log'
  r.result = r.result or 'observed'
  if r.log then
    r.message = string.gsub(r.log, '(https?://[^%s?]+)%?[^%s]+', '%1?[redacted]')
    r.log = nil
  end
  return 1, ts, r
end
