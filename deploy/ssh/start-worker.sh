#!/bin/bash
set -euo pipefail
case "${BEGOM_AGENT_UID:-}" in
  10001|10002|10003) ;;
  *) echo 'Invalid agent identity' >&2; exit 1 ;;
esac
setpriv --reuid=10050 --regid=10050 --clear-groups /usr/sbin/sshd -t -f /etc/begom/terminal-sshd.conf
setpriv --reuid=10050 --regid=10050 --clear-groups /usr/sbin/sshd -D -e -f /etc/begom/terminal-sshd.conf &
terminal_pid=$!
setpriv --reuid="$BEGOM_AGENT_UID" --regid=10000 --clear-groups \
  /usr/local/bin/node-helper serve --config /config/agent.json &
agent_pid=$!
cleanup() {
  kill -TERM "$agent_pid" "$terminal_pid" 2>/dev/null || true
  wait "$agent_pid" "$terminal_pid" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 0' TERM INT
wait -n "$terminal_pid" "$agent_pid"
