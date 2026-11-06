ui = false
disable_mlock = true
api_addr = "https://openbao:8200"
cluster_addr = "https://openbao:8201"
log_format = "json"
storage "raft" {
  path = "/bao/file"
  node_id = "begom-local"
}
listener "tcp" {
  address = "0.0.0.0:8200"
  tls_cert_file = "/run/secrets/openbao.crt"
  tls_key_file = "/run/secrets/openbao.key"
  tls_min_version = "tls13"
  telemetry { unauthenticated_metrics_access = true }
}
telemetry {
  prometheus_retention_time = "30s"
  disable_hostname = true
}
