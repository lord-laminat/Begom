# Приёмка Begom 1.0.0

Профиль: `compose`. Начало: 2026-10-23T09:03:12Z.

| Проверка | Результат | Критерий | Измерение |
|---|---|---|---|
| workers | passed | at least three healthy workers | 3 |
| parallel_operations | passed | peak >= 100 actual mock requests | 100 |
| global_rate_window | passed | at most 80 arrivals per sliding second | 42 |
| confirmed_files | passed | 240 completed IDs | 240 |
| published_integrity | passed | SHA-256 of twenty delivered videos equals catalog | 20 |
| client_range | passed | 206 and sixteen bytes | {"bytes":16,"status":206} |
| canonical_deduplication | passed | subsequent jobs reuse ready file IDs | 20 |
| agent_failure_reassignment | passed | new assignment within 2*2 + 10 = 14 seconds | 10.709779975 |
| coordinator_restart | passed | all confirmed file IDs survive restart | {"confirmed_before":1,"items_after":120} |
| soft_pause | passed | finish in-flight IDs and release the rest of each block | 120 |
| resume_generation | passed | resume changes run_id | null |
| immediate_stop | passed | cancel active HTTP and preserve checkpoints within five seconds | 0.269712406 |
| browser_metric_latency | separate_suite | tests/browser measures collect-to-render <= 5s | null |
| NFS_Kerberos_isolation | separate_stand | deploy/ansible acceptance on independent Linux nodes | null |
