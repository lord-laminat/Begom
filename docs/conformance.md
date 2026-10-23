# Матрица соответствия

Основание: `docs/ТЗ.docx`, ЭП, ТП и согласованный план Go. Таблица связывает требования с исполняемой реализацией и проверками. «Локально» означает Compose на одном хосте; физический NFS/Kerberos и масштабирование на целевом оборудовании требуют отдельного протокола. Уточнения архитектуры записаны в [ADR-0001](adr/0001-go-system.md).

| Требование ТЗ | Реализация | Свидетельство / граница проверки |
|---|---|---|
| §1–2 распределённый сбор разрешённых видео | пять Go-программ, SQL, общий каталог, API/интерфейс | сборка пяти бинарников, сквозной mock-прогон |
| §3 диапазоны | `internal/model`: ParseRange, Number, BlockEnd | TestRanges, TestBlockBoundaries, TestMaximumIdentifierAndCancelledGap, браузер uint64 |
| §4 параметры | JobConfig, WorkerConfig, строгая JSON/YAML конфигурация, UI/CLI | нормализация, валидация, индивидуальные потоки; путь NAS меняется при отключённом узле |
| §5.1 координатор C1–C10 | coordinator, db, dispatch, publication, auth, sshcontrol | интеграционные транзакции, HTTP и стенд |
| §5.2 агент A1–A5 | agent, donor, target, hostmetrics, storage | регистрация, heartbeat, C6/A4, Range/ETag |
| §5.3 подтверждённое состояние | PostgreSQL 18/pgx, миграции 001–004 | повтор миграций, идемпотентность, перезапуск координатора |
| §6.1 ленивые блоки | LeaseResourceTx, SQL locks/SKIP LOCKED | 60 конкурентных выдач без двойного владения |
| §6.2 аренды/попытки | LeaseRef, heartbeat, Sweep, session/run/attempt checks | устаревший отчёт, истечение, перевыдача в пределах 2H+L |
| §6.3 ресурсы и балансировка | CPU/RAM, ошибки, сеть/диск, общий SQL-резерв, fair jobs/C6 | интеграционный резерв и очереди; физическая пропускная способность NAS не измерялась |
| §7 статусы и классификация | progress, donor, agent | 404/403/429/5xx, unknown schema; метаданные skipped отдельно от готовых файлов |
| §8 файлы и метаданные | .part, SHA-256, resume, издательский inode, атомарная публикация | redirect/Range/ETag/corrupt prefix, аварии между этапами, дедупликация одновременных загрузок и восстановление очистки лишней копии, scrub/карантин |
| §9 квоты | C6 check/download, worker RR, окна, slots, persisted feedback/horizon, SQL generation | модульные окна, 120 конкурентных запросов, rollback/retry 429, измерение прихода запросов |
| §10 бесконечный диапазон | nullable frontier/end, ленивое создание, явные stop conditions | max uint64, остановка незавершённого прохода; длительный многодневный прогон отдельно |
| §11.1 панель | ACL snapshot/WSS, скорости/ETA/объём/ошибки | Chromium, скриншоты desktop/mobile, измерение свежести |
| §11.2 серверы | метрики ОС, активные загрузки, SSH, инвентарь, пороги | три изолированных агента, browser SSH; ОС в Compose общая |
| §11.3 управление заданиями | create/start/pause/resume/stop/config/retry/history/export/ACL | мягкая пауза, смена run_id, остановка; export 12001 записи |
| §11.4 SSH | pinned host key, OpenBao, WSS ticket, closed helper, подпись пакета, аудит | browser terminal, whitelist, checksum/signature/permissions; реальный OpenSSH Linux проверяется отдельно |
| §12 наблюдаемость | JSON UTC, Prometheus, Fluent Bit, Loki, Grafana, alerts, PostgreSQL/NGINX exporters | локальные targets и browser collect-to-render; Linux node exporters в Ansible |
| §13 отказы | SQL fence/drain, lease recovery, dependency cancellation, публикация, backup | агент/координатор/SQL/OpenBao/marker, ENOSPC tmpfs; физическая сеть/NAS отдельно |
| §14 доступ и секреты | Argon2id, session/CSRF, роли/ACL, mTLS/revoke/rotate, SSRF, paths, NGINX internal | модульные/браузерные проверки; NFS krb5p negative playbook подготовлен |
| §15 журналы | структурированные события SQL, redaction, audit, bounded log delivery | TestNoSecretsInLogs, browser audit ACL, Loki ingestion |
| §16 API | обязательные endpoints и расширения, API-Version/Idempotency-Key | OpenAPI, автоматическое покрытие маршрутов, SQL replay/body hash |
| §17 модель данных | все обязательные таблицы и дополнительные данные надёжности/доступа | constraints/indexes/migrations, интеграция на настоящем PostgreSQL |
| §18 нефункциональные свойства | UTC, responsive UI, bounded pools, dynamic config, restart, docs | 100 фактически параллельных операций на трёх контейнерах; три независимых сервера не подменяются контейнерами |
| §19 состояния | формальный Transition и SQL действия | TestStates, сквозной pause/resume/stop/completed |
| §20 тесты | unit/race, SQL, browser, fault, mock, backup, CI | фактические команды и результаты в отчёте; большой флот/большие файлы/NFS требуют целевого стенда |
| §21 приёмка | автоматические измерения локального профиля и отдельная программа Linux | [фактический протокол](implementation-status.md); полная физическая приёмка не заявлена |
| §22 состав поставки | код, миграции, OpenAPI, Compose, Ansible/systemd, руководства, диаграммы, отчёты | README и ссылки ниже |
| §23 демонстрация | управляемый mock, acceptance drivers, операционные команды | [testing.md](testing.md); инъекции только в отдельном тестовом профиле |
| §24 дополнительные задания | исключены решением владельца | не входят в объём поставки |

Материалы поставки: [архитектура](implementation-profile.md), [контракты](contracts.md), [локальная установка](install-local.md), [Linux](install-linux.md), [оператор](operator.md), [обновление](maintenance.md), [копирование/восстановление](backup-restore.md), [диаграммы](diagrams/README.md), [протокол](implementation-status.md).
