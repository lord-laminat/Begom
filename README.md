# Begom

Распределённый поиск видеоматериалов по числовым диапазонам, сбор метаданных и скачивание файлов, разрешённых владельцем. Прикладная часть написана на Go 1.26; интерфейс — Go templates, CSS и JavaScript.

В репозитории находятся координатор, агент, CLI, управляемый mock-донор, ограниченный SSH-помощник, миграции PostgreSQL, локальный Compose-стенд и профиль Ubuntu 24.04 / Ansible / systemd / NFSv4 с Kerberos. Исходные ТЗ, ЭП и ТП сохранены в `docs/`.

## Быстрый запуск mock-стенда

Нужны Go 1.26.7, Docker Engine и Docker Compose v2. Все команды выполняются из корня репозитория.

```sh
make build
bin/begomctl init --dir .local
docker compose --progress plain up -d --build
```

Откройте **https://localhost:8443**. Пользователь — `admin`, первоначальный пароль — в `.local/client-password`. Приватный CA лежит в `.local/secrets/ca.crt`; добавьте именно этот сертификат в доверенные для браузера. Инициализация не перезаписывает существующие идентичности. `.local*`, бинарники и рабочие артефакты исключены из Git и контекста Docker.

Локальный профиль использует три изолированных агента по 40 потоков, общий предел 80 RPS и 120 соединений. Общий локальный том — профиль разработки. Настоящие NFS, Kerberos и разные ОС проверяются на отдельном Linux-стенде.

```sh
bin/begomctl jobs create --config .local/client.json --range '[0,99]' --workers worker-1,worker-2,worker-3 --threads 40 --block-size 40
bin/begomctl jobs start --config .local/client.json --id JOB_ID
bin/begomctl jobs show --config .local/client.json --id JOB_ID
```

Также поддерживаются `--from` и `--to`. Для задания можно передать полный JSON/YAML через `--file`. Mock по умолчанию демонстрирует отсутствующие, запрещённые и ограниченные материалы, обрывы и отсутствие разрешения скачивания.

## Проверки

```sh
make check
BEGOM_TEST_DATABASE_URL='postgres://postgres@localhost:55439/postgres?sslmode=disable' make integration
bin/begomctl acceptance --config .local/client.json --output artifacts/acceptance-local
```

Интеграционные тесты требуют **одноразовый PostgreSQL 18**; создают и удаляют только собственные схемы `begom_test_*`. Приёмка изменяет настройки mock-донора, останавливает тестовый агент и перезапускает координатор. Запускайте её на отдельном mock-стенде без других активных заданий. Браузерные и дополнительные отказные проверки описаны в [программе испытаний](docs/testing.md).

Протоколы фактических прогонов, версии и границы проверки приведены в [отчёте реализации](docs/implementation-status.md). Они различают локальные контейнеры и многосерверный стенд; наличие конфигурации Ansible не означает прохождение испытаний на физических узлах.

## Документация

- [Профиль реализации и архитектура](docs/implementation-profile.md), [ADR](docs/adr/0001-go-system.md), [редактируемые диаграммы](docs/diagrams/README.md).
- [Локальная установка и Kinescope](docs/install-local.md), [Ubuntu / Ansible / NFS](docs/install-linux.md).
- [Руководство оператора](docs/operator.md), [обновление и сертификаты](docs/maintenance.md).
- [Резервное копирование и восстановление](docs/backup-restore.md).
- [Контракты, состояния и совместимость](docs/contracts.md), [OpenAPI 3.1](docs/openapi.json).
- [Тестирование](docs/testing.md), [матрица соответствия ТЗ](docs/conformance.md).

Для Kinescope применяется отдельный профиль с исходным общим ограничением **1 RPS**. Это настройка Begom, а не объявленная квота Kinescope. Скачать можно только видео, предоставленное публичным `downloadLink`; отсутствие разрешения сохраняется как `skipped`. Потоковые ссылки, PDF и транскрипты не превращаются в видеофайлы.
