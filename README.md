# Begom

Распределённый поиск видеоматериалов по числовым диапазонам, сбор метаданных и скачивание файлов, разрешённых владельцем. Прикладная часть написана на Go 1.26; интерфейс — Go templates, CSS и JavaScript.

В репозитории находятся координатор, агент, CLI, управляемый mock-донор, служебный SSH-помощник и интерактивный терминал, миграции PostgreSQL, локальный Compose-стенд и профиль Ubuntu 24.04 / Ansible / systemd / NFSv4 с Kerberos. Исходные ТЗ, ЭП и ТП сохранены в `docs/`.

## Запуск системы с настоящим Kinescope

Нужны Go 1.26.7, Docker Engine и Docker Compose. Из корня репозитория:

```sh
bash deploy/local-v2.sh up
```

Откройте **https://localhost:8445**. Учётная запись — `admin`, пароль находится в `.local-v2-kinescope/client-password`, CA — в `.local-v2-kinescope/secrets/ca.crt`. Скрипт создаёт отдельный чистый проект `begom-v2-kinescope` с тремя агентами; существующий профиль и его данные сохраняются. `make up` запускает тот же профиль; управление — `make status`, `make logs`, `make down` (без удаления томов).

1. В «Заданиях» создайте **парсинг**, укажите диапазон и запустите его. Парсинг сохраняет метаданные и разрешённые качества, не скачивая видео.
2. Откройте **«Каталог для скачивания»**, найдите материалы и отметьте нужные видео из одного или нескольких парсингов. Создайте и запустите отдельное задание скачивания.
3. В **«Видеогалерее»** доступны поиск, превью, просмотр и получение опубликованных файлов. Видимость определяется ACL заданий.

Администратору доступен интерактивный SSH-терминал рабочего узла: bash, история, Tab, Ctrl+C, top и nano. Shell работает от отдельного пользователя без sudo. Служебные кнопки управления агентом используют отдельный ограниченный RPC.

CLI выполняет те же две операции:

```sh
bin/begomctl jobs discover --config .local-v2-kinescope/client.json --range '[203613411,203613411]' --workers worker-1,worker-2,worker-3
bin/begomctl jobs start --config .local-v2-kinescope/client.json --id DISCOVERY_JOB_ID
bin/begomctl catalog list --config .local-v2-kinescope/client.json --discovery-job DISCOVERY_JOB_ID
bin/begomctl jobs download --config .local-v2-kinescope/client.json --media-ids MEDIA_ID --workers worker-1,worker-2,worker-3
bin/begomctl jobs start --config .local-v2-kinescope/client.json --id DOWNLOAD_JOB_ID
```

Числовой пример не гарантирует текущего разрешения владельца. Общий исходный предел для Kinescope — **1 RPS**; разрешение и ссылка повторно проверяются перед загрузкой. Парсинг также поддерживает `--from/--to`. API v2 требует чистого профиля и согласованных версий компонентов; старые задания v1 не переносятся автоматически.

## Проверки

```sh
make check
BEGOM_TEST_DATABASE_URL='postgres://postgres@localhost:55439/postgres?sslmode=disable' make integration
make acceptance
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
