# Программа проверки

Результат команды и профиль записываются отдельно. Доступность исходников теста не означает, что сценарий выполнялся. Фактические измерения этой поставки находятся в [отчёте](implementation-status.md) и `docs/reports/`. Реальный донор не используется для нагрузочных и отказных испытаний.

## Go и SQL

```sh
make check
go run ./tools/openapi
git diff --exit-code -- docs/openapi.json
docker compose config --quiet
```

`make check` включает форматирование, `go vet`, `go test -race ./...` и пять бинарников. Если среда запрещает запись в домашний кэш, Makefile использует `/tmp/begom-go-build` и `/tmp/begom-go-mod`. Для отдельных Go-команд задайте те же переменные. Go-тесты HTTP требуют разрешённых loopback-сокетов.

Для интеграции предоставьте **одноразовый PostgreSQL 18**. Пример отдельного контейнера без постоянного тома:

```sh
docker run --rm -d --name begom-test-sql -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:55439:5432 --tmpfs /var/lib/postgresql postgres:18.6-bookworm
BEGOM_TEST_DATABASE_URL='postgres://postgres@127.0.0.1:55439/postgres?sslmode=disable' make integration
docker stop begom-test-sql
```

`trust/sslmode=disable` применимы только к этому изолированному одноразовому тесту. Рабочий Compose использует SCRAM и проверенный TLS. Тесты создают случайные схемы `begom_test_*` и удаляют только собственную схему. Они не должны получать DSN production.

| Область | Проверяемое поведение |
|---|---|
| model | все формы диапазона, ошибки/overflow, max uint64, границы блоков, переходы состояний, точность JSON |
| donor/target | JSON без выполнения JS, неизвестная схема, downloadLink, качество, PDF/транскрипты/потоки, повторы, SSRF/DNS/IP |
| agent | отдельный permit для redirect, Range/If-Range, смена ETag, игнорирование Range, 416, повреждённый префикс |
| storage/publication | marker/mount, traversal/symlink, durable metadata, копия inode издателя, аварии этапов публикации, каноническая дедупликация одновременных загрузок и восстановление удаления лишней копии, отзыв повреждённого каталога |
| SQL | миграции, 60 конкурентных выдач, одна аренда, устаревшие попытки, дубли/body hash, пропуски, границы десятичных разрядов, общий резерв места |
| C6 | очередь 120 транзакционных запросов без вложенного SQL deadlock, чередование классов/агентов, окна квоты, сохранение 429/Retry-After после отката и восстановления |
| access/SSH | Argon2id, безопасный CSV, отсутствие секретов в логах, ограниченные команды/вывод, подпись архива, отказ повторного использования повреждённой версии |
| export/API | потоковый экспорт 12001 строки, OpenAPI охватывает маршруты и ссылки схем |

Отдельный ENOSPC-тест пропускается обычным Go-прогоном: он требует выделенной маленькой файловой системы и не заполняет обычный диск хоста.

## Локальная измеряемая приёмка

Запустите новый mock-стенд по [инструкции](install-local.md), без других работающих заданий. Исходные настройки: 3 × 40 потоков, 120 соединений, 80 RPS, ответы страниц от четырёх секунд, H=2 с, L=10 с, sweep=1 с.

```sh
bin/begomctl acceptance --config .local/client.json --output artifacts/acceptance-local
```

Драйвер проверяет mock-only, три здоровых узла, пик **фактически открытых запросов**, скользящее окно **поступлений** за секунду, 240 конечных результатов, SHA-256 двадцати файлов через NGINX, Range, повторный проход, отказ агента и перевыдачу в пределах 14 с, перезапуск координатора, мягкую паузу, новую run_id и немедленную остановку. Отчёты JSON/Markdown строятся из измерений. Протокол отмечает отдельно браузерную задержку и физическую NFS-проверку. `100 параллельных` не интерпретируется как `100 RPS`.

Браузерный набор:

```sh
python3 -m venv /tmp/begom-browser
/tmp/begom-browser/bin/pip install -r tests/browser/requirements.txt
/tmp/begom-browser/bin/python -m playwright install --with-deps chromium
/tmp/begom-browser/bin/python tests/browser/run.py --config .local/client.json --output artifacts/browser
```

Проверяются фактическое измерение → DOM (150 наблюдений), WSS reconnect с HTTP перечитыванием, ширина 390 px, SSH, максимальные числовые ID, контролируемое XSS-название, CSRF, роли, ACL и закрытые URI. Скриншоты и версия Chromium сохраняются. Браузер допускает локальный приватный CA тестового стенда; Go-клиенты проверяют CA и SAN. Отказные сценарии выполняются после завершения браузерного набора:

```sh
python3 tests/faults/run.py --config .local/client.json --output artifacts/faults
docker compose --profile verification build storage-faults
docker compose --profile verification run --rm --no-deps storage-faults
docker compose --profile backup build backup
python3 tests/backup/run.py --state .local --output artifacts/backup
```

Отказной драйвер временно останавливает только SQL/OpenBao выбранного Compose-проекта, скрывает marker сгенерированного тома, проверяет прекращение новых запросов и восстанавливает зависимости. Кэш OpenBao проверяется после 65 секунд недоступности. Все остановленные службы и marker восстанавливаются в finally. Это не физический unmount NFS. ENOSPC достигается на tmpfs 4 МиБ в контейнере без внешней сети и capabilities. Восстановление копии использует новый каталог/БД и сохраняет их для проверки.

В CI эти наборы выполняются на GitHub runner; протоколы загружаются даже при ошибке, секреты `.local*` не входят в artifacts. Сам факт наличия workflow не является результатом запуска GitHub Actions.

## Многосерверный стенд и дополнительные измерения

```sh
ansible-playbook --syntax-check -i deploy/ansible/inventory.example.yml deploy/ansible/site.yml
ansible-playbook -i /secure/begom-inventory.yml deploy/ansible/acceptance.yml
bin/begomctl acceptance --config /secure/begom-bundle/client.json --driver ansible --inventory /secure/begom-inventory.yml --output artifacts/acceptance-linux
```

Linux-драйвер допустим только с явно заданным mock. Отрицательные NFS-испытания выполняют реальные чтения/записи под UID каждого агента, издателя и веб-шлюза. Убедитесь, что отсутствуют чужие стендовые задания. Добавьте испытания потери NFS-сети, отказа диска/NAS/KDC, истечения Kerberos ticket, отдельного восстановления на backup-узле и проверки host key при подмене. Сохраняйте mount options, UID/GID, principals, версии ОС/пакетов, часы и результаты запрещённых операций.

Нагрузочная программа первой поставки измеряет 120 потоков на трёх узлах с небольшими синтетическими видео. Отдельно выполняются длительный проход большого диапазона, десятки агентов, несколько одновременных больших файлов и измерение NAS/сети на целевом оборудовании. Они не выводятся из теста небольших файлов. Размер mock-файла настраивается до 128 МиБ; для многогигабайтных испытаний используйте принадлежащий владельцу донор с корректными Range/ETag и тем же контрактом.

Для демонстрации §23: покажите конечное задание, lease/checkpoint и файлы, завершите тестовый агент, дождитесь перевыдачи, верните агент, измените потоки/серверы в настройках, включите контролируемый 429, покажите адаптацию C6 и Retry-After, выполните ENOSPC-профиль, перезапустите координатор и сравните подтверждённые file ID/SHA-256. Метрики, журнал и аудит доступны в приложении и административной Grafana.
