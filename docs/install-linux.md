# Установка на Ubuntu 24.04

## Топология и подготовка

Эталонный профиль использует один контрольный узел с координатором, SQL, OpenBao, NGINX и наблюдаемостью, отдельный NAS/KDC, три рабочих сервера и независимый узел резервного копирования. Требуются Ubuntu 24.04 LTS amd64, корректные прямые DNS-имена, доступ Ansible по проверенным SSH host keys и подготовленные файловые системы NAS и резервного узла. Ansible не форматирует устройства и не заменяет корневую файловую систему каталогом.

Перед установкой проверьте DNS и время, доступность служебной сети, место под NAS и restic, отсутствие конфликтов UID 10000–10003, 10020, GID 10000 и 10010. Профиль предназначен для выделенных узлов; он настраивает службы, пользователей, NGINX, NFS и отдельную таблицу сетевых правил. Существующие правила SSH и чужие таблицы nftables не сбрасываются.

Примеры не содержат действующих адресов и секретов. Заполните собственные файлы вне Git:

```sh
make build
cp deploy/linux-profile.example.json /secure/begom-linux.json
bin/begomctl linux-init --file /secure/begom-linux.json --dir /secure/begom-bundle
cp deploy/ansible/inventory.example.yml /secure/begom-inventory.yml
```

DNS-имена контрольного узла в Linux-профиле должны совпадать с inventory. Три `worker_hosts` соответствуют `worker-1..3`. Параметры исходной службы — `kinescope` либо явный `mock_url` и `mock_cidrs` для приёмочного Linux-стенда. Автоматическая нагрузочная программа отказывается работать с реальным донором.

Соберите и сохраните образ копирования для передачи независимому узлу:

```sh
docker compose --profile backup build backup
docker image save begom-backup:1.0.0 -o /secure/begom-bundle/backup-image.tar
python3 -m venv /tmp/begom-ansible
/tmp/begom-ansible/bin/pip install -r deploy/ansible/requirements.txt
```

Укажите абсолютные `begom_bundle`, `begom_binaries`, NAS mount и backup mount в inventory. Закреплённые Go-бинарники должны соответствовать архитектуре узлов. Сначала проверьте конфигурацию:

```sh
/tmp/begom-ansible/bin/ansible-playbook --syntax-check -i /secure/begom-inventory.yml deploy/ansible/site.yml
/tmp/begom-ansible/bin/ansible-playbook -i /secure/begom-inventory.yml deploy/ansible/site.yml
```

Первый запуск создаёт Kerberos realm, выдаёт host/user keytabs, экспортирует NFS с `sec=krb5p`, монтирует его на клиентах, запускает инфраструктуру, заполняет OpenBao по проверенному TLS и устанавливает systemd-службы. Числовой UID/GID одинаков на NAS и клиентах. `root_squash` сохраняется. Keytabs и bootstrap-материал передаются с закрытыми правами и не выводятся задачами Ansible.

## Идентичности и права

| Область | Владелец / доступ |
|---|---|
| `staging/worker-N` | соответствующий агент; группа издателя; другим агентам запрещено |
| `publishing` | только издатель |
| `published` | запись издателю, чтение веб-шлюзу и копированию |
| `quarantine` | только издатель |
| `.begom-volume` | чтение сервисам, изменение владельцу NAS |

Агенты не входят в группу чтения опубликованных файлов и запускаются без root. Веб-шлюз не входит в группу записи временных областей. NFS-служебные учётные записи используют отдельные Kerberos principals; таймер обновляет билеты до истечения. Каждый UID имеет закрытый каталог кэша в `/run/begom-ccaches/UID`; rpc.gssd ищет его через `%U`, как описано в [руководстве nfs-common](https://manpages.debian.org/unstable/nfs-common/gssd.8.en.html). При утрате mount служба связана с `srv-begom.mount`, а приложение проверяет тип ФС и идентичность тома.

SSH-пользователь `begom-admin` имеет `ForceCommand`, запрет forwarding и TTY, проверенный host key и ограниченный sudo только на фиксированный помощник. Подписанный пакет содержит один файл `agent`. Административная Grafana доступна через NGINX после проверки роли.

## Проверка установленного стенда

```sh
/tmp/begom-ansible/bin/ansible-playbook -i /secure/begom-inventory.yml deploy/ansible/acceptance.yml
bin/begomctl acceptance --config /secure/begom-bundle/client.json --driver ansible --inventory /secure/begom-inventory.yml --output artifacts/acceptance-linux
```

Вторая команда допустима только для **mock-профиля**. `operations.yml` завершает выбранный тестовый агент либо координатор и затем запускает службу. Дополнительно требуется физическое испытание потери NFS, сети, диска и независимого восстановления. Записывайте версии ОС, NFS mount options, DNS, skew часов, CPU/RAM, пропускную способность и все измерения. Прохождение Compose не заменяет этот протокол.

После первичной настройки распределите доли OpenBao между хранителями, сохраните KDC и ключ подписи пакетов в отдельной защищённой копии. Удаление root token из оперативной области выполняется после проверки собственного процесса восстановления; root token не используется прикладными процессами. Если он удалён, unseal выполняют хранители, а AppRole-клиенты продолжают работу самостоятельно.

Профиль установки не запускается из CI на вымышленные адреса. В этой поставке выполнение на независимых Ubuntu/NFS/Kerberos узлах фиксируется отдельно от проверки синтаксиса.
