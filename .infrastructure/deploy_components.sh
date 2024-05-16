#!/bin/bash

# Переменные для управления запуском контейнеров
needDeployUpdater=false
postgreVersion="15.4-bookworm"

# Развёртывание pgfacade-updater
if [ "${needDeployUpdater}" = true ]; then
    echo "Процесс запуска контейнера pgfacade-updater..."
    docker run --detach --name "pgfacade-updater" -v /var/run/docker.sock:/var/run/pgfacade/docker.sock -p 9090:8080 "pgfacade-updater"
    if [ $? -ne 0 ]; then
        echo "Не получилось запустить контейнер с pgfacade-updater, " \
             "т.к. обнаружен конфликт контейнеров, скорее всего pgfacade-updater уже запущен."
        exit 1
    fi
else
    echo "Запуск контейнера pgfacade-updater не требуется."
fi

# Проверка наличия и загрузка образа PostgreSQL
echo "Загрузка образа PostgreSQL версии ${postgreVersion}..."
docker pull postgres:${postgreVersion}
if [ $? -ne 0 ]; then
    echo "Ошибка: Образ PostgreSQL с версией ${postgreVersion} " \
    "не найден в Docker Hub. Укажите корректную версию образа postgresql."
    exit 1
else
    echo "Образ PostgreSQL с версией ${postgreVersion} успешно загружен или уже был на машине."
fi

# Выполнение запроса для запуска компонентов через pgfacade-updater
echo "Выполнение запроса для запуска компонентов через pgfacade-updater..."

curl -L 'http://localhost:9090/docker/install-new-postgres' \
-H 'Content-Type: application/json' \
-d '{
    "awaitPgFacadeContainerMs": 15000,
    "pgFacadeImageTag": "pgfacade-pgfacade:latest",
    "postgresImageTag": "postgres:15.4-bookworm",
    "postgresImagePort": 5432,
    "newSuperuserCredentials": {
        "superuserName": "postgres",
        "superuserPassword": "postgres",
        "superuserDatabase": "postgres"
    },
    "postgresConfigurationInfo": {
        "pgFacadeUsername": "pgfacade",
        "pgFacadePassword": "pgfacade",
        "pgFacadeDatabase": "pgfacade",
        "createReplicationUser": true,
        "replicationUsername": "replicant",
        "replicationPassword": "replicant"
    },
    "pgFacadeEnvVars": {
        "PG_FACADE_ORCHESTRATION_COMMON_EXTERNAL_LOAD_BALANCER_DEPLOY": "true",
        "PG_FACADE_ARCHIVING_ENABLED" : "false"
    },
    "mountDockerSock": true,
    "dockerSockPathOnHost": "/var/run/docker.sock",
    "networkBetweenPostgresAndPgFacade": {
        "networkName": "pg-facade-postgres-network",
        "create": false
    },
    "internalPgFacadeNetwork": {
        "networkName": "pg-facade-internal-network",
        "create": false
    },
    "externalPgFacadeNetwork": {
        "networkName": "pg-facade-external-network",
        "create": false
    },
    "loadBalancerNetwork": {
        "networkName": "pg-facade-balancer-network",
        "create": false
    },
    "otherNetworksToConnectPgFacadeContainer": [
    ]
}'

if [ $? -ne 0 ]; then
    echo "Ошибка выполнения запроса installForNewPostgres, смотрите логи для информации об ошибке"
    exit 1
fi

# Поиск ID контейнера PostgreSQL
echo "Поиск ID контейнера PostgreSQL..."
postgresContainerId=$(docker ps -aqf "name=pg-facade-managed-postgres.*$")
if [ -z "$postgresContainerId" ]; then
    echo "Контейнер с именем pg-facade-managed-postgres-... не найден."
    exit 1
fi
echo "ID контейнера: ${postgresContainerId}"

# Установка роли pgfacade как суперпользователь
echo "Установка роли pgfacade как суперпользователь..."
docker exec -e PGPASSWORD=postgres ${postgresContainerId} psql -U postgres -d postgres -c "alter role pgfacade superuser;"
if [ $? -ne 0 ]; then
    echo "Ошибка при изменении роли pgfacade на суперпользователя."
    exit 1
fi

echo "Скрипт завершён успешно."
