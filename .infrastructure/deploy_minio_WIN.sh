#!/bin/bash

# Запуск контейнера Minio
dockerOutput=$(docker run -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER=s3user -e MINIO_ROOT_PASSWORD=s3password --name pgfacade-minio-test --net-alias minio -d minio/minio:latest server --console-address ":9001" /data)

# Получение ID контейнера Docker
PgFacadeMinioContainerId=$(echo $dockerOutput | awk '{print $NF}')
echo "ID контейнера Docker: ${PgFacadeMinioContainerId}"

# Установка mc
curl -sSLo mc.exe https://dl.min.io/client/mc/release/windows-amd64/mc.exe

# Конфигурация mc для подключения к MinIO
./mc.exe alias set minio http://localhost:9000 s3user s3password

# Создание бакета
./mc.exe mb minio/pgfacade

# Создание доступного ключа
./mc.exe admin user svcacct add minio s3user --access-key "Rre6lc6yiubAgi9H" --secret-key "EEmKeAC4ocIX2qOp2cvxNO3bnOsRN121"

# Проверка наличия сети Docker pgfacade_minio-network
echo "Идёт проверка наличия pgfacade_minio-network в среде Docker"
docker network inspect pgfacade_minio-network > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "Сеть pgfacade_minio-network уже существует"
else
    docker network create pgfacade_minio-network
    echo "Сеть pgfacade_minio-network была создана"
fi

# Подключение контейнера MinIO к созданной сети
docker network connect --alias minio pgfacade_minio-network ${PgFacadeMinioContainerId}
echo "Контейнер minio был подключен к сети pgfacade_minio-network. Alias контейнера -> minio."