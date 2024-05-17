#!/bin/bash

# Deploying MinIO container
dockerOutput=$(docker run -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER=s3user -e MINIO_ROOT_PASSWORD=s3password --name pgfacade-minio-test --net-alias minio -d minio/minio:latest server --console-address ":9001" /data)
PgFacadeMinioContainerId=$(echo $dockerOutput | awk '{print $NF}')
echo "ID container Docker: ${PgFacadeMinioContainerId}"

# Downloaded mc (minio client) for configuring minio
curl -sSLo mc.exe https://dl.min.io/client/mc/release/windows-amd64/mc.exe
# Configuring access to MinIO
./mc.exe alias set minio http://localhost:9000 s3user s3password
# Creating bucket pgfacade
./mc.exe mb minio/pgfacade
# Creating access key
./mc.exe admin user svcacct add minio s3user --access-key "Rre6lc6yiubAgi9H" --secret-key "EEmKeAC4ocIX2qOp2cvxNO3bnOsRN121"

# Checking for docker network pgfacade_minio-network
echo "Checking docker network pgfacade_minio-network"
docker network inspect pgfacade_minio-network > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "Network pgfacade_minio-network has founded. Connecting with minio..."
else
    docker network create pgfacade_minio-network
    echo "Network pgfacade_minio-network has created. Connection with minio..."
fi

# Connection minio to docker network
docker network connect --alias minio pgfacade_minio-network ${PgFacadeMinioContainerId}
echo "minio container connected to docker network pgfacade_minio-network. Alias of container -> minio."