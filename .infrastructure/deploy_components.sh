#!/bin/bash

# Variables for start_deploying. You can change it locally for change configuration.
needDeployUpdater=true
postgreVersion="15.4-bookworm"

# Deploying pgfacade-updater if it needed.
if [ "${needDeployUpdater}" = true ]; then
    echo "Starting container with pgfacade-updater..."
    docker run --detach --name "pgfacade-updater" -v /var/run/docker.sock:/var/run/pgfacade/docker.sock -p 9090:8080 "pgfacade-updater"
    if [ $? -ne 0 ]; then
        echo "Error with starting pgfacade-updater, " \
             "detected conflicts with docker, maybe pgfacade-updater is already running?"
        exit 1
    fi
fi

# Checking for needed version of image of PostgreSQL
echo "Downloading PostgreSQL of version -> ${postgreVersion}..."
docker pull postgres:${postgreVersion}
if [ $? -ne 0 ]; then
    echo "Error: image PostgreSQL ${postgreVersion} " \
    "not found in the Docker Hub. Please check version."
    exit 1
else
    echo "Image PostgreSQL ${postgreVersion} downloaded successfully or was already at the machine."
fi

# Calling Updater for deploying app
echo "Call Updater for deploying app..."

curl -L 'http://localhost:9090/docker/install-new-postgres' \
-H 'Content-Type: application/json' \
-d '{
    "awaitPgFacadeContainerMs": 15000,
    "pgFacadeImageTag": "pgfacade-pgfacade:latest",
    "postgresImageTag": "postgres:'"${postgreVersion}"'",
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
    echo "Error with curl installForNewPostgres, watch logs for more..."
    exit 1
fi

# Trying to find master-node container_ID of PostgreSQL
echo "Watching for container id of master-node PostgreSQL..."
postgresContainerId=$(docker ps -aqf "name=pg-facade-managed-postgres.*$")
if [ -z "$postgresContainerId" ]; then
    echo "Error: container pg-facade-managed-postgres-... not found."
    exit 1
fi
echo "pg-facade-managed-postgres container ID: ${postgresContainerId}"

# Grant role pgfacade to superuser
echo "Grant role pgfacade to superuser..."
docker exec -e PGPASSWORD=postgres ${postgresContainerId} psql -U postgres -d postgres -c "alter role pgfacade superuser;"
if [ $? -ne 0 ]; then
    echo "Error: can't grant user pgfacade to superuser on postgres-master-node. See logs for more..."
    exit 1
fi

echo "Deploy finished successfully."
