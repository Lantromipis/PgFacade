## Creating MinIO and deploy it

Creating MinIO container and connect it to docker network. 
Also create bucket in the minio and access key.

Script for Linux:

```shell
 ./deploy_minio.sh
```

Script for Windows:

```shell
 ./deploy_minio_WIN.sh
```

## Creating images for components

Packaging components with Maven and build docker images on it.

```shell
 ./build_updater.sh
 ./build_balancer.sh
 ./build_pgfacade.sh
```

## Deploying docker containers

Deploying Updater and call it to deploy PgFacade, Balancer.
Then grant to superuser pgfacade user in postgres-node

```shell
    ./deploy_components.sh
```

if you'll get error about access denied during **docker run pgfacade-updater...** try to execute 
this command in your terminal