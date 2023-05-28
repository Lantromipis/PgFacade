# Installation Guide
<!-- TOC -->
* [Installation Guide](#installation-guide)
  * [Settings types](#settings-types)
  * [How to get setting name and it's environment variable](#how-to-get-setting-name-and-its-environment-variable)
  * [Important settings](#important-settings)
    * [Raft for PgFacade](#raft-for-pgfacade)
    * [Orchestration](#orchestration)
    * [Archiving](#archiving)
    * [Proxy](#proxy)
    * [REST API](#rest-api)
  * [General installation steps](#general-installation-steps)
  * [How to fill `postgres-nodes-info.json` file](#how-to-fill-postgres-nodes-infojson-file)
    * [When archiving required](#when-archiving-required)
      * [S3 compatible storage](#s3-compatible-storage)
  * [Installation for Docker](#installation-for-docker)
<!-- TOC -->
## Settings types
Some settings have special types with special formats:
1. `duration` Use format like in java.time.Duration.parse(CharSequence text) https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-

## How to get setting name and it's environment variable

To start PgFacade you need to specify some required settings based on how you are going to use PgFacade. Settings are specified via environment variables. 

This guide describes only some settings which are used by PgFacade. To see full list got to `app-pgfacade-root/src/main/resources/application.yaml`. You can also check default settings values here.
To get environment variable for any setting, you need the name of that setting. 
For example, for such setting

```
foo:
    bar:
        baz: some-value
```
You will get setting name `foo.bar.baz`

After you have figured out setting name, convert it to environment variable using these rules:

1. Convert all letters to upper case Example: `foo` -> `FOO`
2. Replace all `.` AND `-` with `_` Example: `foo.bar-baz` -> `FOO_BAR_BAZ`
3. Replace all `"` with `_` Example: `foo."bar".baz-boo` -> `FOO__BAR__BAZ_BOO`

## Important settings

### Raft for PgFacade

| Environment variable       | Type | Values      | Description                                                                                                                                                                                           |
|----------------------------|------|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PG_FACADE_RAFT_NODES_COUNT | int  | 3 or 5 or 7 | Number of PgFacade nodes that will be active all the time. 3 nodes allows `one` PgFacade node failure, 5 nodes allows `two` PgFacade node failures and 7 nodes allows `three` PgFacade node failures. |


### Orchestration

| Environment variable                                          | Type   | Values                | Description                                                                                                                                        |
|---------------------------------------------------------------|--------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| PG_FACADE_ORCHESTRATION_ADAPTER                               | enum   | `docker` `no_adapter` | Which adapter to use for orchestration. `no_adapter` means no orchestration will be done.                                                          |
| PG_FACADE_ORCHESTRATION_DOCKER_HOST                           |        | string                | Docker host to use when `docker` adapter specified. Default: `unix:///var/run/pgfacade/docker.sock`                                                |
| PG_FACADE_ORCHESTRATION_DOCKER_POSTGRES_IMAGE_TAG             | string |                       | Docker Postgres image tag. Must be same as target Postgres version.                                                                                |
| PG_FACADE_ORCHESTRATION_DOCKER_POSTGRES_IMAGE_PG_DATA         | string |                       | Where is PG_DATA located in Postgres image. For example, for official docker images this is `/var/lib/postgresql/data` (and this is default value) |
| PG_FACADE_ORCHESTRATION_DOCKER_POSTGRES_NETWORK_NAME          | string |                       | Docker network which will connect PgFacade and Postgres                                                                                            |
| PG_FACADE_ORCHESTRATION_DOCKER_PGFACADE_INTERNAL_NETWORK_NAME | string |                       | Docker network for PgFacade internal usage (Raft consensus algorithm)                                                                              |
| PG_FACADE_ORCHESTRATION_DOCKER_PGFACADE_EXTERNAL_NETWORK_NAME | string |                       | Docker network which other containers should use to connect to PgFacade Postgres proxy                                                             |


### Archiving

| Environment variable                                      | Type     | Values         | Description                                                                                                                               |
|-----------------------------------------------------------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| PG_FACADE_ARCHIVING_ENABLED                               | boolean  | `true` `false` | If true, enables archiving of WAL and backups. If false, disables. If archiving is disabled, no other setting int this table have effect. |
| PG_FACADE_ARCHIVING_BASEBACKUP_CREATE_INTERVAL            | duration |                | How often to create full backups. Default: 60min                                                                                          |
| PG_FACADE_ARCHIVING_BASEBACKUP_CLEAN_UP_KEEP_OLD_INTERVAL | duration |                | How long full backups should exist before they will be removed. Default: 180 min                                                          |
| PG_FACADE_ARCHIVING_ADAPTER                               | enum     | `s3`           | Specifies which storage system will be used for storing backups and WAL files.                                                            |
| PG_FACADE_ARCHIVING_S3_PROTOCOL                           | enum     | `http` `https` | Protocol which will be used for accessing s3 storage.                                                                                     |
| PG_FACADE_ARCHIVING_S3_ENDPOINT                           | string   |                | URL used for accessing s3 compatible storage. Must include `http://` or `https://` based on protocol.                                     |
| PG_FACADE_ARCHIVING_S3_ACCESS_KEY                         | string   |                | Access key for s3 compatible storage.                                                                                                     |
| PG_FACADE_ARCHIVING_S3_SECRET_KEY                         | string   |                | Secret key for s3 compatible storage.                                                                                                     |
| PG_FACADE_ARCHIVING_S3_BACKUPS_BUCKET                     | string   |                | Name of bucket which will be used for storing backups. Can have same value as PG_FACADE_ARCHIVING_S3_WAL_BUCKET                           |
| PG_FACADE_ARCHIVING_S3_WAL_BUCKET                         | string   |                | Name of bucket which will be used for storing WAL files. Can have same value as PG_FACADE_ARCHIVING_S3_BACKUPS_BUCKET                     |

### Proxy

| Environment variable                                         | Type     | Values | Description                                                                         |
|--------------------------------------------------------------|----------|--------|-------------------------------------------------------------------------------------|
| PG_FACADE_PROXY_PRIMARY_PORT                                 | int      |        | Port used to proxy request to Postgres primary.                                     |
| PG_FACADE_PROXY_INACTIVE_CLIENTS_INACTIVE_CONNECTION_TIMEOUT | duration |        | After which time to disconnect clients that are not sending request. Default: 3 min |

### REST API

| Environment variable                                         | Type     | Values | Description                          |
|--------------------------------------------------------------|----------|--------|--------------------------------------|
| QUARKUS_HTTP_PORT                                            | int      |        | Port used by REST API. Default: 8080 |

## General installation steps

## How to fill `postgres-nodes-info.json` file

`postgres-nodes-info.json` file contains JSON describing current known PostgreSQL instances. You must fill this file for first time, so PgFacade will know what PostgreSQL it needs to manage. Example of file:
```
{
  "41a2f8bb-3d8b-49ed-a2be-cef3f69df0d7": 
  {
    "adapterIdentifier": "11031269250eaf2f6c1e17bbf467b5f35ba99a6fc95fff4f4b3c25d60f885db0",
    "instanceId": "41a2f8bb-3d8b-49ed-a2be-cef3f69df0d7",
    "primary": true
  }
}
```

Replace key `41a2f8bb-3d8b-49ed-a2be-cef3f69df0d7` value and field `instanceId` value with random UUID v4. Replace field `adapterIdentifier` value with Docker container ID.

### When archiving required

#### S3 compatible storage
1. Create bucket for backups and WAL files. You can also use one bucket for both. 
2. Specify bucket names using environment variables.
3. Specify S3 storage connection parameters (access key and secret key)

## Installation for Docker
1. Select Dockerfile or image to use. For that, check Postgres version and select Dockerfile for that Postgres version. For example, for Postgres 15, use file `Dockerfile.jvm-postgres-15`
2. Create network for postgres and PgFacade. Default network name: `pg-facade-postgres-network`. If you want to create network with another name, then additionally set environment variable `PG_FACADE_ORCHESTRATION_DOCKER_POSTGRES_NETWORK_NAME`
3. Create network for PgFacade internal use. Default network name: `pg-facade-internal-network`. If you want to create network with another name, then additionally set environment variable `PG_FACADE_ORCHESTRATION_DOCKER_PGFACADE_INTERNAL_NETWORK_NAME`
4. Create network for PgFacade external usa (this network should be used by containers, which need Postgres). Default network name: `pg-facade-external-network`. If you want to create network with another name, then additionally set environment variable `PG_FACADE_ORCHESTRATION_DOCKER_PGFACADE_EXTERNAL_NETWORK_NAME`
5. Create docker volume with file `postgres-nodes-info.json` and fill it with data. Instructions on how to fill this file are above.
6. Create PgFacade container. Mount volume you created on step 5. Mount Docker socket if you want PgFacade to access Docker API using it. Example `/var/run/docker.sock:/var/run/pgfacade/docker.sock`
7. Connect PgFacade container to network from point 2 (postgres network)
8. Connect PgFacade container to network from point 3 (internal network)
9. Connect PgFacade container to network from point 4 (external network)
10. If you are planning to use archiving with S3, then connect PgFacade container to network with S3 storage
11. Start PgFacade container
12. Wait for PgFacade to scale up Postgres nodes and PgFacade nodes.
