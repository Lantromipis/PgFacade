package com.lantromipis.configuration.properties.constant;

import java.time.Instant;
import java.util.UUID;

public class PgFacadeConstants {

    public static final String RAFT_SERVER_UP_READINESS_CHECK = "Raft readiness check";

    public static final UUID PG_FACADE_RAFT_GROUP_ID = UUID.fromString("a0f1f8e8-6635-4b1e-85d5-5596f50dd57e");

    public static final String PG_FACADE_RAFT_DIR = "raft";

    public static final String PG_FACADE_PERSISTED_PROPERTIES_DIR = "stored";

    public static final String POSTGRES_WAL_STREAM_DIRECTORY_NAME = "wal-stream";

    public static final String POSTGRES_WAL_UPLOAD_DIRECTORY_NAME = "wal-upload";

    public static final String POSTGRES_NODE_INFO_FILE_NAME = "postgres-nodes-info.json";

    public static final String POSTGRES_SETTINGS_INFO_FILE_NAME = "postgres-settings-info.json";
    public static final String POSTGRES_ARCHIVE_INFO_FILE_NAME = "postgres-archive-info.json";

    public static final int DOCKER_SPECIFIC_PGFACADE_RAFT_PORT = 31000;

    public static final Instant DEFAULT_INSTANT_TO_COUNT_FROM = Instant.parse("2023-03-21T22:23:13.572586Z");

    private PgFacadeConstants() {
    }
}
