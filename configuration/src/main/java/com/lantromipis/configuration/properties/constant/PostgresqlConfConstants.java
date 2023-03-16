package com.lantromipis.configuration.properties.constant;

/**
 * Constant setting of PgFacade. Can be changed only be rebuilding.
 */
public class PostgresqlConfConstants {

    public static final String PG_FACADE_POSTGRESQL_CONF_FILE_NAME = "postgresql.pgfacade.conf";
    /**
     * How many connections PgFacade reserves for internal use.
     */
    public static final int PG_FACADE_RESERVED_CONNECTIONS_COUNT = 98;

    private PostgresqlConfConstants() {
    }
}
