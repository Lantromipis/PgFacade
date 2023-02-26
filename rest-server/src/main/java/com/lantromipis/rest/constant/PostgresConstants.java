package com.lantromipis.rest.constant;

import com.lantromipis.configuration.properties.constant.PostgresqlConfConstants;
import com.lantromipis.rest.model.postgres.PostgresSettingContextDescription;

import java.util.List;

public class PostgresConstants {

    public static final List<String> IMPORTANT_NOTES = List.of(
            "Note: fill settings values as if you were manually modifying postgresql.conf file (with units)",
            "Note: if restart is required, PgFacade will automatically apply setting to all Postgres instances and restart them. However, during that operation PgFacade executes switchover, so Postgres will be temporary unavailable.",
            "Note: PgFacade reserves " + PostgresqlConfConstants.PG_FACADE_RESERVED_CONNECTIONS_COUNT + " connections to Postgres for internal needs, so these connections will not be available to end users. Keep that in mind when setting max_connections.",
            "Memory units: B  = bytes, kB = kilobytes, MB = megabytes, GB = gigabytes, TB = terabytes",
            "Time units: us = microseconds, ms = milliseconds, s = seconds, min = minutes, h = hours, d = days"
    );


    public static final List<PostgresSettingContextDescription> SETTING_CONTEXT_DESCRIPTIONS = List.of(
            PostgresSettingContextDescription
                    .builder()
                    .contextName("internal")
                    .modifiable(false)
                    .build(),
            PostgresSettingContextDescription
                    .builder()
                    .contextName("postmaster")
                    .modifiable(true)
                    .restartRequired(true)
                    .build(),
            PostgresSettingContextDescription
                    .builder()
                    .contextName("sighup")
                    .modifiable(true)
                    .restartRequired(false)
                    .build(),
            PostgresSettingContextDescription
                    .builder()
                    .contextName("superuser-backend")
                    .modifiable(true)
                    .restartRequired(false)
                    .build(),
            PostgresSettingContextDescription
                    .builder()
                    .contextName("backend")
                    .modifiable(true)
                    .restartRequired(false)
                    .build(),
            PostgresSettingContextDescription
                    .builder()
                    .contextName("superuser")
                    .modifiable(true)
                    .restartRequired(false)
                    .build(),
            PostgresSettingContextDescription
                    .builder()
                    .contextName("user")
                    .modifiable(true)
                    .restartRequired(false)
                    .build()
    );

    private PostgresConstants() {
    }
}
