package com.lantromipis.rest.constant;

import com.lantromipis.rest.model.api.postgres.PostgresSettingContextDescription;

import java.util.List;

public class PostgresConstants {

    public static final List<String> IMPORTANT_NOTES = List.of(
            "Note: fill settings values as if you were manually modifying postgresql.conf file (with units)",
            "Note: if restart is required, PgFacade will automatically apply setting to all Postgres instances and restart them. However, during that operation PgFacade executes switchover, so Postgres will be temporary unavailable.",
            "Note: it is highly recommended to apply setting that require restart with at least 2 active standbys. PgFacade will restart every node after each other, so, if you have only 1 standby there is a possibility that it will be impossible to failover and cluster can be lost.",
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
