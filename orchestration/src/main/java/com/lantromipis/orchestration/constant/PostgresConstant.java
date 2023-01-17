package com.lantromipis.orchestration.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

public class PostgresConstant {

    public static final String PG_HBA_CONF_START_LINE = "# TYPE  DATABASE        USER            ADDRESS                 METHOD";

    public static final String PG_HBA_CONF_REPLICATION_DB = "replication";

    public static final String PG_HBA_CONF_ALL = "all";

    public static final String PG_DATA_ENV_VAR = "PGDATA";

    //settings
    public static final String PRIMARY_CONN_INFO_SETTING = "primary_conninfo";

    // https://www.postgresql.org/docs/current/auth-pg-hba-conf.html
    @RequiredArgsConstructor
    public enum PgHbaConfHost {
        HOST("host"),
        LOCAL("local"),
        HOST_SSL("hostssl"),
        HOST_NO_SSL("hostnossl"),
        HOST_GSS_ENC("hostgssenc"),
        HOST_NO_GSS_ENC("hostnogssenc");

        @Getter
        private final String value;
    }

    // https://www.postgresql.org/docs/current/auth-pg-hba-conf.html
    @RequiredArgsConstructor
    public enum PgHbaConfAuthMethod {
        TRUST("trust"),
        REJECT("reject"),
        SCRAM_SHA_256("scram-sha-256"),
        MD5("md5"),
        PASSWORD("password"),
        GSS("gss"),
        SSPI("sspi"),
        IDENT("ident"),
        PEER("peer"),
        LDAP("ldap"),
        RADIUS("radius"),
        CERT("cert"),
        PAM("pam"),
        VSD("bsd");

        @Getter
        private final String value;
    }

    private PostgresConstant() {
    }
}
