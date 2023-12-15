package com.lantromipis.postgresprotocol.model.internal.auth;

import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;

public interface PgAuthInfo {
    PostgresProtocolAuthenticationMethod getExpectedAuthMethod();
}
