package com.lantromipis.connectionpool.model.auth;

import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;

public interface PoolAuthInfo {
    PostgresProtocolAuthenticationMethod getExpectedAuthMethod();
}
