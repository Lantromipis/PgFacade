package com.lantromipis.connectionpool.model.common;

import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;

public interface AuthAdditionalInfo {
    PostgresProtocolAuthenticationMethod getExpectedAuthMethod();
}
