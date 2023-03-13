package com.lantromipis.connectionpool.model.common;

import com.lantromipis.postgresprotocol.model.PostgresProtocolAuthenticationMethod;

public interface AuthAdditionalInfo {
    PostgresProtocolAuthenticationMethod getExpectedAuthMethod();
}
