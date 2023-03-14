package com.lantromipis.usermanagement.provider.api;

import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;

public interface UserAuthInfoProvider {

    void initialize();

    PostgresProtocolAuthenticationMethod getAuthMethodForUser(String username);

    String getPasswdForUser(String username);
}
