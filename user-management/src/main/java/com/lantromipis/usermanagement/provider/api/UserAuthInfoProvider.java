package com.lantromipis.usermanagement.provider.api;

import com.lantromipis.postgresprotocol.model.PostgresProtocolAuthenticationMethod;

public interface UserAuthInfoProvider {

    void initialize();

    PostgresProtocolAuthenticationMethod getAuthMethodForUser(String username);

    String getPasswdForUser(String username);
}
