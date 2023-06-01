package com.lantromipis.usermanagement.provider.api;

import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import com.lantromipis.usermanagement.model.UserAuthInfo;

public interface UserAuthInfoProvider {

    void initialize();

    PostgresProtocolAuthenticationMethod getAuthMethodForUser(String username);

    String getPasswdForUser(String username);

    UserAuthInfo getUserAuthInfo(String username);
}
