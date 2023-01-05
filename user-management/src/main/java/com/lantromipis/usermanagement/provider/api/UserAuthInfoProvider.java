package com.lantromipis.usermanagement.provider.api;

import com.lantromipis.postgresprotocol.model.AuthenticationMethod;

public interface UserAuthInfoProvider {

    void initialize();

    AuthenticationMethod getAuthMethodForUser(String username);

    String getPasswdForUser(String username);
}
