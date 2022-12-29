package com.lantromipis.provider.api;

import com.lantromipis.model.AuthenticationMethod;

public interface UserAuthInfoProvider {

    void initialize();

    AuthenticationMethod getAuthMethodForUser(String username);

    String getPasswdForUser(String username);
}
