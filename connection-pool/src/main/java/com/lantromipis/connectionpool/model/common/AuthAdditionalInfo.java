package com.lantromipis.connectionpool.model.common;

import com.lantromipis.postgresprotocol.model.AuthenticationMethod;

public interface AuthAdditionalInfo {
    AuthenticationMethod getExpectedAuthMethod();
}
