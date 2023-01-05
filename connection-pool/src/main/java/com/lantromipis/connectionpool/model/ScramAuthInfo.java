package com.lantromipis.connectionpool.model;

import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.postgresprotocol.model.AuthenticationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScramAuthInfo implements AuthAdditionalInfo {
    private byte[] clientKey;
    private String storedKeyBase64;

    @Override
    public AuthenticationMethod getExpectedAuthMethod() {
        return AuthenticationMethod.SCRAM_SHA256;
    }
}
