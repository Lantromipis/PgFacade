package com.lantromipis.connectionpool.model;

import com.lantromipis.connectionpool.model.common.AuthAdditionalInfo;
import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
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
    public PostgresProtocolAuthenticationMethod getExpectedAuthMethod() {
        return PostgresProtocolAuthenticationMethod.SCRAM_SHA256;
    }
}
