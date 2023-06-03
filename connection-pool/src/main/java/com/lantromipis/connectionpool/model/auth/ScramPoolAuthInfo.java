package com.lantromipis.connectionpool.model.auth;

import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScramPoolAuthInfo implements PoolAuthInfo {
    private byte[] clientKey;
    private String storedKeyBase64;

    @Override
    public PostgresProtocolAuthenticationMethod getExpectedAuthMethod() {
        return PostgresProtocolAuthenticationMethod.SCRAM_SHA256;
    }
}
