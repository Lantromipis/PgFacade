package com.lantromipis.postgresprotocol.model.internal.auth;

import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScramPgAuthInfo implements PgAuthInfo {
    private boolean passwordKnown;

    // when password is known
    private String password;

    // when password is NOT known
    private byte[] clientKey;
    private byte[] storedKey;

    @Override
    public PostgresProtocolAuthenticationMethod getExpectedAuthMethod() {
        return PostgresProtocolAuthenticationMethod.SCRAM_SHA256;
    }
}
