package com.lantromipis.postgresprotocol.model;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolScramConstants;
import lombok.Getter;

public enum PostgresProtocolAuthenticationMethod {
    PLAIN_TEXT("", 0),
    MD5("", 0),
    SCRAM_SHA256(PostgresProtocolScramConstants.SASL_SHA_256_AUTH_MECHANISM_NAME, PostgresProtocolGeneralConstants.SASL_AUTH_INT_MARKER);

    @Getter
    private final String protocolMethodName;
    @Getter
    private final int protocolMethodMarker;

    PostgresProtocolAuthenticationMethod(String protocolMethodName, int protocolMethodMarker) {
        this.protocolMethodName = protocolMethodName;
        this.protocolMethodMarker = protocolMethodMarker;
    }
}
