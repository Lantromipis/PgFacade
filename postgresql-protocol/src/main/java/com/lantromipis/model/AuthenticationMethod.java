package com.lantromipis.model;

import com.lantromipis.constant.PostgreSQLProtocolGeneralConstants;
import com.lantromipis.constant.PostgreSQLProtocolScramConstants;
import lombok.Getter;

public enum AuthenticationMethod {
    PLAIN_TEXT("", 0),
    MD5("", 0),
    SCRAM_SHA256(PostgreSQLProtocolScramConstants.SASL_SHA_256_AUTH_MECHANISM_NAME, PostgreSQLProtocolGeneralConstants.SASL_AUTH_INT_MARKER);

    @Getter
    private final String protocolMethodName;
    @Getter
    private final int protocolMethodMarker;

    AuthenticationMethod(String protocolMethodName, int protocolMethodMarker) {
        this.protocolMethodName = protocolMethodName;
        this.protocolMethodMarker = protocolMethodMarker;
    }
}
