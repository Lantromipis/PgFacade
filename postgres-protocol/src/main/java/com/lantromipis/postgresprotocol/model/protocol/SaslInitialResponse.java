package com.lantromipis.postgresprotocol.model.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SaslInitialResponse {
    private String nameOfSaslAuthMechanism;
    private String saslMechanismSpecificData;
}
