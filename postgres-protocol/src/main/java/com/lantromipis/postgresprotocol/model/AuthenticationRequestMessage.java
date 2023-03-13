package com.lantromipis.postgresprotocol.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuthenticationRequestMessage {
    private PostgresProtocolAuthenticationMethod method;
    private String specificData;
}
