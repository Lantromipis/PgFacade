package com.lantromipis.usermanagement.model;

import com.lantromipis.postgresprotocol.model.protocol.PostgresProtocolAuthenticationMethod;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.LocalDate;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class UserAuthInfo {
    private String username;
    private String passwd;
    private LocalDate valUntil;
    private PostgresProtocolAuthenticationMethod authenticationMethod;
}
