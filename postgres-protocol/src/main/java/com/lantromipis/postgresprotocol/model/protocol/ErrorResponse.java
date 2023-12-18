package com.lantromipis.postgresprotocol.model.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse {
    // not all fields are listed
    // https://www.postgresql.org/docs/current/protocol-error-fields.html
    private String code;
}
