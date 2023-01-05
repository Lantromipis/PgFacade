package com.lantromipis.postgresprotocol.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StartupMessage {
    private short majorVersion;
    private short minorVersion;

    //not expecting duplicate parameters
    private Map<String, String> parameters;


}
