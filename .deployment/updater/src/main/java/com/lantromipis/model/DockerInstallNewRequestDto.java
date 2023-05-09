package com.lantromipis.model;

import lombok.Data;

import java.util.Map;

@Data
public class DockerInstallNewRequestDto {
    private String pgFacadeImageTag;
    private String postgresImageTag;
    private int postgresImagePort;
    private PostgresCredentialsDto newSuperuserCredentials;
    private NewPostgresConfigurationDto postgresConfigurationInfo;
    private Map<String, String> pgFacadeEnvVars;
}
