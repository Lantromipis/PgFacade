package com.lantromipis.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DockerInstallNewRequestDto {
    private int awaitPgFacadeContainerMs;
    private String pgFacadeImageTag;
    private String postgresImageTag;
    private int postgresImagePort;
    private PostgresCredentialsDto newSuperuserCredentials;
    private NewPostgresConfigurationDto postgresConfigurationInfo;
    private Map<String, String> pgFacadeEnvVars;
    private boolean mountDockerSock;
    private String dockerSockPathOnHost;
    private List<DockerNetworkDto> networksToConnectPgFacade;
    private String networkBetweenPostgresAndPgFacade;
}
