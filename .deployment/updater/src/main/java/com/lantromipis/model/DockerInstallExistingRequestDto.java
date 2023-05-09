package com.lantromipis.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DockerInstallExistingRequestDto {
    private String postgresContainerId;
    private int postgresContainerPort;
    private String pgFacadeImageTag;
    private ExistingPostgresConfigurationDto configurationInfo;
    private Map<String, String> pgFacadeEnvVars;
    private Map<String, String> modifiedPostgresConfParams;
    private boolean mountDockerSock;
    private String dockerSockPathOnHost;
    private List<DockerNetworkDto> networksToConnectPgFacade;
    private String networkBetweenPostgresAndPgFacade;
}
