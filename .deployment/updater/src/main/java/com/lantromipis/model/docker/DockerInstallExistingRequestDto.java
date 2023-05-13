package com.lantromipis.model.docker;

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
    private DockerNetworkDto networkBetweenPostgresAndPgFacade;
    private DockerNetworkDto internalPgFacadeNetwork;
    private DockerNetworkDto externalPgFacadeNetwork;
    private DockerNetworkDto loadBalancerNetwork;
    private List<DockerNetworkDto> otherNetworksToConnectPgFacadeContainer;
}
