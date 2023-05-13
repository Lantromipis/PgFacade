package com.lantromipis.model.docker;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DockerRollingUpdateRequestDto {
    private String leaderContainerId;
    private String loadBalancerContainerId;
    private int loadBalancerRefreshIntervalSeconds;
    private String pgFacadeInternalNetworkName;
    private String pgFacadeExternalNetworkName;
    private int pgFacadeHttpPort;
    private String newPgFacadeImageTag;
    private long oldNodesAwaitClientsSeconds;
    private Map<String, String> newPgFacadeEnvVars;
    private boolean mountDockerSock;
    private String dockerSockPathOnHost;
    private List<String> networkNamesToConnect;
}
